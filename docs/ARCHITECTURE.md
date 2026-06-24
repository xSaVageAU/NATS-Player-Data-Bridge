# NATS Player Data Bridge: Architecture & Design Decisions

This document acts as a comprehensive architecture reference for the **NATS Player Data Bridge**. It maps out the design choices, code patterns, and state verification rules that protect player inventory data from race conditions, duplications, and network failures.

---

## 1. Core Architecture Overview

NATS Player Data Bridge is built around the idea that **player inventory synchronization is a distributed state coordination problem**. Instead of using traditional database tables (SQL) and web caching (Redis), it leverages **NATS JetStream Key-Value buckets** to handle data persistence, locking, and synchronization in a single unified layer.

```
       +---------------------------------------------+
       |             Velocity / Proxy                |
       +---------------------------------------------+
          /                                       \
         / (Login Handshake)                       \ (Login Handshake)
        v                                           v
+--------------------------+  (Direct RPC)  +--------------------------+
|  Minecraft Server A      | <============> |  Minecraft Server B      |
|  [DIRTY Lock Owner]      |  Release Lock  |  [Tries to acquire lock] |
+--------------------------+                +--------------------------+
        \                                                 /
         \ (Push CBOR + Zstd)                            / (Fetch & Apply)
          v                                             v
  +-----------------------------------------------------------+
  |              NATS JetStream (KV Buckets)                  |
  |  - player-sync-v1 (Active Session Locks & Player NBT)     |
  |  - player-backups-v1 (Native KV revision backups)         |
  +-----------------------------------------------------------+
```

---

## 2. The Core Lifecycle & Interception Flow

To prevent the common "Join Tick Races" seen in traditional plugins, the mod intercepts Minecraft's loading flow at the lowest possible level rather than relying on standard plugin events.

### A. The Join/Fetch Pipeline
1.  **Early Watchdog Gate:** When a connection is initialized, `ServerLoginConnectionEvents.INIT` checks if the local NATS bridge is fully active. If not (e.g., during startup or reconnection), it rejects the login to prevent player loading on un-synced servers.
2.  **Async Lock & Fetch:** In `ServerLoginConnectionEvents.QUERY_START`, the login process is put on hold. The server spins up an asynchronous task on a **Java 25 Virtual Thread** to:
    *   Acquire the session lock via `SessionStorage`.
    *   Trigger an RPC to release locks held on remote servers if proxy mode is active.
    *   Initiate the fetch request (`SyncService.requestAsyncFetch`) to load the CBOR bundle.
3.  **Low-Level Overwrite:** The vanilla game calls `PlayerList.loadPlayerData` to load the player. The Sponge Mixin in [PlayerSyncMixin.java](file:///d:/Coding%20Projects/JAVA/husksync-natsbridge-comparison/NATS-Player-Data-Bridge/src/main/java/savage/natsplayerdata/mixin/PlayerSyncMixin.java) intercepts this call at the head:
    *   It grabs the pre-fetched `CompoundTag` (NBT) from NATS.
    *   It merges the incoming data with the player's local dat file.
    *   It upgrades NBT structures to the server's version via the vanilla `DataFixTypes.PLAYER.updateToCurrentVersion`.
    *   It writes the merged NBT back to the local disk and returns it, allowing Minecraft to load it naturally as if it were a single-server save.

### B. The Save/Leave Pipeline
1.  **Disconnect Trigger:** When a player disconnects, `ServerPlayConnectionEvents.DISCONNECT` executes.
2.  **Main Thread Capture:** The player's NBT data, achievements, and stats are saved to the server's disk directories, and the live NBT is captured via `player.saveWithoutId(output)` on the main thread (essential to avoid concurrent world-ticking crashes).
3.  **Async Compression & Upload:** A virtual thread compresses the NBT, stats, and achievements into a CBOR byte array, runs Zstd Level 1 compression, pushes the payload to the NATS `bundle.<uuid>` key, and transitions the session state to `CLEAN`.

---

## 3. Session Locking & CAS State Machine

To prevent split-brain issues, duplicate logins, or racing writes, a global player session ledger exists in the `player-sync-v1` bucket under `session.<uuid>`.

### The Three State Flows:
*   `CLEAN`: The player is offline. Their latest data has been successfully saved to NATS. It is safe for any server to claim their lock.
*   `DIRTY`: The player is actively logged in on a server, or their data is currently in the process of saving. No other server can claim their lock blindly.
*   `RESTORING`: An administrator has staged a backup restoration. Incoming saves from server switches are rejected, forcing the player to disconnect so the backup can safely be applied.

```
       +------------+
       |   CLEAN    | <------------------------------------+
       +------------+                                      |
             |                                             |
             | (Player Joins - Acquire CAS Lock)           | (Push Complete /
             v                                             |  Lock Released)
       +------------+                                      |
       |   DIRTY    | -------------------------------------+
       +------------+
             |
             | (Admin triggers rollback)
             v
       +------------+
       | RESTORING  | ---> (Kicks player, applies backup revision on next login)
       +------------+
```

### Optimistic Concurrency Control (CAS):
To prevent race conditions where two servers attempt to write to `session.<uuid>` at the same time:
*   Every lock read returns the current NATS key sequence revision (`entry.getRevision()`).
*   Every lock write using `pushSession` uses NATS's Compare-And-Swap mechanism: `kvBucket.update(key, json, expectedRevision)`.
*   If another server claimed the lock in the microsecond between read and write, the revision mismatch will fail the update (`wrong last sequence` error), and the login is instantly aborted.

---

## 4. The Overlapping Login & Query RPCs

In proxied networks (like Velocity), server transitions can create overlapping connections, and administrators may need to perform safety checks across nodes.

### A. The Overlapping Login Handoff Protocol
1.  **Acquisition Failure:** Server B fails to claim the lock because the status is `DIRTY` on Server A.
2.  **Request Release:** Server B sends a direct NATS Core RPC request on `session.release.<server_id>`.
3.  **Synchronous Capture:** Server A receives this message. It intercepts the player, runs a synchronous main-thread capture of the player's state (`DataMergeService.prepareAndPush`), and starts the async NATS upload.
4.  **Confirm Release:** Once the NATS push is complete and the lock is set to `CLEAN`, Server A publishes an `OK` reply back to Server B.
5.  **Safe Switch:** Server B receives the `OK` confirmation, instantly claims the lock via CAS, and fetches the freshly uploaded data bundle. This guarantees zero duplication or data loss.

### B. Remote Active Player Query
When an admin attempts to force-clean a lock using `/nats sessions clean <target>` on Server B, the system must verify the player is not actually online on another server to prevent split-brain duplicates:
1.  **Status Check:** If the lock state is `DIRTY` on Server A, Server B publishes an RPC query on `session.query.ServerA`.
2.  **State Reply:** Server A checks if the player is currently online. It replies `ONLINE` or `OFFLINE` back to Server B.
3.  **Command Safety Fence:** 
    *   If Server A replies `ONLINE`, Server B aborts the cleanup and blocks the admin.
    *   If Server A replies `OFFLINE`, Server B proceeds with resetting the lock.
    *   If the RPC times out, Server B flags the event as an un-responded timeout (meaning Server A has crashed and it is a true ghost lock) and clears the lock safely.

---

## 5. Resilience & Self-Healing

The bridge is designed to survive hard crashes and connection drops without corrupting player states.

### A. Fail-to-Safety Vaulting
If the NATS cluster becomes unreachable while saving a disconnecting player:
1.  The mod diverts the compiled CBOR bundle to `nats-player-data-bridge/pending_sync/<uuid>.cbor` on local disk.
2.  The session lock remains `DIRTY` in the cluster ledger.

### B. Startup & Reconnection Reconciliation
When the NATS connection is established or the server boots up:
1.  **Vault Recovery:** `reconcileLocalVault` scans the local folder for pending `.cbor` files.
2.  **Stale check:** It compares the timestamp of the local vault file with the current NATS bundle. If NATS already has a newer timestamp (e.g., the player logged back in elsewhere), the vault file is safely discarded. Otherwise, the vault data is pushed, and the lock is set to `CLEAN`.
3.  **Orphaned Lock Healing:** Once vault files are processed, `reconcileLocalSessions` scans for any `DIRTY` session locks owned by the local server ID. If the player is no longer online, it resets the lock to `CLEAN`, resolving "Ghost Locks" caused by unclean server crashes.

---

## 6. Historical Backups via NATS Key History

Instead of implementing custom SQL rollback tables, the mod delegates backup storage to NATS:
*   The `player-backups-v1` bucket is created with `maxHistoryPerKey` (default 20).
*   Every manual backup or automatic policy trigger (e.g., Death, Dimension Change) pushes a `BackupEnvelope` containing metadata (trigger, server, dimension, mod version) and the player bundle to the bucket.
*   To retrieve history, `BackupStorage` calls `backupBucket.history("backup." + uuid)`. To restore, the session is flagged as `RESTORING` with the target revision, blocking any incoming saves and forcing a rollback fetch on the next join.
