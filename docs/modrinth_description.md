# NATS Player Data Bridge

A server-side Fabric mod that synchronizes player inventories, ender chest contents, health, hunger, XP, active effects, statistics, and advancements across multiple Minecraft servers in a cluster using [NATS JetStream](https://nats.io).

When a player leaves one server, their data is saved to a NATS Key-Value bucket. When they join another server in the same cluster, that data is fetched and applied before they spawn in.

---

## Requirements

- **Server-side only.** Clients do not need this mod installed.
- **NATS server with JetStream enabled.**
- **Fabric API.**

---

## Installation

1. Drop the mod jar into your `mods/` folder. The NATS client library is already bundled, no extra files needed.
2. Start the server once to generate two config files.
3. Configure `config/nats-fabric.yml` with your NATS server URL, auth token, and a unique name for this server.
4. Restart.

### Setting up the NATS Server

When setting up your NATS server, create a new text file named `nats-server.conf` and paste the following minimal configuration into it:

```hcl
port: 4222

authorization {
  token: "your_secret_token_here"
}

jetstream {
  store_dir: "./jetstream-data"
}
```

Then, start your NATS server by pointing it to the configuration file you just created:
- **Linux/macOS:** `./nats-server -c nats-server.conf`
- **Windows:** `nats-server.exe -c nats-server.conf`

Ensure the auth token in `config/nats-fabric.yml` matches what you have set here.

---

## Velocity Proxy Support

If you are running Velocity, set `"proxyMode": true` in `config/nats-player-data-bridge.json`. Without this, overlapping logins are rejected outright, which will break server switching.

When proxyMode is on, the mod prevents item duplication during the switch by freezing the player server-side until the transfer is complete.

If you are also using [FabricProxy-Lite](https://modrinth.com/mod/fabricproxy-lite), you must set `hackEarlySend = true` in `FabricProxy-Lite.toml` for the mod to work correctly.

---

## Configuration (`config/nats-player-data-bridge.json`)

| Key | Default | Description |
|---|---|---|
| `proxyMode` | `false` | Enable RPC lock handoff for Velocity proxy setups. |
| `rpcTimeoutSeconds` | `15` | The timeout in seconds for cross-server RPC requests (e.g., during proxy transfers). |
| `syncStats` | `true` | Sync player statistics. |
| `syncAdvancements` | `true` | Sync advancements. |
| `filterMode` | `"whitelist"` | `"whitelist"` syncs only listed NBT keys. `"blacklist"` syncs everything except listed keys. |
| `filterKeys` | Inventory, health, XP, etc. | The NBT keys to include or exclude depending on filterMode. |
| `backupHistoryCount` | `20` | The number of historical snapshots to keep per player in the backup bucket. |

---

## High Availability & Resilience

The bridge is designed for production environments where network stability is critical:

- **Zero-Loss Vaulting:** If NATS is down, player data is saved to a local disk vault (`pending_sync/`) and automatically synced back to the cluster upon reconnection.
- **Atomic Self-Healing:** Servers perform a multi-stage recovery on startup and reconnection to reconcile orphaned locks and clear local vaults.
- **Readiness Gating:** Servers stay in an "Initializing" state (blocking joins) until all background healing and synchronization is 100% complete.
- **Stale Data Protection:** Intelligent timestamp comparison prevents an old server from overwriting newer cluster data during recovery.
- **Infinite Watchdog:** A hardened networking layer that performs automatic, infinite reconnection retries without impacting server stability.

---

## Admin Commands

All commands require operator permissions.

| Command | Description |
|---|---|
| `/nats sync [player]` | Manually push a player's data to the cluster. |
| `/nats sessions list` | View all active session locks in the cluster. |
| `/nats sessions clean <uuid>` | Clear a stuck session lock for a specific player. |
| `/nats backup push <player>` | Create a long-term snapshot of a player's current data. |
| `/nats backup list <player>` | View available snapshots for a player. |
| `/nats backup restore <player> <rev>` | Restore a specific snapshot. If the player is online they will be kicked, and the backup will be applied when they next log in. |

---

## Data Handling Notes

- **Cluster-Wide Locking:** Each player session is assigned a unique lock. A server can only write data if it holds that lock, preventing data corruption from racing servers.
- **Background Operations:** All push/pull operations run on a dedicated virtual-thread executor to ensure zero impact on server TPS.
- **Binary Format:** Data is packed into a compact CBOR binary format, minimizing network overhead and disk usage.
- **Rollbacks:** Admins can restore players to previous snapshots. If the player is online, they are automatically kicked to apply the data safely.

---

## License

MIT
