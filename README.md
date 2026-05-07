# NATS Player Data Bridge

A server-side Fabric mod that synchronizes player inventories, ender chests, health, XP, statistics, and advancements across multiple Minecraft servers using [NATS JetStream](https://nats.io).

When a player leaves, their data is saved to a NATS Key-Value bucket. When they join another server in the same cluster, that data is fetched and applied before they spawn.

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

If you are running your own NATS server, create a new text file named `nats-server.conf` and paste the following minimal configuration into it:

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

## Admin Commands

All commands require operator permissions.

| Command | Description |
|---|---|
| `/nats sync [player]` | Manually push a player's data to the cluster. |
| `/nats sessions list` | View all active session locks in the cluster. |
| `/nats sessions clean <uuid>` | Clear a stuck session lock for a specific player. |
| `/nats backup push <player>` | Create a long-term snapshot of a player's current data. |
| `/nats backup list <player>` | View available snapshots for a player. |
| `/nats backup restore <player> <rev>` | Stage a restoration. **Requires `/nats backup confirm` to execute.** |
| `/nats backup confirm` | Execute a staged restoration. The player will be kicked to apply data safely. |

---

## Data Handling & Resilience

The bridge is built for production environments where data integrity and network stability are critical.

- **Cluster-Wide Locking:** Each player session is assigned a unique lock. A server can only write data if it holds that lock, preventing corruption from racing servers.
- **Fail-to-Safety (Vaulting):** If NATS is unreachable, player data is saved to a local disk vault (`nats-player-data-bridge/pending_sync/`) and automatically synced back when the connection is restored.
- **Self-Healing:** Servers perform an atomic recovery on startup to reconcile orphaned locks and clear any local vault data.
- **Background Processing:** All network operations run on dedicated virtual threads to ensure zero impact on server TPS.
- **Binary Format:** Data is packed into a compact CBOR binary format with Zstd compression for minimal network overhead.
- **Stale Data Protection:** Intelligent timestamping prevents old data from overwriting newer progress during cluster recovery.
- **Readiness Gating:** Servers block player joins until background synchronization and healing are 100% complete.
- **Infinite Watchdog:** The networking layer performs automatic reconnection retries without impacting stability.

---

## License

MIT
