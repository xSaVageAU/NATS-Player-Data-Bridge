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

### Minimal NATS Server Config

```
port: 4222

authorization {
  token: "your_secret_token_here"
}

jetstream {
  store_dir: "./jetstream-data"
}
```

Start with `./nats-server -c nats-server.conf` on Linux/macOS or `nats-server.exe -c nats-server.conf` on Windows.

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
| `syncStats` | `true` | Sync player statistics. |
| `syncAdvancements` | `true` | Sync advancements. |
| `filterMode` | `"whitelist"` | `"whitelist"` syncs only listed NBT keys. `"blacklist"` syncs everything except listed keys. |
| `filterKeys` | Inventory, health, XP, etc. | The NBT keys to include or exclude depending on filterMode. |

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

- **Locking:** Each player session is assigned a cluster-wide lock. A server can only write data if it holds that lock, preventing overwrites from two servers racing to save simultaneously.
- **Background saves:** Push and pull operations run on a background thread so they do not cause lag spikes.
- **Crash recovery:** On startup, each server automatically releases any locks it was holding when it last shut down unexpectedly.
- **Rollbacks:** An admin can restore a player to a previous manual backup snapshot. If the player is online, they are automatically kicked. The backup is applied when they next log in.
- **Data format:** Player data is packed into a compact binary format (CBOR) before being sent to NATS, keeping transfers small and fast.

---

## License

MIT
