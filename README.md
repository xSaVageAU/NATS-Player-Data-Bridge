# NATS Player Data Bridge

A Fabric mod that syncs player data (inventories, stats, and advancements) across your server cluster using [NATS](https://nats.io).

## How it works

When a player leaves, their data is saved to a NATS Key-Value bucket. When they join another server in the cluster, that data is pulled back down and applied before they spawn.

## Installation

1. Put the mod jar in your `mods/` folder.
2. The core NATS library is already included, so you don't need any other files.
3. Start the server once to generate the config.

## NATS Infrastructure Setup

This mod requires **NATS JetStream** to be enabled on your NATS server. Below is a minimal `nats-server.conf` that provides token-based authentication and persistent storage:

```hcl
# Default client port
port: 4222

# Enable token auth on the server
authorization {
  token: "your_secret_token_here"
}

# Enable JetStream for persistent Key-Value storage
jetstream {
    # Path where NATS will store the player bundles.
    # Note for Windows: Use forward slashes (/) to avoid path issues.
    store_dir: "./jetstream-data"
}
```

Save the above as `nats-server.conf` and start your NATS server using:

**Linux / macOS:**
`./nats-server -c nats-server.conf`

**Windows:**
`nats-server.exe -c nats-server.conf`

## Configuration

### 1. Connection (`config/nats-fabric.yml`)
Configure your NATS server details here (URL, Token, and a unique Server Name).

### 2. Bridge Settings (`config/nats-player-data-bridge.json`)
- `syncStats`: Sync player statistics.
- `syncAdvancements`: Sync advancements.
- `filterKeys`: Choose which NBT data to sync (defaults to standard inventory/health etc).

## Commands

All commands require OP/Admin permissions.

| Command | Description |
|---|---|
| `/nats sync [player]` | Manually save player data to the cluster. |
| `/nats sessions list` | Check all active session locks. |
| `/nats sessions clean <uuid>` | Reset a stuck session lock. |
| `/nats backup push <player>` | Create a long-term snapshot of a player's data. |
| `/nats backup list <player>` | View manual snapshots available for a player. |
| `/nats backup restore <player> <rev>` | Restore a specific snapshot (Player MUST be offline). |

## How it handles data

To keep things consistent when moving between servers:
- **Locking**: Prevents data from being overwritten if a player is somehow active on two servers at once.
- **Background Saving**: Saves are handled in the background to avoid causing lag spikes.
- **Rollbacks**: If a player's data is corrupted, an admin can rollback to a previous revision. **Note**: The player must be offline during a rollback to prevent their "live" session from overwriting the restored data.
- **Recovery**: If a server crashes, it will automatically try to clear any locks it was holding when it restarts.
- **Format**: Uses a compact binary format to keep data transfers small and fast.

## License

MIT
