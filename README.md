# NATS Player Data Bridge

A Fabric mod for Minecraft servers that synchronizes player data (inventories, stats, and advancements) across a cluster using the [NATS](https://nats.io) messaging system.

## Overview

This mod allows players to jump between different servers in a cluster while keeping their items and progress intact. When a player leaves a server, their data is saved to a NATS Key-Value bucket; when they join another server, that data is retrieved and applied before they spawn.

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
`.\nats-server.exe -c nats-server.conf`

## Installation

1. Place the `nats-player-data-bridge` jar into the `mods/` folder of all servers in your cluster.
2. Ensure the **[NATS-Fabric](https://github.com/xSaVageAU/NATS-Fabric)** library mod is also installed.
3. Start each server once to generate the configuration files.

## Configuration

### 1. Connection settings (`config/nats-fabric.json` or `.yml`)
You must configure the library mod first so the bridge can talk to your NATS server:
- `natsUrl`: The address of your NATS server (e.g., `nats://127.0.0.1:4222`).
- `serverName`: A unique name for this server instance (e.g., `survival-1`).
- `natsAuthToken`: The secret token configured in your NATS server.
- `natsUsername` / `natsPassword`: Alternative credentials if using user-based auth.

### 2. Bridge settings (`config/nats-player-data-bridge.json`)
- `syncStats`: Enable/disable statistics synchronization.
- `syncAdvancements`: Enable/disable advancement synchronization.
- `filterKeys`: Choose which NBT tags (like `Inventory` or `EnderItems`) to sync or exclude.

## Commands

All commands require administrative permissions.

| Command | Description |
|---|---|
| `/nats sync [player]` | Manually push data to the cluster. Defaults to self. |
| `/nats sessions list` | View all active session locks and their statuses across the cluster. |
| `/nats sessions clean <uuid>` | Manually reset a player's session lock if it becomes stuck. |

---

## Technical Details (Data Integrity)

The bridge is designed with several production-grade safety layers to prevent "split-brain" scenarios and data duplication:

- **Atomic Locking (CAS)**: Uses NATS *Compare-and-Set* (Check-and-Set) logic. A server only acquires a session if and only if the revision matches the last known state, preventing race conditions during simultaneous logins.
- **Multi-Phase Lifecycle Guards**: Captures disconnects in every phase of the networking stack (Login, Configuration, and Play). This ensures orphaned locks are released even if a client force-closes during a loading screen.
- **Fencing Guards**: The server enforces strict local ownership checks. If a session is cleaned or stolen while a server is still running, that server will immediately block all data pushes to prevent stale overwrites.
- **Startup Reconciliation**: On startup, each server automatically scans the cluster for any DIRTY sessions it previously owned (orphaned by a crash) and resets them to CLEAN.
- **Binary Bundles**: Uses `zstd` compression and `CBOR` serialization for maximum efficiency, making data payloads significantly smaller and faster to transfer than standard JSON or Gzipped NBT.
- **Fail-to-Safety**: If the cluster is unreachable or a lock is contested, the player is rejected with a descriptive message rather than being allowed to play with stale or orphanded data.

## License

MIT
