# NATS Player Data Bridge

A Fabric mod for Minecraft servers that synchronizes player data (inventories, stats, and advancements) across a cluster using the [NATS](https://nats.io) messaging system.

## Overview

This mod allows players to jump between different servers in a cluster while keeping their items and progress intact. When a player leaves a server, their data is saved to a NATS Key-Value bucket; when they join another server, that data is retrieved and applied before they spawn.

## Installation

1. Place the `nats-player-data-bridge` jar into the `mods/` folder of all servers in your cluster.
2. Ensure the **[NATS-Fabric](https://github.com/xSaVageAU/NATS-Fabric)** library mod is also installed.
3. Start each server once to generate the configuration files.

## Configuration

### 1. Connection settings (`config/nats-fabric.json`)
You must configure the library mod first so the bridge can talk to your NATS server:
- `natsUrl`: The address of your NATS server (e.g., `nats://127.0.0.1:4222`).
- `serverName`: A unique name for this server instance (e.g., `survival-1`).

### 2. Bridge settings (`config/nats-player-data-bridge.json`)
- `syncStats`: Enable/disable statistics synchronization.
- `syncAdvancements`: Enable/disable advancement synchronization.
- `filterKeys`: Choose which NBT tags (like `Inventory` or `EnderItems`) to sync or exclude.

## Commands

All commands require operator (Level 4) permissions.

| Command | Description |
|---|---|
| `/nats sync` | Manually push your current data to the cluster. |
| `/nats force-clean <uuid>` | Manually reset a player's session lock if it becomes stuck. |

---

## Technical Details (Data Integrity)

The bridge is designed with several safety layers to prevent common "split-brain" or data loss issues found in distributed Minecraft environments:

- **Session Locking**: When a player joins, the server acquires a "Dirty" lock in NATS. While locked, no other server can load or save that player's data. This prevents duplicate items from being generated on multiple servers simultaneously.
- **Atomic Acquisition (CAS)**: The bridge uses NATS optimistic concurrency to ensure that lock acquisition is atomic. If two servers try to claim a player at once, only one will succeed.
- **Startup Reconciliation**: If a server crashes while players are online, their locks will remain "Dirty." When that server restarts, it automatically scans for any sessions it previously owned and resets them to "Clean" so players can log back in.
- **Fail-to-Safety**: If NATS is unreachable or a session is locked by another server, the bridge will disconnect the player rather than letting them play with stale local data.
- **Optimization**: Data is compressed using Zstd and serialized as CBOR to minimize network traffic and storage use.

## License

MIT
