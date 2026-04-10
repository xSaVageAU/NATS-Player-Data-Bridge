# NATS Player Data Bridge

A Fabric server-side mod that synchronizes player data across a cluster of Minecraft servers using [NATS](https://nats.io).

When a player leaves one server, their data is packed into a compressed bundle and pushed to NATS. When they join another server in the cluster, that data is pulled and applied before Minecraft loads the player.

## What it syncs

- Player NBT (inventory, ender chest, XP, health, hunger, attributes, etc.)
- Statistics
- Advancements

By default, inventory contents, ender chest, XP, hunger, and credits status are synced. This can be changed to a blacklist or a different whitelist in the config.

## Duplicate login prevention

Players are locked to one server at a time using an atomic presence lock in NATS. If a player tries to join a second server while already online, they are rejected. Presence keys auto-expire after 60 seconds to recover from crashes.

## Requirements

- Minecraft 26.1.1 (Fabric)
- Java 25+
- [NATS-Fabric](https://github.com/xSaVageAU/NATS-Fabric) library mod (installed separately)
- A NATS server with JetStream enabled

## Setup

1. Install both `nats-fabric` and `nats-player-data-bridge` jars into your server's `mods/` folder.
2. Start the server once to generate config files.
3. Edit `config/nats-fabric.json` — set your NATS server URL and a unique `serverName` for each server.
4. Edit `config/nats-player-data-bridge.json` to configure what data gets synced.
5. Restart the server. Repeat for each server in the cluster.

## Commands

All commands require operator permissions.

| Command | Description |
|---|---|
| `/nats sync` | Force-push your current player data to NATS |
| `/nats online` | Show the local presence cache (diagnostic) |
| `/nats online reset` | Purge all presence records and re-sync the cluster |

## Configuration

`nats-player-data-bridge.json`:

| Field | Default | Description |
|---|---|---|
| `debug` | `false` | Enable verbose logging |
| `syncStats` | `true` | Sync player statistics |
| `syncAdvancements` | `true` | Sync player advancements |
| `filterMode` | `"whitelist"` | `"blacklist"` or `"whitelist"` for NBT key filtering |
| `filterKeys` | `["Inventory", "EnderItems", "SelectedItemSlot", "foodExhaustionLevel", "foodLevel", "foodSaturationLevel", "foodTickTimer", "seenCredits", "XpLevel", "XpP", "XpTotal"]` | NBT keys to filter |

## License

MIT
