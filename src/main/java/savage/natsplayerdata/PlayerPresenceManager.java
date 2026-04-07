package savage.natsplayerdata;

import savage.natsfabric.NatsManager;
import net.minecraft.server.level.ServerPlayer;
import savage.natsplayerdata.storage.PlayerStorage;

import java.util.Map;
import java.util.UUID;

/**
 * High-level manager for tracking player presence across the NATS cluster.
 */
public class PlayerPresenceManager {

    /** Local memory mirror of the NATS presence bucket. */
    private static final Map<UUID, String> LOCAL_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Marks a player as online on this server in the NATS cluster.
     */
    public static void join(ServerPlayer player) {
        UUID uuid = player.getUUID();
        String name = player.getName().getString();
        String serverId = NatsManager.getInstance().getServerName();
        NATSPlayerDataBridge.LOGGER.info("Presence: Player {} ({}) joined on {}", name, uuid, serverId);
        PlayerStorage.getInstance().updatePresence(uuid, name, serverId);
    }

    /**
     * Removes a player's online status from the NATS cluster.
     */
    public static void leave(ServerPlayer player) {
        UUID uuid = player.getUUID();
        NATSPlayerDataBridge.LOGGER.info("Presence: Player {} ({}) disconnected", player.getName().getString(), uuid);
        PlayerStorage.getInstance().clearPresence(uuid);
    }

    /**
     * Checks if the player is already recorded as online in the NATS cluster.
     */
    public static boolean isAlreadyOnline(UUID uuid) {
        return LOCAL_CACHE.containsKey(uuid);
    }

    /**
     * Returns the server ID the player was last seen online on.
     */
    public static String getLastKnownServer(UUID uuid) {
        String raw = LOCAL_CACHE.get(uuid);
        if (raw == null) return null;
        String[] parts = raw.split("\\|", 2);
        return parts.length > 1 ? parts[1] : parts[0];
    }

    /**
     * Updates the local cache from Watcher events.
     */
    public static void updateLocalCache(UUID uuid, String rawValue) {
        if (rawValue == null) {
            LOCAL_CACHE.remove(uuid);
            NATSPlayerDataBridge.LOGGER.info("Cache: Removed presence for {} (Cache Size: {})", uuid, LOCAL_CACHE.size());
        } else {
            LOCAL_CACHE.put(uuid, rawValue);
            NATSPlayerDataBridge.LOGGER.info("Cache: Updated presence for {} -> {} (Cache Size: {})", uuid, rawValue, LOCAL_CACHE.size());
        }
    }

    /**
     * Re-registers all currently online players on this server to the NATS cluster.
     */
    public static void reSyncLocalPlayers(net.minecraft.server.MinecraftServer server) {
        NATSPlayerDataBridge.LOGGER.info("Presence: Re-syncing local players to cluster...");
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            join(player);
        }
    }

    /**
     * Fetches the current online player list from the LOCAL CACHE.
     */
    public static Map<String, String> getClusterOnline() {
        Map<String, String> results = new java.util.HashMap<>();
        LOCAL_CACHE.forEach((uuid, val) -> results.put(uuid.toString(), val));
        return results;
    }
}
