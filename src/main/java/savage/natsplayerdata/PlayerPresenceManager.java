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

    /** Local memory mirror of the NATS presence bucket with entry timestamps. */
    private static final Map<UUID, CacheEntry> LOCAL_CACHE = new java.util.concurrent.ConcurrentHashMap<>();
    private static final long TTL_MS = 60_000; // 60 seconds

    private record CacheEntry(String rawValue, long timestamp) {
        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > TTL_MS;
        }
    }

    /**
     * Marks a player as online on this server in the NATS cluster.
     * @param player The player instance.
     * @param refresh If true, uses put() to refresh TTL. If false, uses create() for initial lock.
     * @return true if lock acquired or refreshed, false if already online elsewhere.
     */
    public static boolean join(ServerPlayer player, boolean refresh) {
        UUID uuid = player.getUUID();
        String name = player.getName().getString();
        String serverId = NatsManager.getInstance().getServerName();
        
        if (refresh) {
            // Heartbeat: Overwrite to keep TTL alive
            updatePresence(uuid, name, serverId);
            return true;
        } else {
            // Initial Join: Atomic create to act as a lock
            boolean locked = tryLock(uuid, name, serverId);
            if (locked) {
                NATSPlayerDataBridge.debugLog("Presence: Lock ACQUIRED for {} ({}) on {}", name, uuid, serverId);
            } else {
                NATSPlayerDataBridge.LOGGER.warn("Presence: Lock FAILED for {} ({}) - already online elsewhere!", name, uuid);
            }
            return locked;
        }
    }

    /**
     * Updates a player's presence in the cluster using standard put().
     */
    private static void updatePresence(UUID uuid, String name, String serverId) {
        try {
            var bucket = PlayerStorage.getInstance().getPresenceBucket();
            if (bucket != null) {
                String value = name + "|" + serverId;
                bucket.put(uuid.toString(), value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to update presence for {}: {}", uuid, e.getMessage());
        }
    }

    /**
     * Attempts to atomically create the presence key in NATS.
     */
    private static boolean tryLock(UUID uuid, String name, String serverId) {
        try {
            var bucket = PlayerStorage.getInstance().getPresenceBucket();
            if (bucket == null) return false;

            String value = name + "|" + serverId;
            // Atomic Create: Fails if key already exists
            bucket.create(uuid.toString(), value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return true;
        } catch (io.nats.client.JetStreamApiException e) {
            // NATS Error Code 10071 is "wrong last sequence", meaning it already exists
            return false; 
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Presence: Lock error for {}: {}", uuid, e.getMessage());
            return false;
        }
    }

    /**
     * Removes a player's online status from the NATS cluster.
     */
    public static void leave(ServerPlayer player) {
        UUID uuid = player.getUUID();
        
        String owner = getLastKnownServer(uuid);
        String localServerId = NatsManager.getInstance().getServerName();
        if (owner != null && !owner.equals(localServerId)) {
            NATSPlayerDataBridge.debugLog("Presence: Ignoring disconnect for {} ({}) - we don't own the lock", player.getName().getString(), uuid);
            return;
        }

        NATSPlayerDataBridge.debugLog("Presence: Player {} ({}) disconnected, clearing lock", player.getName().getString(), uuid);
        PlayerStorage.getInstance().clearPresence(uuid);
    }

    /**
     * Checks if the player is already recorded as online in the NATS cluster.
     */
    public static boolean isAlreadyOnline(UUID uuid) {
        CacheEntry entry = LOCAL_CACHE.get(uuid);
        if (entry == null) return false;
        if (entry.isExpired()) {
            LOCAL_CACHE.remove(uuid);
            return false;
        }
        return true;
    }

    /**
     * Returns the server ID the player was last seen online on.
     */
    public static String getLastKnownServer(UUID uuid) {
        CacheEntry entry = LOCAL_CACHE.get(uuid);
        if (entry == null || entry.isExpired()) {
            if (entry != null) LOCAL_CACHE.remove(uuid);
            return null;
        }
        String[] parts = entry.rawValue().split("\\|", 2);
        return parts.length > 1 ? parts[1] : parts[0];
    }

    /**
     * Updates the local cache from Watcher events.
     */
    public static void updateLocalCache(UUID uuid, String rawValue, long timestamp) {
        if (rawValue == null) {
            LOCAL_CACHE.remove(uuid);
            NATSPlayerDataBridge.debugLog("Cache: Removed presence for {} (Cache Size: {})", uuid, LOCAL_CACHE.size());
        } else {
            LOCAL_CACHE.put(uuid, new CacheEntry(rawValue, timestamp));
            NATSPlayerDataBridge.debugLog("Cache: Updated presence for {} -> {} (Cache Size: {})", uuid, rawValue, LOCAL_CACHE.size());
        }
    }

    /**
     * Re-registers all currently online players on this server to the NATS cluster.
     */
    public static void reSyncLocalPlayers(net.minecraft.server.MinecraftServer server) {
        NATSPlayerDataBridge.debugLog("Presence: Re-syncing local players to cluster...");
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            join(player, false);
        }
    }

    /**
     * Fetches the current online player list from the LOCAL CACHE.
     */
    public static Map<String, String> getClusterOnline() {
        Map<String, String> results = new java.util.HashMap<>();
        LOCAL_CACHE.forEach((uuid, entry) -> {
            if (!entry.isExpired()) {
                results.put(uuid.toString(), entry.rawValue());
            }
        });
        return results;
    }
}
