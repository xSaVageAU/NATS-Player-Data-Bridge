package savage.natsplayerdata.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import savage.natsfabric.NatsManager;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.model.PlayerDataBundle;

import java.util.Optional;
import java.util.UUID;

/**
 * Handles binary persistence of Player Bundles to the NATS cluster.
 */
public class PlayerStorage {

    private static final String BUCKET_NAME = "player-sync-v1";
    private static final String PRESENCE_BUCKET = "player-presence-v1";
    private static final ObjectMapper CBOR_MAPPER = new ObjectMapper(new CBORFactory());
    private static PlayerStorage instance;

    private KeyValue kvBucket;
    private KeyValue presenceBucket;

    private PlayerStorage() {
        init();
    }

    public static PlayerStorage getInstance() {
        if (instance == null) instance = new PlayerStorage();
        return instance;
    }

    private void init() {
        try {
            kvBucket = NatsManager.getInstance().getKeyValue(BUCKET_NAME);
            if (kvBucket == null) {
                NATSPlayerDataBridge.LOGGER.error("Synchronizer: Bucket '{}' not available!", BUCKET_NAME);
            }
            
            presenceBucket = NatsManager.getInstance().getKeyValue(PRESENCE_BUCKET);
            if (presenceBucket == null) {
                NATSPlayerDataBridge.LOGGER.warn("Synchronizer: Presence bucket '{}' not available! Creating or waiting...", PRESENCE_BUCKET);
                // In production, we assume the bucket is pre-provisioned or handled by NatsManager
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to initialize NATS KV storage: {}", e.getMessage());
        }
    }

    /**
     * Checks if the presence system is reachable.
     */
    public boolean isPresenceAvailable() {
        try {
            return presenceBucket != null && NatsManager.getInstance().isConnected();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Checks if a player is NOT currently recorded as online in the cluster.
     * Returns false if the status is unknown (e.g. NATS is down).
     */
    public boolean isOffline(UUID uuid) {
        if (presenceBucket == null) return false;
        try {
            KeyValueEntry entry = presenceBucket.get(uuid.toString());
            return entry == null || entry.getValue() == null;
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to check presence for {}: {}", uuid, e.getMessage());
            return false; // Safe default: if we can't verify offline, assume online/busy
        }
    }

    /**
     * Returns the server ID of the server currently holding the lock for this player.
     * @return Server ID or null if not locked.
     */
    public String getLockOwner(UUID uuid) {
        if (presenceBucket == null) return null;
        try {
            KeyValueEntry entry = presenceBucket.get(uuid.toString());
            if (entry == null || entry.getValue() == null) return null;
            String value = new String(entry.getValue(), java.nio.charset.StandardCharsets.UTF_8);
            String[] parts = value.split("\\|", 2);
            return parts.length > 1 ? parts[1] : null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Updates a player's presence in the cluster.
     */
    public void updatePresence(UUID uuid, String name, String serverId) {
        if (presenceBucket == null) return;
        try {
            String value = name + "|" + serverId;
            presenceBucket.put(uuid.toString(), value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to update presence for {}: {}", uuid, e.getMessage());
        }
    }

    /**
     * Clears a player's presence from the cluster.
     */
    public void clearPresence(UUID uuid) {
        if (presenceBucket == null) return;
        try {
            presenceBucket.delete(uuid.toString());
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to clear presence for {}: {}", uuid, e.getMessage());
        }
    }

    /**
     * Fetches all active presences from the cluster.
     */
    public java.util.Map<String, String> getOnlinePresences() {
        java.util.Map<String, String> presences = new java.util.HashMap<>();
        if (presenceBucket == null) return presences;
        
        try {
            java.util.List<String> keys = presenceBucket.keys();
            for (String key : keys) {
                KeyValueEntry entry = presenceBucket.get(key);
                if (entry != null && entry.getValue() != null) {
                    presences.put(key, new String(entry.getValue(), java.nio.charset.StandardCharsets.UTF_8));
                }
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to fetch online presences: {}", e.getMessage());
        }
        return presences;
    }

    /**
     * Purges all presence records from the cluster.
     * To be called by the reset initiator.
     */
    public void purgeAllPresence() {
        if (presenceBucket == null) return;
        try {
            java.util.List<String> keys = presenceBucket.keys();
            for (String key : keys) {
                presenceBucket.delete(key);
            }
            NATSPlayerDataBridge.LOGGER.info("Cluster: Presence bucket purged.");
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to purge presence bucket: {}", e.getMessage());
        }
    }

    /**
     * Serializes and uploads a player bundle to the cluster.
     */
    public void pushBundle(PlayerDataBundle bundle) {
        if (kvBucket == null) return;
        try {
            byte[] cborBinary = CBOR_MAPPER.writeValueAsBytes(bundle);
            
            long startNanos = System.nanoTime();
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            try (java.util.zip.GZIPOutputStream gzipOut = new java.util.zip.GZIPOutputStream(baos)) {
                gzipOut.write(cborBinary);
            }
            byte[] compressedBinary = baos.toByteArray();
            long compressNanos = System.nanoTime() - startNanos;
            double compressMs = compressNanos / 1_000_000.0;
            
            kvBucket.put(bundle.uuid().toString(), compressedBinary);
            NATSPlayerDataBridge.LOGGER.info("Cluster: Pushed {} bytes (compressed from {} bytes in {}ms) bundle for {}", compressedBinary.length, cborBinary.length, String.format("%.2f", compressMs), bundle.uuid());
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to push CBOR bundle: {}", e.getMessage());
        }
    }

    /**
     * Fetches and deserializes a player bundle from the cluster.
     */
    public Optional<PlayerDataBundle> fetchBundle(UUID uuid) {
        if (kvBucket == null) return Optional.empty();
        try {
            KeyValueEntry entry = kvBucket.get(uuid.toString());
            if (entry == null || entry.getValue() == null) return Optional.empty();

            byte[] compressedBinary = entry.getValue();
            long startNanos = System.nanoTime();
            java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(compressedBinary);
            java.io.ByteArrayOutputStream decompressedBaos = new java.io.ByteArrayOutputStream();
            
            try (java.util.zip.GZIPInputStream gzipIn = new java.util.zip.GZIPInputStream(bais)) {
                gzipIn.transferTo(decompressedBaos);
            }
            byte[] decompressedBinary = decompressedBaos.toByteArray();
            long decompressNanos = System.nanoTime() - startNanos;
            double decompressMs = decompressNanos / 1_000_000.0;
            
            NATSPlayerDataBridge.LOGGER.info("Cluster: Fetched {} bytes (decompressed to {} bytes in {}ms) bundle for {}", compressedBinary.length, decompressedBinary.length, String.format("%.2f", decompressMs), uuid);
            
            PlayerDataBundle bundle = CBOR_MAPPER.readValue(decompressedBinary, PlayerDataBundle.class);
            return Optional.of(bundle);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to fetch/decode CBOR bundle: {}", e.getMessage());
            return Optional.empty();
        }
    }
}
