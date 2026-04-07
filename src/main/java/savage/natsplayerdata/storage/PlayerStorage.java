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
            
            // Presence bucket with 60s TTL
            try {
                presenceBucket = NatsManager.getInstance().getConnection().keyValue("player-presence-v1");
            } catch (Exception e) {
                io.nats.client.KeyValueManagement kvm = NatsManager.getInstance().getConnection().keyValueManagement();
                kvm.create(io.nats.client.api.KeyValueConfiguration.builder()
                    .name(PRESENCE_BUCKET)
                    .ttl(java.time.Duration.ofSeconds(60))
                    .storageType(io.nats.client.api.StorageType.Memory)
                    .build());
                presenceBucket = NatsManager.getInstance().getConnection().keyValue(PRESENCE_BUCKET);
            }

            if (presenceBucket == null) {
                NATSPlayerDataBridge.LOGGER.warn("Synchronizer: Presence bucket '{}' not available!", PRESENCE_BUCKET);
            } else {
                startPresenceWatcher();
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to initialize NATS KV storage: {}", e.getMessage());
        }
    }

    /**
     * Starts a watcher on the presence bucket to maintain a local in-memory cache.
     */
    private void startPresenceWatcher() {
        try {
            if (presenceBucket != null) {
                presenceBucket.watchAll(new io.nats.client.api.KeyValueWatcher() {
                    @Override
                    public void watch(io.nats.client.api.KeyValueEntry entry) {
                        try {
                            String key = entry.getKey();
                            if (key.startsWith("_KV")) return; 
                            UUID uuid = UUID.fromString(key);
                            
                            if (entry.getValue() == null || entry.getOperation() == io.nats.client.api.KeyValueOperation.DELETE || entry.getOperation() == io.nats.client.api.KeyValueOperation.PURGE) {
                                savage.natsplayerdata.PlayerPresenceManager.updateLocalCache(uuid, null);
                            } else {
                                String rawValue = new String(entry.getValue(), java.nio.charset.StandardCharsets.UTF_8);
                                savage.natsplayerdata.PlayerPresenceManager.updateLocalCache(uuid, rawValue);
                            }
                        } catch (Exception ignored) {}
                    }

                    @Override
                    public void endOfData() {
                        NATSPlayerDataBridge.LOGGER.info("Cluster: Presence cache initial sync complete.");
                    }
                });
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Cluster: Failed to start presence watcher: {}", e.getMessage());
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
