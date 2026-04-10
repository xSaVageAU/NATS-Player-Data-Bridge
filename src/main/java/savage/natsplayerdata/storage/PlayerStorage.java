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

    public KeyValue getPresenceBucket() {
        return presenceBucket;
    }

    private void init() {
        try {
            var conn = NatsManager.getInstance().getConnection();
            if (conn == null) {
                NATSPlayerDataBridge.LOGGER.error("Synchronizer: NATS connection not available for init!");
                return;
            }

            // Sync bucket (persistent)
            try {
                kvBucket = conn.keyValue(BUCKET_NAME);
            } catch (Exception e) {
                try {
                    io.nats.client.KeyValueManagement kvm = conn.keyValueManagement();
                    kvm.create(io.nats.client.api.KeyValueConfiguration.builder()
                        .name(BUCKET_NAME)
                        .build());
                    kvBucket = conn.keyValue(BUCKET_NAME);
                } catch (Exception e2) {
                    NATSPlayerDataBridge.LOGGER.error("Synchronizer: Failed to create sync bucket '{}': {}", BUCKET_NAME, e2.getMessage());
                }
            }

            if (kvBucket == null) {
                NATSPlayerDataBridge.LOGGER.error("Synchronizer: Bucket '{}' not available!", BUCKET_NAME);
            }
            
            // Presence bucket with 60s TTL
            try {
                io.nats.client.KeyValueManagement kvm = conn.keyValueManagement();
                boolean create = false;
                try {
                    io.nats.client.api.KeyValueStatus status = kvm.getStatus(PRESENCE_BUCKET);
                    // Check if it's currently persistent or has no TTL
                    if (status.getConfiguration().getStorageType() != io.nats.client.api.StorageType.Memory || status.getConfiguration().getTtl().toSeconds() != 60) {
                        NATSPlayerDataBridge.LOGGER.warn("Cluster: Presence bucket exists with wrong config (Storage: {}, TTL: {}s). Recreating...", 
                            status.getConfiguration().getStorageType(), status.getConfiguration().getTtl().toSeconds());
                        kvm.delete(PRESENCE_BUCKET);
                        create = true;
                    }
                } catch (Exception e) {
                    // Doesn't exist
                    create = true;
                }

                if (create) {
                    kvm.create(io.nats.client.api.KeyValueConfiguration.builder()
                        .name(PRESENCE_BUCKET)
                        .ttl(java.time.Duration.ofSeconds(60))
                        .storageType(io.nats.client.api.StorageType.Memory)
                        .build());
                }
                presenceBucket = conn.keyValue(PRESENCE_BUCKET);
            } catch (Exception e) {
                NATSPlayerDataBridge.LOGGER.error("Cluster: Failed to setup ephemeral presence bucket: {}", e.getMessage());
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
                        } catch (Exception e) {
                            NATSPlayerDataBridge.LOGGER.warn("Cluster: Presence watcher entry error: {}", e.getMessage());
                        }
                    }

                    @Override
                    public void endOfData() {
                        NATSPlayerDataBridge.debugLog("Cluster: Presence cache initial sync complete.");
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
            NATSPlayerDataBridge.debugLog("Cluster: Presence bucket purged.");
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
            // Modern Zstd compression (Level 1 for maximum speed)
            byte[] compressedBinary = com.github.luben.zstd.Zstd.compress(cborBinary, 1);
            long compressNanos = System.nanoTime() - startNanos;
            double compressMs = compressNanos / 1_000_000.0;
            
            kvBucket.put(bundle.uuid().toString(), compressedBinary);
            NATSPlayerDataBridge.debugLog("Cluster: Pushed {} bytes (Zstd compressed from {} bytes in {}ms) bundle for {}", compressedBinary.length, cborBinary.length, String.format("%.2f", compressMs), bundle.uuid());
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
            
            // Modern Zstd decompression
            long decompressedSize = com.github.luben.zstd.Zstd.decompressedSize(compressedBinary);
            byte[] decompressedBinary = com.github.luben.zstd.Zstd.decompress(compressedBinary, (int) decompressedSize);
            
            long decompressNanos = System.nanoTime() - startNanos;
            double decompressMs = decompressNanos / 1_000_000.0;
            
            NATSPlayerDataBridge.debugLog("Cluster: Fetched {} bytes (Zstd decompressed to {} bytes in {}ms) bundle for {}", compressedBinary.length, decompressedBinary.length, String.format("%.2f", decompressMs), uuid);
            
            PlayerDataBundle bundle = CBOR_MAPPER.readValue(decompressedBinary, PlayerDataBundle.class);
            return Optional.of(bundle);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to fetch/decode CBOR bundle: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Fetches a player bundle asynchronously.
     */
    public java.util.concurrent.CompletableFuture<Optional<PlayerDataBundle>> fetchBundleAsync(UUID uuid) {
        return java.util.concurrent.CompletableFuture.supplyAsync(() -> fetchBundle(uuid));
    }
}
