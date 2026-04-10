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

    private static final ObjectMapper CBOR_MAPPER = new ObjectMapper(new CBORFactory());

    private KeyValue kvBucket;
    private KeyValue presenceBucket;

    private static final class Holder {
        private static final PlayerStorage INSTANCE = new PlayerStorage();
    }

    private PlayerStorage() {
        init();
    }

    public static PlayerStorage getInstance() {
        return Holder.INSTANCE;
    }

    public KeyValue getPresenceBucket() {
        return presenceBucket;
    }

    private void init() {
        String dataBucketName = NATSPlayerDataBridge.getConfig() != null && NATSPlayerDataBridge.getConfig().dataBucketName != null 
                ? NATSPlayerDataBridge.getConfig().dataBucketName : "player-sync-v1";
        String presenceBucketName = NATSPlayerDataBridge.getConfig() != null && NATSPlayerDataBridge.getConfig().presenceBucketName != null 
                ? NATSPlayerDataBridge.getConfig().presenceBucketName : "player-presence-v1";

        try {
            var conn = NatsManager.getInstance().getConnection();
            if (conn == null) {
                NATSPlayerDataBridge.LOGGER.error("Synchronizer: NATS connection not available for init!");
                return;
            }

            // Sync bucket (persistent)
            try {
                kvBucket = conn.keyValue(dataBucketName);
            } catch (Exception e) {
                try {
                    io.nats.client.KeyValueManagement kvm = conn.keyValueManagement();
                    kvm.create(io.nats.client.api.KeyValueConfiguration.builder()
                        .name(dataBucketName)
                        .build());
                    kvBucket = conn.keyValue(dataBucketName);
                } catch (Exception e2) {
                    NATSPlayerDataBridge.LOGGER.error("Synchronizer: Failed to create sync bucket '{}': {}", dataBucketName, e2.getMessage());
                }
            }

            if (kvBucket == null) {
                NATSPlayerDataBridge.LOGGER.error("Synchronizer: Bucket '{}' not available!", dataBucketName);
            }
            
            // Presence bucket with 60s TTL
            try {
                io.nats.client.KeyValueManagement kvm = conn.keyValueManagement();
                boolean create = false;
                try {
                    io.nats.client.api.KeyValueStatus status = kvm.getStatus(presenceBucketName);
                    // Check if it's currently persistent or has no TTL
                    if (status.getConfiguration().getStorageType() != io.nats.client.api.StorageType.Memory || status.getConfiguration().getTtl().toSeconds() != 60) {
                        NATSPlayerDataBridge.LOGGER.warn("Cluster: Presence bucket exists with wrong config (Storage: {}, TTL: {}s). Recreating...", 
                            status.getConfiguration().getStorageType(), status.getConfiguration().getTtl().toSeconds());
                        kvm.delete(presenceBucketName);
                        create = true;
                    }
                } catch (Exception e) {
                    // Doesn't exist
                    create = true;
                }

                if (create) {
                    kvm.create(io.nats.client.api.KeyValueConfiguration.builder()
                        .name(presenceBucketName)
                        .ttl(java.time.Duration.ofSeconds(60))
                        .storageType(io.nats.client.api.StorageType.Memory)
                        .build());
                }
                presenceBucket = conn.keyValue(presenceBucketName);
            } catch (Exception e) {
                NATSPlayerDataBridge.LOGGER.error("Cluster: Failed to setup ephemeral presence bucket: {}", e.getMessage());
            }

            if (presenceBucket == null) {
                NATSPlayerDataBridge.LOGGER.warn("Synchronizer: Presence bucket '{}' not available!", presenceBucketName);
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

                            // Use the NATS entry's actual write time so that replayed startup entries
                            // inherit their real age rather than getting a fresh 60s local TTL.
                            long natsTimestamp = entry.getCreated() != null
                                    ? entry.getCreated().toInstant().toEpochMilli()
                                    : System.currentTimeMillis();

                            if (entry.getValue() == null || entry.getOperation() == io.nats.client.api.KeyValueOperation.DELETE || entry.getOperation() == io.nats.client.api.KeyValueOperation.PURGE) {
                                savage.natsplayerdata.PlayerPresenceManager.updateLocalCache(uuid, null, 0);
                            } else {
                                String rawValue = new String(entry.getValue(), java.nio.charset.StandardCharsets.UTF_8);
                                savage.natsplayerdata.PlayerPresenceManager.updateLocalCache(uuid, rawValue, natsTimestamp);
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
            NATSPlayerDataBridge.LOGGER.error("Failed to fetch/decode CBOR bundle for {}: {}", uuid, e.getMessage());
            // Throwing here forces the login sequence to crash out. 
            // This is safer than returning Optional.empty(), which would let Minecraft 
            // load the stale local disk file and cause an inventory rollback.
            throw new RuntimeException("§cFailed to load your player data from the cluster. Please try again.");
        }
    }

    /**
     * Fetches a player bundle asynchronously.
     */
    public java.util.concurrent.CompletableFuture<Optional<PlayerDataBundle>> fetchBundleAsync(UUID uuid) {
        return java.util.concurrent.CompletableFuture.supplyAsync(() -> fetchBundle(uuid));
    }
}
