package savage.natsplayerdata.storage;

import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import savage.natsfabric.NatsManager;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.model.PlayerDataBundle;
import savage.natsplayerdata.util.CompressionUtil;
import savage.natsplayerdata.util.Serialization;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Specialized storage handler for Primary Player Data Bundles in NATS.
 */
public class DataStorage {

    private KeyValue kvBucket;
    public static final ExecutorService VIRTUAL_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private static final class Holder {
        private static final DataStorage INSTANCE = new DataStorage();
    }

    private DataStorage() {
        init();
    }

    public static DataStorage getInstance() {
        return Holder.INSTANCE;
    }

    private void init() {
        String bucketName = NATSPlayerDataBridge.getConfig() != null && NATSPlayerDataBridge.getConfig().dataBucketName != null 
                ? NATSPlayerDataBridge.getConfig().dataBucketName : "player-sync-v1";

        try {
            var conn = NatsManager.getInstance().getConnection();
            if (conn == null) return;

            try {
                kvBucket = conn.keyValue(bucketName);
            } catch (Exception e) {
                io.nats.client.KeyValueManagement kvm = conn.keyValueManagement();
                kvm.create(io.nats.client.api.KeyValueConfiguration.builder()
                    .name(bucketName)
                    .build());
                kvBucket = conn.keyValue(bucketName);
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("DataStorage: Failed to initialize NATS bucket: {}", e.getMessage());
        }
    }

    public boolean isAvailable() {
        return kvBucket != null && NatsManager.getInstance().isConnected();
    }

    /**
     * Serializes, compresses, and uploads a player bundle.
     */
    public void pushBundle(PlayerDataBundle bundle) {
        if (kvBucket == null) return;
        try {
            byte[] cborBinary = Serialization.CBOR.writeValueAsBytes(bundle);
            
            long startNanos = System.nanoTime();
            byte[] compressedBinary = CompressionUtil.compress(cborBinary);
            double compressMs = (System.nanoTime() - startNanos) / 1_000_000.0;
            
            kvBucket.put("bundle." + bundle.uuid().toString(), compressedBinary);
            NATSPlayerDataBridge.debugLog("DataStorage: Pushed {} bytes (Zstd compressed from {} bytes in {}ms) for {}", 
                compressedBinary.length, cborBinary.length, String.format("%.2f", compressMs), bundle.uuid());
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("DataStorage: Failed to push bundle for {}: {}", bundle.uuid(), e.getMessage());
        }
    }

    /**
     * Directly pushes a pre-compressed binary blob to the cluster.
     */
    public void pushRawBundle(UUID uuid, byte[] compressedBinary) {
        if (kvBucket == null) return;
        try {
            kvBucket.put("bundle." + uuid.toString(), compressedBinary);
            NATSPlayerDataBridge.debugLog("DataStorage: Pushed raw binary bundle ({} bytes) for {}", compressedBinary.length, uuid);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("DataStorage: Failed to push raw bundle for {}: {}", uuid, e.getMessage());
        }
    }

    /**
     * Fetches and deserializes a player bundle.
     */
    public Optional<PlayerDataBundle> fetchBundle(UUID uuid) {
        if (kvBucket == null) return Optional.empty();
        try {
            KeyValueEntry entry = kvBucket.get("bundle." + uuid.toString());
            if (entry == null || entry.getValue() == null) return Optional.empty();
            
            return deserializeBundle(entry.getValue());
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("DataStorage: Failed to fetch bundle for {}: {}", uuid, e.getMessage());
            throw new RuntimeException("§cFailed to load your player data from the cluster.");
        }
    }

    public Optional<PlayerDataBundle> deserializeBundle(byte[] compressedBinary) {
        try {
            byte[] decompressed = CompressionUtil.decompress(compressedBinary);
            return Optional.ofNullable(Serialization.CBOR.readValue(decompressed, PlayerDataBundle.class));
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("DataStorage: Failed to deserialize bundle: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public CompletableFuture<Optional<PlayerDataBundle>> fetchBundleAsync(UUID uuid) {
        return CompletableFuture.supplyAsync(() -> fetchBundle(uuid), VIRTUAL_EXECUTOR);
    }
}
