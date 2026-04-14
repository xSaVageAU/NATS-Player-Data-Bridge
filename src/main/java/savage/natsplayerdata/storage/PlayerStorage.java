package savage.natsplayerdata.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import savage.natsfabric.NatsManager;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.model.PlayerDataBundle;
import savage.natsplayerdata.util.CompressionUtil;

import java.util.Optional;
import java.util.UUID;

/**
 * Handles binary persistence of Player Bundles to the NATS cluster.
 */
public class PlayerStorage {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final ObjectMapper CBOR_MAPPER = new ObjectMapper(new CBORFactory());

    private KeyValue kvBucket;

    private static final class Holder {
        private static final PlayerStorage INSTANCE = new PlayerStorage();
    }

    private PlayerStorage() {
        init();
    }

    public static PlayerStorage getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Re-initializes bucket handles after a NATS reconnect.
     */
    public void reinit() {
        NATSPlayerDataBridge.LOGGER.info("Cluster: Reinitializing bucket handles after reconnect...");
        kvBucket = null;
        init();
        NATSPlayerDataBridge.LOGGER.info("Cluster: Bucket handles re-established.");
    }

    private void init() {
        String dataBucketName = NATSPlayerDataBridge.getConfig() != null && NATSPlayerDataBridge.getConfig().dataBucketName != null 
                ? NATSPlayerDataBridge.getConfig().dataBucketName : "player-sync-v1";

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
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to initialize NATS KV storage: {}", e.getMessage());
        }
    }

    /**
     * Checks if the storage system is reachable.
     */
    public boolean isStorageAvailable() {
        try {
            return kvBucket != null && NatsManager.getInstance().isConnected();
        } catch (Exception e) {
            return false;
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
            byte[] compressedBinary = CompressionUtil.compress(cborBinary);
            double compressMs = (System.nanoTime() - startNanos) / 1_000_000.0;
            
            kvBucket.put("bundle." + bundle.uuid().toString(), compressedBinary);
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
            KeyValueEntry entry = kvBucket.get("bundle." + uuid.toString());
            if (entry == null || entry.getValue() == null) return Optional.empty();

            byte[] compressedBinary = entry.getValue();
            long startNanos = System.nanoTime();
            
            byte[] decompressedBinary = CompressionUtil.decompress(compressedBinary);
            double decompressMs = (System.nanoTime() - startNanos) / 1_000_000.0;
            
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

    /**
     * Pushes a player's session state (DIRTY/CLEAN) to the cluster.
     */
    public void pushSession(savage.natsplayerdata.model.SessionState state) {
        if (kvBucket == null) return;
        try {
            byte[] json = JSON_MAPPER.writeValueAsBytes(state);
            kvBucket.put("session." + state.uuid().toString(), json);
            NATSPlayerDataBridge.debugLog("Cluster: Pushed session state {} for {}", state.state(), state.uuid());
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to push session state for {}: {}", state.uuid(), e.getMessage());
        }
    }

    /**
     * Fetches a player's current session state from the cluster.
     */
    public Optional<savage.natsplayerdata.model.SessionState> fetchSession(UUID uuid) {
        if (kvBucket == null) return Optional.empty();
        try {
            KeyValueEntry entry = kvBucket.get("session." + uuid.toString());
            if (entry == null || entry.getValue() == null) return Optional.empty();
            return Optional.of(JSON_MAPPER.readValue(entry.getValue(), savage.natsplayerdata.model.SessionState.class));
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to fetch session state for {}: {}", uuid, e.getMessage());
            return Optional.empty();
        }
    }
}
