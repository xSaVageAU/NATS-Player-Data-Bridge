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
    private static final ObjectMapper CBOR_MAPPER = new ObjectMapper(new CBORFactory());
    private static PlayerStorage instance;

    private KeyValue kvBucket;

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
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to initialize NATS KV storage: {}", e.getMessage());
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
