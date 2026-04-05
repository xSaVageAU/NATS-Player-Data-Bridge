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
            byte[] binary = CBOR_MAPPER.writeValueAsBytes(bundle);
            kvBucket.put(bundle.uuid().toString(), binary);
            NATSPlayerDataBridge.LOGGER.info("Cluster: Pushed {} KB bundle for {}", binary.length / 1024, bundle.uuid());
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

            PlayerDataBundle bundle = CBOR_MAPPER.readValue(entry.getValue(), PlayerDataBundle.class);
            return Optional.of(bundle);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to fetch/decode CBOR bundle: {}", e.getMessage());
            return Optional.empty();
        }
    }
}
