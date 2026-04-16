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
     * Initializes the connection to the NATS Key-Value bucket.
     */
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
     * Fetches a player's current session state along with its NATS revision (for CAS).
     */
    public Optional<SessionEntry> fetchSession(UUID uuid) {
        if (kvBucket == null) return Optional.empty();
        try {
            KeyValueEntry entry = kvBucket.get("session." + uuid.toString());
            if (entry == null || entry.getValue() == null) return Optional.empty();
            
            savage.natsplayerdata.model.SessionState state = JSON_MAPPER.readValue(entry.getValue(), savage.natsplayerdata.model.SessionState.class);
            return Optional.of(new SessionEntry(state, entry.getRevision()));
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to fetch session state for {}: {}", uuid, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Pushes a player's session state. 
     * If expectedRevision is > 0, it uses NATS Optimistic Concurrency (CAS).
     * @return true if the update was successful, false if it failed (e.g. revision mismatch)
     */
    public boolean pushSession(savage.natsplayerdata.model.SessionState state, long expectedRevision) {
        if (kvBucket == null) return false;
        try {
            byte[] json = JSON_MAPPER.writeValueAsBytes(state);
            String key = "session." + state.uuid().toString();

            if (expectedRevision > 0) {
                // Atomic Update (Check-and-Set)
                kvBucket.update(key, json, expectedRevision);
            } else {
                // Standard Put (Blind overwrite)
                kvBucket.put(key, json);
            }
            
            NATSPlayerDataBridge.debugLog("Cluster: Pushed session state {} (Rev: {}) for {}", state.state(), expectedRevision > 0 ? expectedRevision : "blind", state.uuid());
            return true;
        } catch (io.nats.client.JetStreamApiException e) {
            if (e.getErrorCode() == 10071 || e.getMessage().contains("wrong last sequence")) {
                NATSPlayerDataBridge.LOGGER.warn("Cluster: Atomic grab failed for {} (Someone else updated the lock first)", state.uuid());
                return false;
            }
            throw new RuntimeException("NATS API Error", e);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to push session state for {}: {}", state.uuid(), e.getMessage());
            return false;
        }
    }

    /** Helper for standard blind pushes */
    public void pushSession(savage.natsplayerdata.model.SessionState state) {
        pushSession(state, -1);
    }

    /**
     * Data record to hold a session and its NATS revision.
     */
    public record SessionEntry(savage.natsplayerdata.model.SessionState state, long revision) {}
    /**
     * Fetches all session entries currently stored in the cluster.
     */
    public java.util.List<SessionEntry> getAllSessions() {
        java.util.List<SessionEntry> sessions = new java.util.ArrayList<>();
        if (kvBucket == null) return sessions;

        try {
            for (String key : kvBucket.keys("session.*")) {
                try {
                    KeyValueEntry entry = kvBucket.get(key);
                    if (entry == null || entry.getValue() == null) continue;
                    savage.natsplayerdata.model.SessionState session = JSON_MAPPER.readValue(entry.getValue(), savage.natsplayerdata.model.SessionState.class);
                    sessions.add(new SessionEntry(session, entry.getRevision()));
                } catch (Exception e) {
                    NATSPlayerDataBridge.LOGGER.warn("Cluster: Failed to read session key '{}': {}", key, e.getMessage());
                }
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Cluster: Failed to list session keys: {}", e.getMessage());
        }
        return sessions;
    }

    /**
     * Scans for any DIRTY sessions owned by the local server ID and resets them to CLEAN.
     * This "self-heals" sessions that were left hanging after a server crash.
     */
    public void reconcileLocalSessions() {
        if (kvBucket == null) return;
        String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();
        NATSPlayerDataBridge.LOGGER.info("Cluster: Starting session reconciliation for server '{}'...", localServerId);

        try {
            int fixedCount = 0;
            // Iterate over all session keys
            for (String key : kvBucket.keys("session.*")) {
                try {
                    KeyValueEntry entry = kvBucket.get(key);
                    if (entry == null || entry.getValue() == null) continue;

                    savage.natsplayerdata.model.SessionState session = JSON_MAPPER.readValue(entry.getValue(), savage.natsplayerdata.model.SessionState.class);
                    
                    // If it's DIRTY and belongs to US, it's an orphan from a crash.
                    if (session.state() == savage.natsplayerdata.model.PlayerState.DIRTY && localServerId.equals(session.lastServer())) {
                        NATSPlayerDataBridge.debugLog("Cluster: Healing orphaned session for {}", key);
                        pushSession(savage.natsplayerdata.model.SessionState.create(session.uuid(), savage.natsplayerdata.model.PlayerState.CLEAN, localServerId));
                        fixedCount++;
                    }
                } catch (Exception e) {
                    NATSPlayerDataBridge.LOGGER.warn("Cluster: Failed to process session key '{}' during reconciliation: {}", key, e.getMessage());
                }
            }
            if (fixedCount > 0) {
                NATSPlayerDataBridge.LOGGER.info("Cluster: Successfully reconciled {} orphaned sessions.", fixedCount);
            } else {
                NATSPlayerDataBridge.debugLog("Cluster: No orphaned sessions found to reconcile.");
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Cluster: Fatal error during session reconciliation: {}", e.getMessage());
        }
    }
}
