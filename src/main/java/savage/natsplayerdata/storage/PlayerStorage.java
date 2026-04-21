package savage.natsplayerdata.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import savage.natsfabric.NatsManager;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.model.PlayerDataBundle;
import savage.natsplayerdata.util.CompressionUtil;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Optional;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;

/**
 * Handles binary persistence of Player Bundles to the NATS cluster.
 */
public class PlayerStorage {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final ObjectMapper CBOR_MAPPER = new ObjectMapper(new CBORFactory());

    private KeyValue kvBucket;
    public static final ExecutorService VIRTUAL_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

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
     * Directly pushes a pre-compressed binary blob to the cluster.
     * Useful for restoring backups without needing to re-process the data.
     */
    public void pushRawBundle(UUID uuid, byte[] compressedBinary) {
        if (kvBucket == null) return;
        try {
            kvBucket.put("bundle." + uuid.toString(), compressedBinary);
            NATSPlayerDataBridge.debugLog("Cluster: Pushed raw binary bundle ({} bytes) for {}", compressedBinary.length, uuid);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to push raw binary bundle: {}", e.getMessage());
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
            return deserializeBundle(entry.getValue());
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Failed to fetch/decode CBOR bundle for {}: {}", uuid, e.getMessage());
            throw new RuntimeException("§cFailed to load your player data from the cluster. Please try again.");
        }
    }

    /**
     * Helper to deserialize a compressed CBOR bundle.
     */
    public Optional<PlayerDataBundle> deserializeBundle(byte[] compressedBinary) {
        try {
            byte[] decompressed = CompressionUtil.decompress(compressedBinary);
            PlayerDataBundle bundle = CBOR_MAPPER.readValue(decompressed, PlayerDataBundle.class);
            return Optional.ofNullable(bundle);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Cluster: Failed to deserialize bundle: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Fetches a player bundle asynchronously.
     */
    public java.util.concurrent.CompletableFuture<Optional<PlayerDataBundle>> fetchBundleAsync(UUID uuid) {
        return java.util.concurrent.CompletableFuture.supplyAsync(() -> fetchBundle(uuid), VIRTUAL_EXECUTOR);
    }

    /**
     * Fetches current session lock state from NATS.
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
        NATSPlayerDataBridge.LOGGER.info("Cluster: Starting parallel session reconciliation for server '{}'...", localServerId);

        try {
            List<String> keys = kvBucket.keys("session.*");
            if (keys.isEmpty()) return;

            AtomicInteger fixedCount = new AtomicInteger(0);
            List<java.util.concurrent.CompletableFuture<Void>> futures = new ArrayList<>();

            for (String key : keys) {
                futures.add(java.util.concurrent.CompletableFuture.runAsync(() -> {
                    try {
                        KeyValueEntry entry = kvBucket.get(key);
                        if (entry == null || entry.getValue() == null) return;

                        savage.natsplayerdata.model.SessionState session = JSON_MAPPER.readValue(entry.getValue(), savage.natsplayerdata.model.SessionState.class);
                        
                        if (session.state() == savage.natsplayerdata.model.PlayerState.DIRTY && localServerId.equals(session.lastServer())) {
                            NATSPlayerDataBridge.debugLog("Cluster: Healing orphaned session for {}", key);
                            pushSession(savage.natsplayerdata.model.SessionState.create(session.uuid(), savage.natsplayerdata.model.PlayerState.CLEAN, localServerId));
                            fixedCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        NATSPlayerDataBridge.LOGGER.warn("Cluster: Failed to process session key '{}' during reconciliation: {}", key, e.getMessage());
                    }
                }, VIRTUAL_EXECUTOR));
            }

            // Parallel wait for all virtual threads to complete
            java.util.concurrent.CompletableFuture.allOf(futures.toArray(new java.util.concurrent.CompletableFuture[0])).join();

            if (fixedCount.get() > 0) {
                NATSPlayerDataBridge.LOGGER.info("Cluster: Successfully reconciled {} orphaned sessions.", fixedCount.get());
            } else {
                NATSPlayerDataBridge.debugLog("Cluster: No orphaned sessions found to reconcile.");
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Cluster: Fatal error during session reconciliation: {}", e.getMessage());
        }
    }
}
