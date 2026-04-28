package savage.natsplayerdata.storage;

import io.nats.client.KeyValue;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.KeyValueEntry;
import savage.natsfabric.NatsManager;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.model.PlayerState;
import savage.natsplayerdata.model.SessionEntry;
import savage.natsplayerdata.model.SessionState;
import savage.natsplayerdata.util.Serialization;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Specialized storage handler for distributed Session Locks and Lifecycle state.
 */
public class SessionStorage {

    private KeyValue kvBucket;
    public static final ExecutorService VIRTUAL_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private static final class Holder {
        private static final SessionStorage INSTANCE = new SessionStorage();
    }

    private SessionStorage() {
        init();
    }

    public static SessionStorage getInstance() {
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
            NATSPlayerDataBridge.LOGGER.error("SessionStorage: Failed to initialize NATS bucket: {}", e.getMessage());
        }
    }

    public boolean isAvailable() {
        return kvBucket != null && NatsManager.getInstance().isConnected();
    }

    /**
     * Fetches current session lock state from NATS.
     */
    public Optional<SessionEntry> fetchSession(UUID uuid) {
        if (kvBucket == null) return Optional.empty();
        try {
            KeyValueEntry entry = kvBucket.get("session." + uuid.toString());
            if (entry == null || entry.getValue() == null) return Optional.empty();
            
            SessionState state = Serialization.JSON.readValue(entry.getValue(), SessionState.class);
            return Optional.of(new SessionEntry(state, entry.getRevision()));
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("SessionStorage: Failed to fetch session for {}: {}", uuid, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Pushes a player's session state. 
     * If expectedRevision is > 0, it uses NATS Optimistic Concurrency (CAS).
     */
    public boolean pushSession(SessionState state, long expectedRevision) {
        if (kvBucket == null) return false;
        try {
            byte[] json = Serialization.JSON.writeValueAsBytes(state);
            String key = "session." + state.uuid().toString();

            if (expectedRevision > 0) {
                kvBucket.update(key, json, expectedRevision);
            } else {
                kvBucket.put(key, json);
            }
            
            NATSPlayerDataBridge.debugLog("SessionStorage: Pushed state {} (Rev: {}) for {}", 
                state.state(), expectedRevision > 0 ? expectedRevision : "blind", state.uuid());
            return true;
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() == 10071 || e.getMessage().contains("wrong last sequence")) {
                NATSPlayerDataBridge.LOGGER.warn("SessionStorage: Atomic grab failed for {} (Concurrent update detected)", state.uuid());
                return false;
            }
            throw new RuntimeException("NATS API Error", e);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("SessionStorage: Failed to push session for {}: {}", state.uuid(), e.getMessage());
            return false;
        }
    }

    public void pushSession(SessionState state) {
        pushSession(state, -1);
    }

    /**
     * Scans for any DIRTY sessions owned by the local server ID and resets them to CLEAN.
     */
    public void reconcileLocalSessions() {
        if (kvBucket == null) return;
        String localServerId = NatsManager.getInstance().getServerName();
        NATSPlayerDataBridge.LOGGER.info("SessionStorage: Starting parallel session reconciliation for server '{}'...", localServerId);

        try {
            List<String> keys = kvBucket.keys("session.*");
            if (keys.isEmpty()) return;

            AtomicInteger fixedCount = new AtomicInteger(0);
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (String key : keys) {
                futures.add(CompletableFuture.runAsync(() -> {
                    try {
                        KeyValueEntry entry = kvBucket.get(key);
                        if (entry == null || entry.getValue() == null) return;

                        SessionState session = Serialization.JSON.readValue(entry.getValue(), SessionState.class);
                        
                        if (session.state() == PlayerState.DIRTY && localServerId.equals(session.lastServer())) {
                            NATSPlayerDataBridge.debugLog("SessionStorage: Healing orphaned session for {}", key);
                            pushSession(SessionState.create(session.uuid(), PlayerState.CLEAN, localServerId));
                            fixedCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        NATSPlayerDataBridge.LOGGER.warn("SessionStorage: Failed to process key '{}' during reconciliation: {}", key, e.getMessage());
                    }
                }, VIRTUAL_EXECUTOR));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            if (fixedCount.get() > 0) {
                NATSPlayerDataBridge.LOGGER.info("SessionStorage: Successfully reconciled {} orphaned sessions.", fixedCount.get());
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("SessionStorage: Fatal error during reconciliation: {}", e.getMessage());
        }
    }

    /**
     * Fetches all session entries currently stored in the cluster.
     */
    public List<SessionEntry> getAllSessions() {
        List<SessionEntry> sessions = new ArrayList<>();
        if (kvBucket == null) return sessions;

        try {
            for (String key : kvBucket.keys("session.*")) {
                try {
                    KeyValueEntry entry = kvBucket.get(key);
                    if (entry == null || entry.getValue() == null) continue;
                    SessionState session = Serialization.JSON.readValue(entry.getValue(), SessionState.class);
                    sessions.add(new SessionEntry(session, entry.getRevision()));
                } catch (Exception e) {
                    NATSPlayerDataBridge.LOGGER.warn("SessionStorage: Failed to read session key '{}': {}", key, e.getMessage());
                }
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("SessionStorage: Failed to list session keys: {}", e.getMessage());
        }
        return sessions;
    }
}
