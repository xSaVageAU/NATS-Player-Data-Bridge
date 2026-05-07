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

    private static final class Holder {
        private static final SessionStorage INSTANCE = new SessionStorage();
    }

    private SessionStorage() {
        // Initialization is now managed by StorageLifecycleManager
    }

    public static SessionStorage getInstance() {
        return Holder.INSTANCE;
    }

    public void init(io.nats.client.Connection conn, String bucketName) {
        if (kvBucket != null) return;

        try {
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
            NATSPlayerDataBridge.LOGGER.error("SessionStorage: Failed to initialize NATS bucket '{}': {}", bucketName, e.getMessage());
        }
    }

    public boolean isAvailable() {
        return kvBucket != null && NatsManager.getInstance().isConnected();
    }

    private void ensureReady() {
        if (kvBucket == null) {
            throw new IllegalStateException("NATS SessionStorage is NOT initialized. Check NATS connection status.");
        }
    }

    /**
     * Fetches current session lock state from NATS.
     */
    public Optional<SessionEntry> fetchSession(UUID uuid) {
        ensureReady();
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
        ensureReady();
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
        try {
            ensureReady();
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("SessionStorage: Cannot reconcile sessions - Storage not ready!");
            return;
        }
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
                            UUID uuid = session.uuid();

                            // SAFETY: Never heal a session for a player that is currently online on THIS server!
                            var server = NATSPlayerDataBridge.getServer();
                            if (server != null && server.getPlayerList().getPlayer(uuid) != null) {
                                NATSPlayerDataBridge.debugLog("SessionStorage: Skipping reconciliation for {} - Player is currently online.", uuid);
                                return;
                            }

                            // Check if this was a "Ghost Lock" (No local vault data = Hard Crash)
                            if (!PersistenceService.hasPendingSync(uuid)) {
                                NATSPlayerDataBridge.LOGGER.warn("Cluster: Detected unrecoverable desync for {}. Player was online during a hard crash; progress since last auto-save may be lost.", uuid);
                            }

                            NATSPlayerDataBridge.debugLog("SessionStorage: Healing orphaned session for {}", key);
                            pushSession(SessionState.create(uuid, PlayerState.CLEAN, localServerId));
                            fixedCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        NATSPlayerDataBridge.LOGGER.warn("SessionStorage: Failed to process key '{}' during reconciliation: {}", key, e.getMessage());
                    }
                }, StorageLifecycleManager.VIRTUAL_EXECUTOR));
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
        try {
            ensureReady();
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("SessionStorage: Cannot list sessions - Storage not ready!");
            return sessions;
        }

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
