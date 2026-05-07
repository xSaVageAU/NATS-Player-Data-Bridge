package savage.natsplayerdata.storage;

import io.nats.client.Connection;
import net.minecraft.server.MinecraftServer;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.session.SessionManager;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Orchestrates the initialization and lifecycle of all NATS storage buckets.
 * Ensures that Data, Session, and Backup storages are only activated once a
 * valid NATS connection is confirmed.
 */
public class StorageLifecycleManager {

    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean reconciling = new AtomicBoolean(false);
    public static final ExecutorService VIRTUAL_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private static final class Holder {
        private static final StorageLifecycleManager INSTANCE = new StorageLifecycleManager();
    }

    public static StorageLifecycleManager getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * @return True if the bridge is fully initialized and NATS is connected.
     */
    public boolean isReady() {
        return ready.get() && !reconciling.get() && savage.natsfabric.NatsManager.getInstance().isConnected();
    }

    /**
     * Attempts to initialize all storage buckets using the provided NATS connection.
     * @param conn The active NATS connection.
     */
    public synchronized void initialize(Connection conn) {
        if (ready.get()) return;

        if (conn == null) {
            NATSPlayerDataBridge.LOGGER.error("Cluster: Cannot initialize storage - NATS connection is null!");
            return;
        }

        try {
            NATSPlayerDataBridge.LOGGER.info("Cluster: Initializing storage buckets...");

            // Get settings from config
            var config = NATSPlayerDataBridge.getConfig();
            String dataBucket = (config != null && config.dataBucketName != null) ? config.dataBucketName : "player-sync-v1";
            String backupBucket = (config != null && config.backupBucketName != null) ? config.backupBucketName : "player-backups-v1";
            int backupHistory = (config != null) ? config.backupHistoryCount : 20;

            // 1. Initialize Session Storage (shared bucket)
            SessionStorage.getInstance().init(conn, dataBucket);

            // 2. Initialize Data Storage (shared bucket)
            DataStorage.getInstance().init(conn, dataBucket);

            // 3. Initialize Backup Storage (dedicated bucket)
            BackupStorage.getInstance().init(conn, backupBucket, backupHistory);

            ready.set(true);
            NATSPlayerDataBridge.LOGGER.info("Cluster: All storage buckets are READY.");
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Cluster: Storage initialization failed: {}", e.getMessage());
        }
    }

    /**
     * Schedules a background task to wait for NATS and initialize storage.
     * Once initialized, it runs startup tasks like session reconciliation on the server thread.
     */
    public void scheduleInitialization(MinecraftServer server) {
        VIRTUAL_EXECUTOR.execute(() -> {
            NATSPlayerDataBridge.LOGGER.info("Cluster: Starting async storage watchdog...");
            int attempts = 0;
            while (!ready.get() && attempts < 30) {
                try {
                    var conn = savage.natsfabric.NatsManager.getInstance().getConnection();
                    if (conn != null && conn.getStatus() == Connection.Status.CONNECTED) {
                        initialize(conn);

                        // Run post-init tasks on the server thread
                        server.execute(() -> {
                            // 5. Re-arm RPC listeners for this server ID on the main thread
                            SessionManager.initRpcListener(server);

                            // 6. Perform unified healing (Vault + Ghost Locks)
                            reconcileLocalVault();
                        });
                        return;
                    }
                } catch (Exception ignored) {}

                attempts++;
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            if (!ready.get()) {
                NATSPlayerDataBridge.LOGGER.error("Cluster: FATAL - Storage initialization TIMED OUT after 60s. The bridge will NOT be active.");
            }
        });
    }
    /**
     * Reconciles any pending data in the local vault and restores cluster consistency.
     * Fired when the NATS connection is restored.
     */
    public void reconcileLocalVault() {
        if (reconciling.get()) return;
        
        reconciling.set(true);
        VIRTUAL_EXECUTOR.execute(() -> {
            try {
                // Wait a moment for JetStream to fully stabilize
                try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException ignored) {}

                if (!savage.natsfabric.NatsManager.getInstance().isConnected()) return;

                NATSPlayerDataBridge.LOGGER.info("Cluster: Reconciling local vault - Syncing pending bundles...");
                java.util.Set<java.util.UUID> recovered = new java.util.HashSet<>();

                PersistenceService.getPendingUUIDs().forEach(uuid -> {
                    // Safety: If the player is online on THIS server, skip the stale vault file.
                    // Their live session will perform a fresh push when they disconnect.
                    var server = NATSPlayerDataBridge.getServer();
                    if (server != null && server.getPlayerList().getPlayer(uuid) != null) {
                        NATSPlayerDataBridge.debugLog("Cluster: Skipping vault sync for {} - Player is currently online.", uuid);
                        return;
                    }

                    try {
                        var bundle = PersistenceService.consumeFromVault(uuid);
                        if (bundle != null) {
                            // STALE CHECK: Ensure we don't overwrite newer NATS data with an old vault file
                            var currentNatsOpt = savage.natsplayerdata.storage.DataStorage.getInstance().fetchBundle(uuid);
                            if (currentNatsOpt.isPresent() && currentNatsOpt.get().timestamp() > bundle.timestamp()) {
                                NATSPlayerDataBridge.LOGGER.warn("Cluster: Discarding stale vault data for {} - NATS already has newer progress ({} vs {}).", 
                                    uuid, currentNatsOpt.get().timestamp(), bundle.timestamp());
                                return;
                            }

                            // Push the captured bundle and release the lock
                            savage.natsplayerdata.sync.SyncService.pushAsync(uuid, uuid.toString(), bundle, true);
                            NATSPlayerDataBridge.LOGGER.info("Cluster: Local vault successfully restored data and released lock for {}.", uuid);
                            recovered.add(uuid);
                        }
                    } catch (Exception e) {
                        NATSPlayerDataBridge.LOGGER.error("Cluster: Vault reconciliation failed for {}: {}", uuid, e.getMessage());
                    }
                });

                // Phase 2: Cleanup any remaining "Ghost Locks" (online during crash)
                savage.natsplayerdata.storage.SessionStorage.getInstance().reconcileLocalSessions(recovered);

            } finally {
                reconciling.set(false);
                NATSPlayerDataBridge.LOGGER.info("Cluster: Reconciliation complete. Bridge is now READY.");
            }
        });
    }
}
