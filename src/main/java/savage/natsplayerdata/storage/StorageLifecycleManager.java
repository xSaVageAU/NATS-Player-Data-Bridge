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
    public static final ExecutorService VIRTUAL_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private static final class Holder {
        private static final StorageLifecycleManager INSTANCE = new StorageLifecycleManager();
    }

    public static StorageLifecycleManager getInstance() {
        return Holder.INSTANCE;
    }

    public boolean isReady() {
        return ready.get();
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

            // 4. Perform session reconciliation (Heal orphaned locks)
            NATSPlayerDataBridge.LOGGER.info("Cluster: Healing orphaned session locks...");
            SessionStorage.getInstance().reconcileLocalSessions();

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
                            SessionManager.initRpcListener(server);
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
}
