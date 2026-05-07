package savage.natsplayerdata.storage;

import io.nats.client.Connection;
import savage.natsplayerdata.NATSPlayerDataBridge;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Orchestrates the initialization and lifecycle of all NATS storage buckets.
 * Ensures that Data, Session, and Backup storages are only activated once a
 * valid NATS connection is confirmed.
 */
public class StorageLifecycleManager {

    private final AtomicBoolean ready = new AtomicBoolean(false);

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

            ready.set(true);
            NATSPlayerDataBridge.LOGGER.info("Cluster: All storage buckets are READY.");
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Cluster: Storage initialization failed: {}", e.getMessage());
        }
    }
}
