package savage.natsplayerdata.backup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import io.nats.client.KeyValue;
import savage.natsfabric.NatsManager;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.model.PlayerDataBundle;
import savage.natsplayerdata.util.CompressionUtil;

import java.util.UUID;

/**
 * Manages long-term backups of player data in the NATS cluster.
 */
public class BackupManager {

    private static final ObjectMapper CBOR_MAPPER = new ObjectMapper(new CBORFactory());
    private KeyValue backupBucket;

    private static final class Holder {
        private static final BackupManager INSTANCE = new BackupManager();
    }

    private BackupManager() {
        init();
    }

    public static BackupManager getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Ensures the NATS connection and backup bucket are initialized.
     */
    public synchronized boolean init() {
        if (backupBucket != null) return true;

        String bucketName = NATSPlayerDataBridge.getConfig() != null && NATSPlayerDataBridge.getConfig().backupBucketName != null 
                ? NATSPlayerDataBridge.getConfig().backupBucketName : "player-backups-v1";

        try {
            var conn = NatsManager.getInstance().getConnection();
            if (conn == null) return false;

            try {
                backupBucket = conn.keyValue(bucketName);
                return true;
            } catch (Exception e) {
                try {
                    io.nats.client.KeyValueManagement kvm = conn.keyValueManagement();
                    kvm.create(io.nats.client.api.KeyValueConfiguration.builder()
                            .name(bucketName)
                            .maxHistoryPerKey(10)
                            .build());
                    backupBucket = conn.keyValue(bucketName);
                    NATSPlayerDataBridge.LOGGER.info("Cluster: Created long-term backup bucket '{}'", bucketName);
                    return true;
                } catch (Exception e2) {
                    NATSPlayerDataBridge.LOGGER.error("Cluster: Failed to initialize backup bucket '{}': {}", bucketName, e2.getMessage());
                }
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Cluster: Backup system initialization failed: {}", e.getMessage());
        }
        return false;
    }

    /**
     * Creates a manual backup snapshot of the player's current data.
     * @return true if successful
     */
    public boolean createBackup(PlayerDataBundle bundle) {
        if (!init()) {
            NATSPlayerDataBridge.LOGGER.error("Cluster: Cannot create backup - NATS connection not ready!");
            return false;
        }

        try {
            byte[] cborBinary = CBOR_MAPPER.writeValueAsBytes(bundle);
            byte[] compressedBinary = CompressionUtil.compress(cborBinary);

            // Using backup.<uuid> as the key to avoid any collisions
            backupBucket.put("backup." + bundle.uuid(), compressedBinary);
            NATSPlayerDataBridge.LOGGER.info("Cluster: Manual backup snapshot created for {}", bundle.uuid());
            return true;
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Cluster: Backup failed for {}: {}", bundle.uuid(), e.getMessage());
            return false;
        }
    }

    /**
     * Lists the available historical backups for a specific player.
     */
    public java.util.List<io.nats.client.api.KeyValueEntry> getBackupHistory(UUID uuid) {
        if (!init()) return java.util.Collections.emptyList();
        try {
            return backupBucket.history("backup." + uuid);
        } catch (Exception e) {
            return java.util.Collections.emptyList();
        }
    }

    /**
     * Fetches a specific historical revision entry.
     */
    public java.util.Optional<io.nats.client.api.KeyValueEntry> getBackupEntry(UUID uuid, long revision) {
        if (!init()) return java.util.Optional.empty();
        try {
            return java.util.Optional.ofNullable(backupBucket.get("backup." + uuid, revision));
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Cluster: Failed to fetch backup revision {} for {}: {}", revision, uuid, e.getMessage());
            return java.util.Optional.empty();
        }
    }

    /**
     * Fetches a specific historical revision and pushes it to the main sync bucket.
     */
    public boolean restoreRevision(UUID uuid, long revision) {
        var targetOpt = getBackupEntry(uuid, revision);
        if (targetOpt.isEmpty()) return false;
        
        try {
            byte[] compressedData = targetOpt.get().getValue();
            savage.natsplayerdata.storage.PlayerStorage.getInstance().pushRawBundle(uuid, compressedData);
            return true;
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Cluster: Failed to restore revision {} for {}: {}", revision, uuid, e.getMessage());
            return false;
        }
    }
}
