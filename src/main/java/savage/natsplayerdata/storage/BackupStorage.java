package savage.natsplayerdata.storage;

import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueConfiguration;
import savage.natsfabric.NatsManager;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.model.PlayerDataBundle;
import savage.natsplayerdata.util.CompressionUtil;
import savage.natsplayerdata.util.Serialization;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Specialized storage handler for Long-term Backup Snapshots in NATS.
 */
public class BackupStorage {

    private KeyValue backupBucket;

    private static final class Holder {
        private static final BackupStorage INSTANCE = new BackupStorage();
    }

    private BackupStorage() {
        init();
    }

    public static BackupStorage getInstance() {
        return Holder.INSTANCE;
    }

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
                io.nats.client.KeyValueManagement kvm = conn.keyValueManagement();
                kvm.create(KeyValueConfiguration.builder()
                        .name(bucketName)
                        .maxHistoryPerKey(20)
                        .build());
                backupBucket = conn.keyValue(bucketName);
                NATSPlayerDataBridge.LOGGER.info("BackupStorage: Created persistent backup bucket '{}'", bucketName);
                return true;
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("BackupStorage: Initialization failed: {}", e.getMessage());
        }
        return false;
    }

    /**
     * Snapshots a player bundle into the historical bucket.
     */
    public boolean storeBackup(PlayerDataBundle bundle) {
        if (!init()) return false;
        try {
            byte[] cborBinary = Serialization.CBOR.writeValueAsBytes(bundle);
            byte[] compressedBinary = CompressionUtil.compress(cborBinary);

            backupBucket.put("backup." + bundle.uuid(), compressedBinary);
            NATSPlayerDataBridge.LOGGER.info("BackupStorage: Historical snapshot created for {}", bundle.uuid());
            return true;
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("BackupStorage: Failed to store backup for {}: {}", bundle.uuid(), e.getMessage());
            return false;
        }
    }

    /**
     * Lists available revisions for a player.
     */
    public List<KeyValueEntry> getHistory(UUID uuid) {
        if (!init()) return Collections.emptyList();
        try {
            return backupBucket.history("backup." + uuid);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    /**
     * Fetches a specific historical revision entry.
     */
    public Optional<KeyValueEntry> getRevision(UUID uuid, long revision) {
        if (!init()) return Optional.empty();
        try {
            return Optional.ofNullable(backupBucket.get("backup." + uuid, revision));
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("BackupStorage: Failed to fetch revision {} for {}: {}", revision, uuid, e.getMessage());
            return Optional.empty();
        }
    }
}
