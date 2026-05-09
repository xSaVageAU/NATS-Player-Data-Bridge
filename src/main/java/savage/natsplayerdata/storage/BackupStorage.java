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
        // Initialization is now managed by StorageLifecycleManager
    }

    public static BackupStorage getInstance() {
        return Holder.INSTANCE;
    }

    public synchronized boolean init(io.nats.client.Connection conn, String bucketName, int historyCount) {
        if (backupBucket != null) return true;

        try {
            try {
                backupBucket = conn.keyValue(bucketName);
                return true;
            } catch (Exception e) {
                io.nats.client.KeyValueManagement kvm = conn.keyValueManagement();
                kvm.create(KeyValueConfiguration.builder()
                        .name(bucketName)
                        .maxHistoryPerKey(historyCount)
                        .build());
                backupBucket = conn.keyValue(bucketName);
                NATSPlayerDataBridge.LOGGER.info("BackupStorage: Created persistent backup bucket '{}' (History: {})", bucketName, historyCount);
                return true;
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("BackupStorage: Failed to initialize NATS bucket '{}': {}", bucketName, e.getMessage());
        }
        return false;
    }

    private void ensureReady() {
        if (backupBucket == null) {
            throw new IllegalStateException("NATS BackupStorage is NOT initialized.");
        }
    }

    /**
     * Snapshots a player bundle into the historical bucket with metadata.
     */
    public boolean storeBackup(savage.natsplayerdata.model.BackupEnvelope envelope) {
        try {
            ensureReady();
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("BackupStorage: Cannot store backup - Storage not ready!");
            return false;
        }
        try {
            byte[] cborBinary = Serialization.CBOR.writeValueAsBytes(envelope);
            byte[] compressedBinary = CompressionUtil.compress(cborBinary);

            backupBucket.put("backup." + envelope.bundle().uuid(), compressedBinary);
            NATSPlayerDataBridge.LOGGER.info("BackupStorage: Historical snapshot created for {} (Reason: {})", envelope.bundle().uuid(), envelope.metadata().reason());
            return true;
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("BackupStorage: Failed to store backup for {}: {}", envelope.bundle().uuid(), e.getMessage());
            return false;
        }
    }

    /**
     * Deserializes a backup entry with a fallback for legacy Beta 6 bundles.
     */
    public Optional<savage.natsplayerdata.model.BackupEnvelope> deserializeEnvelope(byte[] compressedData) {
        try {
            byte[] decompressed = CompressionUtil.decompress(compressedData);
            return Optional.ofNullable(Serialization.CBOR.readValue(decompressed, savage.natsplayerdata.model.BackupEnvelope.class));
        } catch (Exception e) {
            // FALLBACK: Try reading as a raw PlayerDataBundle (Legacy Beta 6 format)
            try {
                byte[] decompressed = CompressionUtil.decompress(compressedData);
                var bundle = Serialization.CBOR.readValue(decompressed, savage.natsplayerdata.model.PlayerDataBundle.class);
                if (bundle != null) {
                    // Create a synthetic envelope for legacy data
                    var meta = new savage.natsplayerdata.model.BackupMetadata("unknown", "LEGACY", "legacy", "1.0.0-beta.6", bundle.timestamp());
                    return Optional.of(new savage.natsplayerdata.model.BackupEnvelope(meta, bundle));
                }
            } catch (Exception ignored) {}
            
            NATSPlayerDataBridge.LOGGER.error("BackupStorage: Failed to deserialize envelope: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Lists available revisions for a player.
     */
    public List<KeyValueEntry> getHistory(UUID uuid) {
        try {
            ensureReady();
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("BackupStorage: Cannot get history for {} - Storage not ready!", uuid);
            return Collections.emptyList();
        }
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
        try {
            ensureReady();
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("BackupStorage: Cannot get revision {} for {} - Storage not ready!", revision, uuid);
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(backupBucket.get("backup." + uuid, revision));
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("BackupStorage: Failed to fetch revision {} for {}: {}", revision, uuid, e.getMessage());
            return Optional.empty();
        }
    }
}
