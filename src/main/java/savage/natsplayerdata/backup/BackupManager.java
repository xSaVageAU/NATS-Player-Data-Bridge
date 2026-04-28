package savage.natsplayerdata.backup;

import io.nats.client.api.KeyValueEntry;
import savage.natsplayerdata.model.PlayerDataBundle;
import savage.natsplayerdata.storage.BackupStorage;
import savage.natsplayerdata.storage.DataStorage;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * High-level orchestration for player data backups.
 * Delegates storage operations to BackupStorage and DataStorage.
 */
public class BackupManager {

    private static final class Holder {
        private static final BackupManager INSTANCE = new BackupManager();
    }

    private BackupManager() {}

    public static BackupManager getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Creates a manual backup snapshot of the player's current data.
     */
    public boolean createBackup(PlayerDataBundle bundle) {
        return BackupStorage.getInstance().storeBackup(bundle);
    }

    /**
     * Lists the available historical backups for a specific player.
     */
    public List<KeyValueEntry> getBackupHistory(UUID uuid) {
        return BackupStorage.getInstance().getHistory(uuid);
    }

    /**
     * Fetches a specific historical revision entry.
     */
    public Optional<KeyValueEntry> getBackupEntry(UUID uuid, long revision) {
        return BackupStorage.getInstance().getRevision(uuid, revision);
    }

    /**
     * Fetches a specific historical revision and pushes it to the main sync bucket.
     */
    public boolean restoreRevision(UUID uuid, long revision) {
        var targetOpt = getBackupEntry(uuid, revision);
        if (targetOpt.isEmpty()) return false;
        
        byte[] compressedData = targetOpt.get().getValue();
        DataStorage.getInstance().pushRawBundle(uuid, compressedData);
        return true;
    }
}
