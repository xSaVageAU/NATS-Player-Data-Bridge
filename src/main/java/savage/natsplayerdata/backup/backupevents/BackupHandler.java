package savage.natsplayerdata.backup.backupevents;

import savage.natsplayerdata.model.BackupPolicy;

/**
 * Interface for all automatic backup event handlers.
 */
public interface BackupHandler {
    void init(BackupPolicy policy);
    void stop();
}
