package savage.natsplayerdata.model;

/**
 * Defines the types of events that can trigger an automatic backup.
 */
public enum BackupTrigger {
    DEATH,
    DIMENSION_CHANGE,
    INTERVAL,
    SHUTDOWN,
    MANUAL // Included for consistency, though handled via command
}
