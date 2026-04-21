package savage.natsplayerdata.model;

/**
 * Lifecycle states for player data.
 */
public enum PlayerState {
    /** Data in NATS matches the last server's logout; safe to pull. */
    CLEAN,
    
    /** A server has pulled this data for an active session; NATS is now "stale". */
    DIRTY,

    /** A rollback has been requested; the next server to join should pull from the backup bucket. */
    RESTORING
}
