package savage.natsplayerdata.model;

import java.util.UUID;

/**
 * Represents the persistent session state and data-lock for a player.
 * Stored in the sync bucket under 'session:{uuid}'.
 */
public record SessionState(
    UUID uuid,
    PlayerState state,
    String lastServer,
    long timestamp,
    long restoreRevision // The revision in player-backups-v1 to pull from if state is RESTORING
) {
    public static SessionState create(UUID uuid, PlayerState state, String server) {
        return new SessionState(uuid, state, server, System.currentTimeMillis(), -1);
    }

    public static SessionState createRestore(UUID uuid, String server, long revision) {
        return new SessionState(uuid, PlayerState.RESTORING, server, System.currentTimeMillis(), revision);
    }
}
