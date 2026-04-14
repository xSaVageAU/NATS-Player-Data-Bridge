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
    long timestamp
) {
    public static SessionState create(UUID uuid, PlayerState state, String server) {
        return new SessionState(uuid, state, server, System.currentTimeMillis());
    }
}
