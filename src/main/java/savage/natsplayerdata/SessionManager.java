package savage.natsplayerdata;

import savage.natsplayerdata.model.PlayerState;
import savage.natsplayerdata.model.SessionState;
import savage.natsplayerdata.storage.PlayerStorage;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import net.minecraft.server.network.ServerLoginPacketListenerImpl;

/**
 * Handles cluster-wide session orchestration and lock safety.
 * This is the authority for player logon sequences.
 */
public class SessionManager {

    private static final Set<ServerLoginPacketListenerImpl> ACTIVE_LOGIN_HANDLERS = ConcurrentHashMap.newKeySet();

    /**
     * STRICT FAIL-TO-SAFETY LOCK ACQUISITION
     * Returns true ONLY if the session is CLEAN or RESTORING.
     * Dirty joins are always rejected to prevent session desync.
     */
    public static boolean tryAcquireLock(UUID uuid) {
        var sessionOpt = PlayerStorage.getInstance().fetchSession(uuid);
        
        if (sessionOpt.isPresent()) {
            var session = sessionOpt.get().state();
            
            // FAIL-TO-SAFETY: Even if WE own the lock, if it is DIRTY, 
            // the player's session is in an inconsistent state. Reject.
            if (session.state() == PlayerState.DIRTY) {
                return false;
            }
        }
        
        // Lock is either CLEAN, RESTORING, or missing. safe to claim.
        setSessionState(uuid, PlayerState.DIRTY);
        return true;
    }

    /**
     * Updates the global session state for a player.
     */
    public static void setSessionState(UUID uuid, PlayerState state) {
        setSessionState(uuid, state, -1L);
    }

    public static void setSessionState(UUID uuid, PlayerState state, long restoreRevision) {
        String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();
        SessionState newState;
        if (state == PlayerState.RESTORING) {
            newState = SessionState.createRestore(uuid, localServerId, restoreRevision);
        } else {
            newState = SessionState.create(uuid, state, localServerId);
        }
        PlayerStorage.getInstance().pushSession(newState);
    }

    // --- Login Handler Tracking (Ensures clean cleanup during handshake failures) ---

    public static void markLoginHandlerActive(ServerLoginPacketListenerImpl handler) {
        ACTIVE_LOGIN_HANDLERS.add(handler);
    }

    public static boolean isLoginHandlerActive(ServerLoginPacketListenerImpl handler) {
        return ACTIVE_LOGIN_HANDLERS.contains(handler);
    }

    public static void clearLoginHandler(ServerLoginPacketListenerImpl handler) {
        ACTIVE_LOGIN_HANDLERS.remove(handler);
    }
}
