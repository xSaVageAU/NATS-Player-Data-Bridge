package savage.natsplayerdata.session;

import savage.natsplayerdata.model.PlayerState;
import savage.natsplayerdata.model.SessionEntry;
import savage.natsplayerdata.model.SessionState;
import savage.natsplayerdata.storage.SessionStorage;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import net.minecraft.server.network.ServerLoginPacketListenerImpl;

/**
 * Handles cluster-wide session orchestration and lock safety.
 */
public class SessionManager {

    private static final Set<ServerLoginPacketListenerImpl> ACTIVE_LOGIN_HANDLERS = ConcurrentHashMap.newKeySet();

    /**
     * STRICT FAIL-TO-SAFETY LOCK ACQUISITION
     * Returns true ONLY if the session is CLEAN or RESTORING.
     */
    public static boolean tryAcquireLock(UUID uuid) {
        var sessionOpt = SessionStorage.getInstance().fetchSession(uuid);
        String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();
        
        if (sessionOpt.isPresent()) {
            SessionEntry entry = sessionOpt.get();
            SessionState session = entry.state();
            
            if (session.state() == PlayerState.DIRTY) {
                return false;
            }

            // Lock is either CLEAN or RESTORING.
            long restoreRev = session.restoreRevision();
            SessionState newState = new SessionState(uuid, PlayerState.DIRTY, localServerId, System.currentTimeMillis(), restoreRev);
            return SessionStorage.getInstance().pushSession(newState, entry.revision());
        }
        
        // No lock exists. Safe to claim blindly.
        setSessionState(uuid, PlayerState.DIRTY);
        return true;
    }

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
        SessionStorage.getInstance().pushSession(newState);
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
