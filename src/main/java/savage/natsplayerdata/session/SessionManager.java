package savage.natsplayerdata.session;

import savage.natsplayerdata.model.PlayerState;
import savage.natsplayerdata.model.SessionEntry;
import savage.natsplayerdata.model.SessionState;
import savage.natsplayerdata.storage.SessionStorage;
import io.nats.client.Dispatcher;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import net.minecraft.server.network.ServerLoginPacketListenerImpl;
import savage.natsplayerdata.NATSPlayerDataBridge;

/**
 * Handles cluster-wide session orchestration and lock safety.
 */
public class SessionManager {

    private static final Set<ServerLoginPacketListenerImpl> ACTIVE_LOGIN_HANDLERS = ConcurrentHashMap.newKeySet();
    public static final Set<UUID> TRANSFERRING_PLAYERS = ConcurrentHashMap.newKeySet();
    private static Dispatcher rpcDispatcher;

    /**
     * Subscribes to the NATS Core RPC subject to listen for lock release requests
     * from other servers during proxy transitions.
     */
    public static void initRpcListener(net.minecraft.server.MinecraftServer server) {
        String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();
        var conn = savage.natsfabric.NatsManager.getInstance().getConnection();
        if (conn == null) return;

        rpcDispatcher = conn.createDispatcher((msg) -> {
            try {
                String uuidStr = new String(msg.getData());
                UUID targetUuid = UUID.fromString(uuidStr);
                
                NATSPlayerDataBridge.LOGGER.info("Cluster: Received lock release request for {}", targetUuid);
                
                var player = server.getPlayerList().getPlayer(targetUuid);
                if (player != null) {
                    // Tag the player so PlayEvents.DISCONNECT knows this is a proxy transfer
                    TRANSFERRING_PLAYERS.add(targetUuid);
                    
                    server.execute(() -> {
                        // 1. Force a synchronous data capture and async push from the main thread
                        savage.natsplayerdata.merge.DataMergeService.prepareAndPush(player, server, true).thenRun(() -> {
                            // 2. Reply OK ONLY after the push finishes and the lock is CLEAN in NATS
                            conn.publish(msg.getReplyTo(), "OK".getBytes());
                        });
                        
                        // 3. Fail-safe: If Velocity aborts the switch, unlock them after 5 seconds without sleeping a thread
                        java.util.concurrent.CompletableFuture.runAsync(() -> {
                            server.execute(() -> {
                                if (TRANSFERRING_PLAYERS.remove(targetUuid)) {
                                    // Re-acquire the lock for this server since the transfer failed
                                    savage.natsplayerdata.session.SessionManager.setSessionState(targetUuid, savage.natsplayerdata.model.PlayerState.DIRTY);
                                    NATSPlayerDataBridge.LOGGER.warn("Cluster: Proxy transfer timed out for {}, reverted lock and unfroze.", player.getName().getString());
                                }
                            });
                        }, java.util.concurrent.CompletableFuture.delayedExecutor(5, java.util.concurrent.TimeUnit.SECONDS));
                    });
                } else {
                    conn.publish(msg.getReplyTo(), "NOT_FOUND".getBytes());
                }
            } catch (Exception e) {
                NATSPlayerDataBridge.LOGGER.error("RPC Error: Failed to process release request", e);
            }
        });
        
        rpcDispatcher.subscribe("session.release." + localServerId);
        NATSPlayerDataBridge.LOGGER.info("Cluster: RPC Lock Release Listener armed on 'session.release.{}'", localServerId);
    }

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
