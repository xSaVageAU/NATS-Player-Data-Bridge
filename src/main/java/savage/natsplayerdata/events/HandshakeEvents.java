package savage.natsplayerdata.events;

import net.fabricmc.fabric.api.networking.v1.ServerLoginConnectionEvents;
import net.fabricmc.fabric.api.networking.v1.ServerConfigurationConnectionEvents;
import net.minecraft.network.chat.Component;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.SyncService;
import savage.natsplayerdata.storage.DataStorage;
import savage.natsplayerdata.storage.SessionStorage;
import java.util.concurrent.CompletableFuture;

/**
 * Handles the high-stakes "Front Door" of the mod: the Login and Configuration
 * phases.
 * Responsible for NATS availability checks, session lock acquisition, and
 * lazy-restoration.
 */
public class HandshakeEvents {

    public static void register() {

        // --- 1. EARLY BLOCK (NATS OFFLINE) ---
        ServerLoginConnectionEvents.INIT.register((handler, server) -> {
            NATSPlayerDataBridge.debugLog("ServerLoginConnectionEvents.INIT Triggered!");
            if (!SessionStorage.getInstance().isAvailable()) {
                NATSPlayerDataBridge.LOGGER.error("Cluster: Rejecting login - NATS cluster is unreachable!");
                handler.disconnect(Component
                        .literal("§cAuthentication failed: Cluster connection unreachable. Please try again later."));
            }
        });

        // --- 2. ASYNC NATS SYNC (NO TICK DROP) ---
        ServerLoginConnectionEvents.QUERY_START.register((handler, server, sender, synchronizer) -> {
            NATSPlayerDataBridge.debugLog("ServerLoginConnectionEvents.QUERY_START Triggered!");
            java.util.UUID uuid = (handler.authenticatedProfile != null) ? handler.authenticatedProfile.id() : null;
            if (uuid == null)
                return;

            CompletableFuture<Void> loginFuture = CompletableFuture.runAsync(() -> {
                try {
                    // 1. Acquire Local Lock (with FAIL-TO-SAFETY)
                    boolean locked = savage.natsplayerdata.SessionManager.tryAcquireLock(uuid);
                    if (!locked) {
                        handler.disconnect(Component.literal(
                                "§cCluster Error: Your session is locked on another server.\n§7Please wait a moment for the cluster to self-heal."));
                        return;
                    }

                    // 2. Determine Fetch Source (Live vs Rollback)
                    var sessionOpt = SessionStorage.getInstance().fetchSession(uuid);
                    long backupRevision = -1L;
                    if (sessionOpt.isPresent()) {
                        long rev = sessionOpt.get().state().restoreRevision();
                        if (rev != -1) {
                            backupRevision = rev;
                            NATSPlayerDataBridge.LOGGER.info(
                                    "Cluster: Detected rollback instruction for {} - Redirecting fetch to backup rev: {}",
                                    uuid, backupRevision);
                        }
                    }

                    // 3. Initiate Async Data Fetch
                    SyncService.requestAsyncFetch(uuid, backupRevision);
                    savage.natsplayerdata.SessionManager.markLoginHandlerActive(handler);

                } catch (Exception e) {
                    NATSPlayerDataBridge.LOGGER.error("Handshake Error for {}: {}", uuid, e.getMessage());
                    handler.disconnect(Component.literal("§cCluster Error: Failed to synchronize session lock."));
                }
            }, DataStorage.VIRTUAL_EXECUTOR);

            synchronizer.waitFor(loginFuture);
        });

        // --- 3. HANDLE LOGIN ABORT ---
        ServerLoginConnectionEvents.DISCONNECT.register((handler, server) -> {
            NATSPlayerDataBridge.debugLog("ServerLoginConnectionEvents.DISCONNECT Triggered!");
            if (handler.authenticatedProfile != null) {
                java.util.UUID uuid = handler.authenticatedProfile.id();
                SyncService.consumePendingFetch(uuid);

                if (!savage.natsplayerdata.SessionManager.isLoginHandlerActive(handler))
                    return;

                savage.natsplayerdata.SessionManager.clearLoginHandler(handler);
                var entryOpt = SessionStorage.getInstance().fetchSession(uuid);
                String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();

                if (entryOpt.isPresent()) {
                    var session = entryOpt.get().state();
                    if (session.state() == savage.natsplayerdata.model.PlayerState.DIRTY
                            && localServerId.equals(session.lastServer())) {
                        NATSPlayerDataBridge.debugLog(
                                "Event: Player disconnected during login {}, releasing orphaned lock...",
                                handler.authenticatedProfile.name());
                        CompletableFuture.runAsync(
                                () -> savage.natsplayerdata.SessionManager.setSessionState(uuid,
                                        savage.natsplayerdata.model.PlayerState.CLEAN),
                                savage.natsfabric.NatsManager.getInstance().getExecutor());
                    }
                }
            }
        });

        // --- 4. HANDLE CONFIG-PHASE ABORT ---
        ServerConfigurationConnectionEvents.DISCONNECT.register((handler, server) -> {
            NATSPlayerDataBridge.debugLog("ServerConfigurationConnectionEvents.DISCONNECT Triggered!");
            if (handler.getOwner() != null) {
                java.util.UUID uuid = handler.getOwner().id();
                SyncService.consumePendingFetch(uuid);

                var entryOpt = SessionStorage.getInstance().fetchSession(uuid);
                String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();

                if (entryOpt.isPresent()) {
                    var session = entryOpt.get().state();
                    if (session.state() == savage.natsplayerdata.model.PlayerState.DIRTY
                            && localServerId.equals(session.lastServer())) {
                        NATSPlayerDataBridge.debugLog(
                                "Event: Player disconnected during config-phase {}, releasing orphaned lock...",
                                handler.getOwner().name());
                        CompletableFuture.runAsync(
                                () -> savage.natsplayerdata.SessionManager.setSessionState(uuid,
                                        savage.natsplayerdata.model.PlayerState.CLEAN),
                                savage.natsfabric.NatsManager.getInstance().getExecutor());
                    }
                }
            }
        });
    }
}
