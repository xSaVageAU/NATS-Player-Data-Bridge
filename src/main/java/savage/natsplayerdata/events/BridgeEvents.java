package savage.natsplayerdata.events;

import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.fabricmc.fabric.api.networking.v1.ServerLoginConnectionEvents;
import net.fabricmc.fabric.api.networking.v1.ServerPlayConnectionEvents;
import net.minecraft.network.chat.Component;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.PlayerDataManager;
import savage.natsplayerdata.storage.PlayerStorage;

public class BridgeEvents {

    public static void register() {
        ServerLifecycleEvents.SERVER_STARTED.register(server -> {
            // Initialize storage and self-heal crashed sessions
            PlayerStorage.getInstance().reconcileLocalSessions();
        });

        // Join event just logs now, lock acquisition happens in QUERY_START
        ServerPlayConnectionEvents.JOIN.register((handler, sender, server) -> {
            NATSPlayerDataBridge.debugLog("ServerPlayConnectionEvents.JOIN Triggered!");
            NATSPlayerDataBridge.debugLog("Event: Player joined {} (Lock already acquired).", handler.getPlayer().getName().getString());
        });

        // Register Disconnect Event
        ServerPlayConnectionEvents.DISCONNECT.register((handler, server) -> {
            NATSPlayerDataBridge.debugLog("ServerPlayConnectionEvents.DISCONNECT Triggered!");
            NATSPlayerDataBridge.debugLog("Event: Player disconnected {}, saving data and marking session as CLEAN...", handler.getPlayer().getName().getString());
            server.execute(() -> PlayerDataManager.prepareAndPush(handler.getPlayer(), server, true)); // Mark Clean
        });

        // Periodic Cluster Checkpoints (Auto-save hook)
        ServerLifecycleEvents.AFTER_SAVE.register((server, flush, force) -> {
            NATSPlayerDataBridge.debugLog("ServerLifecycleEvents.AFTER_SAVE Triggered!");
            var players = server.getPlayerList().getPlayers();
            if (!players.isEmpty()) {
                NATSPlayerDataBridge.debugLog("Cluster: Auto-save detected, pushing checkpoints for {} players...", players.size());
                for (var player : players) {
                    PlayerDataManager.prepareAndPush(player, server, false); // Keep Dirty
                }
            }
        });

        // --- 1. EARLY BLOCK (NATS OFFLINE) ---
        ServerLoginConnectionEvents.INIT.register((handler, server) -> {
            NATSPlayerDataBridge.debugLog("ServerLoginConnectionEvents.INIT Triggered!");
            if (!PlayerStorage.getInstance().isStorageAvailable()) {
                NATSPlayerDataBridge.LOGGER.error("Cluster: Rejecting login - NATS cluster is unreachable!");
                handler.disconnect(Component.literal("§cAuthentication failed: Cluster connection unreachable. Please try again later."));
            }
        });
        // --- 2. ASYNC NATS SYNC (NO TICK DROP) ---
        ServerLoginConnectionEvents.QUERY_START.register((handler, server, sender, synchronizer) -> {
            NATSPlayerDataBridge.debugLog("ServerLoginConnectionEvents.QUERY_START Triggered!");
            java.util.UUID uuid = (handler.authenticatedProfile != null) ? handler.authenticatedProfile.id() : null;

            if (uuid != null) {
                // 1. LOCAL REDUNDANCY CHECK
                if (server.getPlayerList().getPlayer(uuid) != null) return;

                // 2. ASYNC LOCK & FETCH PIPELINE
                // We run this on the NATS Virtual Thread executor to avoid blocking Netty
                java.util.concurrent.CompletableFuture<Void> loginFuture = java.util.concurrent.CompletableFuture.runAsync(() -> {
                    String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();
                    
                    // a) Atomic Lock Check
                    var entryOpt = PlayerStorage.getInstance().fetchSession(uuid);
                    long expectedRevision = -1;

                    if (entryOpt.isPresent()) {
                        var entry = entryOpt.get();
                        var session = entry.state();
                        expectedRevision = entry.revision();

                        if (session.state() == savage.natsplayerdata.model.PlayerState.DIRTY) {
                            String owner = session.lastServer();
                            boolean isLocal = localServerId.equals(owner);
                            
                            NATSPlayerDataBridge.LOGGER.error("Cluster: Rejecting login for {} - session is {} DIRTY (Owner: {})", uuid, isLocal ? "LOCAL" : "REMOTE", owner);
                            
                            String message = isLocal 
                                ? "§cCluster Error: Your session was left hanging on this server.\n§7Please wait a moment for the cluster to self-heal."
                                : "§cYour player data is currently locked by server: §b" + owner + "\n§7Please wait a moment and try again.";
                            
                            server.execute(() -> handler.disconnect(Component.literal(message)));
                            return;
                        }

                        // ATOMIC REDIRECT: If the state is RESTORING, we accept the join but prepare to pull from backup
                        if (session.state() == savage.natsplayerdata.model.PlayerState.RESTORING) {
                            NATSPlayerDataBridge.LOGGER.info("Cluster: Redirection active for {} - Pulling from Backup Revision {}", uuid, session.restoreRevision());
                            // We don't return here; we fall through to the lock-acquisition below.
                        }
                    }

                    // b) Atomic Acquire Lock (CAS)
                    // We mark it as DIRTY and 'owned' by us.
                    var newState = savage.natsplayerdata.model.SessionState.create(uuid, savage.natsplayerdata.model.PlayerState.DIRTY, localServerId);
                    boolean success = PlayerStorage.getInstance().pushSession(newState, expectedRevision);

                    if (!success) {
                        NATSPlayerDataBridge.LOGGER.error("Cluster: Atomic lock acquisition failed for {} - someone else grabbed it!", uuid);
                        server.execute(() -> handler.disconnect(Component.literal("§cCluster sync error: Lock acquisition race detected.\n§7Please try logging in again.")));
                        return;
                    }

                    NATSPlayerDataBridge.debugLog("Cluster: Atomic lock acquired for {}. Marking DIRTY.", uuid);
                    PlayerDataManager.markLoginHandlerActive(handler);

                    // c) Start data pre-fetch
                    // If we were in RESTORING mode, we need to pass that info to the fetcher!
                    final long targetBackupRevision = (entryOpt.isPresent() && entryOpt.get().state().state() == savage.natsplayerdata.model.PlayerState.RESTORING)
                        ? entryOpt.get().state().restoreRevision() 
                        : -1;

                    PlayerDataManager.requestAsyncFetch(uuid, targetBackupRevision);
                }, savage.natsfabric.NatsManager.getInstance().getExecutor());

                // Tell the login process to wait for our NATS logic to finish
                synchronizer.waitFor(loginFuture);
            }
        });

        // --- 3. HANDLE LOGIN ABORT (CRASH/DISCONNECT DURING LOGIN) ---
        ServerLoginConnectionEvents.DISCONNECT.register((handler, server) -> {
            NATSPlayerDataBridge.debugLog("ServerLoginConnectionEvents.DISCONNECT Triggered!");
            if (handler.authenticatedProfile != null) {
                java.util.UUID uuid = handler.authenticatedProfile.id();
                PlayerDataManager.consumePendingFetch(uuid); // Clean up memory just in case

                // Only release the lock if THIS EXACT handler was the one that acquired it.
                // This perfectly filters out duplicates, while ensuring the original player 
                // always releases the lock if their socket closes before transitioning to Play.
                if (!PlayerDataManager.isLoginHandlerActive(handler)) {
                    return;
                }
                
                PlayerDataManager.clearLoginHandler(handler);

                var entryOpt = PlayerStorage.getInstance().fetchSession(uuid);
                String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();

                if (entryOpt.isPresent()) {
                    var session = entryOpt.get().state();
                    if (session.state() == savage.natsplayerdata.model.PlayerState.DIRTY && localServerId.equals(session.lastServer())) {
                        NATSPlayerDataBridge.debugLog("Event: Player disconnected during login {}, releasing orphaned lock...", handler.authenticatedProfile.name());
                        java.util.concurrent.CompletableFuture.runAsync(() -> 
                            PlayerDataManager.setSessionState(uuid, savage.natsplayerdata.model.PlayerState.CLEAN),
                            savage.natsfabric.NatsManager.getInstance().getExecutor()
                        );
                    }
                }
            }
        });

        // --- 4. HANDLE CONFIG-PHASE ABORT (DISCONNECT DURING LOADING SCREEN) ---
        net.fabricmc.fabric.api.networking.v1.ServerConfigurationConnectionEvents.DISCONNECT.register((handler, server) -> {
            NATSPlayerDataBridge.debugLog("ServerConfigurationConnectionEvents.DISCONNECT Triggered!");
            if (handler.getOwner() != null) {
                java.util.UUID uuid = handler.getOwner().id();
                PlayerDataManager.consumePendingFetch(uuid);

                var entryOpt = PlayerStorage.getInstance().fetchSession(uuid);
                String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();

                if (entryOpt.isPresent()) {
                    var session = entryOpt.get().state();
                    // If they are in the Config phase and we still have them marked DIRTY, we must release.
                    if (session.state() == savage.natsplayerdata.model.PlayerState.DIRTY && localServerId.equals(session.lastServer())) {
                        NATSPlayerDataBridge.debugLog("Event: Player disconnected during config-phase {}, releasing orphaned lock...", handler.getOwner().name());
                        java.util.concurrent.CompletableFuture.runAsync(() -> 
                            PlayerDataManager.setSessionState(uuid, savage.natsplayerdata.model.PlayerState.CLEAN),
                            savage.natsfabric.NatsManager.getInstance().getExecutor()
                        );
                    }
                }
            }
        });
    }
}
