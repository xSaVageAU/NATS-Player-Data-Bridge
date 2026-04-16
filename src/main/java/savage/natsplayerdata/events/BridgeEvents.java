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
            java.util.UUID uuid = null;
            if (handler.authenticatedProfile != null) {
                uuid = handler.authenticatedProfile.id();
            }
            if (uuid != null) {
                // --- 1. LOCAL REDUNDANCY CHECK ---
                // If the player is already online locally, step aside. 
                // The SameServerGuardMixin will handle the rejection.
                if (server.getPlayerList().getPlayer(uuid) != null) {
                    return;
                }

                // 2. ATOMIC LOCK CHECK
                var entryOpt = PlayerStorage.getInstance().fetchSession(uuid);
                String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();

                long expectedRevision = -1; // -1 means new key (blind put if not exists)
                if (entryOpt.isPresent()) {
                    var entry = entryOpt.get();
                    var session = entry.state();
                    expectedRevision = entry.revision();

                    if (session.state() == savage.natsplayerdata.model.PlayerState.DIRTY && !localServerId.equals(session.lastServer())) {
                        NATSPlayerDataBridge.LOGGER.error("Cluster: Rejecting login for {} - session is currently locked by '{}'", uuid, session.lastServer());
                        handler.disconnect(Component.literal("§cYour player data is currently locked by server: §b" + session.lastServer() + "\n§7Please wait a moment and try again."));
                        return; // ABORT! Do not pull data.
                    }
                }

                // 2. ATOMIC ACQUIRE LOCK (CAS)
                var newState = savage.natsplayerdata.model.SessionState.create(uuid, savage.natsplayerdata.model.PlayerState.DIRTY, localServerId);
                boolean success = PlayerStorage.getInstance().pushSession(newState, expectedRevision);

                if (!success) {
                    NATSPlayerDataBridge.LOGGER.error("Cluster: Atomic lock acquisition failed for {} - someone else grabbed it!", uuid);
                    handler.disconnect(Component.literal("§cCluster sync error: Lock acquisition race detected.\n§7Please try logging in again."));
                    return;
                }

                NATSPlayerDataBridge.debugLog("Cluster: Atomic lock acquired for {}. Marking DIRTY.", uuid);
                PlayerDataManager.markLoginHandlerActive(handler);

                // 3. All clear — hold the login until NATS finishes downloading the bundle.
                java.util.concurrent.CompletableFuture<?> fetchFuture = PlayerDataManager.requestAsyncFetch(uuid);
                synchronizer.waitFor(fetchFuture);
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
                        PlayerDataManager.setSessionState(uuid, savage.natsplayerdata.model.PlayerState.CLEAN);
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
                        PlayerDataManager.setSessionState(uuid, savage.natsplayerdata.model.PlayerState.CLEAN);
                    }
                }
            }
        });
    }
}
