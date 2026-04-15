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
            NATSPlayerDataBridge.debugLog("NATS Bridge: Global server instance captured.");
            // Initialize storage
            PlayerStorage.getInstance();
        });

        // Join event just logs now, lock acquisition happens in QUERY_START
        ServerPlayConnectionEvents.JOIN.register((handler, sender, server) -> {
            NATSPlayerDataBridge.debugLog("Event: Player joined {} (Lock already acquired).", handler.getPlayer().getName().getString());
        });

        // Register Disconnect Event
        ServerPlayConnectionEvents.DISCONNECT.register((handler, server) -> {
            NATSPlayerDataBridge.debugLog("Event: Player disconnected {}, saving data and marking session as CLEAN...", handler.getPlayer().getName().getString());
            server.execute(() -> PlayerDataManager.prepareAndPush(handler.getPlayer(), server, true)); // Mark Clean
        });

        // Periodic Cluster Checkpoints (Auto-save hook)
        ServerLifecycleEvents.AFTER_SAVE.register((server, flush, force) -> {
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
            if (!PlayerStorage.getInstance().isStorageAvailable()) {
                NATSPlayerDataBridge.LOGGER.error("Cluster: Rejecting login - NATS cluster is unreachable!");
                handler.disconnect(Component.literal("§cAuthentication failed: Cluster connection unreachable. Please try again later."));
            }
        });

        // --- 2. ASYNC NATS SYNC (NO TICK DROP) ---
        ServerLoginConnectionEvents.QUERY_START.register((handler, server, sender, synchronizer) -> {
            java.util.UUID uuid = null;
            if (handler.authenticatedProfile != null) {
                uuid = handler.authenticatedProfile.id();
            }

            if (uuid != null) {
                // 1. STRICT LOCK CHECK
                java.util.Optional<savage.natsplayerdata.model.SessionState> sessionOpt = PlayerStorage.getInstance().fetchSession(uuid);
                String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();

                boolean canAcquire = true;
                if (sessionOpt.isPresent()) {
                    var session = sessionOpt.get();
                    if (session.state() == savage.natsplayerdata.model.PlayerState.DIRTY && !localServerId.equals(session.lastServer())) {
                        NATSPlayerDataBridge.LOGGER.error("Cluster: Rejecting login for {} - session is currently locked by '{}'", uuid, session.lastServer());
                        handler.disconnect(Component.literal("§cYour player data is currently locked by server: §b" + session.lastServer() + "\n§7Please wait a moment and try again."));
                        return; // ABORT! Do not pull data.
                    }
                }

                if (canAcquire) {
                    // 2. ACQUIRE LOCK (Set DIRTY)
                    NATSPlayerDataBridge.debugLog("Cluster: Lock acquired for {}. Marking DIRTY.", uuid);
                    PlayerDataManager.setSessionState(uuid, savage.natsplayerdata.model.PlayerState.DIRTY);
                }

                // 3. All clear — hold the login until NATS finishes downloading the bundle.
                java.util.concurrent.CompletableFuture<?> fetchFuture = PlayerDataManager.requestAsyncFetch(uuid);
                synchronizer.waitFor(fetchFuture);
            }
        });
    }
}
