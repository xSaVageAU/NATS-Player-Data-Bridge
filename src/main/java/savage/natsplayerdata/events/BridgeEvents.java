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

        // Register Join Event
        ServerPlayConnectionEvents.JOIN.register((handler, sender, server) -> {
            java.util.UUID uuid = handler.getPlayer().getUUID();
            NATSPlayerDataBridge.debugLog("Event: Player joined {}, marking session as DIRTY...", handler.getPlayer().getName().getString());
            
            // Mark session as DIRTY baseline for persistent locking
            PlayerDataManager.setSessionState(uuid, savage.natsplayerdata.model.PlayerState.DIRTY);
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
                // All clear — hold the login until NATS finishes downloading the bundle.
                java.util.concurrent.CompletableFuture<?> fetchFuture = PlayerDataManager.requestAsyncFetch(uuid);
                synchronizer.waitFor(fetchFuture);
            }
        });
    }
}
