package savage.natsplayerdata.events;

import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.fabricmc.fabric.api.networking.v1.ServerPlayConnectionEvents;
import net.minecraft.network.chat.Component;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.PlayerDataManager;
import savage.natsplayerdata.PlayerPresenceManager;
import savage.natsplayerdata.storage.PlayerStorage;

public class BridgeEvents {

    public static void register() {
        ServerLifecycleEvents.SERVER_STARTED.register(server -> {
            NATSPlayerDataBridge.debugLog("NATS Bridge: Global server instance captured.");
            
            // Initialize storage and start watcher
            PlayerStorage.getInstance();

            // Start Presence Heartbeat (Refresh every 30s for 60s TTL)
            savage.natsplayerdata.tasks.PresenceHeartbeatTask.start(server);
            
            // Subscribe to Presence Reset
            var conn = savage.natsfabric.NatsManager.getInstance().getConnection();
            if (conn != null) {
                String resetTopic = (NATSPlayerDataBridge.getConfig() != null && NATSPlayerDataBridge.getConfig().presenceBucketName != null ? NATSPlayerDataBridge.getConfig().presenceBucketName : "player-presence-v1") + ".reset";
                conn.createDispatcher(msg -> {
                    NATSPlayerDataBridge.debugLog("Cluster: Received presence reset request.");
                    PlayerPresenceManager.reSyncLocalPlayers(server);
                }).subscribe(resetTopic);
                NATSPlayerDataBridge.debugLog("NATS Bridge: Subscribed to presence reset topic.");
            }
        });

        // Register Join Event (Set Presence)
        ServerPlayConnectionEvents.JOIN.register((handler, sender, server) -> {
            NATSPlayerDataBridge.debugLog("Event: Player joined {}, setting presence...", handler.getPlayer().getName().getString());
            boolean locked = PlayerPresenceManager.join(handler.getPlayer(), false); // Initial Lock
            
            // Failsafe: If they somehow got past the canPlayerLogin check, disconnect them now.
            if (!locked) {
                NATSPlayerDataBridge.LOGGER.error("Event: CRITICAL LOCK FAILURE for {} - disconnecting.", handler.getPlayer().getName().getString());
                handler.disconnect(Component.literal("§cCluster lock acquisition failed.\n§7You may already be online on another server."));
                return;
            }

            // Force reload advancements from disk to pick up NATS-synced data.
            // Minecraft's PlayerAdvancements may have loaded the old file before fetchAndApply wrote the new one.
            try {
                server.getPlayerList().getPlayerAdvancements(handler.getPlayer()).reload(server.getAdvancements());
                NATSPlayerDataBridge.debugLog("Cluster: Reloaded advancements from disk for {}", handler.getPlayer().getName().getString());
            } catch (Exception e) {
                NATSPlayerDataBridge.LOGGER.warn("Cluster: Failed to reload advancements for {}: {}", handler.getPlayer().getName().getString(), e.getMessage());
            }
        });

        // Register Disconnect Event (Clear Presence & Save Bundle)
        ServerPlayConnectionEvents.DISCONNECT.register((handler, server) -> {
            NATSPlayerDataBridge.debugLog("Event: Player disconnected {}, clearing presence and packing data bundle...", handler.getPlayer().getName().getString());
            PlayerPresenceManager.leave(handler.getPlayer());
            server.execute(() -> PlayerDataManager.prepareAndPush(handler.getPlayer(), server));
        });

        // Periodic Cluster Checkpoints (Auto-save hook)
        ServerLifecycleEvents.AFTER_SAVE.register((server, flush, force) -> {
            var players = server.getPlayerList().getPlayers();
            if (!players.isEmpty()) {
                NATSPlayerDataBridge.debugLog("Cluster: Auto-save detected, pushing checkpoints for {} players...", players.size());
                for (var player : players) {
                    PlayerDataManager.prepareAndPush(player, server);
                }
            }
        });
    }
}
