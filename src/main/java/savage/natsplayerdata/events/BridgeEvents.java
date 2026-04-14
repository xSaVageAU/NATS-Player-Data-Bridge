package savage.natsplayerdata.events;

import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.fabricmc.fabric.api.networking.v1.ServerLoginConnectionEvents;
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
            java.util.UUID uuid = handler.getPlayer().getUUID();
            NATSPlayerDataBridge.debugLog("Event: Player joined {}, marking session as DIRTY...", handler.getPlayer().getName().getString());
            
            // Mark session as DIRTY baseline
            PlayerDataManager.setSessionState(uuid, savage.natsplayerdata.model.PlayerState.DIRTY);

            boolean locked = PlayerPresenceManager.join(handler.getPlayer(), false); // Keep ephemeral presence for now
            if (!locked) {
                NATSPlayerDataBridge.LOGGER.error("Event: CRITICAL LOCK FAILURE for {} - disconnecting.", handler.getPlayer().getName().getString());
                handler.disconnect(Component.literal("§cCluster lock acquisition failed."));
                return;
            }
        });

        // Register Disconnect Event (Clear Presence & Save Bundle)
        ServerPlayConnectionEvents.DISCONNECT.register((handler, server) -> {
            NATSPlayerDataBridge.debugLog("Event: Player disconnected {}, saving data and marking session as CLEAN...", handler.getPlayer().getName().getString());
            PlayerPresenceManager.leave(handler.getPlayer());
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
            if (!PlayerStorage.getInstance().isPresenceAvailable()) {
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
                // Guard: Cross-server duplicate session (NATS presence cache).
                // Same-server duplicates are handled earlier via SameServerGuardMixin → canPlayerLogin().
                if (PlayerPresenceManager.isAlreadyOnline(uuid)) {
                    handler.disconnect(Component.literal("§cYou are already online on another server in this cluster!"));
                    return;
                }

                // All clear — hold the login until NATS finishes downloading the bundle.
                java.util.concurrent.CompletableFuture<?> fetchFuture = PlayerDataManager.requestAsyncFetch(uuid);
                synchronizer.waitFor(fetchFuture);
            }
        });
    }
}
