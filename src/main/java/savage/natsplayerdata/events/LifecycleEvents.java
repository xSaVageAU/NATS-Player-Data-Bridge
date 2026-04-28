package savage.natsplayerdata.events;

import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.merge.DataMergeService;
import savage.natsplayerdata.storage.SessionStorage;

/**
 * Handles server-wide lifecycle events like startup, shutdown, and periodic saves.
 */
public class LifecycleEvents {

    public static void register() {
        // Startup reconciliation
        ServerLifecycleEvents.SERVER_STARTED.register(server -> {
            savage.natsplayerdata.NATSPlayerDataBridge.setServer(server);
            SessionStorage.getInstance().reconcileLocalSessions();
        });

        // Shutdown data drain
        ServerLifecycleEvents.SERVER_STOPPING.register(server -> {
            NATSPlayerDataBridge.setStopping(true);
            NATSPlayerDataBridge.LOGGER.info("NATS Bridge: Server stopping, draining player data pushes...");
            for (var player : server.getPlayerList().getPlayers()) {
                DataMergeService.prepareAndPush(player, server, true); // Mark Clean
            }
            savage.natsplayerdata.NATSPlayerDataBridge.setServer(null);
        });

        // Periodic auto-save synchronisation
        ServerLifecycleEvents.AFTER_SAVE.register((server, flush, force) -> {
            if (NATSPlayerDataBridge.isStopping()) return;

            var players = server.getPlayerList().getPlayers();
            if (!players.isEmpty()) {
                NATSPlayerDataBridge.debugLog("Cluster: Auto-save detected, pushing checkpoints for {} players...", players.size());
                for (var player : players) {
                    DataMergeService.prepareAndPush(player, server, false); // Keep Dirty
                }
            }
        });
    }
}
