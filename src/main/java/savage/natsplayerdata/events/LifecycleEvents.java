package savage.natsplayerdata.events;

import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import savage.natsfabric.NatsManager;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.merge.DataMergeService;
import savage.natsplayerdata.storage.StorageLifecycleManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Handles server-wide lifecycle events like startup, shutdown, and periodic saves.
 */
public class LifecycleEvents {

    public static void register() {
        // Startup reconciliation
        ServerLifecycleEvents.SERVER_STARTED.register(server -> {
            NATSPlayerDataBridge.setServer(server);

            // Centralized Async Initialization (Watchdog)
            StorageLifecycleManager.getInstance().scheduleInitialization(server);
        });

        // Shutdown data drain
        ServerLifecycleEvents.SERVER_STOPPING.register(server -> {
            NATSPlayerDataBridge.setStopping(true);
            NATSPlayerDataBridge.LOGGER.info("NATS Bridge: Server stopping, draining player data pushes...");

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (var player : server.getPlayerList().getPlayers()) {
                futures.add(DataMergeService.prepareAndPush(player, server, true)); // Mark Clean
            }

            if (!futures.isEmpty()) {
                try {
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                            .get(30, TimeUnit.SECONDS);
                    NATSPlayerDataBridge.LOGGER.info("NATS Bridge: All player data pushed successfully.");
                } catch (TimeoutException e) {
                    NATSPlayerDataBridge.LOGGER.warn("NATS Bridge: Shutdown data push timed out after 30s. Some data may not have been pushed.");
                } catch (Exception e) {
                    NATSPlayerDataBridge.LOGGER.error("NATS Bridge: Shutdown push error: {}", e.getMessage());
                }
            }

            // Signal to NATS-Fabric that this mod is done — safe to close the connection
            NatsManager.getInstance().deregisterClient(NATSPlayerDataBridge.MOD_ID);
            NATSPlayerDataBridge.setServer(null);
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
