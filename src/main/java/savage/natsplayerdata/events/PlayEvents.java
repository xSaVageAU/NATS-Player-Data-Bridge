package savage.natsplayerdata.events;

import net.fabricmc.fabric.api.networking.v1.ServerPlayConnectionEvents;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.DataMergeService;

/**
 * Handles events that occurs while a player is in the 'Play' phase (actively in the world).
 */
public class PlayEvents {

    public static void register() {
        // Logging join events (actual acquisition happens in Login phase)
        ServerPlayConnectionEvents.JOIN.register((handler, sender, server) -> {
            NATSPlayerDataBridge.debugLog("ServerPlayConnectionEvents.JOIN Triggered!");
            NATSPlayerDataBridge.debugLog("Event: Player joined {} (Lock already acquired).", handler.getPlayer().getName().getString());
        });

        // Handle player disconnects (Normal logout saving)
        ServerPlayConnectionEvents.DISCONNECT.register((handler, server) -> {
            NATSPlayerDataBridge.debugLog("ServerPlayConnectionEvents.DISCONNECT Triggered!");
            
            // If the server is stopping, the Lifecycle listener handles the final save.
            if (NATSPlayerDataBridge.isStopping()) return;

            NATSPlayerDataBridge.debugLog("Event: Player disconnected {}, saving data and marking session as CLEAN...", handler.getPlayer().getName().getString());
            server.execute(() -> DataMergeService.prepareAndPush(handler.getPlayer(), server, true)); // Mark Clean
        });
    }
}
