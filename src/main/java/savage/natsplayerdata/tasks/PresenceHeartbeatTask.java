package savage.natsplayerdata.tasks;

import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.PlayerPresenceManager;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Background task that periodically refreshes the player's presence TTL in the NATS cluster.
 */
public class PresenceHeartbeatTask implements Runnable {
    private final MinecraftServer server;

    private PresenceHeartbeatTask(MinecraftServer server) {
        this.server = server;
    }

    public static void start(MinecraftServer server) {
        Thread thread = new Thread(new PresenceHeartbeatTask(server), "NATS-Presence-Heartbeat");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void run() {
        while (NATSPlayerDataBridge.getServer() != null) {
            try {
                Thread.sleep(30000); // 30 seconds
                if (NATSPlayerDataBridge.getServer() == null) break;
                
                // Take a snapshot of the player list on the main thread to safely iterate over.
                CompletableFuture<List<ServerPlayer>> future = new CompletableFuture<>();
                server.execute(() -> {
                    future.complete(List.copyOf(server.getPlayerList().getPlayers()));
                });
                
                List<ServerPlayer> safePlayers = future.get();

                if (!safePlayers.isEmpty()) {
                    NATSPlayerDataBridge.debugLog("Cluster: Refreshing presence for {} online players...", safePlayers.size());
                    for (ServerPlayer player : safePlayers) {
                        PlayerPresenceManager.join(player, true);
                    }
                }
            } catch (InterruptedException e) {
                break;
            } catch (Exception e) {
                NATSPlayerDataBridge.LOGGER.error("Cluster: Error in presence heartbeat", e);
            }
        }
    }
}
