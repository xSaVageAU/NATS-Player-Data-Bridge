package savage.natsplayerdata;

import savage.natsfabric.NatsManager;
import net.minecraft.server.level.ServerPlayer;
import savage.natsplayerdata.storage.PlayerStorage;

import java.util.Map;
import java.util.UUID;

/**
 * High-level manager for tracking player presence across the NATS cluster.
 */
public class PlayerPresenceManager {

    /**
     * Marks a player as online on this server in the NATS cluster.
     */
    public static void join(ServerPlayer player) {
        UUID uuid = player.getUUID();
        String name = player.getName().getString();
        String serverId = NatsManager.getInstance().getServerName();
        NATSPlayerDataBridge.LOGGER.info("Presence: Player {} ({}) joined on {}", name, uuid, serverId);
        PlayerStorage.getInstance().updatePresence(uuid, name, serverId);
    }

    /**
     * Removes a player's online status from the NATS cluster.
     */
    public static void leave(ServerPlayer player) {
        UUID uuid = player.getUUID();
        NATSPlayerDataBridge.LOGGER.info("Presence: Player {} ({}) disconnected", player.getName().getString(), uuid);
        PlayerStorage.getInstance().clearPresence(uuid);
    }

    /**
     * Checks if the player is already recorded as online in the NATS cluster.
     */
    public static boolean isAlreadyOnline(UUID uuid) {
        return !PlayerStorage.getInstance().isOffline(uuid);
    }

    /**
     * Re-registers all currently online players on this server to the NATS cluster.
     * Used after a cluster-wide presence reset.
     */
    public static void reSyncLocalPlayers(net.minecraft.server.MinecraftServer server) {
        NATSPlayerDataBridge.LOGGER.info("Presence: Re-syncing local players to cluster...");
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            join(player);
        }
    }

    /**
     * Fetches the current online player list from the cluster.
     * @return A map of UUID->Name|ServerID entries.
     */
    public static Map<String, String> getClusterOnline() {
        return PlayerStorage.getInstance().getOnlinePresences();
    }
}
