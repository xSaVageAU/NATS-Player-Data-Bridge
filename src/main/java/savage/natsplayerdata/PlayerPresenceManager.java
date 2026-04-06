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
     * Fetches the current online player list from the cluster.
     * @return A map of Patient->ServerID entries.
     */
    public static Map<String, String> getClusterOnline() {
        return PlayerStorage.getInstance().getOnlinePresences();
    }
}
