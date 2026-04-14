package savage.natsplayerdata.commands;

import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.minecraft.commands.Commands;
import net.minecraft.network.chat.Component;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.PlayerDataManager;
import savage.natsplayerdata.PlayerPresenceManager;
import savage.natsplayerdata.config.BridgeConfig;
import savage.natsplayerdata.storage.PlayerStorage;

public class BridgeCommands {

    public static void register() {
        CommandRegistrationCallback.EVENT.register((dispatcher, registryAccess, environment) -> {
            dispatcher.register(Commands.literal("nats")
                .then(Commands.literal("sync")
                    .requires(src -> src.permissions().hasPermission(net.minecraft.server.permissions.Permissions.COMMANDS_ADMIN))
                    .executes(ctx -> {
                        var player = ctx.getSource().getPlayerOrException();
                        PlayerDataManager.prepareAndPush(player, ctx.getSource().getServer(), false); // Still online
                        ctx.getSource().sendSuccess(() -> Component.literal("§aCluster bundle successfully pushed for " + player.getName().getString()), true);
                        return 1;
                    }))
                .then(Commands.literal("online")
                    .requires(src -> src.permissions().hasPermission(net.minecraft.server.permissions.Permissions.COMMANDS_ADMIN))
                    .executes(ctx -> {
                        var presences = PlayerPresenceManager.getClusterOnline();
                        if (presences.isEmpty()) {
                            ctx.getSource().sendSuccess(() -> Component.literal("§cNo players currently trackable in NATS cluster."), false);
                            return 0;
                        }
                        
                        ctx.getSource().sendSuccess(() -> Component.literal("§6--- NATS Cluster Online (" + presences.size() + ") ---"), false);
                        presences.forEach((uuid, value) -> {
                            String[] parts = value.split("\\|", 2);
                            String name = parts[0];
                            String serverId = parts.length > 1 ? parts[1] : "Unknown";
                            ctx.getSource().sendSuccess(() -> Component.literal("§7- §f" + name + " §8(" + uuid + ") §7on §b" + serverId), false);
                        });
                        return 1;
                    })
                    .then(Commands.literal("reset")
                        .executes(ctx -> {
                            BridgeConfig config = NATSPlayerDataBridge.getConfig();
                            // 1. Purge the NATS bucket (The initiator does this)
                            PlayerStorage.getInstance().purgeAllPresence();
                            
                            // 2. Notify all servers to re-sync
                            var conn = savage.natsfabric.NatsManager.getInstance().getConnection();
                            if (conn != null) {
                                String resetTopic = (config != null && config.presenceBucketName != null ? config.presenceBucketName : "player-presence-v1") + ".reset";
                                conn.publish(resetTopic, new byte[0]);
                                ctx.getSource().sendSuccess(() -> Component.literal("§aPresence reset triggered! Purged bucket and notified cluster."), true);
                            } else {
                                ctx.getSource().sendFailure(Component.literal("§cNATS connection unavailable."));
                            }
                            return 1;
                        }))));
        });
    }
}
