package savage.natsplayerdata.commands;

import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.minecraft.commands.Commands;
import net.minecraft.network.chat.Component;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.PlayerDataManager;

public class BridgeCommands {

    public static void register() {
        CommandRegistrationCallback.EVENT.register((dispatcher, registryAccess, environment) -> {
            dispatcher.register(net.minecraft.commands.Commands.literal("nats")
                .requires(src -> src.permissions().hasPermission(net.minecraft.server.permissions.Permissions.COMMANDS_ADMIN))
                
                // --- /nats sync [player] ---
                .then(net.minecraft.commands.Commands.literal("sync")
                    .then(net.minecraft.commands.Commands.argument("player", net.minecraft.commands.arguments.EntityArgument.player())
                        .executes(ctx -> {
                            var player = net.minecraft.commands.arguments.EntityArgument.getPlayer(ctx, "player");
                            PlayerDataManager.prepareAndPush(player, ctx.getSource().getServer(), false);
                            ctx.getSource().sendSuccess(() -> Component.literal("§aCluster bundle successfully pushed for " + player.getName().getString()), true);
                            return 1;
                        }))
                    .executes(ctx -> {
                        var player = ctx.getSource().getPlayerOrException();
                        PlayerDataManager.prepareAndPush(player, ctx.getSource().getServer(), false);
                        ctx.getSource().sendSuccess(() -> Component.literal("§aCluster bundle successfully pushed for " + player.getName().getString()), true);
                        return 1;
                    }))

                // --- /nats sessions ... ---
                .then(net.minecraft.commands.Commands.literal("sessions")
                    
                    // list
                    .then(net.minecraft.commands.Commands.literal("list")
                        .executes(ctx -> {
                            var sessions = savage.natsplayerdata.storage.PlayerStorage.getInstance().getAllSessions();
                            if (sessions.isEmpty()) {
                                ctx.getSource().sendSuccess(() -> Component.literal("§7No active sessions found in cluster."), false);
                                return 1;
                            }

                            ctx.getSource().sendSuccess(() -> Component.literal("§b--- Cluster Session Ledger ---"), false);
                            for (var entry : sessions) {
                                var s = entry.state();
                                String stateColor = s.state() == savage.natsplayerdata.model.PlayerState.DIRTY ? "§c" : "§a";
                                
                                // Simple flat text for now to ensure compatibility
                                var text = Component.literal("§e" + s.uuid().toString().substring(0, 8) + "... ")
                                    .append(Component.literal(stateColor + "[" + s.state() + "] "))
                                    .append(Component.literal("§7on §f" + s.lastServer() + " §8(Rev: " + entry.revision() + ")"));
                                
                                ctx.getSource().sendSuccess(() -> text, false);
                            }
                            return 1;
                        }))

                    // clean <uuid>
                    .then(net.minecraft.commands.Commands.literal("clean")
                        .then(net.minecraft.commands.Commands.argument("target", net.minecraft.commands.arguments.UuidArgument.uuid())
                            .executes(ctx -> {
                                java.util.UUID targetUuid = net.minecraft.commands.arguments.UuidArgument.getUuid(ctx, "target");
                                savage.natsplayerdata.storage.PlayerStorage.getInstance().pushSession(
                                    savage.natsplayerdata.model.SessionState.create(targetUuid, savage.natsplayerdata.model.PlayerState.CLEAN, "admin-clean")
                                );
                                ctx.getSource().sendSuccess(() -> Component.literal("§aSuccessfully marked session for §e" + targetUuid + "§a as CLEAN."), true);
                                return 1;
                            })))
                )
            );
        });
    }
}
