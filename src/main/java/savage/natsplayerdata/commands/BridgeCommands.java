package savage.natsplayerdata.commands;

import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.minecraft.network.chat.Component;
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

                // --- /nats backup ---
                .then(net.minecraft.commands.Commands.literal("backup")
                    // push <player>
                    .then(net.minecraft.commands.Commands.literal("push")
                        .then(net.minecraft.commands.Commands.argument("player", net.minecraft.commands.arguments.EntityArgument.player())
                            .executes(ctx -> {
                                var player = net.minecraft.commands.arguments.EntityArgument.getPlayer(ctx, "player");
                                PlayerDataManager.backUp(player, ctx.getSource().getServer());
                                ctx.getSource().sendSuccess(() -> Component.literal("§aLong-term backup snapshot initiated for §e" + player.getName().getString()), true);
                                return 1;
                            }))
                        .executes(ctx -> {
                            var player = ctx.getSource().getPlayerOrException();
                            PlayerDataManager.backUp(player, ctx.getSource().getServer());
                            ctx.getSource().sendSuccess(() -> Component.literal("§aLong-term backup snapshot initiated for your data."), true);
                            return 1;
                        }))

                    // list <player>
                    .then(net.minecraft.commands.Commands.literal("list")
                        .then(net.minecraft.commands.Commands.argument("player", net.minecraft.commands.arguments.GameProfileArgument.gameProfile())
                            .executes(ctx -> {
                                var profiles = net.minecraft.commands.arguments.GameProfileArgument.getGameProfiles(ctx, "player");
                                if (profiles.isEmpty()) return 0;
                                var profile = profiles.iterator().next();
                                var uuid = profile.id();
                                
                                var history = savage.natsplayerdata.backup.BackupManager.getInstance().getBackupHistory(uuid);
                                
                                if (history.isEmpty()) {
                                    ctx.getSource().sendSuccess(() -> Component.literal("§7No historical backups found for " + profile.name()), false);
                                    return 1;
                                }

                                ctx.getSource().sendSuccess(() -> Component.literal("§b--- Backup History for " + profile.name() + " ---"), false);
                                java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                                
                                for (var entry : history) {
                                    var time = java.time.LocalDateTime.ofInstant(entry.getCreated().toInstant(), java.time.ZoneId.systemDefault());
                                    ctx.getSource().sendSuccess(() -> Component.literal("§eRev: " + entry.getRevision() + " §8| §f" + time.format(formatter)), false);
                                }
                                ctx.getSource().sendSuccess(() -> Component.literal("§7Use /nats backup restore <player> <rev> to restore."), false);
                                return 1;
                            })))
                    
                    // restore <player> <revision>
                    .then(net.minecraft.commands.Commands.literal("restore")
                        .then(net.minecraft.commands.Commands.argument("player", net.minecraft.commands.arguments.GameProfileArgument.gameProfile())
                            .then(net.minecraft.commands.Commands.argument("revision", com.mojang.brigadier.arguments.LongArgumentType.longArg(1))
                                .executes(ctx -> {
                                    var profiles = net.minecraft.commands.arguments.GameProfileArgument.getGameProfiles(ctx, "player");
                                    if (profiles.isEmpty()) return 0;
                                    var profile = profiles.iterator().next();
                                    var uuid = profile.id();
                                    long rev = com.mojang.brigadier.arguments.LongArgumentType.getLong(ctx, "revision");
                                    
                                    // Safety: Refuse rollback if player is online.
                                    if (ctx.getSource().getServer().getPlayerList().getPlayer(uuid) != null) {
                                        ctx.getSource().sendFailure(Component.literal("§cCannot restore an online player. Please kick them first."));
                                        return 1;
                                    }

                                    boolean success = savage.natsplayerdata.backup.BackupManager.getInstance().restoreRevision(uuid, rev);
                                    if (success) {
                                        ctx.getSource().sendSuccess(() -> Component.literal("§aSuccessfully restored " + profile.name() + " to revision §e" + rev), true);
                                    } else {
                                        ctx.getSource().sendFailure(Component.literal("§cFailed to restore backup. Does the revision exist?"));
                                    }
                                    return 1;
                                }))))
                )
            );
        });
    }
}
