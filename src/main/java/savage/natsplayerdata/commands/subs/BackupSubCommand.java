package savage.natsplayerdata.commands.subs;

import com.mojang.brigadier.arguments.LongArgumentType;
import com.mojang.brigadier.builder.LiteralArgumentBuilder;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.commands.arguments.EntityArgument;
import net.minecraft.commands.arguments.GameProfileArgument;
import net.minecraft.network.chat.Component;
import net.minecraft.server.level.ServerPlayer;
import savage.natsplayerdata.DataMergeService;
import savage.natsplayerdata.SessionManager;
import savage.natsplayerdata.backup.BackupManager;
import savage.natsplayerdata.model.PlayerState;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Handles long-term backups and restoration logic.
 */
public class BackupSubCommand {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static LiteralArgumentBuilder<CommandSourceStack> register() {
        return Commands.literal("backup")
            // push
            .then(Commands.literal("push")
                .then(Commands.argument("player", EntityArgument.player())
                    .executes(ctx -> captureBackup(ctx.getSource(), EntityArgument.getPlayer(ctx, "player"))))
                .executes(ctx -> captureBackup(ctx.getSource(), ctx.getSource().getPlayerOrException())))

            // list
            .then(Commands.literal("list")
                .then(Commands.argument("player", GameProfileArgument.gameProfile())
                    .executes(ctx -> {
                        var profiles = GameProfileArgument.getGameProfiles(ctx, "player");
                        if (profiles.isEmpty()) return 0;
                        var profile = profiles.iterator().next();
                        return listBackups(ctx.getSource(), profile.id(), profile.name());
                    })))
            
            // restore
            .then(Commands.literal("restore")
                .then(Commands.argument("player", GameProfileArgument.gameProfile())
                    .then(Commands.argument("revision", LongArgumentType.longArg(1))
                        .executes(ctx -> {
                            var profiles = GameProfileArgument.getGameProfiles(ctx, "player");
                            if (profiles.isEmpty()) return 0;
                            var profile = profiles.iterator().next();
                            long rev = LongArgumentType.getLong(ctx, "revision");
                            return postRestoreInstruction(ctx.getSource(), profile.id(), profile.name(), rev);
                        }))));
    }

    private static int captureBackup(CommandSourceStack source, ServerPlayer player) {
        DataMergeService.backUp(player, source.getServer());
        source.sendSuccess(() -> Component.literal("§aLong-term backup snapshot initiated for §e" + player.getName().getString()), true);
        return 1;
    }

    private static int listBackups(CommandSourceStack source, UUID uuid, String name) {
        var history = BackupManager.getInstance().getBackupHistory(uuid);
        
        if (history.isEmpty()) {
            source.sendSuccess(() -> Component.literal("§7No historical backups found for " + name), false);
            return 1;
        }

        source.sendSuccess(() -> Component.literal("§b--- Backup History for " + name + " ---"), false);
        for (var entry : history) {
            var time = LocalDateTime.ofInstant(entry.getCreated().toInstant(), ZoneId.systemDefault());
            source.sendSuccess(() -> Component.literal("§eRev: " + entry.getRevision() + " §8| §f" + time.format(TIME_FORMAT)), false);
        }
        source.sendSuccess(() -> Component.literal("§7Use /nats backup restore <player> <rev> to restore."), false);
        return 1;
    }

    private static int postRestoreInstruction(CommandSourceStack source, UUID uuid, String name, long revision) {
        // Atomic Instruction
        SessionManager.setSessionState(uuid, PlayerState.RESTORING, revision);

        // Disconnect if online
        var onlinePlayer = source.getServer().getPlayerList().getPlayer(uuid);
        if (onlinePlayer != null) {
            onlinePlayer.connection.disconnect(Component.literal("§cYour data is being restored by an administrator.\n§7Please log back in to apply the backup."));
        }

        source.sendSuccess(() -> Component.literal("§aRollback instruction posted for " + name + " (Revision §e" + revision + "§a)"), true);
        return 1;
    }
}
