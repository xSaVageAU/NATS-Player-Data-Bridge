package savage.natsplayerdata.commands.subs;

import com.mojang.brigadier.arguments.LongArgumentType;
import com.mojang.brigadier.builder.LiteralArgumentBuilder;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.commands.arguments.EntityArgument;
import net.minecraft.commands.arguments.GameProfileArgument;
import net.minecraft.network.chat.Component;
import net.minecraft.network.chat.MutableComponent;
import net.minecraft.server.level.ServerPlayer;
import savage.natsplayerdata.merge.DataMergeService;
import savage.natsplayerdata.session.SessionManager;
import savage.natsplayerdata.backup.BackupManager;
import savage.natsplayerdata.model.PlayerState;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles long-term backups and restoration logic with safety confirmation.
 */
public class BackupSubCommand {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Map<UUID, PendingRestore> PENDING_CONFIRMATIONS = new ConcurrentHashMap<>();

    private record PendingRestore(UUID targetUuid, String targetName, long revision, long expiry) {}

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
            
            // stage restore (requires confirmation)
            .then(Commands.literal("restore")
                .then(Commands.argument("player", GameProfileArgument.gameProfile())
                    .then(Commands.argument("revision", LongArgumentType.longArg(1))
                        .executes(ctx -> {
                            var profiles = GameProfileArgument.getGameProfiles(ctx, "player");
                            if (profiles.isEmpty()) return 0;
                            var profile = profiles.iterator().next();
                            long rev = LongArgumentType.getLong(ctx, "revision");
                            return stageRestore(ctx.getSource(), profile.id(), profile.name(), rev);
                        }))))

            // confirm restore
            .then(Commands.literal("confirm")
                .executes(ctx -> confirmRestore(ctx.getSource())));
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
            long rev = entry.getRevision();
            var time = LocalDateTime.ofInstant(entry.getCreated().toInstant(), ZoneId.systemDefault());
            
            MutableComponent text = createInteractive(String.valueOf(rev), name, source);
            text.append(Component.literal(" §8| §f" + time.format(TIME_FORMAT)));
            
            source.sendSuccess(() -> text, false);
        }
        source.sendSuccess(() -> Component.literal("§7Click a revision above to stage a restoration."), false);
        return 1;
    }

    private static MutableComponent createInteractive(String rev, String name, CommandSourceStack source) {
        String json = "{\"text\":\"§e[Rev: " + rev + "]\",\"click_event\":{\"action\":\"suggest_command\",\"value\":\"/nats backup restore " + name + " " + rev + "\"},\"hover_event\":{\"action\":\"show_text\",\"value\":\"§7Click to stage restoration for revision #" + rev + "\"}}";
        try {
            // 1.21+ Year.Drop Serialization Path
            var ops = source.registryAccess().createSerializationContext(com.mojang.serialization.JsonOps.INSTANCE);
            return (MutableComponent) net.minecraft.network.chat.ComponentSerialization.CODEC
                .parse(ops, com.google.gson.JsonParser.parseString(json))
                .getOrThrow();
        } catch (Exception e) {
            return Component.literal("§e[Rev: " + rev + "]");
        }
    }

    private static int stageRestore(CommandSourceStack source, UUID targetUuid, String targetName, long revision) {
        ServerPlayer admin = source.getPlayer();
        if (admin == null) return 0;

        PENDING_CONFIRMATIONS.put(admin.getUUID(), new PendingRestore(
            targetUuid, targetName, revision, System.currentTimeMillis() + 30000
        ));

        source.sendSuccess(() -> Component.literal("\n§c§l[!!] WARNING: ROLLBACK STAGED [!!]")
            .append("\n§cYou are about to restore §e" + targetName + " §cto revision §e#" + revision)
            .append("\n§cThis will overwrite their current data and cannot be undone.")
            .append("\n§cType §e/nats backup confirm §cor click below to execute.")
            .append("\n"), false);

        String confirmJson = "{\"text\":\"§a§l[ CLICK TO CONFIRM ROLLBACK ]\",\"click_event\":{\"action\":\"run_command\",\"value\":\"/nats backup confirm\"},\"hover_event\":{\"action\":\"show_text\",\"value\":\"§aProceed with restoration of " + targetName + "\"}}";
        
        MutableComponent confirmBtn;
        try {
            var ops = source.registryAccess().createSerializationContext(com.mojang.serialization.JsonOps.INSTANCE);
            confirmBtn = (MutableComponent) net.minecraft.network.chat.ComponentSerialization.CODEC
                .parse(ops, com.google.gson.JsonParser.parseString(confirmJson))
                .getOrThrow();
        } catch (Exception e) {
            confirmBtn = Component.literal("§a§l[ CLICK TO CONFIRM ROLLBACK ]");
        }

        MutableComponent finalConfirmBtn = confirmBtn;
        source.sendSuccess(() -> finalConfirmBtn, false);
        return 1;
    }

    private static int confirmRestore(CommandSourceStack source) {
        ServerPlayer admin = source.getPlayer();
        if (admin == null) return 0;

        PendingRestore pending = PENDING_CONFIRMATIONS.remove(admin.getUUID());
        if (pending == null || System.currentTimeMillis() > pending.expiry()) {
            source.sendFailure(Component.literal("§cNo pending restoration found or session expired."));
            return 0;
        }

        // --- EXECUTION ---
        SessionManager.setSessionState(pending.targetUuid(), PlayerState.RESTORING, pending.revision());

        // Kick the player
        var onlinePlayer = source.getServer().getPlayerList().getPlayer(pending.targetUuid());
        if (onlinePlayer != null) {
            onlinePlayer.connection.disconnect(Component.literal("§cYour data is being restored by an administrator.\n§7Please log back in to apply the backup."));
        }

        source.sendSuccess(() -> Component.literal("§a§lSUCCESS: §aRollback instruction posted for " + pending.targetName() + " (Revision §e" + pending.revision() + "§a)"), true);
        return 1;
    }
}
