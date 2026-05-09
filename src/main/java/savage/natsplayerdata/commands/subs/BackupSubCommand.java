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
import savage.natsplayerdata.NATSPlayerDataBridge;
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
            try {
                long rev = entry.getRevision();
                var envelopeOpt = savage.natsplayerdata.storage.BackupStorage.getInstance().deserializeEnvelope(entry.getValue());
                
                String metaInfo = "";
                String hoverInfo = "§7Click to stage restoration for revision #" + rev;

                if (envelopeOpt.isPresent()) {
                    var meta = envelopeOpt.get().metadata();
                    metaInfo = String.format(" §8| §7%s", meta.reason());
                    hoverInfo = String.format("§eRevision #%d\n§7Reason: §f%s\n§7Dimension: §f%s\n§7Server: §f%s\n§7Version: §f%s\n\n§aClick to stage restoration", 
                        rev, meta.reason(), meta.dimension(), meta.serverId(), meta.modVersion());
                }

                var time = LocalDateTime.ofInstant(entry.getCreated().toInstant(), ZoneId.systemDefault());
                
                // .copy() ensures we have a mutable component in 26.1+
                MutableComponent text = createInteractive(String.valueOf(rev), name, hoverInfo, source).copy();
                text.append(Component.literal(" §8| §f" + time.format(TIME_FORMAT)));
                text.append(Component.literal(metaInfo));
                
                source.sendSuccess(() -> text, false);
            } catch (Exception e) {
                long rev = entry.getRevision();
                source.sendSuccess(() -> Component.literal("§c[!] Error loading revision #" + rev), false);
            }
        }
        source.sendSuccess(() -> Component.literal("\n§7Click a revision above to stage a restoration."), false);
        return 1;
    }

    private static MutableComponent createInteractive(String rev, String name, String hover, CommandSourceStack source) {
        return Component.literal("§e[Rev: " + rev + "]")
            .withStyle(style -> style
                .withClickEvent(new net.minecraft.network.chat.ClickEvent.SuggestCommand("/nats backup restore " + name + " " + rev))
                .withHoverEvent(new net.minecraft.network.chat.HoverEvent.ShowText(Component.literal(hover)))
            );
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

        MutableComponent confirmBtn = Component.literal("§a§l[ CLICK TO CONFIRM ROLLBACK ]")
            .withStyle(style -> style
                .withClickEvent(new net.minecraft.network.chat.ClickEvent.RunCommand("/nats backup confirm"))
                .withHoverEvent(new net.minecraft.network.chat.HoverEvent.ShowText(Component.literal("§aProceed with restoration of " + targetName)))
            );

        source.sendSuccess(() -> confirmBtn, false);
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
        var onlinePlayer = source.getServer().getPlayerList().getPlayer(pending.targetUuid());

        // 1. Set cluster state to RESTORING to block incoming pushes
        SessionManager.setSessionState(pending.targetUuid(), PlayerState.RESTORING, pending.revision());

        // 3. Kick the player to force a re-login/sync
        if (onlinePlayer != null) {
            onlinePlayer.connection.disconnect(Component.literal("§cYour data is being restored by an administrator.\n§7Please log back in to apply the backup."));
        }

        source.sendSuccess(() -> Component.literal("§a§lSUCCESS: §aRollback instruction posted for " + pending.targetName() + " (Revision §e" + pending.revision() + "§a)"), true);
        return 1;
    }
}
