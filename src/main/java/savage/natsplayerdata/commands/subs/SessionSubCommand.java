package savage.natsplayerdata.commands.subs;

import com.mojang.brigadier.builder.LiteralArgumentBuilder;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.network.chat.Component;
import savage.natsplayerdata.session.SessionManager;
import savage.natsplayerdata.model.PlayerState;
import savage.natsplayerdata.storage.SessionStorage;

import java.util.UUID;

/**
 * Handles cluster-wide session and lock management.
 */
public class SessionSubCommand {

    public static LiteralArgumentBuilder<CommandSourceStack> register() {
        return Commands.literal("sessions")
            // list [page]
            .then(Commands.literal("list")
                .then(Commands.argument("page", com.mojang.brigadier.arguments.IntegerArgumentType.integer(1))
                    .executes(ctx -> listSessions(ctx.getSource(), com.mojang.brigadier.arguments.IntegerArgumentType.getInteger(ctx, "page"))))
                .executes(ctx -> listSessions(ctx.getSource(), 1)))
            
            // clean <target>
            .then(Commands.literal("clean")
                .then(Commands.argument("target", com.mojang.brigadier.arguments.StringArgumentType.string())
                    .executes(ctx -> {
                        String target = com.mojang.brigadier.arguments.StringArgumentType.getString(ctx, "target");
                        return cleanSession(ctx.getSource(), target);
                    })));
    }

    private static int listSessions(CommandSourceStack source, int page) {
        var allSessions = SessionStorage.getInstance().getAllSessions();
        
        // 1. Filter to only show DIRTY sessions
        var dirtySessions = allSessions.stream()
            .filter(e -> e.state().state() == PlayerState.DIRTY)
            .toList();

        if (dirtySessions.isEmpty()) {
            source.sendSuccess(() -> Component.literal("§aNo DIRTY sessions found. Cluster is healthy!"), false);
            return 1;
        }

        int pageSize = 15;
        int total = dirtySessions.size();
        int totalPages = (int) Math.ceil((double) total / pageSize);
        
        int currentPage = Math.max(1, Math.min(page, totalPages));
        int start = (currentPage - 1) * pageSize;
        int end = Math.min(start + pageSize, total);

        source.sendSuccess(() -> Component.literal("§b--- Found " + total + " Dirty Sessions (Page " + currentPage + "/" + totalPages + ") ---"), false);
        
        for (int i = start; i < end; i++) {
            var entry = dirtySessions.get(i);
            var s = entry.state();
            String displayName = (s.lastKnownName() != null) ? s.lastKnownName() : s.uuid().toString().substring(0, 8) + "...";
            
            var text = Component.literal("§e" + displayName + " ")
                .append(Component.literal("§7on §f" + s.lastServer() + " §8(Rev: " + entry.revision() + ")"));
            
            source.sendSuccess(() -> text, false);
        }

        // Navigation Footer
        if (totalPages > 1) {
            var nav = Component.literal("\n§7Page: ");
            if (currentPage > 1) {
                nav.append(Component.literal("§e[< Prev] ").withStyle(st -> st.withClickEvent(new net.minecraft.network.chat.ClickEvent.RunCommand("/nats sessions list " + (currentPage - 1)))));
            }
            nav.append(Component.literal("§f" + currentPage + " / " + totalPages + " "));
            if (currentPage < totalPages) {
                nav.append(Component.literal("§e[Next >]").withStyle(st -> st.withClickEvent(new net.minecraft.network.chat.ClickEvent.RunCommand("/nats sessions list " + (currentPage + 1)))));
            }
            source.sendSuccess(() -> nav, false);
        }
        return 1;
    }

    private static int cleanSession(CommandSourceStack source, String target) {
        var sessions = SessionStorage.getInstance().getAllSessions();
        UUID targetUuid = null;
        String targetName = target;

        // 1. Try to parse as UUID first
        try {
            targetUuid = UUID.fromString(target);
        } catch (Exception ignored) {
            // 2. Not a UUID, search by name in active sessions
            for (var entry : sessions) {
                if (target.equalsIgnoreCase(entry.state().lastKnownName())) {
                    targetUuid = entry.state().uuid();
                    targetName = entry.state().lastKnownName();
                    break;
                }
            }
        }

        if (targetUuid == null) {
            source.sendFailure(Component.literal("§cCould not find a session for: " + target));
            return 0;
        }

        final String finalTargetName = targetName;
        SessionManager.setSessionState(targetUuid, targetName, PlayerState.CLEAN);
        source.sendSuccess(() -> Component.literal("§aSuccessfully marked session for §e" + finalTargetName + "§a as CLEAN."), true);
        return 1;
    }
}
