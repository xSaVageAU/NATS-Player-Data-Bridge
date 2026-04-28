package savage.natsplayerdata.commands.subs;

import com.mojang.brigadier.builder.LiteralArgumentBuilder;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.commands.arguments.UuidArgument;
import net.minecraft.network.chat.Component;
import savage.natsplayerdata.SessionManager;
import savage.natsplayerdata.model.PlayerState;
import savage.natsplayerdata.storage.SessionStorage;

import java.util.UUID;

/**
 * Handles cluster-wide session and lock management.
 */
public class SessionSubCommand {

    public static LiteralArgumentBuilder<CommandSourceStack> register() {
        return Commands.literal("sessions")
            // list
            .then(Commands.literal("list")
                .executes(ctx -> listSessions(ctx.getSource())))
            
            // clean <uuid>
            .then(Commands.literal("clean")
                .then(Commands.argument("target", UuidArgument.uuid())
                    .executes(ctx -> {
                        UUID targetUuid = UuidArgument.getUuid(ctx, "target");
                        return cleanSession(ctx.getSource(), targetUuid);
                    })));
    }

    private static int listSessions(CommandSourceStack source) {
        var sessions = SessionStorage.getInstance().getAllSessions();
        if (sessions.isEmpty()) {
            source.sendSuccess(() -> Component.literal("§7No active sessions found in cluster."), false);
            return 1;
        }

        source.sendSuccess(() -> Component.literal("§b--- Cluster Session Ledger ---"), false);
        for (var entry : sessions) {
            var s = entry.state();
            String stateColor = s.state() == PlayerState.DIRTY ? "§c" : "§a";
            
            var text = Component.literal("§e" + s.uuid().toString().substring(0, 8) + "... ")
                .append(Component.literal(stateColor + "[" + s.state() + "] "))
                .append(Component.literal("§7on §f" + s.lastServer() + " §8(Rev: " + entry.revision() + ")"));
            
            source.sendSuccess(() -> text, false);
        }
        return 1;
    }

    private static int cleanSession(CommandSourceStack source, UUID uuid) {
        SessionManager.setSessionState(uuid, PlayerState.CLEAN);
        source.sendSuccess(() -> Component.literal("§aSuccessfully marked session for §e" + uuid + "§a as CLEAN."), true);
        return 1;
    }
}
