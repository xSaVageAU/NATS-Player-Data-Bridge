package savage.natsplayerdata.commands.subs;

import com.mojang.brigadier.builder.LiteralArgumentBuilder;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.commands.arguments.EntityArgument;
import net.minecraft.network.chat.Component;
import net.minecraft.server.level.ServerPlayer;
import savage.natsplayerdata.DataMergeService;

/**
 * Handles manual player data synchronization.
 */
public class SyncSubCommand {

    public static LiteralArgumentBuilder<CommandSourceStack> register() {
        return Commands.literal("sync")
            .then(Commands.argument("player", EntityArgument.player())
                .executes(ctx -> {
                    ServerPlayer player = EntityArgument.getPlayer(ctx, "player");
                    return execute(ctx.getSource(), player);
                }))
            .executes(ctx -> {
                ServerPlayer player = ctx.getSource().getPlayerOrException();
                return execute(ctx.getSource(), player);
            });
    }

    private static int execute(CommandSourceStack source, ServerPlayer player) {
        DataMergeService.prepareAndPush(player, source.getServer(), false);
        source.sendSuccess(() -> Component.literal("§aCluster bundle successfully pushed for " + player.getName().getString()), true);
        return 1;
    }
}
