package savage.natsplayerdata.commands.subs;

import com.mojang.brigadier.builder.LiteralArgumentBuilder;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.commands.arguments.EntityArgument;
import net.minecraft.network.chat.Component;
import net.minecraft.server.level.ServerPlayer;
import savage.natsplayerdata.merge.DataMergeService;

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
        source.sendSuccess(() -> Component.literal("§7Initiating data sync for §e" + player.getName().getString() + "§7..."), false);
        DataMergeService.prepareAndPush(player, source.getServer(), false)
            .thenRun(() -> source.sendSuccess(() -> Component.literal("§aCluster bundle successfully pushed for §e" + player.getName().getString()), true))
            .exceptionally(ex -> {
                source.sendFailure(Component.literal("§cFailed to sync data for §e" + player.getName().getString() + "§c: " + ex.getMessage()));
                return null;
            });
        return 1;
    }
}
