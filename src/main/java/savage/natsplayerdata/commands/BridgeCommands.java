package savage.natsplayerdata.commands;

import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.minecraft.commands.Commands;
import net.minecraft.network.chat.Component;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.PlayerDataManager;

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
                .then(Commands.literal("force-clean")
                    .requires(src -> src.permissions().hasPermission(net.minecraft.server.permissions.Permissions.COMMANDS_ADMIN))
                    .then(Commands.argument("target", net.minecraft.commands.arguments.UuidArgument.uuid())
                        .executes(ctx -> {
                            java.util.UUID targetUuid = net.minecraft.commands.arguments.UuidArgument.getUuid(ctx, "target");
                            savage.natsplayerdata.storage.PlayerStorage.getInstance().pushSession(
                                savage.natsplayerdata.model.SessionState.create(targetUuid, savage.natsplayerdata.model.PlayerState.CLEAN, "force-cleared")
                            );
                            ctx.getSource().sendSuccess(() -> Component.literal("§aForcefully cleared cluster lock for UUID: §e" + targetUuid), true);
                            return 1;
                        })))
            );
        });
    }
}
