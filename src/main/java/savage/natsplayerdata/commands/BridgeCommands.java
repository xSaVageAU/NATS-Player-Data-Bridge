package savage.natsplayerdata.commands;

import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.minecraft.commands.Commands;
import savage.natsplayerdata.commands.subs.AdminSubCommand;
import savage.natsplayerdata.commands.subs.BackupSubCommand;
import savage.natsplayerdata.commands.subs.SessionSubCommand;
import savage.natsplayerdata.commands.subs.SyncSubCommand;

/**
 * Main command registry for the /nats root command.
 * Delegates actual logic to sub-command handlers in the .subs package.
 */
public class BridgeCommands {

    public static void register() {
        CommandRegistrationCallback.EVENT.register((dispatcher, registryAccess, environment) -> {
            dispatcher.register(Commands.literal("nats")
                .requires(src -> src.permissions().hasPermission(net.minecraft.server.permissions.Permissions.COMMANDS_ADMIN))
                
                // --- Delegated Sub-commands ---
                .then(AdminSubCommand.register())
                .then(SyncSubCommand.register())
                .then(SessionSubCommand.register())
                .then(BackupSubCommand.register())
            );
        });
    }
}
