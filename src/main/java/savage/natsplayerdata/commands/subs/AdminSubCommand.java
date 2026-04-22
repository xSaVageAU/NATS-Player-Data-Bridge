package savage.natsplayerdata.commands.subs;

import com.mojang.brigadier.builder.LiteralArgumentBuilder;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.network.chat.Component;
import savage.natsfabric.NatsManager;
import savage.natsplayerdata.NATSPlayerDataBridge;

/**
 * Handles administrative status and diagnostic commands.
 */
public class AdminSubCommand {

    public static LiteralArgumentBuilder<CommandSourceStack> registerInfo() {
        return Commands.literal("info")
            .executes(ctx -> showInfo(ctx.getSource()));
    }

    public static LiteralArgumentBuilder<CommandSourceStack> registerDebug() {
        return Commands.literal("debug")
            .then(Commands.literal("on").executes(ctx -> setDebug(ctx.getSource(), true)))
            .then(Commands.literal("off").executes(ctx -> setDebug(ctx.getSource(), false)))
            .executes(ctx -> toggleDebug(ctx.getSource()));
    }

    private static int toggleDebug(CommandSourceStack source) {
        var config = NATSPlayerDataBridge.getConfig();
        if (config == null) return 0;
        
        config.debug = !config.debug;
        config.save();
        
        source.sendSuccess(() -> Component.literal("§7NATS Bridge: Debug mode " + (config.debug ? "§aENABLED" : "§cDISABLED")), true);
        return 1;
    }

    private static int setDebug(CommandSourceStack source, boolean value) {
        var config = NATSPlayerDataBridge.getConfig();
        if (config == null) return 0;
        
        config.debug = value;
        config.save();
        
        source.sendSuccess(() -> Component.literal("§7NATS Bridge: Debug mode " + (config.debug ? "§aENABLED" : "§cDISABLED")), true);
        return 1;
    }

    private static int showInfo(CommandSourceStack source) {
        var config = NATSPlayerDataBridge.getConfig();
        boolean connected = NatsManager.getInstance().isConnected();
        String serverName = NatsManager.getInstance().getServerName();

        source.sendSuccess(() -> Component.literal("§b--- NATS Player Data Bridge Status ---"), false);
        
        // Connection & Identity
        source.sendSuccess(() -> Component.literal("§7Status: " + (connected ? "§aCONNECTED" : "§cDISCONNECTED")), false);
        source.sendSuccess(() -> Component.literal("§7Server Node: §f" + serverName), false);
        
        if (config != null) {
            source.sendSuccess(() -> Component.literal("§7Data Bucket: §e" + config.dataBucketName), false);
            source.sendSuccess(() -> Component.literal("§7Backup Bucket: §e" + config.backupBucketName), false);
            source.sendSuccess(() -> Component.literal("§7Sync Stats/Adv: §f" + config.syncStats + " / " + config.syncAdvancements), false);
            source.sendSuccess(() -> Component.literal("§7Filter Mode: §f" + config.filterMode.toUpperCase()), false);
            source.sendSuccess(() -> Component.literal("§7Debug Mode: " + (config.debug ? "§aENABLED" : "§7DISABLED")), false);
        }

        return 1;
    }
}
