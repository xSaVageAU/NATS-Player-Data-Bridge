package savage.natsplayerdata;

import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.fabricmc.fabric.api.networking.v1.ServerPlayConnectionEvents;
import net.minecraft.commands.Commands;
import net.minecraft.network.chat.Component;
import net.minecraft.server.MinecraftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the NATS Player Data Bridge.
 * Synchronizes binary player state (NBT, Stats, Advancements) via CBOR across clusters.
 */
public class NATSPlayerDataBridge implements ModInitializer {
	public static final String MOD_ID = "nats-player-data-bridge";
	public static final Logger LOGGER = LoggerFactory.getLogger(MOD_ID);

	private static MinecraftServer SERVER;

	public static MinecraftServer getServer() {
		return SERVER;
	}

	@Override
	public void onInitialize() {
		LOGGER.info("NATS Player Data Bridge: Initializing real-time synchronization cache...");

		// Capture Server Instance
		ServerLifecycleEvents.SERVER_STARTED.register(server -> {
			SERVER = server;
			LOGGER.info("NATS Bridge: Global server instance captured.");
		});

		ServerLifecycleEvents.SERVER_STOPPING.register(server -> {
			SERVER = null;
		});

		// Register Disconnect Event (Save to Cluster)
		ServerPlayConnectionEvents.DISCONNECT.register((handler, server) -> {
			LOGGER.info("Event: Disconnect detected for {}, packing data bundle...", handler.getPlayer().getName().getString());
			PlayerDataManager.prepareAndPush(handler.getPlayer(), server);
		});

		// Manual Sync Command (For testing)
		CommandRegistrationCallback.EVENT.register((dispatcher, registryAccess, environment) -> {
			dispatcher.register(Commands.literal("nats")
				.then(Commands.literal("sync")
					.requires(src -> src.permissions().hasPermission(net.minecraft.server.permissions.Permissions.COMMANDS_ADMIN))
					.executes(ctx -> {
						var player = ctx.getSource().getPlayerOrException();
						PlayerDataManager.prepareAndPush(player, ctx.getSource().getServer());
						ctx.getSource().sendSuccess(() -> Component.literal("§aCluster bundle successfully pushed for " + player.getName().getString()), true);
						return 1;
					})));
		});

		LOGGER.info("NATS Player Data Bridge: Binary CBOR engine ready.");
	}
}