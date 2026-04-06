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

		// Register Join Event (Set Presence)
		ServerPlayConnectionEvents.JOIN.register((handler, sender, server) -> {
			LOGGER.info("Event: Player joined {}, setting presence...", handler.getPlayer().getName().getString());
			PlayerPresenceManager.join(handler.getPlayer());
		});

		// Register Disconnect Event (Clear Presence & Save Bundle)
		ServerPlayConnectionEvents.DISCONNECT.register((handler, server) -> {
			LOGGER.info("Event: Player disconnected {}, clearing presence and packing data bundle...", handler.getPlayer().getName().getString());
			PlayerPresenceManager.leave(handler.getPlayer());
			PlayerDataManager.prepareAndPush(handler.getPlayer(), server);
		});

		// Manual Sync & Online Command
		CommandRegistrationCallback.EVENT.register((dispatcher, registryAccess, environment) -> {
			dispatcher.register(Commands.literal("nats")
				.then(Commands.literal("sync")
					.requires(src -> src.permissions().hasPermission(net.minecraft.server.permissions.Permissions.COMMANDS_ADMIN))
					.executes(ctx -> {
						var player = ctx.getSource().getPlayerOrException();
						PlayerDataManager.prepareAndPush(player, ctx.getSource().getServer());
						ctx.getSource().sendSuccess(() -> Component.literal("§aCluster bundle successfully pushed for " + player.getName().getString()), true);
						return 1;
					}))
				.then(Commands.literal("online")
					.requires(src -> src.permissions().hasPermission(net.minecraft.server.permissions.Permissions.COMMANDS_ADMIN))
					.executes(ctx -> {
						var presences = PlayerPresenceManager.getClusterOnline();
						if (presences.isEmpty()) {
							ctx.getSource().sendSuccess(() -> Component.literal("§cNo players currently trackable in NATS cluster."), false);
							return 0;
						}
						
						ctx.getSource().sendSuccess(() -> Component.literal("§6--- NATS Cluster Online (" + presences.size() + ") ---"), false);
						presences.forEach((uuid, value) -> {
							String[] parts = value.split("\\|", 2);
							String name = parts[0];
							String serverId = parts.length > 1 ? parts[1] : "Unknown";
							ctx.getSource().sendSuccess(() -> Component.literal("§7- §f" + name + " §8(" + uuid + ") §7on §b" + serverId), false);
						});
						return 1;
					})));
		});

		LOGGER.info("NATS Player Data Bridge: Binary CBOR engine ready.");
	}
}