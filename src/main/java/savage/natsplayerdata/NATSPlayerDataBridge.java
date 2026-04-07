package savage.natsplayerdata;

import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.fabricmc.fabric.api.networking.v1.ServerPlayConnectionEvents;
import net.minecraft.commands.Commands;
import net.minecraft.network.chat.Component;
import net.minecraft.server.MinecraftServer;
import savage.natsplayerdata.storage.PlayerStorage;
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

		ServerLifecycleEvents.SERVER_STARTED.register(server -> {
			SERVER = server;
			LOGGER.info("NATS Bridge: Global server instance captured.");
			
			// Initialize storage and start watcher
			PlayerStorage.getInstance();

			// Start Presence Heartbeat (Refresh every 30s for 60s TTL)
			startPresenceHeartbeat(server);
		});

		ServerLifecycleEvents.SERVER_STOPPING.register(server -> {
			SERVER = null;
		});

		// Register Join Event (Set Presence)
		ServerPlayConnectionEvents.JOIN.register((handler, sender, server) -> {
			LOGGER.info("Event: Player joined {}, setting presence...", handler.getPlayer().getName().getString());
			boolean locked = PlayerPresenceManager.join(handler.getPlayer(), false); // Initial Lock
			
			// Failsafe: If they somehow got past the canPlayerLogin check, disconnect them now.
			if (!locked) {
				LOGGER.error("Event: CRITICAL LOCK FAILURE for {} - disconnecting.", handler.getPlayer().getName().getString());
				handler.disconnect(Component.literal("§cCluster lock acquisition failed.\n§7You may already be online on another server."));
			}
		});

		// Register Disconnect Event (Clear Presence & Save Bundle)
		ServerPlayConnectionEvents.DISCONNECT.register((handler, server) -> {
			LOGGER.info("Event: Player disconnected {}, clearing presence and packing data bundle...", handler.getPlayer().getName().getString());
			PlayerPresenceManager.leave(handler.getPlayer());
			PlayerDataManager.prepareAndPush(handler.getPlayer(), server);
		});

		// Periodic Cluster Checkpoints (Auto-save hook)
		ServerLifecycleEvents.AFTER_SAVE.register((server, flush, force) -> {
			var players = server.getPlayerList().getPlayers();
			if (!players.isEmpty()) {
				LOGGER.info("Cluster: Auto-save detected, pushing checkpoints for {} players...", players.size());
				for (var player : players) {
					PlayerDataManager.prepareAndPush(player, server);
				}
			}
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
					})
					.then(Commands.literal("reset")
						.executes(ctx -> {
							// 1. Purge the NATS bucket (The initiator does this)
							PlayerStorage.getInstance().purgeAllPresence();
							
							// 2. Notify all servers to re-sync
							var conn = savage.natsfabric.NatsManager.getInstance().getConnection();
							if (conn != null) {
								conn.publish("player-presence.reset", new byte[0]);
								ctx.getSource().sendSuccess(() -> Component.literal("§aPresence reset triggered! Purged bucket and notified cluster."), true);
							} else {
								ctx.getSource().sendFailure(Component.literal("§cNATS connection unavailable."));
							}
							return 1;
						}))));
		});

		// Subscribe to Presence Reset
		ServerLifecycleEvents.SERVER_STARTED.register(server -> {
			var conn = savage.natsfabric.NatsManager.getInstance().getConnection();
			if (conn != null) {
				conn.createDispatcher(msg -> {
					LOGGER.info("Cluster: Received presence reset request.");
					PlayerPresenceManager.reSyncLocalPlayers(server);
				}).subscribe("player-presence.reset");
				LOGGER.info("NATS Bridge: Subscribed to presence reset topic.");
			}
		});

		LOGGER.info("NATS Player Data Bridge: Binary CBOR engine ready.");
	}

	private void startPresenceHeartbeat(net.minecraft.server.MinecraftServer server) {
		Thread heartbeat = new Thread(() -> {
			while (SERVER != null) {
				try {
					Thread.sleep(30000); // 30 seconds
					if (SERVER == null) break;
					
					var players = SERVER.getPlayerList().getPlayers();
					if (!players.isEmpty()) {
						LOGGER.info("Cluster: Refreshing presence for {} online players...", players.size());
						for (var player : players) {
							PlayerPresenceManager.join(player, true);
						}
					}
				} catch (InterruptedException e) {
					break;
				} catch (Exception e) {
					LOGGER.error("Cluster: Error in presence heartbeat", e);
				}
			}
		}, "NATS-Presence-Heartbeat");
		heartbeat.setDaemon(true);
		heartbeat.start();
	}
}