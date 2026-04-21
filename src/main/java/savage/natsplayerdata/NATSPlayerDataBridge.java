package savage.natsplayerdata;

import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.minecraft.server.MinecraftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import savage.natsplayerdata.commands.BridgeCommands;
import savage.natsplayerdata.config.BridgeConfig;
import savage.natsplayerdata.events.BridgeEvents;
import savage.natsplayerdata.events.LifecycleEvents;
import savage.natsplayerdata.events.PlayEvents;

/**
 * Main entry point for the NATS Player Data Bridge.
 * Synchronizes binary player state (NBT, Stats, Advancements) via CBOR across clusters.
 */
public class NATSPlayerDataBridge implements ModInitializer {
	public static final String MOD_ID = "nats-player-data-bridge";
	public static final Logger LOGGER = LoggerFactory.getLogger(MOD_ID);
	private static BridgeConfig config;

	private static MinecraftServer SERVER;
	private static boolean stopping = false;

	public static MinecraftServer getServer() {
		return SERVER;
	}

	public static void setServer(MinecraftServer server) {
		SERVER = server;
	}

	public static boolean isStopping() {
		return stopping;
	}

	public static void setStopping(boolean value) {
		stopping = value;
	}

	public static BridgeConfig getConfig() {
		return config;
	}

	public static void debugLog(String message, Object... args) {
		if (config != null && config.debug) {
			LOGGER.info("[DEBUG] " + message, args);
		}
	}

	@Override
	public void onInitialize() {
		LOGGER.info("NATS Player Data Bridge initializing...");
		
		// Load config
		config = BridgeConfig.load();

		// Initialize Backup System
		savage.natsplayerdata.backup.BackupManager.getInstance();

		// Register all backend services and events
		LifecycleEvents.register();
		PlayEvents.register();
		BridgeCommands.register();
		BridgeEvents.register();

		LOGGER.info("NATS Player Data Bridge: Binary CBOR engine ready.");
	}
}
