package savage.natsplayerdata.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.fabricmc.loader.api.FabricLoader;
import savage.natsplayerdata.NATSPlayerDataBridge;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for the NATS Player Data Bridge.
 */
public class BridgeConfig {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final Path CONFIG_PATH = FabricLoader.getInstance().getConfigDir()
            .resolve("nats-player-data-bridge.json");

    /** Whether to enable verbose debug logging. */
    public boolean debug = false;

    /** Whether to synchronize player statistics. */
    public boolean syncStats = true;

    /** Whether to synchronize player advancements. */
    public boolean syncAdvancements = true;

    /**
     * Mode for NBT filtering.
     * "blacklist" = sync everything except keys in filterKeys.
     * "whitelist" = sync ONLY keys in filterKeys.
     */
    public String filterMode = "whitelist";

    /**
     * Top-level NBT keys to be filtered.
     */
    public List<String> filterKeys = new ArrayList<>();

    public BridgeConfig() {
        filterKeys.add("Inventory");
        filterKeys.add("EnderItems");
        filterKeys.add("SelectedItemSlot");
        filterKeys.add("Health");
        filterKeys.add("foodExhaustionLevel");
        filterKeys.add("foodLevel");
        filterKeys.add("foodSaturationLevel");
        filterKeys.add("foodTickTimer");
        filterKeys.add("seenCredits");
        filterKeys.add("XpLevel");
        filterKeys.add("XpP");
        filterKeys.add("XpTotal");
        filterKeys.add("active_effects");
        filterKeys.add("AbsorptionAmount");

    }

    /**
     * Loads the config from disk, or creates a default one if it doesn't exist.
     */
    public static BridgeConfig load() {
        if (!Files.exists(CONFIG_PATH)) {
            BridgeConfig defaults = new BridgeConfig();
            defaults.save();
            return defaults;
        }

        try (var reader = Files.newBufferedReader(CONFIG_PATH)) {
            BridgeConfig config = GSON.fromJson(reader, BridgeConfig.class);
            return config != null ? config : new BridgeConfig();
        } catch (IOException e) {
            NATSPlayerDataBridge.LOGGER.error("[BridgeConfig] Failed to load config", e);
            return new BridgeConfig();
        }
    }

    public void save() {
        try {
            Files.createDirectories(CONFIG_PATH.getParent());
            try (var writer = Files.newBufferedWriter(CONFIG_PATH)) {
                GSON.toJson(this, writer);
            }
        } catch (IOException e) {
            NATSPlayerDataBridge.LOGGER.error("[BridgeConfig] Failed to save config", e);
        }
    }
}
