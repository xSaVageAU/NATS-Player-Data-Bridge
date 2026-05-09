package savage.natsplayerdata.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.fabricmc.loader.api.FabricLoader;
import savage.natsplayerdata.NATSPlayerDataBridge;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Configuration for the NATS Player Data Bridge.
 */
public class BridgeConfig {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final Path CONFIG_PATH = FabricLoader.getInstance().getConfigDir()
            .resolve("nats-player-data-bridge.json");

    /** Whether to enable verbose debug logging. */
    public boolean debug = false;

    /** 
     * Whether the backend server is behind a proxy (e.g., Velocity).
     * If true, enables RPC lock-stealing to allow seamless cross-server proxy switches.
     * If false, immediately rejects overlapping logins to explicitly prevent dual-logins.
     */
    public boolean proxyMode = false;

    /** The timeout in seconds for cross-server RPC requests (e.g., during proxy transfers). */
    public int rpcTimeoutSeconds = 15;

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
     * Default NBT keys to synchronize between servers.
     */
    private static final List<String> DEFAULT_FILTER_KEYS = List.of(
            "Inventory", "EnderItems", "SelectedItemSlot", "Health",
            "foodExhaustionLevel", "foodLevel", "foodSaturationLevel", "foodTickTimer",
            "seenCredits", "XpLevel", "XpP", "XpTotal",
            "active_effects", "AbsorptionAmount", "equipment"
    );

    /**
     * Top-level NBT keys to be filtered.
     */
    public List<String> filterKeys = new ArrayList<>(DEFAULT_FILTER_KEYS);

    /** The NATS KV bucket name for player data sync. */
    public String dataBucketName = "player-sync-v1";

    /** The NATS KV bucket name for long-term backups. */
    public String backupBucketName = "player-backups-v1";

    /** The maximum number of historical revisions to keep per player in the backup bucket. */
    public int backupHistoryCount = 20;

    /**
     * Automatic backup policies.
     */
    public List<savage.natsplayerdata.model.BackupPolicy> backupPolicies = createDefaultPolicies();

    private static List<savage.natsplayerdata.model.BackupPolicy> createDefaultPolicies() {
        List<savage.natsplayerdata.model.BackupPolicy> policies = new ArrayList<>();
        
        // Death Trigger (No options needed)
        policies.add(new savage.natsplayerdata.model.BackupPolicy(true, savage.natsplayerdata.model.BackupTrigger.DEATH, new HashMap<>()));

        // Dimension Change (Enabled for Nether/End by default)
        Map<String, Object> dimOptions = new HashMap<>();
        dimOptions.put("dimensions", java.util.List.of("minecraft:the_nether", "minecraft:the_end"));
        policies.add(new savage.natsplayerdata.model.BackupPolicy(true, savage.natsplayerdata.model.BackupTrigger.DIMENSION_CHANGE, dimOptions));

        return policies;
    }

    public BridgeConfig() {
        // Defaults are now initialized directly in the field.
    }

    /**
     * Loads the config from disk, or creates a default one if it doesn't exist.
     */
    public static BridgeConfig load() {
        BridgeConfig config;
        if (!Files.exists(CONFIG_PATH)) {
            config = new BridgeConfig();
        } else {
            try (var reader = Files.newBufferedReader(CONFIG_PATH)) {
                config = GSON.fromJson(reader, BridgeConfig.class);
                if (config == null) config = new BridgeConfig();
            } catch (IOException e) {
                NATSPlayerDataBridge.LOGGER.error("[BridgeConfig] Failed to load config", e);
                config = new BridgeConfig();
            }
        }

        // Ensure new default policies are merged in if missing
        config.validate();

        // Always save back to disk to ensure any new fields are added to the file
        config.save();
        return config;
    }

    /**
     * Ensures that all default backup policies exist in the configuration.
     * This allows new triggers added in updates to appear in existing config files.
     */
    public void validate() {
        if (backupPolicies == null) {
            backupPolicies = createDefaultPolicies();
            return;
        }

        List<savage.natsplayerdata.model.BackupPolicy> defaults = createDefaultPolicies();
        for (var defaultPolicy : defaults) {
            boolean exists = backupPolicies.stream()
                .anyMatch(p -> p.trigger() == defaultPolicy.trigger());
            
            if (!exists) {
                backupPolicies.add(defaultPolicy);
            }
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
