package savage.natsplayerdata.util;

import net.minecraft.nbt.CompoundTag;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.config.BridgeConfig;

public class NbtFilterUtil {

    /**
     * Filters the CompoundTag based on the BridgeConfig.
     */
    public static void filterNbt(CompoundTag tag) {
        BridgeConfig config = NATSPlayerDataBridge.getConfig();
        if (config == null || config.filterKeys == null || config.filterKeys.isEmpty()) return;

        switch (config.filterMode.toLowerCase()) {
            case "whitelist" -> {
                // Whitelist Mode: sync ONLY keys in filterKeys
                var whitelist = java.util.Set.copyOf(config.filterKeys);
                var keys = new java.util.HashSet<>(tag.keySet());
                for (String key : keys) {
                    if (!whitelist.contains(key)) {
                        tag.remove(key);
                    }
                }
            }
            case "blacklist" -> {
                // Blacklist Mode: sync everything except keys in filterKeys
                for (String key : config.filterKeys) {
                    tag.remove(key);
                }
            }
            default -> NATSPlayerDataBridge.LOGGER.warn("Unknown filter mode: {}", config.filterMode);
        }
    }
}
