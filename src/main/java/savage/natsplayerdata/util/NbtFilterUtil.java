package savage.natsplayerdata.util;

import net.minecraft.nbt.CompoundTag;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.config.BridgeConfig;

import java.util.HashSet;
import java.util.Set;

public class NbtFilterUtil {

    /**
     * Filters the CompoundTag based on the BridgeConfig.
     */
    public static void filterNbt(CompoundTag tag) {
        BridgeConfig config = NATSPlayerDataBridge.getConfig();
        if (config == null || config.filterKeys == null || config.filterKeys.isEmpty()) return;

        boolean isWhitelist = "whitelist".equalsIgnoreCase(config.filterMode);

        if (isWhitelist) {
            // Whitelist Mode: sync ONLY keys in filterKeys
            Set<String> whitelist = new HashSet<>(config.filterKeys);
            Set<String> keys = new HashSet<>(tag.keySet());
            for (String key : keys) {
                if (!whitelist.contains(key)) {
                    tag.remove(key);
                }
            }
        } else {
            // Blacklist Mode: sync everything except keys in filterKeys
            for (String key : config.filterKeys) {
                tag.remove(key);
            }
        }
    }
}
