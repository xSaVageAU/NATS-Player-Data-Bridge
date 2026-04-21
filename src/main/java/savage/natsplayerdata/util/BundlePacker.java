package savage.natsplayerdata.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.level.storage.LevelResource;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.model.PlayerDataBundle;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * Handles the extraction and assembly of player data from the Minecraft server environment
 * into a portable PlayerDataBundle.
 */
public class BundlePacker {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    /**
     * Reads player statistics from the world's stats directory.
     */
    public static Map<String, Object> captureStats(UUID uuid, MinecraftServer server) {
        try {
            Path statsPath = server.getWorldPath(LevelResource.PLAYER_STATS_DIR).resolve(uuid + ".json");
            String statsJson = LocalDiskIO.readText(statsPath);
            return JSON_MAPPER.readValue(statsJson, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    /**
     * Reads player advancements from the world's advancements directory.
     */
    public static Map<String, Object> captureAdv(UUID uuid, MinecraftServer server) {
        try {
            Path advPath = server.getWorldPath(LevelResource.PLAYER_ADVANCEMENTS_DIR).resolve(uuid + ".json");
            String advJson = LocalDiskIO.readText(advPath);
            return JSON_MAPPER.readValue(advJson, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    /**
     * Assembles a complete PlayerDataBundle from raw components.
     * Handles NBT filtering and binary serialization.
     */
    public static PlayerDataBundle captureBundle(UUID uuid, String playerName, CompoundTag nbt, Map<String, Object> stats, Map<String, Object> adv) {
        try {
            // Apply NBT filtering (e.g., removing coordinates if configured)
            NbtFilterUtil.filterNbt(nbt);
            
            // Serialize NBT to bytes
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            NbtIo.write(nbt, new DataOutputStream(bos));
            byte[] nbtBytes = bos.toByteArray();

            return new PlayerDataBundle(uuid, nbtBytes, stats, adv, System.currentTimeMillis());
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Bundle Creation Error for {}: {}", playerName, e.getMessage());
            return null;
        }
    }
}
