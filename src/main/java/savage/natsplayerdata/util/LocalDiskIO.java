package savage.natsplayerdata.util;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.level.storage.LevelResource;
import org.apache.commons.io.FileUtils;
import savage.natsplayerdata.NATSPlayerDataBridge;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.UUID;

public class LocalDiskIO {

    /**
     * Reads the current local player NBT from disk if it exists.
     */
    public static CompoundTag readLocalNbt(UUID uuid, MinecraftServer server) {
        Path path = server.getWorldPath(LevelResource.PLAYER_DATA_DIR).resolve(uuid + ".dat");
        File file = path.toFile();
        if (!file.exists()) return new CompoundTag();
        
        try (java.io.FileInputStream fis = new java.io.FileInputStream(file)) {
            return NbtIo.readCompressed(fis, net.minecraft.nbt.NbtAccounter.unlimitedHeap());
        } catch (Exception e) {
            NATSPlayerDataBridge.debugLog("Sync: No valid local NBT found for {} (New player?), starting fresh.", uuid);
            return new CompoundTag();
        }
    }

    public static String readText(Path path) throws IOException {
        File file = path.toFile();
        if (!file.exists()) return "{}";
        
        String content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);

        return content.isBlank() ? "{}" : content;
    }

    public static void writeBinary(Path path, byte[] data) throws IOException {
        if (data == null || data.length == 0) return;
        FileUtils.writeByteArrayToFile(path.toFile(), data);
    }

    public static void writeText(Path path, String data) throws IOException {
        if (data == null || data.isEmpty() || data.equals("{}")) return;
        FileUtils.writeStringToFile(path.toFile(), data, StandardCharsets.UTF_8);
    }
}
