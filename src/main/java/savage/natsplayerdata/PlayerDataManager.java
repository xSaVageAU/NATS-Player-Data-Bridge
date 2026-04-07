package savage.natsplayerdata;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.storage.LevelResource;
import org.apache.commons.io.FileUtils;
import savage.natsplayerdata.model.PlayerDataBundle;
import savage.natsplayerdata.storage.PlayerStorage;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Handles the packing and unpacking of physical player files into CBOR bundles.
 */
public class PlayerDataManager {

    private static final LevelResource ROOT = LevelResource.ROOT;

    /**
     * Packs local world files into a NATS-ready CBOR bundle.
     */
    public static void prepareAndPush(ServerPlayer player, MinecraftServer server) {
        UUID uuid = player.getUUID();
        
        // SESSION LOCK: Only push if we own the lock or no one does
        String owner = PlayerPresenceManager.getLastKnownServer(uuid);
        String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();
        if (owner != null && !owner.equals(localServerId)) {
            NATSPlayerDataBridge.LOGGER.warn("Sync: Refusing to PUSH data for {} - session locked by server '{}'", player.getName().getString(), owner);
            return;
        }

        try {
            // Force save stats and advancements to disk before reading
            server.getPlayerList().getPlayerStats(player).save();
            server.getPlayerList().getPlayerAdvancements(player).save();

            // 1. Capture LIVE NBT (Inventory, EnderChest, Attributes, XP, etc.)
            net.minecraft.world.level.storage.TagValueOutput output = net.minecraft.world.level.storage.TagValueOutput.createWithContext(net.minecraft.util.ProblemReporter.DISCARDING, server.registryAccess());
            player.saveWithoutId(output);
            CompoundTag nbt = output.buildResult();
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            NbtIo.writeCompressed(nbt, bos);
            byte[] nbtBytes = bos.toByteArray();

            // 2. Read Stats/Advancements from Disk (These are less volatile)
            String statsJson = readText(server.getWorldPath(LevelResource.PLAYER_STATS_DIR).resolve(uuid + ".json"));
            String advJson = readText(server.getWorldPath(LevelResource.PLAYER_ADVANCEMENTS_DIR).resolve(uuid + ".json"));

            PlayerDataBundle bundle = new PlayerDataBundle(
                uuid, nbtBytes, statsJson, advJson, System.currentTimeMillis()
            );

            PlayerStorage.getInstance().pushBundle(bundle);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Sync Error: Failed to pack bundle for {}: {}", player.getName().getString(), e.getMessage());
        }
    }

    /**
     * Fetches cluster data and writes it to local disk before Minecraft loads it.
     */
    public static java.util.Optional<CompoundTag> fetchAndApply(UUID uuid, MinecraftServer server) {
        // SESSION LOCK: Only pull if we are the ones joining (no lock owner yet)
        // If someone else owns the lock, pulling is dangerous as data is 'live' elsewhere.
        String owner = PlayerPresenceManager.getLastKnownServer(uuid);
        String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();
        if (owner != null && !owner.equals(localServerId)) {
            NATSPlayerDataBridge.LOGGER.error("Sync: Refusing to PULL data for {} - session locked by server '{}'", uuid, owner);
            return java.util.Optional.empty();
        }

        var bundleOpt = PlayerStorage.getInstance().fetchBundle(uuid);
        if (bundleOpt.isEmpty()) return java.util.Optional.empty();

        PlayerDataBundle bundle = bundleOpt.get();
        try {
            writeBinary(server.getWorldPath(LevelResource.PLAYER_DATA_DIR).resolve(uuid + ".dat"), bundle.nbt());
            writeText(server.getWorldPath(LevelResource.PLAYER_STATS_DIR).resolve(uuid + ".json"), bundle.stats());
            writeText(server.getWorldPath(LevelResource.PLAYER_ADVANCEMENTS_DIR).resolve(uuid + ".json"), bundle.advancements());
            
            NATSPlayerDataBridge.LOGGER.info("Cluster: Applied NATS bundle for {}", uuid);
            
            java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(bundle.nbt());
            CompoundTag tag = NbtIo.readCompressed(bis, net.minecraft.nbt.NbtAccounter.unlimitedHeap());
            int version = net.minecraft.nbt.NbtUtils.getDataVersion(tag);
            tag = net.minecraft.util.datafix.DataFixTypes.PLAYER.updateToCurrentVersion(server.getFixerUpper(), tag, version);
            return java.util.Optional.of(tag);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Sync Error: Failed to unpack bundle for {}: {}", uuid, e.getMessage());
            return java.util.Optional.empty();
        }
    }

    // --- File Utils ---

    private static String readText(Path path) throws IOException {
        File file = path.toFile();
        return file.exists() ? FileUtils.readFileToString(file, StandardCharsets.UTF_8) : "{}";
    }

    private static void writeBinary(Path path, byte[] data) throws IOException {
        if (data == null || data.length == 0) return;
        FileUtils.writeByteArrayToFile(path.toFile(), data);
    }

    private static void writeText(Path path, String data) throws IOException {
        if (data == null || data.isEmpty() || data.equals("{}")) return;
        FileUtils.writeStringToFile(path.toFile(), data, StandardCharsets.UTF_8);
    }
}
