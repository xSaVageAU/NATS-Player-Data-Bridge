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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Handles the packing and unpacking of physical player files into CBOR bundles.
 */
public class PlayerDataManager {

    private static final Map<UUID, CompletableFuture<Optional<PlayerDataBundle>>> PENDING_FETCHES = new ConcurrentHashMap<>();
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON_MAPPER = new com.fasterxml.jackson.databind.ObjectMapper();

    /**
     * Starts an asynchronous fetch for player data from the cluster.
     * This should be called early in the login process (e.g., PreLogin).
     */
    public static void requestAsyncFetch(UUID uuid) {
        if (PENDING_FETCHES.containsKey(uuid)) return;
        
        NATSPlayerDataBridge.debugLog("Cluster: Starting async pre-fetch for {}", uuid);
        CompletableFuture<Optional<PlayerDataBundle>> future = PlayerStorage.getInstance().fetchBundleAsync(uuid);
        PENDING_FETCHES.put(uuid, future);
        
        // Cleanup after 30 seconds if never consumed
        future.orTimeout(30, TimeUnit.SECONDS).whenComplete((res, ex) -> {
            // We don't remove here because we want fetchAndApply to find it. 
            // The removal happens in fetchAndApply.
        });
    }

    /**
     * Packs local world files into a NATS-ready CBOR bundle.
     */
    @SuppressWarnings("unchecked")
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
            
            // Write RAW NBT to bundle (Zstd will compress this much better than Gzip inside Gzip)
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            NbtIo.write(nbt, new java.io.DataOutputStream(bos));
            byte[] nbtBytes = bos.toByteArray();

            // 2. Read Stats/Advancements from Disk (Temporary transition: Read JSON and convert to Maps)
            String statsJson = readText(server.getWorldPath(LevelResource.PLAYER_STATS_DIR).resolve(uuid + ".json"));
            String advJson = readText(server.getWorldPath(LevelResource.PLAYER_ADVANCEMENTS_DIR).resolve(uuid + ".json"));

            Map<String, Object> statsMap = JSON_MAPPER.readValue(statsJson, Map.class);
            Map<String, Object> advMap = JSON_MAPPER.readValue(advJson, Map.class);

            NATSPlayerDataBridge.debugLog("Sync: Packing bundle for {} [NBT: {}b, Stats: {} top-level keys, Adv: {} keys]", 
                player.getName().getString(), nbtBytes.length, statsMap.size(), advMap.size());

            PlayerDataBundle bundle = new PlayerDataBundle(
                uuid, nbtBytes, statsMap, advMap, System.currentTimeMillis()
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

        // Check for pending async fetch
        CompletableFuture<Optional<PlayerDataBundle>> future = PENDING_FETCHES.remove(uuid);
        Optional<PlayerDataBundle> bundleOpt;

        if (future != null) {
            try {
                // Wait for the async fetch to complete (max 5s timeout to prevent hanging the main thread indefinitely)
                bundleOpt = future.get(5, TimeUnit.SECONDS);
                NATSPlayerDataBridge.debugLog("Cluster: Consumed async pre-fetch for {}", uuid);
            } catch (Exception e) {
                NATSPlayerDataBridge.LOGGER.warn("Cluster: Async pre-fetch failed or timed out for {}: {}", uuid, e.getMessage());
                bundleOpt = PlayerStorage.getInstance().fetchBundle(uuid); // Fallback to sync fetch
            }
        } else {
            // Fallback to sync fetch if no async fetch was started
            bundleOpt = PlayerStorage.getInstance().fetchBundle(uuid);
        }

        if (bundleOpt.isEmpty()) return java.util.Optional.empty();

        PlayerDataBundle bundle = bundleOpt.get();
        try {
            NATSPlayerDataBridge.debugLog("Sync: Unpacking bundle for {} [NBT: {}b, Stats: {} keys, Adv: {} keys]", 
                uuid, bundle.nbt().length, bundle.stats().size(), bundle.advancements().size());

            // Read RAW NBT from bundle
            java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(bundle.nbt());
            CompoundTag tag = NbtIo.read(new java.io.DataInputStream(bis), net.minecraft.nbt.NbtAccounter.unlimitedHeap());
            
            // To be a "good citizen", we write GZIPPED NBT to disk (Vanilla compatibility)
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            NbtIo.writeCompressed(tag, bos);
            writeBinary(server.getWorldPath(LevelResource.PLAYER_DATA_DIR).resolve(uuid + ".dat"), bos.toByteArray());

            // Temporary transition: Convert Maps back to JSON for disk writing
            writeText(server.getWorldPath(LevelResource.PLAYER_STATS_DIR).resolve(uuid + ".json"), JSON_MAPPER.writeValueAsString(bundle.stats()));
            writeText(server.getWorldPath(LevelResource.PLAYER_ADVANCEMENTS_DIR).resolve(uuid + ".json"), JSON_MAPPER.writeValueAsString(bundle.advancements()));
            
            NATSPlayerDataBridge.debugLog("Cluster: Applied NATS bundle for {}", uuid);
            
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
        if (!file.exists()) return "{}";
        
        String content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        
        // If content is empty or just {}, try one more time after a tiny delay (race condition protection)
        if (content.length() <= 2) {
            try { Thread.sleep(50); } catch (InterruptedException ignored) {}
            content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        }
        
        return content;
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
