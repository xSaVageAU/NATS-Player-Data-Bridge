package savage.natsplayerdata;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.storage.LevelResource;
import org.apache.commons.io.FileUtils;
import savage.natsplayerdata.config.BridgeConfig;
import savage.natsplayerdata.model.PlayerDataBundle;
import savage.natsplayerdata.storage.PlayerStorage;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
        BridgeConfig config = NATSPlayerDataBridge.getConfig();
        
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

            // 2. Filter NBT based on Config
            filterNbt(nbt);
            
            // Write RAW NBT to bundle (Zstd will compress this much better than Gzip inside Gzip)
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            NbtIo.write(nbt, new java.io.DataOutputStream(bos));
            byte[] nbtBytes = bos.toByteArray();

            // 3. Read Stats/Advancements from Disk if enabled
            Map<String, Object> statsMap = Collections.emptyMap();
            Map<String, Object> advMap = Collections.emptyMap();

            if (config == null || config.syncStats) {
                String statsJson = readText(server.getWorldPath(LevelResource.PLAYER_STATS_DIR).resolve(uuid + ".json"));
                statsMap = JSON_MAPPER.readValue(statsJson, Map.class);
            }

            if (config == null || config.syncAdvancements) {
                String advJson = readText(server.getWorldPath(LevelResource.PLAYER_ADVANCEMENTS_DIR).resolve(uuid + ".json"));
                advMap = JSON_MAPPER.readValue(advJson, Map.class);
            }

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
     * Filters the CompoundTag based on the BridgeConfig.
     */
    private static void filterNbt(CompoundTag tag) {
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

    /**
     * Fetches cluster data and writes it to local disk before Minecraft loads it.
     */
    public static java.util.Optional<CompoundTag> fetchAndApply(UUID uuid, MinecraftServer server) {
        BridgeConfig config = NATSPlayerDataBridge.getConfig();
        
        // SESSION LOCK: Only pull if we are the ones joining (no lock owner yet)
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
                bundleOpt = future.get(5, TimeUnit.SECONDS);
                NATSPlayerDataBridge.debugLog("Cluster: Consumed async pre-fetch for {}", uuid);
            } catch (Exception e) {
                NATSPlayerDataBridge.LOGGER.warn("Cluster: Async pre-fetch failed or timed out for {}: {}", uuid, e.getMessage());
                bundleOpt = PlayerStorage.getInstance().fetchBundle(uuid);
            }
        } else {
            bundleOpt = PlayerStorage.getInstance().fetchBundle(uuid);
        }

        if (bundleOpt.isEmpty()) return java.util.Optional.empty();

        PlayerDataBundle bundle = bundleOpt.get();
        try {
            NATSPlayerDataBridge.debugLog("Sync: Unpacking bundle for {} [NBT: {}b, Stats: {} keys, Adv: {} keys]", 
                uuid, bundle.nbt().length, bundle.stats().size(), bundle.advancements().size());

            // 1. Read synced delta NBT from bundle
            java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(bundle.nbt());
            CompoundTag natsTag = NbtIo.read(new java.io.DataInputStream(bis), net.minecraft.nbt.NbtAccounter.unlimitedHeap());
            
            // 2. Load the current local base data from disk
            CompoundTag finalTag = readLocalNbt(uuid, server);
            
            // 3. Merge: Synced NATS keys overwrite local data
            for (String key : natsTag.keySet()) {
                finalTag.put(key, natsTag.get(key));
            }

            // 4. Update the combined result to current version
            int version = net.minecraft.nbt.NbtUtils.getDataVersion(finalTag);
            finalTag = net.minecraft.util.datafix.DataFixTypes.PLAYER.updateToCurrentVersion(server.getFixerUpper(), finalTag, version);

            // 5. Write combined GZIPPED NBT back to disk (Vanilla compatibility)
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            NbtIo.writeCompressed(finalTag, bos);
            writeBinary(server.getWorldPath(LevelResource.PLAYER_DATA_DIR).resolve(uuid + ".dat"), bos.toByteArray());

            // 6. Write Stats and Advancements only if enabled AND data was actually synced
            if ((config == null || config.syncStats) && !bundle.stats().isEmpty()) {
                writeText(server.getWorldPath(LevelResource.PLAYER_STATS_DIR).resolve(uuid + ".json"), JSON_MAPPER.writeValueAsString(bundle.stats()));
            }
            
            if ((config == null || config.syncAdvancements) && !bundle.advancements().isEmpty()) {
                writeText(server.getWorldPath(LevelResource.PLAYER_ADVANCEMENTS_DIR).resolve(uuid + ".json"), JSON_MAPPER.writeValueAsString(bundle.advancements()));
            }
            
            NATSPlayerDataBridge.debugLog("Cluster: Applied NATS bundle merge for {}", uuid);
            
            return java.util.Optional.of(finalTag);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Sync Error: Failed to unpack bundle for {}: {}", uuid, e.getMessage());
            return java.util.Optional.empty();
        }
    }

    /**
     * Reads the current local player NBT from disk if it exists.
     */
    private static CompoundTag readLocalNbt(UUID uuid, MinecraftServer server) {
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

    // --- File Utils ---

    private static String readText(Path path) throws IOException {
        File file = path.toFile();
        if (!file.exists()) return "{}";
        
        String content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);

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
