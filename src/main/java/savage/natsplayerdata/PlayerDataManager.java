package savage.natsplayerdata;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.storage.LevelResource;
import savage.natsplayerdata.config.BridgeConfig;
import savage.natsplayerdata.model.PlayerDataBundle;
import savage.natsplayerdata.storage.PlayerStorage;
import savage.natsplayerdata.util.BundlePacker;
import savage.natsplayerdata.util.LocalDiskIO;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Handles the packing and unpacking of physical player files into CBOR bundles.
 */
public class PlayerDataManager {

    private static final com.fasterxml.jackson.databind.ObjectMapper JSON_MAPPER = new com.fasterxml.jackson.databind.ObjectMapper();

    /**
     * Starts an asynchronous fetch for player data from the cluster.
     * Packs local world files into a NATS-ready CBOR bundle.
     * 
     * @param markClean If true, marks the session as CLEAN in NATS.
     */
    public static void prepareAndPush(ServerPlayer player, MinecraftServer server, boolean markClean) {
        UUID uuid = player.getUUID();
        String playerName = player.getName().getString();

        // 1. Capture LIVE NBT (MUST BE MAIN THREAD)
        net.minecraft.world.level.storage.TagValueOutput output = net.minecraft.world.level.storage.TagValueOutput
                .createWithContext(net.minecraft.util.ProblemReporter.DISCARDING, server.registryAccess());
        player.saveWithoutId(output);
        CompoundTag nbt = output.buildResult();

        // 2. Capture Stats/Advancement saving (Triggers Vanilla disk writes)
        server.getPlayerList().getPlayerStats(player).save();
        server.getPlayerList().getPlayerAdvancements(player).save();

        Map<String, Object> stats = BundlePacker.captureStats(uuid, server);
        Map<String, Object> adv = BundlePacker.captureAdv(uuid, server);

        // 3. Offload the heavy lifting to the Sync Service
        SyncService.pushAsync(uuid, playerName, BundlePacker.captureBundle(uuid, playerName, nbt, stats, adv),
                markClean);
    }

    /**
     * Creates a manual backup snapshot for a player and pushes it to the backup
     * bucket.
     */
    public static void backUp(ServerPlayer player, MinecraftServer server) {
        UUID uuid = player.getUUID();
        String playerName = player.getName().getString();

        // 1. Capture LIVE NBT (Main Thread)
        net.minecraft.world.level.storage.TagValueOutput output = net.minecraft.world.level.storage.TagValueOutput
                .createWithContext(net.minecraft.util.ProblemReporter.DISCARDING, server.registryAccess());
        player.saveWithoutId(output);
        CompoundTag nbt = output.buildResult();

        // 2. Capture Stats/Advancements (Main Thread - Sync Disk Read to avoid races)
        server.getPlayerList().getPlayerStats(player).save();
        server.getPlayerList().getPlayerAdvancements(player).save();

        Map<String, Object> stats = BundlePacker.captureStats(uuid, server);
        Map<String, Object> adv = BundlePacker.captureAdv(uuid, server);

        // 3. Async Backup Push
        SyncService.pushBackupAsync(BundlePacker.captureBundle(uuid, playerName, nbt, stats, adv));
    }

    /**
     * Fetches cluster data and writes it to local disk before Minecraft loads it.
     */
    public static java.util.Optional<CompoundTag> fetchAndApply(UUID uuid, MinecraftServer server) {
        BridgeConfig config = NATSPlayerDataBridge.getConfig();

        // SESSION LOCK: STRICT PULL GUARD
        var entryOpt = PlayerStorage.getInstance().fetchSession(uuid);
        String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();
        if (entryOpt.isPresent()) {
            var session = entryOpt.get().state();
            // Data should ALWAYS be marked DIRTY by the time we pull, because QUERY_START
            // explicitly sets it.
            // If it's not locked by US right now, something is deeply wrong.
            if (session.state() != savage.natsplayerdata.model.PlayerState.DIRTY
                    || !localServerId.equals(session.lastServer())) {
                SyncService.consumePendingFetch(uuid); // Clear the pending task to prevent memory leaks
                throw new RuntimeException("CATASTROPHIC DATA SAFETY FAULT: Attempted to pull data for " + uuid
                        + " without explicit local lock acquisition! Lock owned by: " + session.lastServer());
            }
        } else {
            // We should never reach here, QUERY_START guarantees a lock is set before the
            // pull starts.
            throw new RuntimeException(
                    "CATASTROPHIC DATA SAFETY FAULT: Attempted to pull data for " + uuid + " but NO lock exists!");
        }

        // Check for pending async fetch
        Optional<PlayerDataBundle> bundleOpt = SyncService.consumePendingFetch(uuid);
        if (bundleOpt.isPresent()) {
            NATSPlayerDataBridge.debugLog("Cluster: Successfully consumed async pre-fetch for {}", uuid);
        } else {
            NATSPlayerDataBridge.debugLog("Cluster: No async fetch found for {}, performing blocking fallback pull...", uuid);
            bundleOpt = PlayerStorage.getInstance().fetchBundle(uuid);
        }

        if (bundleOpt.isEmpty())
            return java.util.Optional.empty();

        PlayerDataBundle bundle = bundleOpt.get();
        try {
            NATSPlayerDataBridge.debugLog("Sync: Unpacking bundle for {} [NBT: {}b, Stats: {} keys, Adv: {} keys]",
                    uuid, bundle.nbt().length, bundle.stats().size(), bundle.advancements().size());

            // 1. Read synced delta NBT from bundle
            java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(bundle.nbt());
            CompoundTag natsTag = NbtIo.read(new java.io.DataInputStream(bis),
                    net.minecraft.nbt.NbtAccounter.unlimitedHeap());

            // 2. Load the current local base data from disk
            CompoundTag finalTag = LocalDiskIO.readLocalNbt(uuid, server);

            // 3. Merge: Synced NATS keys overwrite local data
            for (String key : natsTag.keySet()) {
                finalTag.put(key, natsTag.get(key));
            }

            // 3b. Handle keys that were omitted from the NATS bundle (e.g., emptied slots).
            // Without this, the merge silently falls back to the stale local disk data for
            // missing keys, causing duplication of dropped or cleared items.
            if (config != null) {
                switch (config.filterMode.toLowerCase()) {
                    case "whitelist" -> {
                        for (String key : config.filterKeys) {
                            if (!natsTag.contains(key)) {
                                finalTag.remove(key);
                            }
                        }
                    }
                    case "blacklist" -> {
                        // Iterate over a safe copy of the keys to avoid ConcurrentModificationException
                        java.util.Set<String> localKeys = new java.util.HashSet<>(finalTag.keySet());
                        for (String localKey : localKeys) {
                            // If it's not coming from NATS, and we didn't explicitly blacklist it,
                            // must have been deleted by the source server.
                            if (!natsTag.contains(localKey) && !config.filterKeys.contains(localKey)) {
                                finalTag.remove(localKey);
                            }
                        }
                    }
                    default -> {
                    }
                }
            }

            // 4. Update the combined result to current version
            int version = net.minecraft.nbt.NbtUtils.getDataVersion(finalTag);
            finalTag = net.minecraft.util.datafix.DataFixTypes.PLAYER.updateToCurrentVersion(server.getFixerUpper(),
                    finalTag, version);

            // 5. Write combined GZIPPED NBT back to disk (Vanilla compatibility)
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            NbtIo.writeCompressed(finalTag, bos);
            LocalDiskIO.writeBinary(server.getWorldPath(LevelResource.PLAYER_DATA_DIR).resolve(uuid + ".dat"),
                    bos.toByteArray());

            // 6. Write Stats and Advancements only if enabled AND data was actually synced
            if ((config == null || config.syncStats) && !bundle.stats().isEmpty()) {
                LocalDiskIO.writeText(server.getWorldPath(LevelResource.PLAYER_STATS_DIR).resolve(uuid + ".json"),
                        JSON_MAPPER.writeValueAsString(bundle.stats()));
            }

            if ((config == null || config.syncAdvancements) && !bundle.advancements().isEmpty()) {
                LocalDiskIO.writeText(
                        server.getWorldPath(LevelResource.PLAYER_ADVANCEMENTS_DIR).resolve(uuid + ".json"),
                        JSON_MAPPER.writeValueAsString(bundle.advancements()));
            }

            NATSPlayerDataBridge.debugLog("Cluster: Applied NATS bundle merge for {}", uuid);

            return java.util.Optional.of(finalTag);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Sync Error: Failed to unpack bundle for {}: {}", uuid, e.getMessage());
            return java.util.Optional.empty();
        }
    }
}
