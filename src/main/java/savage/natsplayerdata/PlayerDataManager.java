package savage.natsplayerdata;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.storage.LevelResource;
import savage.natsplayerdata.config.BridgeConfig;
import savage.natsplayerdata.model.PlayerDataBundle;
import savage.natsplayerdata.storage.PlayerStorage;
import savage.natsplayerdata.util.LocalDiskIO;
import savage.natsplayerdata.util.NbtFilterUtil;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
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
    private static final java.util.Set<net.minecraft.server.network.ServerLoginPacketListenerImpl> ACTIVE_LOGIN_HANDLERS = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON_MAPPER = new com.fasterxml.jackson.databind.ObjectMapper();

    public static void markLoginHandlerActive(net.minecraft.server.network.ServerLoginPacketListenerImpl handler) {
        ACTIVE_LOGIN_HANDLERS.add(handler);
    }

    public static boolean isLoginHandlerActive(net.minecraft.server.network.ServerLoginPacketListenerImpl handler) {
        return ACTIVE_LOGIN_HANDLERS.contains(handler);
    }

    public static void clearLoginHandler(net.minecraft.server.network.ServerLoginPacketListenerImpl handler) {
        ACTIVE_LOGIN_HANDLERS.remove(handler);
    }

    /**
     * Starts an asynchronous fetch for player data from the cluster.
     * This should be called early in the login process (e.g., PreLogin).
     */
    public static java.util.concurrent.CompletableFuture<Optional<PlayerDataBundle>> requestAsyncFetch(UUID uuid, long backupRevision) {
        if (PENDING_FETCHES.containsKey(uuid)) return PENDING_FETCHES.get(uuid);
        
        NATSPlayerDataBridge.debugLog("Cluster: Starting async {} fetch for {}", (backupRevision != -1 ? "BACKUP" : "SYNC"), uuid);
        
        var future = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try {
                if (backupRevision != -1) {
                    // --- REDIRECT TO BACKUP BUCKET ---
                    var history = savage.natsplayerdata.backup.BackupManager.getInstance().getBackupHistory(uuid);
                    var targetEntry = history.stream()
                        .filter(e -> e.getRevision() == backupRevision)
                        .findFirst();

                    if (targetEntry.isPresent()) {
                        byte[] compressedData = targetEntry.get().getValue();
                        
                        // Commit this rollback to the main sync bucket immediately
                        PlayerStorage.getInstance().pushRawBundle(uuid, compressedData);
                        
                        // Deserialize and return the bundle for local use
                        return PlayerStorage.getInstance().deserializeBundle(compressedData);
                    } else {
                        NATSPlayerDataBridge.LOGGER.error("Cluster: Backup redirect failed for {} - Revision {} not found!", uuid, backupRevision);
                        // Fallback to sync bucket if backup fails
                    }
                }

                // Standard sync bucket fetch
                return PlayerStorage.getInstance().fetchBundle(uuid);
            } catch (Exception e) {
                NATSPlayerDataBridge.LOGGER.error("Cluster: Async fetch failed for {}: {}", uuid, e.getMessage());
                return Optional.<PlayerDataBundle>empty();
            }
        }, PlayerStorage.VIRTUAL_EXECUTOR);

        // Cleanup after 30 seconds if never consumed
        future.orTimeout(30, TimeUnit.SECONDS).whenComplete((res, ex) -> {
            if (ex != null) {
                if (PENDING_FETCHES.remove(uuid) != null) {
                    NATSPlayerDataBridge.debugLog("Cluster: Pending fetch removed due to timeout/error for {}", uuid);
                }
            }
        });
        
        return future;
    }

    /**
     * Packs local world files into a NATS-ready CBOR bundle.
     * @param markClean If true, marks the session as CLEAN in NATS.
     */
    public static void prepareAndPush(ServerPlayer player, MinecraftServer server, boolean markClean) {
        UUID uuid = player.getUUID();
        String playerName = player.getName().getString();
        
        // 1. Capture LIVE NBT (MUST BE MAIN THREAD)
        net.minecraft.world.level.storage.TagValueOutput output = net.minecraft.world.level.storage.TagValueOutput.createWithContext(net.minecraft.util.ProblemReporter.DISCARDING, server.registryAccess());
        player.saveWithoutId(output);
        CompoundTag nbt = output.buildResult();

        // 2. Capture Stats/Advancement saving (Triggers Vanilla disk writes)
        server.getPlayerList().getPlayerStats(player).save();
        server.getPlayerList().getPlayerAdvancements(player).save();

        // 3. Offload the heavy lifting to a Virtual Thread
        java.util.concurrent.CompletableFuture.runAsync(() -> {
            try {
                // SESSION LOCK: STRICT PUSH GUARD
                var entryOpt = PlayerStorage.getInstance().fetchSession(uuid);
                String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();
                
                if (entryOpt.isPresent()) {
                    var session = entryOpt.get().state();
                    
                    // GLOBAL FENCING: If an admin has marked this session as RESTORING, we must ABORT!
                    if (session.state() == savage.natsplayerdata.model.PlayerState.RESTORING) {
                        NATSPlayerDataBridge.LOGGER.info("Cluster: Aborting data push for {} - A global rollback is pending. Leaving lock as RESTORING.", playerName);
                        return;
                    }

                    if (session.state() != savage.natsplayerdata.model.PlayerState.DIRTY || !localServerId.equals(session.lastServer())) {
                        NATSPlayerDataBridge.LOGGER.error("CATASTROPHIC DATA SAFETY FAULT: Attempted to push data for {} but session lock is either CLEAN or owned by another server: {}", playerName, session.lastServer());
                        return;
                    }
                } else {
                    NATSPlayerDataBridge.LOGGER.error("CATASTROPHIC DATA SAFETY FAULT: Attempted to push data for {} but NO lock exists!", playerName);
                    return;
                }

                PlayerDataBundle bundle = captureBundle(uuid, playerName, nbt, server);
                if (bundle == null) return;

                // Networking (NATS Push)
                PlayerStorage.getInstance().pushBundle(bundle);
                
                if (markClean) {
                    setSessionState(uuid, savage.natsplayerdata.model.PlayerState.CLEAN);
                }
            } catch (Exception e) {
                NATSPlayerDataBridge.LOGGER.error("Async Sync Error: Failed to push bundle for {}: {}", playerName, e.getMessage());
            }
        }, PlayerStorage.VIRTUAL_EXECUTOR);
    }

    /**
     * Creates a manual backup snapshot for a player and pushes it to the backup bucket.
     */
    public static void backUp(ServerPlayer player, MinecraftServer server) {
        UUID uuid = player.getUUID();
        String playerName = player.getName().getString();

        // 1. Capture LIVE NBT (Main Thread)
        net.minecraft.world.level.storage.TagValueOutput output = net.minecraft.world.level.storage.TagValueOutput.createWithContext(net.minecraft.util.ProblemReporter.DISCARDING, server.registryAccess());
        player.saveWithoutId(output);
        CompoundTag nbt = output.buildResult();

        // 2. Capture Stats/Advancements (Main Thread)
        server.getPlayerList().getPlayerStats(player).save();
        server.getPlayerList().getPlayerAdvancements(player).save();

        // 3. Async Backup Push
        java.util.concurrent.CompletableFuture.runAsync(() -> {
            PlayerDataBundle bundle = captureBundle(uuid, playerName, nbt, server);
            if (bundle != null) {
                savage.natsplayerdata.backup.BackupManager.getInstance().createBackup(bundle);
            }
        }, PlayerStorage.VIRTUAL_EXECUTOR);
    }

    private static PlayerDataBundle captureBundle(UUID uuid, String playerName, CompoundTag nbt, MinecraftServer server) {
        BridgeConfig config = NATSPlayerDataBridge.getConfig();
        try {
            // Filtering
            NbtFilterUtil.filterNbt(nbt);
            
            // Serialize NBT to bytes
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            NbtIo.write(nbt, new java.io.DataOutputStream(bos));
            byte[] nbtBytes = bos.toByteArray();

            // Read Stats/Advancements from Disk
            Map<String, Object> statsMap = Collections.emptyMap();
            Map<String, Object> advMap = Collections.emptyMap();

            if (config == null || config.syncStats) {
                java.nio.file.Path statsPath = server.getWorldPath(LevelResource.PLAYER_STATS_DIR).resolve(uuid + ".json");
                String statsJson = LocalDiskIO.readText(statsPath);
                statsMap = JSON_MAPPER.readValue(statsJson, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
            }

            if (config == null || config.syncAdvancements) {
                java.nio.file.Path advPath = server.getWorldPath(LevelResource.PLAYER_ADVANCEMENTS_DIR).resolve(uuid + ".json");
                String advJson = LocalDiskIO.readText(advPath);
                advMap = JSON_MAPPER.readValue(advJson, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
            }

            return new PlayerDataBundle(uuid, nbtBytes, statsMap, advMap, System.currentTimeMillis());
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Bundle Creation Error for {}: {}", playerName, e.getMessage());
            return null;
        }
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
            // Data should ALWAYS be marked DIRTY by the time we pull, because QUERY_START explicitly sets it.
            // If it's not locked by US right now, something is deeply wrong.
            if (session.state() != savage.natsplayerdata.model.PlayerState.DIRTY || !localServerId.equals(session.lastServer())) {
                PENDING_FETCHES.remove(uuid); // Clear the pending task to prevent memory leaks
                throw new RuntimeException("CATASTROPHIC DATA SAFETY FAULT: Attempted to pull data for " + uuid + " without explicit local lock acquisition! Lock owned by: " + session.lastServer());
            }
        } else {
            // We should never reach here, QUERY_START guarantees a lock is set before the pull starts.
             throw new RuntimeException("CATASTROPHIC DATA SAFETY FAULT: Attempted to pull data for " + uuid + " but NO lock exists!");
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
                    default -> {}
                }
            }

            // 4. Update the combined result to current version
            int version = net.minecraft.nbt.NbtUtils.getDataVersion(finalTag);
            finalTag = net.minecraft.util.datafix.DataFixTypes.PLAYER.updateToCurrentVersion(server.getFixerUpper(), finalTag, version);

            // 5. Write combined GZIPPED NBT back to disk (Vanilla compatibility)
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            NbtIo.writeCompressed(finalTag, bos);
            LocalDiskIO.writeBinary(server.getWorldPath(LevelResource.PLAYER_DATA_DIR).resolve(uuid + ".dat"), bos.toByteArray());

            // 6. Write Stats and Advancements only if enabled AND data was actually synced
            if ((config == null || config.syncStats) && !bundle.stats().isEmpty()) {
                LocalDiskIO.writeText(server.getWorldPath(LevelResource.PLAYER_STATS_DIR).resolve(uuid + ".json"), JSON_MAPPER.writeValueAsString(bundle.stats()));
            }
            
            if ((config == null || config.syncAdvancements) && !bundle.advancements().isEmpty()) {
                LocalDiskIO.writeText(server.getWorldPath(LevelResource.PLAYER_ADVANCEMENTS_DIR).resolve(uuid + ".json"), JSON_MAPPER.writeValueAsString(bundle.advancements()));
            }
            
            NATSPlayerDataBridge.debugLog("Cluster: Applied NATS bundle merge for {}", uuid);
            
            return java.util.Optional.of(finalTag);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.error("Sync Error: Failed to unpack bundle for {}: {}", uuid, e.getMessage());
            return java.util.Optional.empty();
        }
    }

    /**
     * Updates the persistent session state for a player in the NATS cluster.
     */
    public static void setSessionState(UUID uuid, savage.natsplayerdata.model.PlayerState state) {
        String serverId = savage.natsfabric.NatsManager.getInstance().getServerName();
        var session = savage.natsplayerdata.model.SessionState.create(uuid, state, serverId);
        PlayerStorage.getInstance().pushSession(session);
    }

    /**
     * Clears a pending fetch from the cache. Should be called after the player joins or login fails.
     * @return true if a pending fetch was actually removed
     */
    public static boolean consumePendingFetch(UUID uuid) {
        if (PENDING_FETCHES.remove(uuid) != null) {
            NATSPlayerDataBridge.debugLog("Cluster: Consumed and cleared pending fetch for {}", uuid);
            return true;
        }
        return false;
    }
}
