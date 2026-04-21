package savage.natsplayerdata;

import savage.natsplayerdata.model.PlayerDataBundle;
import savage.natsplayerdata.storage.PlayerStorage;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrates the asynchronous synchronization of player data across the NATS
 * cluster.
 * Handles fetch caching, backup redirection, and safety fencing.
 */
public class SyncService {

    private static final Map<UUID, CompletableFuture<Optional<PlayerDataBundle>>> PENDING_FETCHES = new ConcurrentHashMap<>();

    /**
     * Starts an asynchronous fetch for player data from the cluster.
     * Handles redirection to historical backups if a ROLLBACK is pending.
     */
    public static CompletableFuture<Optional<PlayerDataBundle>> requestAsyncFetch(UUID uuid, long backupRevision) {
        // Only return cached future for standard logins. 
        // If this is a RESTORE/ROLLBACK, we MUST force a fresh redirection fetch.
        if (backupRevision == -1 && PENDING_FETCHES.containsKey(uuid))
            return PENDING_FETCHES.get(uuid);

        NATSPlayerDataBridge.LOGGER.info("Cluster: Initiating async {} fetch for {}",
                (backupRevision != -1 ? "BACKUP (Rev: " + backupRevision + ")" : "SYNC"), uuid);

        var future = CompletableFuture.supplyAsync(() -> {
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
                        NATSPlayerDataBridge.LOGGER.error(
                                "Cluster: Backup redirect failed for {} - Revision {} not found!", uuid,
                                backupRevision);
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

        PENDING_FETCHES.put(uuid, future);
        return future;
    }

    /**
     * Consumes and removes a pending fetch from the cache.
     */
    public static Optional<PlayerDataBundle> consumePendingFetch(UUID uuid) {
        CompletableFuture<Optional<PlayerDataBundle>> future = PENDING_FETCHES.remove(uuid);
        if (future == null)
            return Optional.empty();

        try {
            return future.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.warn("Cluster: Async pre-fetch consumption failed for {}: {}", uuid,
                    e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Handles the asynchronous push of a player bundle to NATS.
     * Implements strict cluster-wide fencing to prevent data overwrites.
     */
    public static void pushAsync(UUID uuid, String playerName, PlayerDataBundle bundle, boolean markClean) {
        CompletableFuture.runAsync(() -> {
            try {
                // SESSION LOCK: STRICT PUSH GUARD
                var entryOpt = PlayerStorage.getInstance().fetchSession(uuid);
                String localServerId = savage.natsfabric.NatsManager.getInstance().getServerName();

                if (entryOpt.isPresent()) {
                    var session = entryOpt.get().state();

                    // GLOBAL FENCING: If an admin has marked this session as RESTORING, we must
                    // ABORT!
                    if (session.state() == savage.natsplayerdata.model.PlayerState.RESTORING) {
                        NATSPlayerDataBridge.LOGGER
                                .info("Cluster: Aborting data push for {} - A global rollback is pending.", playerName);
                        return;
                    }

                    if (session.state() != savage.natsplayerdata.model.PlayerState.DIRTY
                            || !localServerId.equals(session.lastServer())) {
                        if (NATSPlayerDataBridge.isStopping())
                            return;
                        NATSPlayerDataBridge.LOGGER.error(
                                "CATASTROPHIC DATA SAFETY FAULT: Push aborted for {} - Session lock owned by: {}",
                                playerName, session.lastServer());
                        return;
                    }
                } else {
                    if (NATSPlayerDataBridge.isStopping())
                        return;
                    NATSPlayerDataBridge.LOGGER
                            .error("CATASTROPHIC DATA SAFETY FAULT: Push aborted for {} - No lock exists!", playerName);
                    return;
                }

                // Push the binary bundle
                PlayerStorage.getInstance().pushBundle(bundle);

                if (markClean) {
                    SessionManager.setSessionState(uuid, savage.natsplayerdata.model.PlayerState.CLEAN);
                }
            } catch (Exception e) {
                NATSPlayerDataBridge.LOGGER.error("Sync Error: Failed to push bundle for {}: {}", playerName,
                        e.getMessage());
            }
        }, PlayerStorage.VIRTUAL_EXECUTOR);
    }

    /**
     * Handles the asynchronous offloading of a long-term backup snapshot.
     */
    public static void pushBackupAsync(PlayerDataBundle bundle) {
        CompletableFuture.runAsync(() -> {
            if (bundle != null) {
                savage.natsplayerdata.backup.BackupManager.getInstance().createBackup(bundle);
            }
        }, PlayerStorage.VIRTUAL_EXECUTOR);
    }
}
