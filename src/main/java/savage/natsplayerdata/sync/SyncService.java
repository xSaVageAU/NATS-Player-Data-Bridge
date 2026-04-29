package savage.natsplayerdata.sync;

import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.model.PlayerDataBundle;
import savage.natsplayerdata.model.PlayerState;
import savage.natsplayerdata.session.SessionManager;
import savage.natsplayerdata.storage.DataStorage;
import savage.natsplayerdata.storage.SessionStorage;
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
                    // --- DIRECT REDIRECT TO BACKUP BUCKET ---
                    var targetOpt = savage.natsplayerdata.backup.BackupManager.getInstance().getBackupEntry(uuid, backupRevision);

                    if (targetOpt.isPresent()) {
                        byte[] compressedData = targetOpt.get().getValue();

                        // Commit this rollback to the main sync bucket immediately
                        DataStorage.getInstance().pushRawBundle(uuid, compressedData);

                        // Deserialize and return the bundle for local use
                        return DataStorage.getInstance().deserializeBundle(compressedData);
                    } else {
                        NATSPlayerDataBridge.LOGGER.error(
                                "Cluster: Backup redirect failed for {} - Revision {} not found!", uuid,
                                backupRevision);
                    }
                }

                // Standard sync bucket fetch
                return DataStorage.getInstance().fetchBundle(uuid);
            } catch (Exception e) {
                NATSPlayerDataBridge.LOGGER.error("Cluster: Async fetch failed for {}: {}", uuid, e.getMessage());
                return Optional.<PlayerDataBundle>empty();
            }
        }, DataStorage.VIRTUAL_EXECUTOR);

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
    public static CompletableFuture<Void> pushAsync(UUID uuid, String playerName, PlayerDataBundle bundle, boolean markClean) {
        return CompletableFuture.runAsync(() -> {
            try {
                // SESSION LOCK: STRICT PUSH GUARD
                var entryOpt = SessionStorage.getInstance().fetchSession(uuid);
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
                DataStorage.getInstance().pushBundle(bundle);

                if (markClean) {
                    SessionManager.releaseLockSafely(uuid);
                }
            } catch (Exception e) {
                NATSPlayerDataBridge.LOGGER.error("Sync Error: Failed to push bundle for {}: {}", playerName,
                        e.getMessage());
            }
        }, DataStorage.VIRTUAL_EXECUTOR);
    }

    /**
     * Handles the asynchronous offloading of a long-term backup snapshot.
     */
    public static void pushBackupAsync(PlayerDataBundle bundle) {
        CompletableFuture.runAsync(() -> {
            if (bundle != null) {
                savage.natsplayerdata.backup.BackupManager.getInstance().createBackup(bundle);
            }
        }, DataStorage.VIRTUAL_EXECUTOR);
    }
}
