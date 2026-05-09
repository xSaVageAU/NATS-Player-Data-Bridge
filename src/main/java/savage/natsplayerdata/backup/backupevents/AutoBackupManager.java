package savage.natsplayerdata.backup.backupevents;

import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.config.BridgeConfig;
import savage.natsplayerdata.model.BackupPolicy;
import savage.natsplayerdata.model.BackupTrigger;

import java.util.HashMap;
import java.util.Map;

/**
 * Orchestrates the automatic backup system.
 * This is the hub that connects configuration policies to actual event logic.
 */
public class AutoBackupManager {
    private static final AutoBackupManager INSTANCE = new AutoBackupManager();
    private final Map<BackupTrigger, BackupHandler> handlers = new HashMap<>();

    public static AutoBackupManager getInstance() {
        return INSTANCE;
    }

    private AutoBackupManager() {
        registerHandler(BackupTrigger.DEATH, new DeathHandler());
    }

    /**
     * Bootstraps the auto-backup system.
     */
    public void init(BridgeConfig config) {
        stop(); 

        if (config.backupPolicies == null) return;

        int activeCount = 0;
        for (BackupPolicy policy : config.backupPolicies) {
            if (policy.enabled()) {
                BackupHandler handler = handlers.get(policy.trigger());
                if (handler != null) {
                    handler.init(policy);
                    activeCount++;
                }
            }
        }
        
        if (activeCount > 0) {
            NATSPlayerDataBridge.LOGGER.info("AutoBackup: System initialized with {} active policies.", activeCount);
        }
    }

    public void registerHandler(BackupTrigger trigger, BackupHandler handler) {
        handlers.put(trigger, handler);
    }

    public void stop() {
        handlers.values().forEach(BackupHandler::stop);
    }
}
