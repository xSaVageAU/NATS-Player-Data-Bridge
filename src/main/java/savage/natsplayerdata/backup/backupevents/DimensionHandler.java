package savage.natsplayerdata.backup.backupevents;

import net.minecraft.server.level.ServerPlayer;
import savage.natsplayerdata.merge.DataMergeService;
import savage.natsplayerdata.model.BackupMetadata;
import savage.natsplayerdata.model.BackupPolicy;

import java.util.List;

/**
 * Handles automatic backups triggered when a player changes dimensions.
 */
public class DimensionHandler implements BackupHandler {
    private static DimensionHandler instance;
    private boolean enabled = false;
    private List<String> targetDimensions;

    @Override
    @SuppressWarnings("unchecked")
    public void init(BackupPolicy policy) {
        this.enabled = policy.enabled();
        instance = this;

        // Check for 'dimensions' whitelist in options
        Object dims = policy.options().get("dimensions");
        if (dims instanceof List) {
            this.targetDimensions = (List<String>) dims;
        }
    }

    /**
     * Triggered by ServerPlayerMixin when a dimension change is detected.
     */
    public static void trigger(ServerPlayer player, net.minecraft.server.level.ServerLevel destination) {
        if (instance != null && instance.enabled) {
            String destId = destination.dimension().identifier().toString();
            
            // Only trigger if the destination is explicitly in the whitelist
            if (instance.targetDimensions != null && instance.targetDimensions.contains(destId)) {
                DataMergeService.backUpWithReason(player, savage.natsplayerdata.NATSPlayerDataBridge.getServer(), BackupMetadata.REASON_AUTO, "dim_change_" + destId.replace(":", "_"));
            }
        }
    }

    @Override
    public void stop() {
        this.enabled = false;
    }
}
