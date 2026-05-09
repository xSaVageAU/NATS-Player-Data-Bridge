package savage.natsplayerdata.backup.backupevents;

import net.fabricmc.fabric.api.entity.event.v1.ServerLivingEntityEvents;
import net.minecraft.server.level.ServerPlayer;
import savage.natsplayerdata.merge.DataMergeService;
import savage.natsplayerdata.model.BackupMetadata;
import savage.natsplayerdata.model.BackupPolicy;

/**
 * Handles automatic backups triggered by player death.
 */
public class DeathHandler implements BackupHandler {
    // Temporary storage for data captured just before death
    private final java.util.Map<java.util.UUID, savage.natsplayerdata.model.PlayerDataBundle> pendingBackups = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.Map<java.util.UUID, savage.natsplayerdata.model.BackupMetadata> pendingMeta = new java.util.concurrent.ConcurrentHashMap<>();
    private boolean enabled = false;

    @Override
    public void init(BackupPolicy policy) {
        this.enabled = policy.enabled();
        
        // STAGE 1: Capture data while inventory is still intact
        ServerLivingEntityEvents.ALLOW_DEATH.register((entity, damageSource, damageAmount) -> {
            if (enabled && entity instanceof ServerPlayer player) {
                var server = savage.natsplayerdata.NATSPlayerDataBridge.getServer();
                var uuid = player.getUUID();

                // 1. Capture NBT and fix health
                net.minecraft.world.level.storage.TagValueOutput output = net.minecraft.world.level.storage.TagValueOutput
                        .createWithContext(net.minecraft.util.ProblemReporter.DISCARDING, server.registryAccess());
                player.saveWithoutId(output);
                net.minecraft.nbt.CompoundTag nbt = output.buildResult();
                
                nbt.putFloat("Health", player.getMaxHealth());
                nbt.putShort("DeathTime", (short) 0);
                nbt.putInt("HurtTime", 0);

                // 2. Store in pending map
                var stats = savage.natsplayerdata.util.BundlePacker.captureStats(uuid, server);
                var adv = savage.natsplayerdata.util.BundlePacker.captureAdv(uuid, server);
                
                pendingBackups.put(uuid, savage.natsplayerdata.util.BundlePacker.captureBundle(uuid, player.getName().getString(), nbt, stats, adv));
                pendingMeta.put(uuid, savage.natsplayerdata.util.BundlePacker.captureMetadata(player, BackupMetadata.REASON_AUTO, "death_snapshot"));

                // 3. Cleanup: If they are ALIVE next tick, it was a totem. If they are DEAD, Stage 2 will handle it.
                server.execute(() -> {
                    if (!player.isDeadOrDying() && player.getHealth() > 0) {
                        pendingBackups.remove(uuid);
                        pendingMeta.remove(uuid);
                    }
                });
            }
            return true;
        });

        // STAGE 2: If AFTER_DEATH fires, it means the totem failed and they actually died
        ServerLivingEntityEvents.AFTER_DEATH.register((entity, damageSource) -> {
            if (enabled && entity instanceof ServerPlayer player) {
                var uuid = player.getUUID();
                var bundle = pendingBackups.remove(uuid);
                var meta = pendingMeta.remove(uuid);
                
                if (bundle != null && meta != null) {
                    savage.natsplayerdata.sync.SyncService.pushBackupAsync(bundle, meta);
                }
            }
        });
    }

    @Override
    public void stop() {
        this.enabled = false;
    }
}
