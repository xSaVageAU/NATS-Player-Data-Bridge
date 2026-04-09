package savage.natsplayerdata.mixin;

import net.minecraft.server.MinecraftServer;
import net.minecraft.server.players.PlayerList;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.PlayerDataManager;

import java.util.UUID;

/**
 * Ensures player data is fetched from the NATS cluster *before* Minecraft reads the local disk.
 */
@Mixin(PlayerList.class)
public abstract class PlayerSyncMixin {

    @Shadow @Final private MinecraftServer server;

    @Inject(method = "loadPlayerData", at = @At("HEAD"), cancellable = true)
    private void onLoadData(net.minecraft.server.players.NameAndId nameAndId, org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable<java.util.Optional<net.minecraft.nbt.CompoundTag>> cir) {
        UUID uuid = nameAndId.id();
        NATSPlayerDataBridge.debugLog("Cluster: Intercepted load for {}, checking NATS ledger...", uuid);
        
        // Fetch from cluster, overwrite local disk, and return the tag directly
        java.util.Optional<net.minecraft.nbt.CompoundTag> opt = PlayerDataManager.fetchAndApply(uuid, this.server);
        if (opt.isPresent()) {
            cir.setReturnValue(opt);
        }
    }
}
