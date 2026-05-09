package savage.natsplayerdata.mixin;

import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import savage.natsplayerdata.backup.backupevents.DimensionHandler;

@Mixin(ServerPlayer.class)
public abstract class ServerPlayerMixin {

    @Inject(method = "setServerLevel", at = @At("RETURN"))
    private void onSetServerLevel(ServerLevel level, CallbackInfo ci) {
        ServerPlayer player = (ServerPlayer) (Object) this;
        
        DimensionHandler.trigger(player, level);
    }
}
