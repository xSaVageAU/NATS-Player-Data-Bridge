package savage.natsplayerdata.mixin;

import net.minecraft.network.protocol.game.ServerboundPlayerActionPacket;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.server.network.ServerGamePacketListenerImpl;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import savage.natsplayerdata.session.SessionManager;

/**
 * The "Iron Curtain" packet filter and entity lock.
 * Contains all mixins responsible for preventing state modifications while a player is in a proxy transfer.
 */
public class TransferFreezeMixin {

    @Mixin(ServerGamePacketListenerImpl.class)
    public static abstract class PacketListenerMixin {
        @Shadow public ServerPlayer player;

        @Inject(method = "handlePlayerAction", at = @At("HEAD"), cancellable = true)
        private void blockPlayerActions(ServerboundPlayerActionPacket packet, CallbackInfo ci) {
            if (player != null && SessionManager.TRANSFERRING_PLAYERS.contains(this.player.getUUID())) ci.cancel();
        }

        @Inject(method = "handleContainerClick", at = @At("HEAD"), cancellable = true)
        private void blockContainerClick(net.minecraft.network.protocol.game.ServerboundContainerClickPacket packet, CallbackInfo ci) {
            if (player != null && SessionManager.TRANSFERRING_PLAYERS.contains(this.player.getUUID())) ci.cancel();
        }

        @Inject(method = "handleInteract", at = @At("HEAD"), cancellable = true)
        private void blockInteract(net.minecraft.network.protocol.game.ServerboundInteractPacket packet, CallbackInfo ci) {
            if (player != null && SessionManager.TRANSFERRING_PLAYERS.contains(this.player.getUUID())) ci.cancel();
        }

        @Inject(method = "handleUseItem", at = @At("HEAD"), cancellable = true)
        private void blockUseItem(net.minecraft.network.protocol.game.ServerboundUseItemPacket packet, CallbackInfo ci) {
            if (player != null && SessionManager.TRANSFERRING_PLAYERS.contains(this.player.getUUID())) ci.cancel();
        }

        @Inject(method = "handleUseItemOn", at = @At("HEAD"), cancellable = true)
        private void blockUseItemOn(net.minecraft.network.protocol.game.ServerboundUseItemOnPacket packet, CallbackInfo ci) {
            if (player != null && SessionManager.TRANSFERRING_PLAYERS.contains(this.player.getUUID())) ci.cancel();
        }
    }

    @Mixin(net.minecraft.world.entity.player.Player.class)
    public static abstract class PlayerCombatMixin {
        @Inject(method = "attack", at = @At("HEAD"), cancellable = true)
        private void blockAttack(net.minecraft.world.entity.Entity target, CallbackInfo ci) {
            if (SessionManager.TRANSFERRING_PLAYERS.contains(((net.minecraft.world.entity.player.Player)(Object)this).getUUID())) ci.cancel();
        }
    }

    @Mixin(net.minecraft.world.entity.item.ItemEntity.class)
    public static abstract class ItemPickupMixin {
        @Inject(method = "playerTouch", at = @At("HEAD"), cancellable = true)
        private void blockPickup(net.minecraft.world.entity.player.Player player, CallbackInfo ci) {
            if (SessionManager.TRANSFERRING_PLAYERS.contains(player.getUUID())) ci.cancel();
        }
    }
}
