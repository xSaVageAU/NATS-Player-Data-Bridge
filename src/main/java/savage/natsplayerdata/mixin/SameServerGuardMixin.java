package savage.natsplayerdata.mixin;

import net.minecraft.network.chat.Component;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.players.NameAndId;
import net.minecraft.server.players.PlayerList;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import java.net.SocketAddress;
import java.util.UUID;

/**
 * Intercepts canPlayerLogin() to detect same-server duplicate sessions.
 *
 * Returning a non-null Component from canPlayerLogin() causes Vanilla to reject
 * the incoming connection CLEANLY without kicking the already-connected player.
 * This is the ONLY correct hook point for this — QUERY_START fires too late
 * (Vanilla's duplicate-session kick is already scheduled by then).
 */
@Mixin(PlayerList.class)
public abstract class SameServerGuardMixin {

    @Shadow @Final private MinecraftServer server;

    @Inject(method = "canPlayerLogin", at = @At("HEAD"), cancellable = true)
    private void onCanPlayerLogin(SocketAddress socketAddress, NameAndId nameAndId,
                                  CallbackInfoReturnable<Component> cir) {
        if (nameAndId == null) return;

        UUID uuid = nameAndId.id();
        if (server.getPlayerList().getPlayer(uuid) != null) {
            savage.natsplayerdata.NATSPlayerDataBridge.LOGGER.warn(
                "Cluster: Rejecting same-server duplicate login for {} via canPlayerLogin.", uuid);
            cir.setReturnValue(Component.literal("\u00a7cYou are already connected to this server!"));
        }
    }
}
