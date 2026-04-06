package savage.natsplayerdata.mixin;

import net.minecraft.network.chat.Component;
import net.minecraft.server.players.NameAndId;
import net.minecraft.server.players.PlayerList;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;
import savage.natsplayerdata.PlayerPresenceManager;

import java.net.SocketAddress;
import java.util.UUID;

@Mixin(PlayerList.class)
public abstract class LoginPresenceMixin {

    /**
     * Checks if the player is already online in the NATS cluster before allowing login.
     * In Minecraft 26.1.1, this method uses NameAndId instead of GameProfile.
     */
    @Inject(method = "canPlayerLogin", at = @At("HEAD"), cancellable = true)
    private void onCanPlayerLogin(SocketAddress socketAddress, NameAndId nameAndId, CallbackInfoReturnable<Component> cir) {
        if (nameAndId != null) {
            UUID uuid = nameAndId.id();
            boolean online = PlayerPresenceManager.isAlreadyOnline(uuid);
            
            savage.natsplayerdata.NATSPlayerDataBridge.LOGGER.info("Cluster: Checking presence for {} ({}). Already online: {}", 
                nameAndId.name(), uuid, online);
            
            if (online) {
                // Reject the login if already online on another server in the cluster
                cir.setReturnValue(Component.literal("§cYou are already online on another server in this cluster!"));
            }
        }
    }
}
