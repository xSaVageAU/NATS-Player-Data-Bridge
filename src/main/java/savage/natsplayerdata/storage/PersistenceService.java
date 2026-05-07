package savage.natsplayerdata.storage;

import net.fabricmc.loader.api.FabricLoader;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.model.PlayerDataBundle;
import savage.natsplayerdata.util.Serialization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Manages local persistence for player data bundles when NATS is unreachable.
 * Acts as a temporary "Vault" for pending synchronizations.
 */
public class PersistenceService {

    private static final Path VAULT_PATH = FabricLoader.getInstance().getGameDir()
            .resolve("nats-player-data-bridge").resolve("pending_sync");

    /**
     * Saves a player data bundle to the local vault.
     */
    public static void saveToVault(UUID uuid, PlayerDataBundle bundle) {
        try {
            Files.createDirectories(VAULT_PATH);
            Path file = VAULT_PATH.resolve(uuid.toString() + ".cbor");
            
            byte[] data = Serialization.CBOR.writeValueAsBytes(bundle);
            Files.write(file, data);
            
            NATSPlayerDataBridge.LOGGER.info("Persistence: Saved pending sync bundle for {} to local vault.", uuid);
        } catch (IOException e) {
            NATSPlayerDataBridge.LOGGER.error("Persistence: Failed to save bundle to vault for {}: {}", uuid, e.getMessage());
        }
    }

    /**
     * Checks if a specific player has a pending bundle in the vault.
     */
    public static boolean hasPendingSync(UUID uuid) {
        return Files.exists(VAULT_PATH.resolve(uuid.toString() + ".cbor"));
    }

    /**
     * Checks if there are any pending bundles in the vault.
     */
    public static boolean hasPendingSyncs() {
        if (!Files.exists(VAULT_PATH)) return false;
        try (Stream<Path> files = Files.list(VAULT_PATH)) {
            return files.anyMatch(path -> path.toString().endsWith(".cbor"));
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * @return A stream of all pending UUIDs in the vault.
     */
    public static Stream<UUID> getPendingUUIDs() {
        if (!Files.exists(VAULT_PATH)) return Stream.empty();
        try {
            return Files.list(VAULT_PATH)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(s -> s.endsWith(".cbor"))
                    .map(s -> s.replace(".cbor", ""))
                    .map(UUID::fromString);
        } catch (IOException e) {
            return Stream.empty();
        }
    }

    /**
     * Loads a bundle from the vault and deletes the file.
     */
    public static PlayerDataBundle consumeFromVault(UUID uuid) throws IOException {
        Path file = VAULT_PATH.resolve(uuid.toString() + ".cbor");
        if (!Files.exists(file)) return null;

        byte[] data = Files.readAllBytes(file);
        PlayerDataBundle bundle = Serialization.CBOR.readValue(data, PlayerDataBundle.class);
        
        Files.delete(file);
        return bundle;
    }
}
