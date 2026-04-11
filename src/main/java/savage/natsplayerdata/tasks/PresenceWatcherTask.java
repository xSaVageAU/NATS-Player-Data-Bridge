package savage.natsplayerdata.tasks;

import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.api.KeyValueWatcher;
import savage.natsplayerdata.NATSPlayerDataBridge;
import savage.natsplayerdata.PlayerPresenceManager;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Watcher for the NATS presence bucket. Maintains a local in-memory cache of online players.
 */
public class PresenceWatcherTask implements KeyValueWatcher {

    @Override
    public void watch(KeyValueEntry entry) {
        try {
            String key = entry.getKey();
            if (key.startsWith("_KV")) return;
            UUID uuid = UUID.fromString(key);

            // Use the NATS entry's actual write time so that replayed startup entries
            // inherit their real age rather than getting a fresh 60s local TTL.
            long natsTimestamp = entry.getCreated() != null
                    ? entry.getCreated().toInstant().toEpochMilli()
                    : System.currentTimeMillis();

            if (entry.getValue() == null || entry.getOperation() == KeyValueOperation.DELETE || entry.getOperation() == KeyValueOperation.PURGE) {
                PlayerPresenceManager.updateLocalCache(uuid, null, 0);
            } else {
                String rawValue = new String(entry.getValue(), StandardCharsets.UTF_8);
                PlayerPresenceManager.updateLocalCache(uuid, rawValue, natsTimestamp);
            }
        } catch (Exception e) {
            NATSPlayerDataBridge.LOGGER.warn("Cluster: Presence watcher entry error: {}", e.getMessage());
        }
    }

    @Override
    public void endOfData() {
        NATSPlayerDataBridge.debugLog("Cluster: Presence cache initial sync complete.");
    }
}
