package savage.natsplayerdata.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Represents a single rule for triggering automatic backups.
 * trigger-specific settings are stored in the options map to keep the JSON clean.
 */
public record BackupPolicy(
    @JsonProperty("enabled") @SerializedName("enabled") boolean enabled,
    @JsonProperty("trigger") @SerializedName("trigger") BackupTrigger trigger,
    @JsonProperty("options") @SerializedName("options") Map<String, Object> options
) {
    public BackupPolicy {
        if (options == null) options = new HashMap<>();
    }

    /**
     * Gets a string option (e.g., for filters).
     */
    public Optional<String> getOptionString(String key) {
        Object val = options.get(key);
        return val instanceof String ? Optional.of((String) val) : Optional.empty();
    }

    /**
     * Gets a numeric option (e.g., for intervals).
     */
    public int getOptionInt(String key, int defaultValue) {
        Object val = options.get(key);
        if (val instanceof Number) return ((Number) val).intValue();
        return defaultValue;
    }
}
