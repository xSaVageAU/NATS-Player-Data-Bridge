package savage.natsplayerdata.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Stores context about a backup snapshot.
 */
public record BackupMetadata(
    @JsonProperty("s") String serverId,
    @JsonProperty("r") String reason,      // MANUAL, AUTO
    @JsonProperty("t") String tag,         // Optional label
    @JsonProperty("v") String modVersion,
    @JsonProperty("d") String dimension,
    @JsonProperty("ts") long timestamp
) {
    public static final String REASON_MANUAL = "MANUAL";
    public static final String REASON_AUTO = "AUTO";
}
