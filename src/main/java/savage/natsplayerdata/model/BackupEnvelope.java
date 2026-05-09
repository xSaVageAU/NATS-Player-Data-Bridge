package savage.natsplayerdata.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A wrapper for player data backups that includes context metadata.
 */
public record BackupEnvelope(
    @JsonProperty("m") BackupMetadata metadata,
    @JsonProperty("b") PlayerDataBundle bundle
) {}
