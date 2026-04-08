package savage.natsplayerdata.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.UUID;

/**
 * A highly compressed binary bundle of a player's entire state.
 * Using CBOR allows us to store NBT and JSON components in one single packet.
 */
public record PlayerDataBundle(
    @JsonProperty("u") UUID uuid,
    @JsonProperty("n") byte[] nbt,          // Raw Player .dat bytes
    @JsonProperty("s") Map<String, Object> stats, // Native CBOR Map for Stats (Nested)
    @JsonProperty("a") Map<String, Object> advancements, // Native CBOR Map for Advancements
    @JsonProperty("t") long timestamp      // To prevent stale syncs
) {}
