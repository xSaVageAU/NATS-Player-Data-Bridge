package savage.natsplayerdata.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;

/**
 * Centralized serialization utility for the NATS Player Data Bridge.
 * Ensures consistent mapper configurations across the cluster.
 */
public class Serialization {

    /**
     * Standard JSON mapper for stats and advancements.
     */
    public static final ObjectMapper JSON = new ObjectMapper();

    /**
     * High-performance CBOR mapper for binary player data bundles.
     */
    public static final ObjectMapper CBOR = new ObjectMapper(new CBORFactory());

    // Private constructor to prevent instantiation
    private Serialization() {}
}
