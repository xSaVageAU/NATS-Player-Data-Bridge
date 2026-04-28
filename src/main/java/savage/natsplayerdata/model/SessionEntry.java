package savage.natsplayerdata.model;

/**
 * Technical wrapper for a SessionState paired with its NATS sequence revision number.
 * Used for Optimistic Concurrency Control (CAS).
 */
public record SessionEntry(SessionState state, long revision) {}
