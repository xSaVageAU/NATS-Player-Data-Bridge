# Backup Metadata System

## Metadata Structure
Context stored alongside the binary bundle to allow identification without full deserialization.

```java
public record BackupMetadata(
    String serverId,
    String reason,      // MANUAL, AUTO, PRE_RESTORE
    String tag,         // Optional label
    String modVersion,
    long timestamp
) {}

public record BackupEnvelope(
    BackupMetadata metadata,
    PlayerDataBundle bundle
) {}
```

## Implementation Plan
1. **Models**: Add `BackupMetadata` and `BackupEnvelope` records.
2. **Storage**: Update `BackupStorage` to handle the envelope and provide fallback for legacy plain bundles.
3. **Logic**: Update `DataMergeService` to capture metadata and `BackupManager` to unpack envelopes during restore.
4. **UI**: Update `/nats backup list` to display reason, tag, and server.
