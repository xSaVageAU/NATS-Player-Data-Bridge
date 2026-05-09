# Backup Metadata System
 
The NATS Player Data Bridge uses a metadata-wrapped envelope system for backups. This allows the cluster to identify the context of a backup (why it was created, which server owned it) without having to deserialize the heavy binary player data.
 
## Structure
 
Every backup in the `player-backups-v1` bucket is stored as a `BackupEnvelope`:
 
```java
public record BackupMetadata(
    String serverId,    // Origin server
    String reason,      // MANUAL, AUTO, PRE_RESTORE
    String tag,         // Contextual label (e.g., 'death', 'dim_change:the_nether')
    String modVersion,  // Version of the bridge that created it
    long timestamp      // Epoch milliseconds
) {}
 
public record BackupEnvelope(
    BackupMetadata metadata,
    PlayerDataBundle bundle
) {}
```
 
## Auto-Backup Policies
 
As of **Beta.7**, the bridge supports automatic snapshots triggered by high-risk game events. These are configured in the `autoBackupPolicies` list.
 
| Trigger | Tag Format | Description |
|---|---|---|
| `DEATH` | `death` | Created immediately when a player dies, before any items are dropped. |
| `DIMENSION_CHANGE` | `dim_change:<id>` | Created when a player changes dimensions (Nether, End, etc). |
 
## Tooltips & UI
 
When using `/nats backup list <player>`, the main list remains clean for readability. However, you can **hover over any backup entry** to see its full metadata context, including the specific reason and tag that triggered it.
 
## Legacy Compatibility
 
The bridge includes a fallback parser that can still read plain `PlayerDataBundle` files (backups created before Beta.7). These legacy backups will appear with a `LEGACY` reason in the UI.
