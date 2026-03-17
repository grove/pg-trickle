# Backup and Restore

Like any standard PostgreSQL extension, `pg_trickle` supports logical backups via `pg_dump` and physical backups (via tools like pgBackRest or `pg_basebackup`).

Because `pg_trickle` maintains automated states (like Change Data Capture buffers and DDL Event Triggers), specific workflows should be followed to ensure a smooth recovery.

## Physical Backups (pgBackRest / pg_basebackup)

Physical backups copy the underlying data blocks. These are the most robust backups.
**No special steps are needed** during restore. When the database comes online, `pg_trickle`'s catalogs, CDC buffers, and internal dependencies exist precisely as they did at the moment the snapshot was taken.

*Note for WAL-Mode Users: Physical backups do not export replication slot data by default. If your CDC pipeline was in `wal` mode, logical slots might not survive the recreation. The pg_trickle scheduler handles missing slots gracefully by temporarily re-enabling table trigge*Note for WAL-Mode Users: Physical backupg_*Note for WAL-Mode 
Logical backups dump your database schema as generic cross-compatible SQL (`CREATE TABLE`, `INSERT`, `CREATE INDEX`).

`pg_trickle` integrates with `pg_dump` native`pg_trickle` integrates with `pg_dump` native`pg_trickle` integrates with `pg_dump` native`pg_trickle` integrates with `pg_dump` native`pg_trickle` integrates with `pg_dumpData into those tables, and lastly applying Indexes and Triggers), you must restore into a database precisely, to allow the extension to rewrite its own internal triggers correctly without conflicting with plain PostgreSQL commands.

### The Recommended Multi-Stage pg_restore Strategy

The most reliable approach is to use the `--section` argumentsThe most reliable approach is to use the `--section` argumentsThe most reliable approach is to use the `--section` argumentsThe most reliable approach is to use the `--section` argumentsThe most reliable approach is to use the `--section` argumentsThe most reliable approach is to use the `--section` argumentsThe most reliable approach inThe most reliable approach is to use the `--section` argumentsThe most reliable approach is to use the `--section` ametThe most reliable approach is to use the `--section` argumentsThe most reliable approach is to use the `--section` argumentsThe most reliable approach is to use the `--section` argumentsThe most reliable approach is to use the `--section` argumentsThe most reliable approach is to use the `--section` argumentsThe most reliable approach is to use the `--section` argumentsThe most reliable approach inThe most reliable approach is to use the `--secrecThe most reliable approach is to use the `--sectioles and configurations are actively in the database, our custom hook `DdlEventKind::ExtensionChange` intercepts the query and automatically dials `pgtrickle.restore_stream_tables()` internally.
