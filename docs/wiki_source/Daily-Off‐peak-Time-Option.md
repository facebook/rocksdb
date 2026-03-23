# Background

This feature is relevant only when `periodic_compaction_seconds` is set. If there are files older than `periodic_compaction_seconds`, RocksDB includes these files in the compaction process to ensure the SST files are updated and not older than `periodic_compaction_seconds`. Periodic compactions, also known as TTL-based compactions, can occur anytime. They are often considered "low-priority" as they are not immediately essential for RocksDB to function correctly. 

Since version 8.9, the off-peak period context allows users to restrict the hours during the day for processing low-priority compactions, preventing them from competing for resources during peak hours.

# Enabling the Feature

To enable this feature, set `daily_offpeak_time_utc` in the HH:mm-HH:mm (00:00-23:59) format. This represents the start and end times (inclusive) of the day in UTC. As part of `mutable_db_options`, it can be set via the `SetDBOptions()` API, and a DB restart is not required.

The default value is an empty string, "", indicating no off-peak period.

Please note that when the option changes via the `SetDBOptions()` API, the DB checks if a compaction needs to be triggered (similar to when `max_background_flushes`, `max_background_compactions` options change in flight).

# How the feature works

When this feature is enabled, RocksDB pre-processes the periodic compactions during the off-peak period just before the start of the next peak cycle. For instance, if `periodic_compaction_seconds` is set for 25 days, the files may be compacted during the off-peak hours of the 24th day.