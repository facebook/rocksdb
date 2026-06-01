Remote compaction now falls back to local compaction when the primary cannot deserialize a successful `CompactionServiceResult` before installing any remote output files.
