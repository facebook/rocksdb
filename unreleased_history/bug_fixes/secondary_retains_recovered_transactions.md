Fixed unbounded retention of recovered transaction write batches by a long-lived secondary opened with `allow_2pc` that misses their commit or rollback markers.
