Fixed a liveness bug where RocksDB could remain indefinitely in recovery from a soft background error when concurrent writes continuously generated normal flush requests.
