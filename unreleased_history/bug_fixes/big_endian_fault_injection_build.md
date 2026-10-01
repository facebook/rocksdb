Fixed RocksDB builds on big-endian architectures such as s390x by disabling the optional persistent fault-injection log, whose binary format is little-endian-only.
