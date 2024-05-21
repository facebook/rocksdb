* While WAL is locked with LockWAL(), some operations like Flush() and IngestExternalFile() are now blocked as they should have been.
