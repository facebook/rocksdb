Reduced commit latency when ingesting many external files by allowing `IngestExternalFileOptions::file_opening_threads` to open table readers for committed ingested files using multiple threads.
