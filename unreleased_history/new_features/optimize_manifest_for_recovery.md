Add mutable DBOption `optimize_manifest_for_recovery` (default false). When enabled, RocksDB can reduce recovery work after a clean shutdown, which may lower DB::Open latency on warm reopens.
