# Fix Summary: file_io

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 12 |
| Completeness | 6 |
| Structure/Style | 3 |
| Depth | 3 |
| **Total** | **24** |

## Disagreements Found

0 true disagreements. 2 near-debates where reviewers addressed the same issue from different angles (INT64_MAX overflow, rate limiter fairness). Details in file_io_debates.md.

## Changes Made

| File | Changes |
|------|---------|
| index.md | Rewrote seen_error_ bullet in Key Invariants: now describes sticky error as behavioral policy with Close() exception, not as a hard invariant |
| 01_env_and_filesystem.md | Fixed wrapper classes location (composite_env.cc, not header); added missing EnvOptions fields (set_fd_cloexec, fallocate_with_keep_size, compaction_readahead_size); added missing FileOptions fields (write_hint, file_checksum_gen_factory, file_checksum_func_name) |
| 03_random_access_file_reader.md | Fixed ReadAsync fallback: clarified that RandomAccessFileReader does not implement sync fallback itself; the fallback is in FilePrefetchBuffer::ReadAsync() |
| 04_sequential_reader_and_prefetch.md | Fixed rate limiter charging description (charges for granted token amount, not n); added note distinguishing raw ReadaheadParams defaults from block-based table effective defaults; narrowed FS buffer reuse claim (explicit Prefetch() disables it); distinguished tickers vs histograms in prefetch statistics |
| 05_direct_io.md | Fixed sysfs path (/sys/dev/block/<major>:<minor> with partition resolution); mentioned runtime InvalidArgument check in Create() as primary safeguard for direct write buffer size |
| 06_async_io.md | Added env/fs_posix.cc and file/random_access_file_reader.cc to Files line; fixed Poll description (PosixFileSystem ignores min_completions, documented TODO); expanded fallback behavior to clarify FilePrefetchBuffer is responsible; removed unverifiable 6-15% CPU overhead claim |
| 07_rate_limiter.md | Rewrote fairness description (two independent Bernoulli trials, not simple 1/10 chance); fixed strict RangeSync (syncs from offset 0 to offset+nbytes, covering all bytes written so far) |
| 08_io_classification.md | Fixed IOActivity custom range from 0x80-0xAE to 0x80-0xFE |
| 09_checksums_and_verification.md | Added include/rocksdb/options.h to Files line; documented checksum_handoff_file_types as the user-facing option; added table showing per-file-type behavior (SST/blob vs MANIFEST/WAL) |
| 10_directory_fsync.md | Added file/filename.cc and include/rocksdb/options.h to Files line; expanded btrfs kFileRenamed detail (opens, fsyncs, closes the renamed file; error on open/close failure) |
| 11_error_handling.md | Fixed Close() behavior with seen_error_ (still calls underlying Close); noted IOStatus attributes are defined in base Status class |
| 12_best_practices.md | Fixed INT64_MAX overflow claim (now says "meaninglessly large"); fixed fairness description; removed unverifiable 6-15% CPU number; fixed advise_random_on_open (Hint API, not direct fadvise; includes blob files); added FileSystemWrapper recommendation for custom FileSystem implementations |
