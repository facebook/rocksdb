//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <utility>
#include <vector>

#include "db/version_set.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class TableCache;
struct FileMetaData;
class InternalKeyComparator;
class InternalStats;

// Load table handlers (i.e., open SST files and populate the table cache) for
// the given set of files. Used during DB open to eagerly warm the table cache.
Status LoadTableHandlersHelper(
    const std::vector<std::pair<FileMetaData*, int>>& files_meta,
    TableCache* table_cache, const FileOptions& file_options,
    const InternalKeyComparator& internal_comparator,
    InternalStats* internal_stats, int max_threads,
    bool prefetch_index_and_filter_in_cache,
    const MutableCFOptions& mutable_cf_options,
    size_t max_file_size_for_l0_meta_pin, const ReadOptions& read_options,
    std::atomic<bool>* stop = nullptr);

// Instead of opening a `DB` to perform certain manifest updates, this
// uses the underlying `VersionSet` API to read and modify the MANIFEST. This
// allows us to use the user's real options, while not having to worry about
// the DB persisting new SST files via flush/compaction or attempting to read/
// compact files which may fail, particularly for the file we intend to remove
// (the user may want to remove an already deleted file from MANIFEST).
class OfflineManifestWriter {
 public:
  OfflineManifestWriter(const DBOptions& options, const std::string& db_path)
      : wc_(options.delayed_write_rate),
        wb_(options.db_write_buffer_size),
        immutable_db_options_(WithDbPath(options, db_path)),
        tc_(NewLRUCache(1 << 20 /* capacity */,
                        options.table_cache_numshardbits)),
        versions_(db_path, &immutable_db_options_, MutableDBOptions{options},
                  sopt_, tc_.get(), &wb_, &wc_,
                  /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                  /*db_id=*/"", /*db_session_id=*/"",
                  options.daily_offpeak_time_utc,
                  /*error_handler=*/nullptr,
                  /*read_only=*/false) {}

  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families) {
    return versions_.Recover(column_families, /*read_only*/ false,
                             /*db_id*/ nullptr,
                             /*no_error_if_files_missing*/ true);
  }

  Status LogAndApply(const ReadOptions& read_options,
                     const WriteOptions& write_options, ColumnFamilyData* cfd,
                     VersionEdit* edit,
                     FSDirectory* dir_contains_current_file) {
    // Use `mutex` to imitate a locked DB mutex when calling `LogAndApply()`.
    InstrumentedMutex mutex;
    mutex.Lock();
    Status s = versions_.LogAndApply(cfd, read_options, write_options, edit,
                                     &mutex, dir_contains_current_file,
                                     false /* new_descriptor_log */);
    mutex.Unlock();
    return s;
  }

  VersionSet& Versions() { return versions_; }
  const ImmutableDBOptions& IOptions() { return immutable_db_options_; }

 private:
  WriteController wc_;
  WriteBufferManager wb_;
  ImmutableDBOptions immutable_db_options_;
  std::shared_ptr<Cache> tc_;
  EnvOptions sopt_;
  VersionSet versions_;

  static ImmutableDBOptions WithDbPath(const DBOptions& options,
                                       const std::string& db_path) {
    ImmutableDBOptions rv(options);
    if (rv.db_paths.empty()) {
      // `VersionSet` expects options that have been through
      // `SanitizeOptions()`, which would sanitize an empty `db_paths`.
      rv.db_paths.emplace_back(db_path, 0 /* target_size */);
    }
    return rv;
  }
};

}  // namespace ROCKSDB_NAMESPACE
