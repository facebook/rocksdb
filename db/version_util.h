//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "db/version_set.h"

namespace ROCKSDB_NAMESPACE {

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
        versions_(db_path, &immutable_db_options_, sopt_, tc_.get(), &wb_, &wc_,
                  /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                  /*db_id*/ "", /*db_session_id*/ "") {}

  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families) {
    return versions_.Recover(column_families, /*read_only*/ false,
                             /*db_id*/ nullptr,
                             /*no_error_if_files_missing*/ true);
  }

  Status LogAndApply(ColumnFamilyData* cfd, VersionEdit* edit,
                     FSDirectory* dir_contains_current_file) {
    // Use `mutex` to imitate a locked DB mutex when calling `LogAndApply()`.
    InstrumentedMutex mutex;
    mutex.Lock();
    Status s = versions_.LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(),
                                     edit, &mutex, dir_contains_current_file,
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
