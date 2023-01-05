//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/experimental.h"

#include "db/db_impl/db_impl.h"
#include "db/version_util.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {
namespace experimental {

#ifndef ROCKSDB_LITE

Status SuggestCompactRange(DB* db, ColumnFamilyHandle* column_family,
                           const Slice* begin, const Slice* end) {
  if (db == nullptr) {
    return Status::InvalidArgument("DB is empty");
  }

  return db->SuggestCompactRange(column_family, begin, end);
}

Status PromoteL0(DB* db, ColumnFamilyHandle* column_family, int target_level) {
  if (db == nullptr) {
    return Status::InvalidArgument("Didn't recognize DB object");
  }
  return db->PromoteL0(column_family, target_level);
}

#else  // ROCKSDB_LITE

Status SuggestCompactRange(DB* /*db*/, ColumnFamilyHandle* /*column_family*/,
                           const Slice* /*begin*/, const Slice* /*end*/) {
  return Status::NotSupported("Not supported in RocksDB LITE");
}

Status PromoteL0(DB* /*db*/, ColumnFamilyHandle* /*column_family*/,
                 int /*target_level*/) {
  return Status::NotSupported("Not supported in RocksDB LITE");
}

#endif  // ROCKSDB_LITE

Status SuggestCompactRange(DB* db, const Slice* begin, const Slice* end) {
  return SuggestCompactRange(db, db->DefaultColumnFamily(), begin, end);
}

Status UpdateManifestForFilesState(
    const DBOptions& db_opts, const std::string& db_name,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    const UpdateManifestForFilesStateOptions& opts) {
  OfflineManifestWriter w(db_opts, db_name);
  Status s = w.Recover(column_families);

  size_t files_updated = 0;
  size_t cfs_updated = 0;
  auto fs = db_opts.env->GetFileSystem();

  for (auto cfd : *w.Versions().GetColumnFamilySet()) {
    if (!s.ok()) {
      break;
    }
    assert(cfd);

    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }

    const auto* current = cfd->current();
    assert(current);

    const auto* vstorage = current->storage_info();
    assert(vstorage);

    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());

    /* SST files */
    for (int level = 0; level < cfd->NumberLevels(); level++) {
      if (!s.ok()) {
        break;
      }
      const auto& level_files = vstorage->LevelFiles(level);

      for (const auto& lf : level_files) {
        assert(lf);

        uint64_t number = lf->fd.GetNumber();
        std::string fname =
            TableFileName(w.IOptions().db_paths, number, lf->fd.GetPathId());

        std::unique_ptr<FSSequentialFile> f;
        FileOptions fopts;
        // Use kUnknown to signal the FileSystem to search all tiers for the
        // file.
        fopts.temperature = Temperature::kUnknown;

        IOStatus file_ios =
            fs->NewSequentialFile(fname, fopts, &f, /*dbg*/ nullptr);
        if (file_ios.ok()) {
          if (opts.update_temperatures) {
            Temperature temp = f->GetTemperature();
            if (temp != Temperature::kUnknown && temp != lf->temperature) {
              // Current state inconsistent with manifest
              ++files_updated;
              edit.DeleteFile(level, number);
              edit.AddFile(
                  level, number, lf->fd.GetPathId(), lf->fd.GetFileSize(),
                  lf->smallest, lf->largest, lf->fd.smallest_seqno,
                  lf->fd.largest_seqno, lf->marked_for_compaction, temp,
                  lf->oldest_blob_file_number, lf->oldest_ancester_time,
                  lf->file_creation_time, lf->epoch_number, lf->file_checksum,
                  lf->file_checksum_func_name, lf->unique_id,
                  lf->compensated_range_deletion_size);
            }
          }
        } else {
          s = file_ios;
          break;
        }
      }
    }

    if (s.ok() && edit.NumEntries() > 0) {
      std::unique_ptr<FSDirectory> db_dir;
      s = fs->NewDirectory(db_name, IOOptions(), &db_dir, nullptr);
      if (s.ok()) {
        s = w.LogAndApply(cfd, &edit, db_dir.get());
      }
      if (s.ok()) {
        ++cfs_updated;
      }
    }
  }

  if (cfs_updated > 0) {
    ROCKS_LOG_INFO(db_opts.info_log,
                   "UpdateManifestForFilesState: updated %zu files in %zu CFs",
                   files_updated, cfs_updated);
  } else if (s.ok()) {
    ROCKS_LOG_INFO(db_opts.info_log,
                   "UpdateManifestForFilesState: no updates needed");
  }
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_opts.info_log, "UpdateManifestForFilesState failed: %s",
                    s.ToString().c_str());
  }

  return s;
}

}  // namespace experimental
}  // namespace ROCKSDB_NAMESPACE
