//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/version_builder.h"
#ifndef ROCKSDB_LITE

#include "db/import_column_family_job.h"

#include <algorithm>
#include <cinttypes>
#include <string>
#include <vector>

#include "db/version_edit.h"
#include "file/file_util.h"
#include "file/random_access_file_reader.h"
#include "logging/logging.h"
#include "table/merging_iterator.h"
#include "table/scoped_arena_iterator.h"
#include "table/sst_file_writer_collectors.h"
#include "table/table_builder.h"
#include "table/unique_id_impl.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

Status ImportColumnFamilyJob::Prepare(uint64_t next_file_number,
                                      SuperVersion* sv) {
  Status status;

  // Read the information of files we are importing
  for (const auto& file_metadata : metadata_) {
    const auto file_path = file_metadata.db_path + "/" + file_metadata.name;
    IngestedFileInfo file_to_import;
    status =
        GetIngestedFileInfo(file_path, next_file_number++, &file_to_import, sv);
    if (!status.ok()) {
      return status;
    }
    files_to_import_.push_back(file_to_import);
  }

  auto num_files = files_to_import_.size();
  if (num_files == 0) {
    return Status::InvalidArgument("The list of files is empty");
  }

  for (const auto& f : files_to_import_) {
    if (f.num_entries == 0) {
      return Status::InvalidArgument("File contain no entries");
    }

    if (!f.smallest_internal_key.Valid() || !f.largest_internal_key.Valid()) {
      return Status::Corruption("File has corrupted keys");
    }
  }

  // Copy/Move external files into DB
  auto hardlink_files = import_options_.move_files;
  for (auto& f : files_to_import_) {
    const auto path_outside_db = f.external_file_path;
    const auto path_inside_db = TableFileName(
        cfd_->ioptions()->cf_paths, f.fd.GetNumber(), f.fd.GetPathId());

    if (hardlink_files) {
      status =
          fs_->LinkFile(path_outside_db, path_inside_db, IOOptions(), nullptr);
      if (status.IsNotSupported()) {
        // Original file is on a different FS, use copy instead of hard linking
        hardlink_files = false;
        ROCKS_LOG_INFO(db_options_.info_log,
                       "Try to link file %s but it's not supported : %s",
                       f.internal_file_path.c_str(), status.ToString().c_str());
      }
    }
    if (!hardlink_files) {
      status =
          CopyFile(fs_.get(), path_outside_db, path_inside_db, 0,
                   db_options_.use_fsync, io_tracer_, Temperature::kUnknown);
    }
    if (!status.ok()) {
      break;
    }
    f.copy_file = !hardlink_files;
    f.internal_file_path = path_inside_db;
  }

  if (!status.ok()) {
    // We failed, remove all files that we copied into the db
    for (const auto& f : files_to_import_) {
      if (f.internal_file_path.empty()) {
        break;
      }
      const auto s =
          fs_->DeleteFile(f.internal_file_path, IOOptions(), nullptr);
      if (!s.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "AddFile() clean up for file %s failed : %s",
                       f.internal_file_path.c_str(), s.ToString().c_str());
      }
    }
  }

  return status;
}

// REQUIRES: we have become the only writer by entering both write_thread_ and
// nonmem_write_thread_
Status ImportColumnFamilyJob::Run() {
  // We use the import time as the ancester time. This is the time the data
  // is written to the database.
  int64_t temp_current_time = 0;
  uint64_t oldest_ancester_time = kUnknownOldestAncesterTime;
  uint64_t current_time = kUnknownOldestAncesterTime;
  if (clock_->GetCurrentTime(&temp_current_time).ok()) {
    current_time = oldest_ancester_time =
        static_cast<uint64_t>(temp_current_time);
  }

  // Recover files' epoch number using dummy VersionStorageInfo
  VersionBuilder dummy_version_builder(
      cfd_->current()->version_set()->file_options(), cfd_->ioptions(),
      cfd_->table_cache(), cfd_->current()->storage_info(),
      cfd_->current()->version_set(),
      cfd_->GetFileMetadataCacheReservationManager());
  VersionStorageInfo dummy_vstorage(
      &cfd_->internal_comparator(), cfd_->user_comparator(),
      cfd_->NumberLevels(), cfd_->ioptions()->compaction_style,
      nullptr /* src_vstorage */, cfd_->ioptions()->force_consistency_checks,
      EpochNumberRequirement::kMightMissing);
  Status s;
  for (size_t i = 0; s.ok() && i < files_to_import_.size(); ++i) {
    const auto& f = files_to_import_[i];
    const auto& file_metadata = metadata_[i];

    VersionEdit dummy_version_edit;
    dummy_version_edit.AddFile(
        file_metadata.level, f.fd.GetNumber(), f.fd.GetPathId(),
        f.fd.GetFileSize(), f.smallest_internal_key, f.largest_internal_key,
        file_metadata.smallest_seqno, file_metadata.largest_seqno, false,
        file_metadata.temperature, kInvalidBlobFileNumber, oldest_ancester_time,
        current_time, file_metadata.epoch_number, kUnknownFileChecksum,
        kUnknownFileChecksumFuncName, f.unique_id, 0);
    s = dummy_version_builder.Apply(&dummy_version_edit);
  }
  if (s.ok()) {
    s = dummy_version_builder.SaveTo(&dummy_vstorage);
  }
  if (s.ok()) {
    dummy_vstorage.RecoverEpochNumbers(cfd_);
  }

  // Record changes from this CF import in VersionEdit, including files with
  // recovered epoch numbers
  if (s.ok()) {
    edit_.SetColumnFamily(cfd_->GetID());

    for (int level = 0; level < dummy_vstorage.num_levels(); level++) {
      for (FileMetaData* file_meta : dummy_vstorage.LevelFiles(level)) {
        edit_.AddFile(level, *file_meta);
        // If incoming sequence number is higher, update local sequence number.
        if (file_meta->fd.largest_seqno > versions_->LastSequence()) {
          versions_->SetLastAllocatedSequence(file_meta->fd.largest_seqno);
          versions_->SetLastPublishedSequence(file_meta->fd.largest_seqno);
          versions_->SetLastSequence(file_meta->fd.largest_seqno);
        }
      }
    }
  }

  // Release resources occupied by the dummy VersionStorageInfo
  for (int level = 0; level < dummy_vstorage.num_levels(); level++) {
    for (FileMetaData* file_meta : dummy_vstorage.LevelFiles(level)) {
      file_meta->refs--;
      if (file_meta->refs <= 0) {
        delete file_meta;
      }
    }
  }
  return s;
}

void ImportColumnFamilyJob::Cleanup(const Status& status) {
  if (!status.ok()) {
    // We failed to add files to the database remove all the files we copied.
    for (const auto& f : files_to_import_) {
      const auto s =
          fs_->DeleteFile(f.internal_file_path, IOOptions(), nullptr);
      if (!s.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "AddFile() clean up for file %s failed : %s",
                       f.internal_file_path.c_str(), s.ToString().c_str());
      }
    }
  } else if (status.ok() && import_options_.move_files) {
    // The files were moved and added successfully, remove original file links
    for (IngestedFileInfo& f : files_to_import_) {
      const auto s =
          fs_->DeleteFile(f.external_file_path, IOOptions(), nullptr);
      if (!s.ok()) {
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "%s was added to DB successfully but failed to remove original "
            "file link : %s",
            f.external_file_path.c_str(), s.ToString().c_str());
      }
    }
  }
}

Status ImportColumnFamilyJob::GetIngestedFileInfo(
    const std::string& external_file, uint64_t new_file_number,
    IngestedFileInfo* file_to_import, SuperVersion* sv) {
  file_to_import->external_file_path = external_file;

  // Get external file size
  Status status = fs_->GetFileSize(external_file, IOOptions(),
                                   &file_to_import->file_size, nullptr);
  if (!status.ok()) {
    return status;
  }

  // Assign FD with number
  file_to_import->fd =
      FileDescriptor(new_file_number, 0, file_to_import->file_size);

  // Create TableReader for external file
  std::unique_ptr<TableReader> table_reader;
  std::unique_ptr<FSRandomAccessFile> sst_file;
  std::unique_ptr<RandomAccessFileReader> sst_file_reader;

  status =
      fs_->NewRandomAccessFile(external_file, env_options_, &sst_file, nullptr);
  if (!status.ok()) {
    return status;
  }
  sst_file_reader.reset(new RandomAccessFileReader(
      std::move(sst_file), external_file, nullptr /*Env*/, io_tracer_));

  status = cfd_->ioptions()->table_factory->NewTableReader(
      TableReaderOptions(
          *cfd_->ioptions(), sv->mutable_cf_options.prefix_extractor,
          env_options_, cfd_->internal_comparator(),
          /*skip_filters*/ false, /*immortal*/ false,
          /*force_direct_prefetch*/ false, /*level*/ -1,
          /*block_cache_tracer*/ nullptr,
          /*max_file_size_for_l0_meta_pin*/ 0, versions_->DbSessionId(),
          /*cur_file_num*/ new_file_number),
      std::move(sst_file_reader), file_to_import->file_size, &table_reader);
  if (!status.ok()) {
    return status;
  }

  // Get the external file properties
  auto props = table_reader->GetTableProperties();

  // Set original_seqno to 0.
  file_to_import->original_seqno = 0;

  // Get number of entries in table
  file_to_import->num_entries = props->num_entries;

  ParsedInternalKey key;
  ReadOptions ro;
  // During reading the external file we can cache blocks that we read into
  // the block cache, if we later change the global seqno of this file, we will
  // have block in cache that will include keys with wrong seqno.
  // We need to disable fill_cache so that we read from the file without
  // updating the block cache.
  ro.fill_cache = false;
  std::unique_ptr<InternalIterator> iter(table_reader->NewIterator(
      ro, sv->mutable_cf_options.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kExternalSSTIngestion));

  // Get first (smallest) key from file
  iter->SeekToFirst();
  Status pik_status =
      ParseInternalKey(iter->key(), &key, db_options_.allow_data_in_errors);
  if (!pik_status.ok()) {
    return Status::Corruption("Corrupted Key in external file. ",
                              pik_status.getState());
  }
  file_to_import->smallest_internal_key.SetFrom(key);

  // Get last (largest) key from file
  iter->SeekToLast();
  pik_status =
      ParseInternalKey(iter->key(), &key, db_options_.allow_data_in_errors);
  if (!pik_status.ok()) {
    return Status::Corruption("Corrupted Key in external file. ",
                              pik_status.getState());
  }
  file_to_import->largest_internal_key.SetFrom(key);

  file_to_import->cf_id = static_cast<uint32_t>(props->column_family_id);

  file_to_import->table_properties = *props;

  auto s = GetSstInternalUniqueId(props->db_id, props->db_session_id,
                                  props->orig_file_number,
                                  &(file_to_import->unique_id));
  if (!s.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to get SST unique id for file %s",
                   file_to_import->internal_file_path.c_str());
  }

  return status;
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE
