//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/db_impl.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include "db/builder.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/sst_file_writer.h"
#include "table/table_builder.h"
#include "util/file_reader_writer.h"
#include "util/file_util.h"
#include "util/sync_point.h"

namespace rocksdb {
#ifndef ROCKSDB_LITE
Status DBImpl::AddFile(ColumnFamilyHandle* column_family,
                       const std::string& file_path, bool move_file) {
  Status status;
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();

  ExternalSstFileInfo file_info;
  file_info.file_path = file_path;
  status = env_->GetFileSize(file_path, &file_info.file_size);
  if (!status.ok()) {
    return status;
  }

  // Access the file using TableReader to extract
  // version, number of entries, smallest user key, largest user key
  std::unique_ptr<RandomAccessFile> sst_file;
  status = env_->NewRandomAccessFile(file_path, &sst_file, env_options_);
  if (!status.ok()) {
    return status;
  }
  std::unique_ptr<RandomAccessFileReader> sst_file_reader;
  sst_file_reader.reset(new RandomAccessFileReader(std::move(sst_file)));

  std::unique_ptr<TableReader> table_reader;
  status = cfd->ioptions()->table_factory->NewTableReader(
      TableReaderOptions(*cfd->ioptions(), env_options_,
                         cfd->internal_comparator()),
      std::move(sst_file_reader), file_info.file_size, &table_reader);
  if (!status.ok()) {
    return status;
  }

  // Get the external sst file version from table properties
  const UserCollectedProperties& user_collected_properties =
      table_reader->GetTableProperties()->user_collected_properties;
  UserCollectedProperties::const_iterator external_sst_file_version_iter =
      user_collected_properties.find(ExternalSstFilePropertyNames::kVersion);
  if (external_sst_file_version_iter == user_collected_properties.end()) {
    return Status::InvalidArgument("Generated table version not found");
  }

  file_info.version =
      DecodeFixed32(external_sst_file_version_iter->second.c_str());
  if (file_info.version == 1) {
    // version 1 imply that all sequence numbers in table equal 0
    file_info.sequence_number = 0;
  } else {
    return Status::InvalidArgument("Generated table version is not supported");
  }

  // Get number of entries in table
  file_info.num_entries = table_reader->GetTableProperties()->num_entries;

  ParsedInternalKey key;
  std::unique_ptr<InternalIterator> iter(
      table_reader->NewIterator(ReadOptions()));

  // Get first (smallest) key from file
  iter->SeekToFirst();
  if (!ParseInternalKey(iter->key(), &key)) {
    return Status::Corruption("Generated table have corrupted keys");
  }
  if (key.sequence != 0) {
    return Status::Corruption("Generated table have non zero sequence number");
  }
  file_info.smallest_key = key.user_key.ToString();

  // Get last (largest) key from file
  iter->SeekToLast();
  if (!ParseInternalKey(iter->key(), &key)) {
    return Status::Corruption("Generated table have corrupted keys");
  }
  if (key.sequence != 0) {
    return Status::Corruption("Generated table have non zero sequence number");
  }
  file_info.largest_key = key.user_key.ToString();

  return AddFile(column_family, &file_info, move_file);
}

Status DBImpl::AddFile(ColumnFamilyHandle* column_family,
                       const ExternalSstFileInfo* file_info, bool move_file) {
  Status status;
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();
  const uint64_t start_micros = env_->NowMicros();

  if (file_info->num_entries == 0) {
    return Status::InvalidArgument("File contain no entries");
  }
  if (file_info->version != 1) {
    return Status::InvalidArgument("Generated table version is not supported");
  }
  // version 1 imply that file have only Put Operations with Sequence Number = 0

  FileMetaData meta;
  meta.smallest =
      InternalKey(file_info->smallest_key, file_info->sequence_number,
                  ValueType::kTypeValue);
  meta.largest = InternalKey(file_info->largest_key, file_info->sequence_number,
                             ValueType::kTypeValue);
  if (!meta.smallest.Valid() || !meta.largest.Valid()) {
    return Status::Corruption("Generated table have corrupted keys");
  }
  meta.smallest_seqno = file_info->sequence_number;
  meta.largest_seqno = file_info->sequence_number;
  if (meta.smallest_seqno != 0 || meta.largest_seqno != 0) {
    return Status::InvalidArgument(
        "Non zero sequence numbers are not supported");
  }

  // Generate a location for the new table
  std::list<uint64_t>::iterator pending_outputs_inserted_elem;
  {
    InstrumentedMutexLock l(&mutex_);
    pending_outputs_inserted_elem = CaptureCurrentFileNumberInPendingOutputs();
    meta.fd =
        FileDescriptor(versions_->NewFileNumber(), 0, file_info->file_size);
  }

  std::string db_fname = TableFileName(
      db_options_.db_paths, meta.fd.GetNumber(), meta.fd.GetPathId());

  if (move_file) {
    status = env_->LinkFile(file_info->file_path, db_fname);
    if (status.IsNotSupported()) {
      // Original file is on a different FS, use copy instead of hard linking
      status = CopyFile(env_, file_info->file_path, db_fname, 0);
    }
  } else {
    status = CopyFile(env_, file_info->file_path, db_fname, 0);
  }
  TEST_SYNC_POINT("DBImpl::AddFile:FileCopied");
  if (!status.ok()) {
    return status;
  }

  // The level the file will be ingested into
  int target_level = 0;
  {
    InstrumentedMutexLock l(&mutex_);
    const MutableCFOptions mutable_cf_options =
        *cfd->GetLatestMutableCFOptions();

    WriteThread::Writer w;
    write_thread_.EnterUnbatched(&w, &mutex_);

    if (!snapshots_.empty()) {
      // Check that no snapshots are being held
      status =
          Status::NotSupported("Cannot add a file while holding snapshots");
    }

    if (status.ok()) {
      // Verify that added file key range dont overlap with any keys in DB
      SuperVersion* sv = cfd->GetSuperVersion()->Ref();
      Arena arena;
      ReadOptions ro;
      ro.total_order_seek = true;
      ScopedArenaIterator iter(NewInternalIterator(ro, cfd, sv, &arena));

      InternalKey range_start(file_info->smallest_key, kMaxSequenceNumber,
                              kTypeValue);
      iter->Seek(range_start.Encode());
      status = iter->status();

      if (status.ok() && iter->Valid()) {
        ParsedInternalKey seek_result;
        if (ParseInternalKey(iter->key(), &seek_result)) {
          auto* vstorage = cfd->current()->storage_info();
          if (vstorage->InternalComparator()->user_comparator()->Compare(
                  seek_result.user_key, file_info->largest_key) <= 0) {
            status = Status::NotSupported("Cannot add overlapping range");
          }
        } else {
          status = Status::Corruption("DB have corrupted keys");
        }
      }
    }

    if (status.ok()) {
      // Add file to the lowest possible level
      target_level = PickLevelForIngestedFile(cfd, file_info);
      VersionEdit edit;
      edit.SetColumnFamily(cfd->GetID());
      edit.AddFile(target_level, meta.fd.GetNumber(), meta.fd.GetPathId(),
                   meta.fd.GetFileSize(), meta.smallest, meta.largest,
                   meta.smallest_seqno, meta.largest_seqno,
                   meta.marked_for_compaction);

      status = versions_->LogAndApply(cfd, mutable_cf_options, &edit, &mutex_,
                                      directories_.GetDbDir());
    }
    write_thread_.ExitUnbatched(&w);

    if (status.ok()) {
      delete InstallSuperVersionAndScheduleWork(cfd, nullptr,
                                                mutable_cf_options);

      // Update internal stats
      InternalStats::CompactionStats stats(1);
      stats.micros = env_->NowMicros() - start_micros;
      stats.bytes_written = meta.fd.GetFileSize();
      stats.num_output_files = 1;
      cfd->internal_stats()->AddCompactionStats(target_level, stats);
      cfd->internal_stats()->AddCFStats(InternalStats::BYTES_INGESTED_ADD_FILE,
                                        meta.fd.GetFileSize());
    }
    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);
  }

  if (!status.ok()) {
    // We failed to add the file to the database
    Status s = env_->DeleteFile(db_fname);
    if (!s.ok()) {
      Log(InfoLogLevel::WARN_LEVEL, db_options_.info_log,
          "AddFile() clean up for file %s failed : %s", db_fname.c_str(),
          s.ToString().c_str());
    }
  } else {
    // File was ingested successfully
    Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
        "New file %" PRIu64
        " was added to L%d (Size: %.2f MB, "
        "entries: %" PRIu64 ")",
        meta.fd.GetNumber(), target_level,
        static_cast<double>(meta.fd.GetFileSize()) / 1048576.0,
        file_info->num_entries);

    if (move_file) {
      // The file was moved and added successfully, remove original file link
      Status s = env_->DeleteFile(file_info->file_path);
      if (!s.ok()) {
        Log(InfoLogLevel::WARN_LEVEL, db_options_.info_log,
            "%s was added to DB successfully but failed to remove original "
            "file link : %s",
            file_info->file_path.c_str(), s.ToString().c_str());
      }
    }
  }
  return status;
}

// Finds the lowest level in the DB that the ingested file can be added to
int DBImpl::PickLevelForIngestedFile(ColumnFamilyData* cfd,
                                     const ExternalSstFileInfo* file_info) {
  mutex_.AssertHeld();

  int target_level = 0;
  auto* vstorage = cfd->current()->storage_info();
  auto* ucmp = vstorage->InternalComparator()->user_comparator();

  Slice file_smallest_user_key(file_info->smallest_key);
  Slice file_largest_user_key(file_info->largest_key);

  for (int lvl = cfd->NumberLevels() - 1; lvl >= vstorage->base_level();
       lvl--) {
    if (vstorage->OverlapInLevel(lvl, &file_smallest_user_key,
                                 &file_largest_user_key) == false) {
      // Make sure that the file dont overlap with the output of any
      // compaction running right now
      Slice compaction_smallest_user_key;
      Slice compaction_largest_user_key;
      bool overlap_with_compaction_output = false;
      for (Compaction* c : running_compactions_) {
        if (c->column_family_data()->GetID() != cfd->GetID() ||
            c->output_level() != lvl) {
          continue;
        }

        compaction_smallest_user_key = c->GetSmallestUserKey();
        compaction_largest_user_key = c->GetLargestUserKey();

        if (ucmp->Compare(file_smallest_user_key,
                          compaction_largest_user_key) <= 0 &&
            ucmp->Compare(file_largest_user_key,
                          compaction_smallest_user_key) >= 0) {
          overlap_with_compaction_output = true;
          break;
        }
      }

      if (overlap_with_compaction_output == false) {
        // Level lvl is the lowest level that dont have any files with key
        // range overlapping with our file key range and no compactions
        // planning to add overlapping files in it.
        target_level = lvl;
        break;
      }
    }
  }

  return target_level;
}
#endif  // ROCKSDB_LITE

}  // namespace rocksdb
