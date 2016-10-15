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

Status DBImpl::ReadExternalSstFileInfo(ColumnFamilyHandle* column_family,
                                       const std::string& file_path,
                                       ExternalSstFileInfo* file_info) {
  Status status;
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  file_info->file_path = file_path;
  status = env_->GetFileSize(file_path, &file_info->file_size);
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
      std::move(sst_file_reader), file_info->file_size, &table_reader);
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

  file_info->version =
      DecodeFixed32(external_sst_file_version_iter->second.c_str());
  if (file_info->version == 1) {
    // version 1 imply that all sequence numbers in table equal 0
    file_info->sequence_number = 0;
  } else {
    return Status::InvalidArgument("Generated table version is not supported");
  }
  // Get number of entries in table
  file_info->num_entries = table_reader->GetTableProperties()->num_entries;

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
  file_info->smallest_key = key.user_key.ToString();

  // Get last (largest) key from file
  iter->SeekToLast();
  if (!ParseInternalKey(iter->key(), &key)) {
    return Status::Corruption("Generated table have corrupted keys");
  }
  if (key.sequence != 0) {
    return Status::Corruption("Generated table have non zero sequence number");
  }
  file_info->largest_key = key.user_key.ToString();

  return Status::OK();
}

Status DBImpl::AddFile(ColumnFamilyHandle* column_family,
                       const std::vector<std::string>& file_path_list,
                       bool move_file, bool skip_snapshot_check) {
  Status status;
  auto num_files = file_path_list.size();
  if (num_files == 0) {
    return Status::InvalidArgument("The list of files is empty");
  }

  std::vector<ExternalSstFileInfo> file_info_list(num_files);
  for (size_t i = 0; i < num_files; i++) {
    status = ReadExternalSstFileInfo(column_family, file_path_list[i],
                                     &file_info_list[i]);
    if (!status.ok()) {
      return status;
    }
  }
  return AddFile(column_family, file_info_list, move_file, skip_snapshot_check);
}

Status DBImpl::AddFile(ColumnFamilyHandle* column_family,
                       const std::vector<ExternalSstFileInfo>& file_info_list,
                       bool move_file, bool skip_snapshot_check) {
  Status status;
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();
  const Comparator* user_cmp = cfd->internal_comparator().user_comparator();

  auto num_files = file_info_list.size();
  if (num_files == 0) {
    return Status::InvalidArgument("The list of files is empty");
  }

  // Verify that passed files dont have overlapping ranges
  if (num_files > 1) {
    std::vector<const ExternalSstFileInfo*> sorted_file_info_list(num_files);
    for (size_t i = 0; i < num_files; i++) {
      sorted_file_info_list[i] = &file_info_list[i];
    }

    std::sort(sorted_file_info_list.begin(), sorted_file_info_list.end(),
              [&user_cmp, &file_info_list](const ExternalSstFileInfo* info1,
                                           const ExternalSstFileInfo* info2) {
                return user_cmp->Compare(info1->smallest_key,
                                         info2->smallest_key) < 0;
              });

    for (size_t i = 0; i < num_files - 1; i++) {
      if (user_cmp->Compare(sorted_file_info_list[i]->largest_key,
                            sorted_file_info_list[i + 1]->smallest_key) >= 0) {
        return Status::NotSupported("Files have overlapping ranges");
      }
    }
  }

  std::vector<uint64_t> micro_list(num_files, 0);
  std::vector<FileMetaData> meta_list(num_files);
  for (size_t i = 0; i < num_files; i++) {
    StopWatch sw(env_, nullptr, 0, &micro_list[i], false);
    if (file_info_list[i].num_entries == 0) {
      return Status::InvalidArgument("File contain no entries");
    }
    if (file_info_list[i].version != 1) {
      return Status::InvalidArgument(
          "Generated table version is not supported");
    }
    // version 1 imply that file have only Put Operations with Sequence Number =
    // 0

    meta_list[i].smallest =
        InternalKey(file_info_list[i].smallest_key,
                    file_info_list[i].sequence_number, ValueType::kTypeValue);
    meta_list[i].largest =
        InternalKey(file_info_list[i].largest_key,
                    file_info_list[i].sequence_number, ValueType::kTypeValue);
    if (!meta_list[i].smallest.Valid() || !meta_list[i].largest.Valid()) {
      return Status::Corruption("Generated table have corrupted keys");
    }
    meta_list[i].smallest_seqno = file_info_list[i].sequence_number;
    meta_list[i].largest_seqno = file_info_list[i].sequence_number;
    if (meta_list[i].smallest_seqno != 0 || meta_list[i].largest_seqno != 0) {
      return Status::InvalidArgument(
          "Non zero sequence numbers are not supported");
    }
  }

  std::vector<std::list<uint64_t>::iterator> pending_outputs_inserted_elem_list(
      num_files);
  // Generate locations for the new tables
  {
    InstrumentedMutexLock l(&mutex_);
    for (size_t i = 0; i < num_files; i++) {
      StopWatch sw(env_, nullptr, 0, &micro_list[i], false);
      pending_outputs_inserted_elem_list[i] =
          CaptureCurrentFileNumberInPendingOutputs();
      meta_list[i].fd = FileDescriptor(versions_->NewFileNumber(), 0,
                                       file_info_list[i].file_size);
    }
  }

  // Copy/Move external files into DB
  std::vector<std::string> db_fname_list(num_files);
  size_t j = 0;
  for (; j < num_files; j++) {
    StopWatch sw(env_, nullptr, 0, &micro_list[j], false);
    db_fname_list[j] =
        TableFileName(immutable_db_options_.db_paths,
                      meta_list[j].fd.GetNumber(), meta_list[j].fd.GetPathId());
    if (move_file) {
      status = env_->LinkFile(file_info_list[j].file_path, db_fname_list[j]);
      if (status.IsNotSupported()) {
        // Original file is on a different FS, use copy instead of hard linking
        status =
            CopyFile(env_, file_info_list[j].file_path, db_fname_list[j], 0);
      }
    } else {
      status = CopyFile(env_, file_info_list[j].file_path, db_fname_list[j], 0);
    }
    TEST_SYNC_POINT("DBImpl::AddFile:FileCopied");
    if (!status.ok()) {
      for (size_t i = 0; i < j; i++) {
        Status s = env_->DeleteFile(db_fname_list[i]);
        if (!s.ok()) {
          Log(InfoLogLevel::WARN_LEVEL, immutable_db_options_.info_log,
              "AddFile() clean up for file %s failed : %s",
              db_fname_list[i].c_str(), s.ToString().c_str());
        }
      }
      return status;
    }
  }

  {
    InstrumentedMutexLock l(&mutex_);
    TEST_SYNC_POINT("DBImpl::AddFile:MutexLock");

    const MutableCFOptions mutable_cf_options =
        *cfd->GetLatestMutableCFOptions();

    WriteThread::Writer w;
    write_thread_.EnterUnbatched(&w, &mutex_);

    num_running_addfile_++;

    if (!skip_snapshot_check && !snapshots_.empty()) {
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

      for (size_t i = 0; i < num_files; i++) {
        StopWatch sw(env_, nullptr, 0, &micro_list[i], false);
        InternalKey range_start(file_info_list[i].smallest_key,
                                kMaxSequenceNumber, kValueTypeForSeek);
        iter->Seek(range_start.Encode());
        status = iter->status();

        if (status.ok() && iter->Valid()) {
          ParsedInternalKey seek_result;
          if (ParseInternalKey(iter->key(), &seek_result)) {
            if (user_cmp->Compare(seek_result.user_key,
                                  file_info_list[i].largest_key) <= 0) {
              status = Status::NotSupported("Cannot add overlapping range");
              break;
            }
          } else {
            status = Status::Corruption("DB have corrupted keys");
            break;
          }
        }
      }
    }

    // The levels that the files will be ingested into
    std::vector<int> target_level_list(num_files, 0);
    if (status.ok()) {
      VersionEdit edit;
      edit.SetColumnFamily(cfd->GetID());
      for (size_t i = 0; i < num_files; i++) {
        StopWatch sw(env_, nullptr, 0, &micro_list[i], false);
        // Add file to the lowest possible level
        target_level_list[i] = PickLevelForIngestedFile(cfd, file_info_list[i]);
        edit.AddFile(target_level_list[i], meta_list[i].fd.GetNumber(),
                     meta_list[i].fd.GetPathId(), meta_list[i].fd.GetFileSize(),
                     meta_list[i].smallest, meta_list[i].largest,
                     meta_list[i].smallest_seqno, meta_list[i].largest_seqno,
                     meta_list[i].marked_for_compaction);
      }
      status = versions_->LogAndApply(cfd, mutable_cf_options, &edit, &mutex_,
                                      directories_.GetDbDir());
    }
    write_thread_.ExitUnbatched(&w);

    if (status.ok()) {
      delete InstallSuperVersionAndScheduleWork(cfd, nullptr,
                                                mutable_cf_options);

      // Update internal stats for new ingested files
      uint64_t total_keys = 0;
      uint64_t total_l0_files = 0;
      for (size_t i = 0; i < num_files; i++) {
        InternalStats::CompactionStats stats(1);
        stats.micros = micro_list[i];
        stats.bytes_written = meta_list[i].fd.GetFileSize();
        stats.num_output_files = 1;
        cfd->internal_stats()->AddCompactionStats(target_level_list[i], stats);
        cfd->internal_stats()->AddCFStats(
            InternalStats::BYTES_INGESTED_ADD_FILE,
            meta_list[i].fd.GetFileSize());
        total_keys += file_info_list[i].num_entries;
        if (target_level_list[i] == 0) {
          total_l0_files += 1;
        }
        Log(InfoLogLevel::INFO_LEVEL, immutable_db_options_.info_log,
            "[AddFile] External SST file %s was ingested in L%d with path %s\n",
            file_info_list[i].file_path.c_str(), target_level_list[i],
            db_fname_list[i].c_str());
      }
      cfd->internal_stats()->AddCFStats(InternalStats::INGESTED_NUM_KEYS_TOTAL,
                                        total_keys);
      cfd->internal_stats()->AddCFStats(InternalStats::INGESTED_NUM_FILES_TOTAL,
                                        num_files);
      cfd->internal_stats()->AddCFStats(
          InternalStats::INGESTED_LEVEL0_NUM_FILES_TOTAL, total_l0_files);
    }

    for (size_t i = 0; i < num_files; i++) {
      ReleaseFileNumberFromPendingOutputs(
          pending_outputs_inserted_elem_list[i]);
    }

    num_running_addfile_--;
    if (num_running_addfile_ == 0) {
      bg_cv_.SignalAll();
    }
    TEST_SYNC_POINT("DBImpl::AddFile:MutexUnlock");
  }  // mutex_ is unlocked here;

  if (!status.ok()) {
    // We failed to add the files to the database
    for (size_t i = 0; i < num_files; i++) {
      Status s = env_->DeleteFile(db_fname_list[i]);
      if (!s.ok()) {
        Log(InfoLogLevel::WARN_LEVEL, immutable_db_options_.info_log,
            "AddFile() clean up for file %s failed : %s",
            db_fname_list[i].c_str(), s.ToString().c_str());
      }
    }
  } else if (status.ok() && move_file) {
    // The files were moved and added successfully, remove original file links
    for (size_t i = 0; i < num_files; i++) {
      Status s = env_->DeleteFile(file_info_list[i].file_path);
      if (!s.ok()) {
        Log(InfoLogLevel::WARN_LEVEL, immutable_db_options_.info_log,
            "%s was added to DB successfully but failed to remove original "
            "file "
            "link : %s",
            file_info_list[i].file_path.c_str(), s.ToString().c_str());
      }
    }
  }
  return status;
}

// Finds the lowest level in the DB that the ingested file can be added to
int DBImpl::PickLevelForIngestedFile(ColumnFamilyData* cfd,
                                     const ExternalSstFileInfo& file_info) {
  mutex_.AssertHeld();

  int target_level = 0;
  auto* vstorage = cfd->current()->storage_info();
  Slice file_smallest_user_key(file_info.smallest_key);
  Slice file_largest_user_key(file_info.largest_key);

  for (int lvl = cfd->NumberLevels() - 1; lvl >= vstorage->base_level();
       lvl--) {
    // Make sure that the file fits in Level `lvl` and dont overlap with
    // the output of any compaction running right now.
    if (vstorage->OverlapInLevel(lvl, &file_smallest_user_key,
                                 &file_largest_user_key) == false &&
        cfd->RangeOverlapWithCompaction(file_smallest_user_key,
                                        file_largest_user_key, lvl) == false) {
      // Level lvl is the lowest level that dont have any files with key
      // range overlapping with our file key range and no compactions
      // planning to add overlapping files in it.
      target_level = lvl;
      break;
    }
  }

  return target_level;
}

void DBImpl::WaitForAddFile() {
  mutex_.AssertHeld();
  while (num_running_addfile_ > 0) {
    bg_cv_.Wait();
  }
}
#endif  // ROCKSDB_LITE

}  // namespace rocksdb
