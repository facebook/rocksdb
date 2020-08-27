//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "db/external_sst_file_ingestion_job.h"

#include <algorithm>
#include <cinttypes>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/version_edit.h"
#include "file/file_util.h"
#include "file/random_access_file_reader.h"
#include "table/merging_iterator.h"
#include "table/scoped_arena_iterator.h"
#include "table/sst_file_writer_collectors.h"
#include "table/table_builder.h"
#include "test_util/sync_point.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

Status ExternalSstFileIngestionJob::Prepare(
    const std::vector<std::string>& external_files_paths,
    const std::vector<std::string>& files_checksums,
    const std::vector<std::string>& files_checksum_func_names,
    uint64_t next_file_number, SuperVersion* sv) {
  Status status;

  // Read the information of files we are ingesting
  for (const std::string& file_path : external_files_paths) {
    IngestedFileInfo file_to_ingest;
    status = GetIngestedFileInfo(file_path, &file_to_ingest, sv);
    if (!status.ok()) {
      return status;
    }
    files_to_ingest_.push_back(file_to_ingest);
  }

  for (const IngestedFileInfo& f : files_to_ingest_) {
    if (f.cf_id !=
            TablePropertiesCollectorFactory::Context::kUnknownColumnFamily &&
        f.cf_id != cfd_->GetID()) {
      return Status::InvalidArgument(
          "External file column family id don't match");
    }
  }

  const Comparator* ucmp = cfd_->internal_comparator().user_comparator();
  auto num_files = files_to_ingest_.size();
  if (num_files == 0) {
    return Status::InvalidArgument("The list of files is empty");
  } else if (num_files > 1) {
    // Verify that passed files don't have overlapping ranges
    autovector<const IngestedFileInfo*> sorted_files;
    for (size_t i = 0; i < num_files; i++) {
      sorted_files.push_back(&files_to_ingest_[i]);
    }

    std::sort(
        sorted_files.begin(), sorted_files.end(),
        [&ucmp](const IngestedFileInfo* info1, const IngestedFileInfo* info2) {
          return sstableKeyCompare(ucmp, info1->smallest_internal_key,
                                   info2->smallest_internal_key) < 0;
        });

    for (size_t i = 0; i + 1 < num_files; i++) {
      if (sstableKeyCompare(ucmp, sorted_files[i]->largest_internal_key,
                            sorted_files[i + 1]->smallest_internal_key) >= 0) {
        files_overlap_ = true;
        break;
      }
    }
  }

  if (ingestion_options_.ingest_behind && files_overlap_) {
    return Status::NotSupported("Files have overlapping ranges");
  }

  for (IngestedFileInfo& f : files_to_ingest_) {
    if (f.num_entries == 0 && f.num_range_deletions == 0) {
      return Status::InvalidArgument("File contain no entries");
    }

    if (!f.smallest_internal_key.Valid() || !f.largest_internal_key.Valid()) {
      return Status::Corruption("Generated table have corrupted keys");
    }
  }

  // Copy/Move external files into DB
  std::unordered_set<size_t> ingestion_path_ids;
  for (IngestedFileInfo& f : files_to_ingest_) {
    f.fd = FileDescriptor(next_file_number++, 0, f.file_size);
    f.copy_file = false;
    const std::string path_outside_db = f.external_file_path;
    const std::string path_inside_db =
        TableFileName(cfd_->ioptions()->cf_paths, f.fd.GetNumber(),
                      f.fd.GetPathId());
    if (ingestion_options_.move_files) {
      status =
          fs_->LinkFile(path_outside_db, path_inside_db, IOOptions(), nullptr);
      if (status.ok()) {
        // It is unsafe to assume application had sync the file and file
        // directory before ingest the file. For integrity of RocksDB we need
        // to sync the file.
        std::unique_ptr<FSWritableFile> file_to_sync;
        status = fs_->ReopenWritableFile(path_inside_db, env_options_,
                                         &file_to_sync, nullptr);
        if (status.ok()) {
          TEST_SYNC_POINT(
              "ExternalSstFileIngestionJob::BeforeSyncIngestedFile");
          status = SyncIngestedFile(file_to_sync.get());
          TEST_SYNC_POINT("ExternalSstFileIngestionJob::AfterSyncIngestedFile");
          if (!status.ok()) {
            ROCKS_LOG_WARN(db_options_.info_log,
                           "Failed to sync ingested file %s: %s",
                           path_inside_db.c_str(), status.ToString().c_str());
          }
        }
      } else if (status.IsNotSupported() &&
                 ingestion_options_.failed_move_fall_back_to_copy) {
        // Original file is on a different FS, use copy instead of hard linking.
        f.copy_file = true;
      }
    } else {
      f.copy_file = true;
    }

    if (f.copy_file) {
      TEST_SYNC_POINT_CALLBACK("ExternalSstFileIngestionJob::Prepare:CopyFile",
                               nullptr);
      // CopyFile also sync the new file.
      status = CopyFile(fs_.get(), path_outside_db, path_inside_db, 0,
                        db_options_.use_fsync, io_tracer_);
    }
    TEST_SYNC_POINT("ExternalSstFileIngestionJob::Prepare:FileAdded");
    if (!status.ok()) {
      break;
    }
    f.internal_file_path = path_inside_db;
    // Initialize the checksum information of ingested files.
    f.file_checksum = kUnknownFileChecksum;
    f.file_checksum_func_name = kUnknownFileChecksumFuncName;
    ingestion_path_ids.insert(f.fd.GetPathId());
  }

  TEST_SYNC_POINT("ExternalSstFileIngestionJob::BeforeSyncDir");
  if (status.ok()) {
    for (auto path_id : ingestion_path_ids) {
      status = directories_->GetDataDir(path_id)->Fsync(IOOptions(), nullptr);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Failed to sync directory %" ROCKSDB_PRIszt
                       " while ingest file: %s",
                       path_id, status.ToString().c_str());
        break;
      }
    }
  }
  TEST_SYNC_POINT("ExternalSstFileIngestionJob::AfterSyncDir");

  // Generate and check the sst file checksum. Note that, if
  // IngestExternalFileOptions::write_global_seqno is true, we will not update
  // the checksum information in the files_to_ingests_ here, since the file is
  // upadted with the new global_seqno. After global_seqno is updated, DB will
  // generate the new checksum and store it in the Manifest. In all other cases
  // if ingestion_options_.write_global_seqno == true and
  // verify_file_checksum is false, we only check the checksum function name.
  if (status.ok() && db_options_.file_checksum_gen_factory != nullptr) {
    if (ingestion_options_.verify_file_checksum == false &&
        files_checksums.size() == files_to_ingest_.size() &&
        files_checksum_func_names.size() == files_to_ingest_.size()) {
      // Only when verify_file_checksum == false and the checksum for ingested
      // files are provided, DB will use the provided checksum and does not
      // generate the checksum for ingested files.
      need_generate_file_checksum_ = false;
    } else {
      need_generate_file_checksum_ = true;
    }
    FileChecksumGenContext gen_context;
    std::unique_ptr<FileChecksumGenerator> file_checksum_gen =
        db_options_.file_checksum_gen_factory->CreateFileChecksumGenerator(
            gen_context);
    std::vector<std::string> generated_checksums;
    std::vector<std::string> generated_checksum_func_names;
    // Step 1: generate the checksum for ingested sst file.
    if (need_generate_file_checksum_) {
      for (size_t i = 0; i < files_to_ingest_.size(); i++) {
        std::string generated_checksum, generated_checksum_func_name;
        IOStatus io_s = GenerateOneFileChecksum(
            fs_.get(), files_to_ingest_[i].internal_file_path,
            db_options_.file_checksum_gen_factory.get(), &generated_checksum,
            &generated_checksum_func_name,
            ingestion_options_.verify_checksums_readahead_size,
            db_options_.allow_mmap_reads, io_tracer_);
        if (!io_s.ok()) {
          status = io_s;
          ROCKS_LOG_WARN(db_options_.info_log,
                         "Sst file checksum generation of file: %s failed: %s",
                         files_to_ingest_[i].internal_file_path.c_str(),
                         status.ToString().c_str());
          break;
        }
        if (ingestion_options_.write_global_seqno == false) {
          files_to_ingest_[i].file_checksum = generated_checksum;
          files_to_ingest_[i].file_checksum_func_name =
              generated_checksum_func_name;
        }
        generated_checksums.push_back(generated_checksum);
        generated_checksum_func_names.push_back(generated_checksum_func_name);
      }
    }

    // Step 2: based on the verify_file_checksum and ingested checksum
    // information, do the verification.
    if (status.ok()) {
      if (files_checksums.size() == files_to_ingest_.size() &&
          files_checksum_func_names.size() == files_to_ingest_.size()) {
        // Verify the checksum and checksum function name.
        if (ingestion_options_.verify_file_checksum) {
          for (size_t i = 0; i < files_to_ingest_.size(); i++) {
            if (files_checksum_func_names[i] !=
                generated_checksum_func_names[i]) {
              status = Status::InvalidArgument(
                  "Checksum function name does not match with the checksum "
                  "function name of this DB");
              ROCKS_LOG_WARN(
                  db_options_.info_log,
                  "Sst file checksum verification of file: %s failed: %s",
                  external_files_paths[i].c_str(), status.ToString().c_str());
              break;
            }
            if (files_checksums[i] != generated_checksums[i]) {
              status = Status::Corruption(
                  "Ingested checksum does not match with the generated "
                  "checksum");
              ROCKS_LOG_WARN(
                  db_options_.info_log,
                  "Sst file checksum verification of file: %s failed: %s",
                  files_to_ingest_[i].internal_file_path.c_str(),
                  status.ToString().c_str());
              break;
            }
          }
        } else {
          // If verify_file_checksum is not enabled, we only verify the
          // checksum function name. If it does not match, fail the ingestion.
          // If matches, we trust the ingested checksum information and store
          // in the Manifest.
          for (size_t i = 0; i < files_to_ingest_.size(); i++) {
            if (files_checksum_func_names[i] != file_checksum_gen->Name()) {
              status = Status::InvalidArgument(
                  "Checksum function name does not match with the checksum "
                  "function name of this DB");
              ROCKS_LOG_WARN(
                  db_options_.info_log,
                  "Sst file checksum verification of file: %s failed: %s",
                  external_files_paths[i].c_str(), status.ToString().c_str());
              break;
            }
            files_to_ingest_[i].file_checksum = files_checksums[i];
            files_to_ingest_[i].file_checksum_func_name =
                files_checksum_func_names[i];
          }
        }
      } else if (files_checksums.size() != files_checksum_func_names.size() ||
                 (files_checksums.size() == files_checksum_func_names.size() &&
                  files_checksums.size() != 0)) {
        // The checksum or checksum function name vector are not both empty
        // and they are incomplete.
        status = Status::InvalidArgument(
            "The checksum information of ingested sst files are nonempty and "
            "the size of checksums or the size of the checksum function "
            "names "
            "does not match with the number of ingested sst files");
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "The ingested sst files checksum information is incomplete: %s",
            status.ToString().c_str());
      }
    }
  }

  // TODO: The following is duplicated with Cleanup().
  if (!status.ok()) {
    // We failed, remove all files that we copied into the db
    for (IngestedFileInfo& f : files_to_ingest_) {
      if (f.internal_file_path.empty()) {
        continue;
      }
      Status s = env_->DeleteFile(f.internal_file_path);
      if (!s.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "AddFile() clean up for file %s failed : %s",
                       f.internal_file_path.c_str(), s.ToString().c_str());
      }
    }
  }

  return status;
}

Status ExternalSstFileIngestionJob::NeedsFlush(bool* flush_needed,
                                               SuperVersion* super_version) {
  autovector<Range> ranges;
  for (const IngestedFileInfo& file_to_ingest : files_to_ingest_) {
    ranges.emplace_back(file_to_ingest.smallest_internal_key.user_key(),
                        file_to_ingest.largest_internal_key.user_key());
  }
  Status status =
      cfd_->RangesOverlapWithMemtables(ranges, super_version, flush_needed);
  if (status.ok() && *flush_needed &&
      !ingestion_options_.allow_blocking_flush) {
    status = Status::InvalidArgument("External file requires flush");
  }
  return status;
}

// REQUIRES: we have become the only writer by entering both write_thread_ and
// nonmem_write_thread_
Status ExternalSstFileIngestionJob::Run() {
  Status status;
  SuperVersion* super_version = cfd_->GetSuperVersion();
#ifndef NDEBUG
  // We should never run the job with a memtable that is overlapping
  // with the files we are ingesting
  bool need_flush = false;
  status = NeedsFlush(&need_flush, super_version);
  assert(status.ok() && need_flush == false);
#endif

  bool force_global_seqno = false;

  if (ingestion_options_.snapshot_consistency && !db_snapshots_->empty()) {
    // We need to assign a global sequence number to all the files even
    // if the don't overlap with any ranges since we have snapshots
    force_global_seqno = true;
  }
  // It is safe to use this instead of LastAllocatedSequence since we are
  // the only active writer, and hence they are equal
  SequenceNumber last_seqno = versions_->LastSequence();
  edit_.SetColumnFamily(cfd_->GetID());
  // The levels that the files will be ingested into

  for (IngestedFileInfo& f : files_to_ingest_) {
    SequenceNumber assigned_seqno = 0;
    if (ingestion_options_.ingest_behind) {
      status = CheckLevelForIngestedBehindFile(&f);
    } else {
      status = AssignLevelAndSeqnoForIngestedFile(
          super_version, force_global_seqno, cfd_->ioptions()->compaction_style,
          last_seqno, &f, &assigned_seqno);
    }
    if (!status.ok()) {
      return status;
    }
    status = AssignGlobalSeqnoForIngestedFile(&f, assigned_seqno);
    TEST_SYNC_POINT_CALLBACK("ExternalSstFileIngestionJob::Run",
                             &assigned_seqno);
    if (assigned_seqno > last_seqno) {
      assert(assigned_seqno == last_seqno + 1);
      last_seqno = assigned_seqno;
      ++consumed_seqno_count_;
    }
    if (!status.ok()) {
      return status;
    }

    status = GenerateChecksumForIngestedFile(&f);
    if (!status.ok()) {
      return status;
    }

    // We use the import time as the ancester time. This is the time the data
    // is written to the database.
    int64_t temp_current_time = 0;
    uint64_t current_time = kUnknownFileCreationTime;
    uint64_t oldest_ancester_time = kUnknownOldestAncesterTime;
    if (env_->GetCurrentTime(&temp_current_time).ok()) {
      current_time = oldest_ancester_time =
          static_cast<uint64_t>(temp_current_time);
    }

    edit_.AddFile(f.picked_level, f.fd.GetNumber(), f.fd.GetPathId(),
                  f.fd.GetFileSize(), f.smallest_internal_key,
                  f.largest_internal_key, f.assigned_seqno, f.assigned_seqno,
                  false, kInvalidBlobFileNumber, oldest_ancester_time,
                  current_time, f.file_checksum, f.file_checksum_func_name);
  }
  return status;
}

void ExternalSstFileIngestionJob::UpdateStats() {
  // Update internal stats for new ingested files
  uint64_t total_keys = 0;
  uint64_t total_l0_files = 0;
  uint64_t total_time = env_->NowMicros() - job_start_time_;

  EventLoggerStream stream = event_logger_->Log();
  stream << "event"
         << "ingest_finished";
  stream << "files_ingested";
  stream.StartArray();

  for (IngestedFileInfo& f : files_to_ingest_) {
    InternalStats::CompactionStats stats(CompactionReason::kExternalSstIngestion, 1);
    stats.micros = total_time;
    // If actual copy occurred for this file, then we need to count the file
    // size as the actual bytes written. If the file was linked, then we ignore
    // the bytes written for file metadata.
    // TODO (yanqin) maybe account for file metadata bytes for exact accuracy?
    if (f.copy_file) {
      stats.bytes_written = f.fd.GetFileSize();
    } else {
      stats.bytes_moved = f.fd.GetFileSize();
    }
    stats.num_output_files = 1;
    cfd_->internal_stats()->AddCompactionStats(f.picked_level,
                                               Env::Priority::USER, stats);
    cfd_->internal_stats()->AddCFStats(InternalStats::BYTES_INGESTED_ADD_FILE,
                                       f.fd.GetFileSize());
    total_keys += f.num_entries;
    if (f.picked_level == 0) {
      total_l0_files += 1;
    }
    ROCKS_LOG_INFO(
        db_options_.info_log,
        "[AddFile] External SST file %s was ingested in L%d with path %s "
        "(global_seqno=%" PRIu64 ")\n",
        f.external_file_path.c_str(), f.picked_level,
        f.internal_file_path.c_str(), f.assigned_seqno);
    stream << "file" << f.internal_file_path << "level" << f.picked_level;
  }
  stream.EndArray();

  stream << "lsm_state";
  stream.StartArray();
  auto vstorage = cfd_->current()->storage_info();
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    stream << vstorage->NumLevelFiles(level);
  }
  stream.EndArray();

  cfd_->internal_stats()->AddCFStats(InternalStats::INGESTED_NUM_KEYS_TOTAL,
                                     total_keys);
  cfd_->internal_stats()->AddCFStats(InternalStats::INGESTED_NUM_FILES_TOTAL,
                                     files_to_ingest_.size());
  cfd_->internal_stats()->AddCFStats(
      InternalStats::INGESTED_LEVEL0_NUM_FILES_TOTAL, total_l0_files);
}

void ExternalSstFileIngestionJob::Cleanup(const Status& status) {
  if (!status.ok()) {
    // We failed to add the files to the database
    // remove all the files we copied
    for (IngestedFileInfo& f : files_to_ingest_) {
      if (f.internal_file_path.empty()) {
        continue;
      }
      Status s = env_->DeleteFile(f.internal_file_path);
      if (!s.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "AddFile() clean up for file %s failed : %s",
                       f.internal_file_path.c_str(), s.ToString().c_str());
      }
    }
    consumed_seqno_count_ = 0;
    files_overlap_ = false;
  } else if (status.ok() && ingestion_options_.move_files) {
    // The files were moved and added successfully, remove original file links
    for (IngestedFileInfo& f : files_to_ingest_) {
      Status s = env_->DeleteFile(f.external_file_path);
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

Status ExternalSstFileIngestionJob::GetIngestedFileInfo(
    const std::string& external_file, IngestedFileInfo* file_to_ingest,
    SuperVersion* sv) {
  file_to_ingest->external_file_path = external_file;

  // Get external file size
  Status status = fs_->GetFileSize(external_file, IOOptions(),
                                   &file_to_ingest->file_size, nullptr);
  if (!status.ok()) {
    return status;
  }

  // Create TableReader for external file
  std::unique_ptr<TableReader> table_reader;
  std::unique_ptr<FSRandomAccessFile> sst_file;
  std::unique_ptr<RandomAccessFileReader> sst_file_reader;

  status = fs_->NewRandomAccessFile(external_file, env_options_,
                                    &sst_file, nullptr);
  if (!status.ok()) {
    return status;
  }
  sst_file_reader.reset(new RandomAccessFileReader(
      std::move(sst_file), external_file, nullptr /*Env*/, io_tracer_));

  status = cfd_->ioptions()->table_factory->NewTableReader(
      TableReaderOptions(*cfd_->ioptions(),
                         sv->mutable_cf_options.prefix_extractor.get(),
                         env_options_, cfd_->internal_comparator()),
      std::move(sst_file_reader), file_to_ingest->file_size, &table_reader);
  if (!status.ok()) {
    return status;
  }

  if (ingestion_options_.verify_checksums_before_ingest) {
    // If customized readahead size is needed, we can pass a user option
    // all the way to here. Right now we just rely on the default readahead
    // to keep things simple.
    ReadOptions ro;
    ro.readahead_size = ingestion_options_.verify_checksums_readahead_size;
    status = table_reader->VerifyChecksum(
        ro, TableReaderCaller::kExternalSSTIngestion);
  }
  if (!status.ok()) {
    return status;
  }

  // Get the external file properties
  auto props = table_reader->GetTableProperties();
  const auto& uprops = props->user_collected_properties;

  // Get table version
  auto version_iter = uprops.find(ExternalSstFilePropertyNames::kVersion);
  if (version_iter == uprops.end()) {
    return Status::Corruption("External file version not found");
  }
  file_to_ingest->version = DecodeFixed32(version_iter->second.c_str());

  auto seqno_iter = uprops.find(ExternalSstFilePropertyNames::kGlobalSeqno);
  if (file_to_ingest->version == 2) {
    // version 2 imply that we have global sequence number
    if (seqno_iter == uprops.end()) {
      return Status::Corruption(
          "External file global sequence number not found");
    }

    // Set the global sequence number
    file_to_ingest->original_seqno = DecodeFixed64(seqno_iter->second.c_str());
    auto offsets_iter = props->properties_offsets.find(
        ExternalSstFilePropertyNames::kGlobalSeqno);
    if (offsets_iter == props->properties_offsets.end() ||
        offsets_iter->second == 0) {
      file_to_ingest->global_seqno_offset = 0;
      return Status::Corruption("Was not able to find file global seqno field");
    }
    file_to_ingest->global_seqno_offset = static_cast<size_t>(offsets_iter->second);
  } else if (file_to_ingest->version == 1) {
    // SST file V1 should not have global seqno field
    assert(seqno_iter == uprops.end());
    file_to_ingest->original_seqno = 0;
    if (ingestion_options_.allow_blocking_flush ||
            ingestion_options_.allow_global_seqno) {
      return Status::InvalidArgument(
            "External SST file V1 does not support global seqno");
    }
  } else {
    return Status::InvalidArgument("External file version is not supported");
  }
  // Get number of entries in table
  file_to_ingest->num_entries = props->num_entries;
  file_to_ingest->num_range_deletions = props->num_range_deletions;

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
  std::unique_ptr<InternalIterator> range_del_iter(
      table_reader->NewRangeTombstoneIterator(ro));

  // Get first (smallest) and last (largest) key from file.
  file_to_ingest->smallest_internal_key =
      InternalKey("", 0, ValueType::kTypeValue);
  file_to_ingest->largest_internal_key =
      InternalKey("", 0, ValueType::kTypeValue);
  bool bounds_set = false;
  iter->SeekToFirst();
  if (iter->Valid()) {
    if (!ParseInternalKey(iter->key(), &key)) {
      return Status::Corruption("external file have corrupted keys");
    }
    if (key.sequence != 0) {
      return Status::Corruption("external file have non zero sequence number");
    }
    file_to_ingest->smallest_internal_key.SetFrom(key);

    iter->SeekToLast();
    if (!ParseInternalKey(iter->key(), &key)) {
      return Status::Corruption("external file have corrupted keys");
    }
    if (key.sequence != 0) {
      return Status::Corruption("external file have non zero sequence number");
    }
    file_to_ingest->largest_internal_key.SetFrom(key);

    bounds_set = true;
  }

  // We may need to adjust these key bounds, depending on whether any range
  // deletion tombstones extend past them.
  const Comparator* ucmp = cfd_->internal_comparator().user_comparator();
  if (range_del_iter != nullptr) {
    for (range_del_iter->SeekToFirst(); range_del_iter->Valid();
         range_del_iter->Next()) {
      if (!ParseInternalKey(range_del_iter->key(), &key)) {
        return Status::Corruption("external file have corrupted keys");
      }
      RangeTombstone tombstone(key, range_del_iter->value());

      InternalKey start_key = tombstone.SerializeKey();
      if (!bounds_set ||
          sstableKeyCompare(ucmp, start_key,
                            file_to_ingest->smallest_internal_key) < 0) {
        file_to_ingest->smallest_internal_key = start_key;
      }
      InternalKey end_key = tombstone.SerializeEndKey();
      if (!bounds_set ||
          sstableKeyCompare(ucmp, end_key,
                            file_to_ingest->largest_internal_key) > 0) {
        file_to_ingest->largest_internal_key = end_key;
      }
      bounds_set = true;
    }
  }

  file_to_ingest->cf_id = static_cast<uint32_t>(props->column_family_id);

  file_to_ingest->table_properties = *props;

  return status;
}

Status ExternalSstFileIngestionJob::AssignLevelAndSeqnoForIngestedFile(
    SuperVersion* sv, bool force_global_seqno, CompactionStyle compaction_style,
    SequenceNumber last_seqno, IngestedFileInfo* file_to_ingest,
    SequenceNumber* assigned_seqno) {
  Status status;
  *assigned_seqno = 0;
  if (force_global_seqno) {
    *assigned_seqno = last_seqno + 1;
    if (compaction_style == kCompactionStyleUniversal || files_overlap_) {
      file_to_ingest->picked_level = 0;
      return status;
    }
  }

  bool overlap_with_db = false;
  Arena arena;
  ReadOptions ro;
  ro.total_order_seek = true;
  int target_level = 0;
  auto* vstorage = cfd_->current()->storage_info();

  for (int lvl = 0; lvl < cfd_->NumberLevels(); lvl++) {
    if (lvl > 0 && lvl < vstorage->base_level()) {
      continue;
    }

    if (vstorage->NumLevelFiles(lvl) > 0) {
      bool overlap_with_level = false;
      status = sv->current->OverlapWithLevelIterator(
          ro, env_options_, file_to_ingest->smallest_internal_key.user_key(),
          file_to_ingest->largest_internal_key.user_key(), lvl,
          &overlap_with_level);
      if (!status.ok()) {
        return status;
      }
      if (overlap_with_level) {
        // We must use L0 or any level higher than `lvl` to be able to overwrite
        // the keys that we overlap with in this level, We also need to assign
        // this file a seqno to overwrite the existing keys in level `lvl`
        overlap_with_db = true;
        break;
      }

      if (compaction_style == kCompactionStyleUniversal && lvl != 0) {
        const std::vector<FileMetaData*>& level_files =
            vstorage->LevelFiles(lvl);
        const SequenceNumber level_largest_seqno =
            (*max_element(level_files.begin(), level_files.end(),
                          [](FileMetaData* f1, FileMetaData* f2) {
                            return f1->fd.largest_seqno < f2->fd.largest_seqno;
                          }))
                ->fd.largest_seqno;
        // should only assign seqno to current level's largest seqno when
        // the file fits
        if (level_largest_seqno != 0 &&
            IngestedFileFitInLevel(file_to_ingest, lvl)) {
          *assigned_seqno = level_largest_seqno;
        } else {
          continue;
        }
      }
    } else if (compaction_style == kCompactionStyleUniversal) {
      continue;
    }

    // We don't overlap with any keys in this level, but we still need to check
    // if our file can fit in it
    if (IngestedFileFitInLevel(file_to_ingest, lvl)) {
      target_level = lvl;
    }
  }
  // If files overlap, we have to ingest them at level 0 and assign the newest
  // sequence number
  if (files_overlap_) {
    target_level = 0;
    *assigned_seqno = last_seqno + 1;
  }
 TEST_SYNC_POINT_CALLBACK(
      "ExternalSstFileIngestionJob::AssignLevelAndSeqnoForIngestedFile",
      &overlap_with_db);
  file_to_ingest->picked_level = target_level;
  if (overlap_with_db && *assigned_seqno == 0) {
    *assigned_seqno = last_seqno + 1;
  }
  return status;
}

Status ExternalSstFileIngestionJob::CheckLevelForIngestedBehindFile(
    IngestedFileInfo* file_to_ingest) {
  auto* vstorage = cfd_->current()->storage_info();
  // first check if new files fit in the bottommost level
  int bottom_lvl = cfd_->NumberLevels() - 1;
  if(!IngestedFileFitInLevel(file_to_ingest, bottom_lvl)) {
    return Status::InvalidArgument(
      "Can't ingest_behind file as it doesn't fit "
      "at the bottommost level!");
  }

  // second check if despite allow_ingest_behind=true we still have 0 seqnums
  // at some upper level
  for (int lvl = 0; lvl < cfd_->NumberLevels() - 1; lvl++) {
    for (auto file : vstorage->LevelFiles(lvl)) {
      if (file->fd.smallest_seqno == 0) {
        return Status::InvalidArgument(
          "Can't ingest_behind file as despite allow_ingest_behind=true "
          "there are files with 0 seqno in database at upper levels!");
      }
    }
  }

  file_to_ingest->picked_level = bottom_lvl;
  return Status::OK();
}

Status ExternalSstFileIngestionJob::AssignGlobalSeqnoForIngestedFile(
    IngestedFileInfo* file_to_ingest, SequenceNumber seqno) {
  if (file_to_ingest->original_seqno == seqno) {
    // This file already have the correct global seqno
    return Status::OK();
  } else if (!ingestion_options_.allow_global_seqno) {
    return Status::InvalidArgument("Global seqno is required, but disabled");
  } else if (file_to_ingest->global_seqno_offset == 0) {
    return Status::InvalidArgument(
        "Trying to set global seqno for a file that don't have a global seqno "
        "field");
  }

  if (ingestion_options_.write_global_seqno) {
    // Determine if we can write global_seqno to a given offset of file.
    // If the file system does not support random write, then we should not.
    // Otherwise we should.
    std::unique_ptr<FSRandomRWFile> rwfile;
    Status status =
        fs_->NewRandomRWFile(file_to_ingest->internal_file_path, env_options_,
                             &rwfile, nullptr);
    if (status.ok()) {
      std::string seqno_val;
      PutFixed64(&seqno_val, seqno);
      status = rwfile->Write(file_to_ingest->global_seqno_offset, seqno_val,
                             IOOptions(), nullptr);
      if (status.ok()) {
        TEST_SYNC_POINT("ExternalSstFileIngestionJob::BeforeSyncGlobalSeqno");
        status = SyncIngestedFile(rwfile.get());
        TEST_SYNC_POINT("ExternalSstFileIngestionJob::AfterSyncGlobalSeqno");
        if (!status.ok()) {
          ROCKS_LOG_WARN(db_options_.info_log,
                         "Failed to sync ingested file %s after writing global "
                         "sequence number: %s",
                         file_to_ingest->internal_file_path.c_str(),
                         status.ToString().c_str());
        }
      }
      if (!status.ok()) {
        return status;
      }
    } else if (!status.IsNotSupported()) {
      return status;
    }
  }

  file_to_ingest->assigned_seqno = seqno;
  return Status::OK();
}

IOStatus ExternalSstFileIngestionJob::GenerateChecksumForIngestedFile(
    IngestedFileInfo* file_to_ingest) {
  if (db_options_.file_checksum_gen_factory == nullptr ||
      need_generate_file_checksum_ == false ||
      ingestion_options_.write_global_seqno == false) {
    // If file_checksum_gen_factory is not set, we are not able to generate
    // the checksum. if write_global_seqno is false, it means we will use
    // file checksum generated during Prepare(). This step will be skipped.
    return IOStatus::OK();
  }
  std::string file_checksum, file_checksum_func_name;
  IOStatus io_s = GenerateOneFileChecksum(
      fs_.get(), file_to_ingest->internal_file_path,
      db_options_.file_checksum_gen_factory.get(), &file_checksum,
      &file_checksum_func_name,
      ingestion_options_.verify_checksums_readahead_size,
      db_options_.allow_mmap_reads, io_tracer_);
  if (!io_s.ok()) {
    return io_s;
  }
  file_to_ingest->file_checksum = file_checksum;
  file_to_ingest->file_checksum_func_name = file_checksum_func_name;
  return IOStatus::OK();
}

bool ExternalSstFileIngestionJob::IngestedFileFitInLevel(
    const IngestedFileInfo* file_to_ingest, int level) {
  if (level == 0) {
    // Files can always fit in L0
    return true;
  }

  auto* vstorage = cfd_->current()->storage_info();
  Slice file_smallest_user_key(
      file_to_ingest->smallest_internal_key.user_key());
  Slice file_largest_user_key(file_to_ingest->largest_internal_key.user_key());

  if (vstorage->OverlapInLevel(level, &file_smallest_user_key,
                               &file_largest_user_key)) {
    // File overlap with another files in this level, we cannot
    // add it to this level
    return false;
  }
  if (cfd_->RangeOverlapWithCompaction(file_smallest_user_key,
                                       file_largest_user_key, level)) {
    // File overlap with a running compaction output that will be stored
    // in this level, we cannot add this file to this level
    return false;
  }

  // File did not overlap with level files, our compaction output
  return true;
}

template <typename TWritableFile>
Status ExternalSstFileIngestionJob::SyncIngestedFile(TWritableFile* file) {
  assert(file != nullptr);
  if (db_options_.use_fsync) {
    return file->Fsync(IOOptions(), nullptr);
  } else {
    return file->Sync(IOOptions(), nullptr);
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE
