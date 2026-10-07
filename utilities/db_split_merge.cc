//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/db_split_merge.h"

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/write_batch.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {

class OpenedCheckpoint {
 public:
  OpenedCheckpoint() = default;
  OpenedCheckpoint(const OpenedCheckpoint&) = delete;
  OpenedCheckpoint& operator=(const OpenedCheckpoint&) = delete;
  OpenedCheckpoint(OpenedCheckpoint&&) = delete;
  OpenedCheckpoint& operator=(OpenedCheckpoint&&) = delete;

  ~OpenedCheckpoint() {
    for (ColumnFamilyHandle* handle : handles) {
      delete handle;
    }
  }

  std::unique_ptr<DB> db;
  std::vector<ColumnFamilyHandle*> handles;
  std::unordered_map<uint32_t, ColumnFamilyHandle*> handles_by_id;
};

Status DestroyDBDirectory(Env* env, const std::string& path) {
  Options options;
  options.env = env;
  return DestroyDB(path, options);
}

class ScopedDBDirectoryCleanup {
 public:
  ScopedDBDirectoryCleanup(Env* env, std::string path)
      : env_(env), path_(std::move(path)) {}
  ScopedDBDirectoryCleanup(const ScopedDBDirectoryCleanup&) = delete;
  ScopedDBDirectoryCleanup& operator=(const ScopedDBDirectoryCleanup&) = delete;
  ScopedDBDirectoryCleanup(ScopedDBDirectoryCleanup&&) = delete;
  ScopedDBDirectoryCleanup& operator=(ScopedDBDirectoryCleanup&&) = delete;

  ~ScopedDBDirectoryCleanup() {
    if (enabled_) {
      DestroyDBDirectory(env_, path_).PermitUncheckedError();
    }
  }

  void Release() { enabled_ = false; }

 private:
  Env* env_;
  std::string path_;
  bool enabled_ = true;
};

Status CheckPathDoesNotExist(Env* env, const std::string& path,
                             const std::string& description) {
  if (path.empty()) {
    return Status::InvalidArgument(description + " is empty");
  }
  Status status = env->FileExists(path);
  if (status.ok()) {
    return Status::InvalidArgument(description + " already exists: " + path);
  }
  if (status.IsNotFound()) {
    return Status::OK();
  }
  return status;
}

// Checkpoint::CreateCheckpoint() stages into "<path>.tmp" and deletes any files
// already there, so that path must not exist either.
Status CheckCheckpointPathsDoNotExist(Env* env, const std::string& path,
                                      const std::string& description) {
  Status status = CheckPathDoesNotExist(env, path, description);
  if (!status.ok()) {
    return status;
  }
  const size_t final_nonslash_idx = path.find_last_not_of('/');
  if (final_nonslash_idx == std::string::npos) {
    return Status::InvalidArgument(description +
                                   " is not a valid directory name: " + path);
  }
  const std::string staging_path =
      path.substr(0, final_nonslash_idx + 1) + ".tmp";
  return CheckPathDoesNotExist(env, staging_path,
                               description + " staging path");
}

Status ValidateColumnFamilyHandle(DB* db, ColumnFamilyHandle* handle,
                                  const char* description) {
  if (handle == nullptr) {
    return Status::InvalidArgument(std::string(description) + " is null");
  }
  DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
  ColumnFamilyHandleImpl* handle_impl =
      static_cast_with_check<ColumnFamilyHandleImpl>(handle);
  if (handle_impl->db() != db_impl) {
    return Status::InvalidArgument(std::string(description) +
                                   " does not belong to the expected DB");
  }
  if (handle_impl->cfd()->IsDropped()) {
    return Status::InvalidArgument(std::string(description) +
                                   " refers to a dropped column family");
  }
  return Status::OK();
}

Status ValidateRange(ColumnFamilyHandle* handle, const Slice& begin_key,
                     const Slice& end_key) {
  if (handle->GetComparator()->Compare(begin_key, end_key) >= 0) {
    return Status::InvalidArgument("begin_key must compare before end_key");
  }
  return Status::OK();
}

Status ValidateSupportedColumnFamily(ColumnFamilyHandle* handle,
                                     ColumnFamilyDescriptor* descriptor) {
  Status status = handle->GetDescriptor(descriptor);
  if (!status.ok()) {
    return status;
  }
  if (handle->GetComparator()->timestamp_size() != 0) {
    return Status::NotSupported(
        "user-defined timestamps are not supported by DB split/merge");
  }
  if (descriptor->options.enable_blob_files) {
    return Status::NotSupported(
        "integrated BlobDB is not supported by DB split/merge");
  }
  if (descriptor->options.compaction_style != kCompactionStyleLevel &&
      descriptor->options.compaction_style != kCompactionStyleUniversal) {
    return Status::NotSupported(
        "only leveled and universal compaction are supported by DB "
        "split/merge");
  }
  return Status::OK();
}

Status ValidateSplits(DB* source, const SplitDBOptions& options,
                      std::vector<ColumnFamilyHandle*>* source_cfs) {
  if (options.column_family_splits.empty()) {
    return Status::InvalidArgument("column_family_splits is empty");
  }
  std::unordered_set<uint32_t> source_cf_ids;
  source_cfs->reserve(options.column_family_splits.size());
  for (const ColumnFamilySplit& split : options.column_family_splits) {
    Status status =
        ValidateColumnFamilyHandle(source, split.source_cf, "source CF");
    if (!status.ok()) {
      return status;
    }
    if (!source_cf_ids.insert(split.source_cf->GetID()).second) {
      return Status::InvalidArgument(
          "column_family_splits contains a duplicate source CF");
    }
    status = ValidateRange(split.source_cf, split.begin_key, split.end_key);
    if (!status.ok()) {
      return status;
    }
    ColumnFamilyDescriptor descriptor;
    status = ValidateSupportedColumnFamily(split.source_cf, &descriptor);
    if (!status.ok()) {
      return status;
    }
    source_cfs->push_back(split.source_cf);
  }
  return Status::OK();
}

Status ValidateCompatibleColumnFamilies(
    const ColumnFamilyDescriptor& source,
    const ColumnFamilyDescriptor& destination) {
  const ColumnFamilyOptions& source_options = source.options;
  const ColumnFamilyOptions& destination_options = destination.options;
  if (std::string(source_options.comparator->Name()) !=
      destination_options.comparator->Name()) {
    return Status::InvalidArgument("source and destination comparators differ");
  }

  const char* source_merge_operator =
      source_options.merge_operator == nullptr
          ? nullptr
          : source_options.merge_operator->Name();
  const char* destination_merge_operator =
      destination_options.merge_operator == nullptr
          ? nullptr
          : destination_options.merge_operator->Name();
  if ((source_merge_operator == nullptr) !=
          (destination_merge_operator == nullptr) ||
      (source_merge_operator != nullptr &&
       std::string(source_merge_operator) != destination_merge_operator)) {
    return Status::InvalidArgument(
        "source and destination merge operators differ");
  }

  if (std::string(source_options.table_factory->Name()) !=
      destination_options.table_factory->Name()) {
    return Status::InvalidArgument(
        "source and destination table factories differ");
  }
  return Status::OK();
}

Status CreateCheckpoint(DB* source,
                        const std::vector<ColumnFamilyHandle*>& source_cfs,
                        const std::string& checkpoint_directory,
                        uint64_t* checkpoint_sequence) {
  Checkpoint* checkpoint = nullptr;
  Status status = Checkpoint::Create(source, &checkpoint);
  if (!status.ok()) {
    return status;
  }
  std::unique_ptr<Checkpoint> checkpoint_guard(checkpoint);
  return checkpoint->CreateCheckpoint(checkpoint_directory, source_cfs,
                                      /*log_size_for_flush=*/0,
                                      checkpoint_sequence);
}

// Default column family first, then the selected ones.
Status GetSourceDescriptors(DB* source,
                            const std::vector<ColumnFamilyHandle*>& source_cfs,
                            std::vector<ColumnFamilyDescriptor>* descriptors) {
  descriptors->reserve(source_cfs.size() + 1);
  const uint32_t default_cf_id = source->DefaultColumnFamily()->GetID();
  ColumnFamilyDescriptor default_descriptor;
  Status status =
      source->DefaultColumnFamily()->GetDescriptor(&default_descriptor);
  if (!status.ok()) {
    return status;
  }
  default_descriptor.options.cf_paths.clear();
  descriptors->push_back(std::move(default_descriptor));

  for (ColumnFamilyHandle* source_cf : source_cfs) {
    if (source_cf->GetID() == default_cf_id) {
      continue;
    }
    ColumnFamilyDescriptor descriptor;
    status = source_cf->GetDescriptor(&descriptor);
    if (!status.ok()) {
      return status;
    }
    descriptor.options.cf_paths.clear();
    descriptors->push_back(std::move(descriptor));
  }
  return Status::OK();
}

// The checkpoint is a separate DB, so it must not be tracked, observed, or
// served by objects that belong to source.
DBOptions GetCheckpointDBOptions(DB* source) {
  DBOptions db_options = source->GetDBOptions();
  db_options.create_if_missing = false;
  db_options.create_missing_column_families = false;
  db_options.error_if_exists = false;
  db_options.db_paths.clear();
  db_options.wal_dir.clear();
  db_options.db_log_dir.clear();
  db_options.sst_file_manager.reset();
  db_options.listeners.clear();
  db_options.statistics.reset();
  db_options.info_log.reset();
  db_options.compaction_service.reset();
  return db_options;
}

Status OpenCheckpointDB(DB* source,
                        const std::vector<ColumnFamilyHandle*>& source_cfs,
                        const std::string& checkpoint_directory,
                        OpenedCheckpoint* checkpoint) {
  std::vector<ColumnFamilyDescriptor> descriptors;
  Status status = GetSourceDescriptors(source, source_cfs, &descriptors);
  if (!status.ok()) {
    return status;
  }
  // Clipping must not run source compaction filters over the copied data,
  // and the file set must not change before it is validated or ingested.
  for (ColumnFamilyDescriptor& descriptor : descriptors) {
    descriptor.options.disable_auto_compactions = true;
    descriptor.options.compaction_filter = nullptr;
    descriptor.options.compaction_filter_factory.reset();
  }

  status = DB::Open(GetCheckpointDBOptions(source), checkpoint_directory,
                    descriptors, &checkpoint->handles, &checkpoint->db);
  if (!status.ok()) {
    return status;
  }
  for (ColumnFamilyHandle* handle : checkpoint->handles) {
    checkpoint->handles_by_id.emplace(handle->GetID(), handle);
  }
  return Status::OK();
}

// DB::Open() persists the OPTIONS file, so reopening with the unmodified source
// column family options replaces the clipping overrides recorded by
// OpenCheckpointDB().
Status PersistSourceOptions(DB* source,
                            const std::vector<ColumnFamilyHandle*>& source_cfs,
                            const std::string& db_path) {
  std::vector<ColumnFamilyDescriptor> descriptors;
  Status status = GetSourceDescriptors(source, source_cfs, &descriptors);
  if (!status.ok()) {
    return status;
  }
  OpenedCheckpoint reopened;
  return DB::Open(GetCheckpointDBOptions(source), db_path, descriptors,
                  &reopened.handles, &reopened.db);
}

ColumnFamilyHandle* FindCheckpointColumnFamily(
    const OpenedCheckpoint& checkpoint, ColumnFamilyHandle* source_cf) {
  const std::unordered_map<uint32_t, ColumnFamilyHandle*>::const_iterator it =
      checkpoint.handles_by_id.find(source_cf->GetID());
  return it == checkpoint.handles_by_id.end() ? nullptr : it->second;
}

bool IncludesColumnFamily(const std::vector<ColumnFamilyHandle*>& cfs,
                          uint32_t cf_id) {
  return std::any_of(cfs.begin(), cfs.end(), [cf_id](ColumnFamilyHandle* cf) {
    return cf->GetID() == cf_id;
  });
}

// DeleteFilesInRanges() drops non-L0 files without reading them, so only the
// remaining L0 files are rewritten by the tombstone compaction.
Status EmptyColumnFamily(DB* db, ColumnFamilyHandle* column_family) {
  FlushOptions flush_options;
  flush_options.allow_write_stall = true;
  Status status = db->Flush(flush_options, column_family);
  if (!status.ok()) {
    return status;
  }
  const RangeOpt whole_key_space;
  status = DeleteFilesInRanges(db, column_family, &whole_key_space, 1);
  if (!status.ok()) {
    return status;
  }

  ColumnFamilyMetaData metadata;
  db->GetColumnFamilyMetaData(column_family, &metadata);
  if (metadata.file_count == 0) {
    return Status::OK();
  }
  const Comparator* comparator = column_family->GetComparator();
  const SstFileMetaData* smallest = nullptr;
  const SstFileMetaData* largest = nullptr;
  for (const LevelMetaData& level : metadata.levels) {
    for (const SstFileMetaData& file : level.files) {
      if (smallest == nullptr ||
          comparator->Compare(file.smallestkey, smallest->smallestkey) < 0) {
        smallest = &file;
      }
      if (largest == nullptr ||
          comparator->Compare(file.largestkey, largest->largestkey) > 0) {
        largest = &file;
      }
    }
  }
  if (smallest == nullptr || largest == nullptr) {
    return Status::Corruption("column family file metadata is inconsistent");
  }
  WriteBatch batch;
  status = batch.DeleteRange(column_family, smallest->smallestkey,
                             largest->largestkey);
  if (status.ok()) {
    status = batch.Delete(column_family, largest->largestkey);
  }
  if (status.ok()) {
    status = db->Write(WriteOptions(), &batch);
  }
  if (!status.ok()) {
    return status;
  }
  CompactRangeOptions compact_options;
  compact_options.exclusive_manual_compaction = true;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  status = db->CompactRange(compact_options, column_family, nullptr, nullptr);
  if (!status.ok()) {
    return status;
  }

  std::unique_ptr<Iterator> iterator(
      db->NewIterator(ReadOptions(), column_family));
  iterator->SeekToFirst();
  if (iterator->Valid()) {
    return Status::Corruption("column family is not empty after clearing it");
  }
  return iterator->status();
}

Status VerifyRangeContents(DB* db, ColumnFamilyHandle* column_family,
                           const Slice& begin_key, const Slice& end_key,
                           bool expect_inside) {
  const Comparator* comparator = column_family->GetComparator();
  std::unique_ptr<Iterator> iterator(
      db->NewIterator(ReadOptions(), column_family));
  if (expect_inside) {
    iterator->SeekToFirst();
    if (iterator->Valid() &&
        comparator->Compare(iterator->key(), begin_key) < 0) {
      return Status::Corruption(
          "clipped column family contains a key before "
          "begin_key");
    }
    iterator->Seek(end_key);
    if (iterator->Valid()) {
      return Status::Corruption(
          "clipped column family contains a key at or "
          "after end_key");
    }
  } else {
    iterator->Seek(begin_key);
    if (iterator->Valid() &&
        comparator->Compare(iterator->key(), end_key) < 0) {
      return Status::Corruption(
          "source column family still contains a key in the split range");
    }
  }
  return iterator->status();
}

Status ClipCheckpointColumnFamily(DB* checkpoint_db,
                                  ColumnFamilyHandle* checkpoint_cf,
                                  const Slice& begin_key,
                                  const Slice& end_key) {
  Status status =
      checkpoint_db->ClipColumnFamily(checkpoint_cf, begin_key, end_key);
  if (!status.ok()) {
    return status;
  }
  return VerifyRangeContents(checkpoint_db, checkpoint_cf, begin_key, end_key,
                             /*expect_inside=*/true);
}

Status ExciseSourceRanges(DB* source,
                          const std::vector<ColumnFamilySplit>& splits) {
  WriteBatch batch;
  std::vector<ColumnFamilyHandle*> source_cfs;
  source_cfs.reserve(splits.size());
  for (const ColumnFamilySplit& split : splits) {
    Status status =
        batch.DeleteRange(split.source_cf, split.begin_key, split.end_key);
    if (!status.ok()) {
      return status;
    }
    source_cfs.push_back(split.source_cf);
  }

  Status status = source->Write(WriteOptions(), &batch);
  if (!status.ok()) {
    return status;
  }

  FlushOptions flush_options;
  flush_options.wait = true;
  status = source->Flush(flush_options, source_cfs);
  if (!status.ok()) {
    return status;
  }

  CompactRangeOptions compact_options;
  compact_options.exclusive_manual_compaction = true;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  for (const ColumnFamilySplit& split : splits) {
    const Slice begin_key(split.begin_key);
    const Slice end_key(split.end_key);
    status = source->CompactRange(compact_options, split.source_cf, &begin_key,
                                  &end_key);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status CountAndValidateFiles(DB* db, ColumnFamilyHandle* column_family,
                             const Slice& begin_key, const Slice& end_key,
                             SplitMergeResult* result) {
  ColumnFamilyMetaData metadata;
  db->GetColumnFamilyMetaData(column_family, &metadata);
  if (!metadata.blob_files.empty()) {
    return Status::NotSupported(
        "integrated BlobDB files are not supported by DB split/merge");
  }

  const Comparator* comparator = column_family->GetComparator();
  for (const LevelMetaData& level : metadata.levels) {
    for (const SstFileMetaData& file : level.files) {
      if (comparator->Compare(file.smallestkey, begin_key) < 0 ||
          comparator->Compare(file.largestkey, end_key) >= 0) {
        return Status::Corruption(
            "clipped SST extends outside the requested range");
      }
      ++result->transferred_files;
      result->transferred_bytes += file.size;
    }
  }
  return Status::OK();
}

Status AddIngestionArg(
    DB* checkpoint_db, ColumnFamilyHandle* checkpoint_cf,
    ColumnFamilyHandle* destination_cf, bool verify_checksums,
    IngestExternalFileArg* arg,
    std::vector<std::shared_ptr<const PreparedFileInfo>>* prepared_file_infos,
    SplitMergeResult* result) {
  ColumnFamilyMetaData metadata;
  checkpoint_db->GetColumnFamilyMetaData(checkpoint_cf, &metadata);
  if (!metadata.blob_files.empty()) {
    return Status::NotSupported(
        "integrated BlobDB files are not supported by DB split/merge");
  }

  arg->column_family = destination_cf;
  arg->options.allow_db_generated_files = true;
  arg->options.snapshot_consistency = true;
  arg->options.allow_global_seqno = false;
  arg->options.allow_blocking_flush = false;
  arg->options.link_files = true;
  arg->options.failed_move_fall_back_to_copy = true;
  arg->options.fill_cache = false;
  arg->options.verify_checksums_before_ingest = verify_checksums;

  for (std::vector<LevelMetaData>::const_reverse_iterator level =
           metadata.levels.crbegin();
       level != metadata.levels.crend(); ++level) {
    for (std::vector<SstFileMetaData>::const_reverse_iterator file =
             level->files.crbegin();
         file != level->files.crend(); ++file) {
      const std::string path =
          file->directory + kFilePathSeparator + file->relative_filename;
      std::shared_ptr<const PreparedFileInfo> file_info;
      Status status = checkpoint_db->GetPreparedFileInfoForExternalSstIngestion(
          path, &file_info);
      if (!status.ok()) {
        return status;
      }
      arg->external_files.push_back(path);
      arg->file_infos.push_back(file_info.get());
      prepared_file_infos->push_back(std::move(file_info));
      ++result->transferred_files;
      result->transferred_bytes += file->size;
    }
  }
  return Status::OK();
}

Status ClipAndIngest(DB* source, DB* destination, const MergeDBOptions& options,
                     const std::vector<ColumnFamilyHandle*>& source_cfs,
                     SplitMergeResult* result) {
  OpenedCheckpoint checkpoint;
  Status status = OpenCheckpointDB(source, source_cfs,
                                   options.checkpoint_directory, &checkpoint);
  if (!status.ok()) {
    return status;
  }

  std::vector<IngestExternalFileArg> ingestion_args;
  std::vector<std::vector<std::shared_ptr<const PreparedFileInfo>>>
      prepared_file_infos;
  ingestion_args.reserve(options.column_family_merges.size());
  prepared_file_infos.reserve(options.column_family_merges.size());
  for (const ColumnFamilyMerge& merge : options.column_family_merges) {
    ColumnFamilyHandle* checkpoint_cf =
        FindCheckpointColumnFamily(checkpoint, merge.source_cf);
    if (checkpoint_cf == nullptr) {
      return Status::Corruption(
          "selected column family is missing from checkpoint");
    }
    status = ClipCheckpointColumnFamily(checkpoint.db.get(), checkpoint_cf,
                                        merge.begin_key, merge.end_key);
    if (!status.ok()) {
      return status;
    }

    IngestExternalFileArg arg;
    prepared_file_infos.emplace_back();
    status = AddIngestionArg(checkpoint.db.get(), checkpoint_cf,
                             merge.destination_cf, options.verify_checksums,
                             &arg, &prepared_file_infos.back(), result);
    if (!status.ok()) {
      return status;
    }
    if (!arg.external_files.empty()) {
      ingestion_args.push_back(std::move(arg));
    }
  }

  if (ingestion_args.empty()) {
    return Status::OK();
  }

  std::unique_ptr<FileIngestionHandle> ingestion_handle;
  status = destination->PrepareFileIngestion(ingestion_args, &ingestion_handle);
  if (!status.ok()) {
    return status;
  }
  return destination->CommitFileIngestionHandle(std::move(ingestion_handle));
}

}  // namespace

Status SplitDB(DB* source, const SplitDBOptions& options,
               SplitMergeResult* result) {
  if (source == nullptr) {
    return Status::InvalidArgument("source DB is null");
  }
  if (result == nullptr) {
    return Status::InvalidArgument("result is null");
  }
  *result = {};

  Status status = CheckCheckpointPathsDoNotExist(
      source->GetEnv(), options.destination_db_path, "destination DB path");
  if (!status.ok()) {
    return status;
  }
  std::vector<ColumnFamilyHandle*> source_cfs;
  status = ValidateSplits(source, options, &source_cfs);
  if (!status.ok()) {
    return status;
  }

  // Armed before CreateCheckpoint() because it can fail after publishing
  // the destination path.
  ScopedDBDirectoryCleanup cleanup(source->GetEnv(),
                                   options.destination_db_path);
  status = CreateCheckpoint(source, source_cfs, options.destination_db_path,
                            &result->checkpoint_sequence);
  if (!status.ok()) {
    return status;
  }

  {
    OpenedCheckpoint checkpoint;
    status = OpenCheckpointDB(source, source_cfs, options.destination_db_path,
                              &checkpoint);
    if (!status.ok()) {
      return status;
    }
    // Checkpoint always copies the default column family.
    if (!IncludesColumnFamily(source_cfs,
                              source->DefaultColumnFamily()->GetID())) {
      status = EmptyColumnFamily(checkpoint.db.get(),
                                 checkpoint.db->DefaultColumnFamily());
      if (!status.ok()) {
        return status;
      }
    }
    for (const ColumnFamilySplit& split : options.column_family_splits) {
      ColumnFamilyHandle* checkpoint_cf =
          FindCheckpointColumnFamily(checkpoint, split.source_cf);
      if (checkpoint_cf == nullptr) {
        return Status::Corruption(
            "selected column family is missing from checkpoint");
      }
      status = ClipCheckpointColumnFamily(checkpoint.db.get(), checkpoint_cf,
                                          split.begin_key, split.end_key);
      if (!status.ok()) {
        return status;
      }
      status = CountAndValidateFiles(checkpoint.db.get(), checkpoint_cf,
                                     split.begin_key, split.end_key, result);
      if (!status.ok()) {
        return status;
      }
    }
    if (options.verify_checksums) {
      status = checkpoint.db->VerifyChecksum(ReadOptions());
      if (!status.ok()) {
        return status;
      }
    }
  }
  status =
      PersistSourceOptions(source, source_cfs, options.destination_db_path);
  if (!status.ok()) {
    return status;
  }

  cleanup.Release();
  return Status::OK();
}

Status DeleteSplitRanges(DB* source, const SplitDBOptions& options) {
  if (source == nullptr) {
    return Status::InvalidArgument("source DB is null");
  }
  std::vector<ColumnFamilyHandle*> source_cfs;
  Status status = ValidateSplits(source, options, &source_cfs);
  if (!status.ok()) {
    return status;
  }
  status = ExciseSourceRanges(source, options.column_family_splits);
  if (!status.ok()) {
    return status;
  }
  for (const ColumnFamilySplit& split : options.column_family_splits) {
    status = VerifyRangeContents(source, split.source_cf, split.begin_key,
                                 split.end_key, /*expect_inside=*/false);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status MergeDB(DB* source, DB* destination, const MergeDBOptions& options,
               SplitMergeResult* result) {
  if (source == nullptr || destination == nullptr) {
    return Status::InvalidArgument("source and destination DBs must be set");
  }
  if (source->GetRootDB() == destination->GetRootDB()) {
    return Status::InvalidArgument(
        "source and destination must be different DBs");
  }
  if (result == nullptr) {
    return Status::InvalidArgument("result is null");
  }
  *result = {};
  if (options.column_family_merges.empty()) {
    return Status::InvalidArgument("column_family_merges is empty");
  }

  Status status = CheckCheckpointPathsDoNotExist(
      source->GetEnv(), options.checkpoint_directory, "checkpoint directory");
  if (!status.ok()) {
    return status;
  }
  // Ingestion would also reject this, but with a misleading overlap error.
  uint64_t destination_snapshot_count = 0;
  if (!destination->GetIntProperty(DB::Properties::kNumSnapshots,
                                   &destination_snapshot_count)) {
    return Status::Incomplete("failed to get destination snapshot count");
  }
  if (destination_snapshot_count != 0) {
    return Status::InvalidArgument(
        "destination DB has live snapshots, which would observe merged keys");
  }

  std::unordered_set<uint32_t> source_cf_ids;
  std::unordered_set<uint32_t> destination_cf_ids;
  std::vector<ColumnFamilyHandle*> source_cfs;
  source_cfs.reserve(options.column_family_merges.size());
  for (const ColumnFamilyMerge& merge : options.column_family_merges) {
    status = ValidateColumnFamilyHandle(source, merge.source_cf, "source CF");
    if (!status.ok()) {
      return status;
    }
    status = ValidateColumnFamilyHandle(destination, merge.destination_cf,
                                        "destination CF");
    if (!status.ok()) {
      return status;
    }
    if (!source_cf_ids.insert(merge.source_cf->GetID()).second) {
      return Status::InvalidArgument(
          "column_family_merges contains a duplicate source CF");
    }
    if (!destination_cf_ids.insert(merge.destination_cf->GetID()).second) {
      return Status::InvalidArgument(
          "column_family_merges contains a duplicate destination CF");
    }
    status = ValidateRange(merge.source_cf, merge.begin_key, merge.end_key);
    if (!status.ok()) {
      return status;
    }
    if (merge.destination_cf->GetComparator()->Compare(merge.begin_key,
                                                       merge.end_key) >= 0) {
      return Status::InvalidArgument(
          "begin_key must compare before end_key in destination CF");
    }
    ColumnFamilyDescriptor source_descriptor;
    status = ValidateSupportedColumnFamily(merge.source_cf, &source_descriptor);
    if (!status.ok()) {
      return status;
    }
    ColumnFamilyDescriptor destination_descriptor;
    status = ValidateSupportedColumnFamily(merge.destination_cf,
                                           &destination_descriptor);
    if (!status.ok()) {
      return status;
    }
    status = ValidateCompatibleColumnFamilies(source_descriptor,
                                              destination_descriptor);
    if (!status.ok()) {
      return status;
    }
    source_cfs.push_back(merge.source_cf);
  }

  // Armed before CreateCheckpoint() because it can fail after publishing
  // the checkpoint path.
  ScopedDBDirectoryCleanup cleanup(source->GetEnv(),
                                   options.checkpoint_directory);
  status = CreateCheckpoint(source, source_cfs, options.checkpoint_directory,
                            &result->checkpoint_sequence);
  if (!status.ok()) {
    return status;
  }

  status = ClipAndIngest(source, destination, options, source_cfs, result);
  if (!status.ok()) {
    return status;
  }
  cleanup.Release();
  // The merge is committed, so a leftover checkpoint directory is reported in
  // the destination log rather than as a failure.
  status = DestroyDBDirectory(source->GetEnv(), options.checkpoint_directory);
  if (!status.ok()) {
    ROCKS_LOG_WARN(destination->GetDBOptions().info_log.get(),
                   "MergeDB: failed to remove checkpoint directory %s: %s",
                   options.checkpoint_directory.c_str(),
                   status.ToString().c_str());
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
