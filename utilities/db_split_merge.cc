//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/db_split_merge.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "file/filename.h"
#include "rocksdb/comparator.h"
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
      Options options;
      options.env = env_;
      DestroyDB(path_, options).PermitUncheckedError();
    }
  }

  void Enable() { enabled_ = true; }
  void Release() { enabled_ = false; }

 private:
  Env* env_;
  std::string path_;
  bool enabled_ = false;
};

Status CheckPathDoesNotExist(Env* env, const std::string& path,
                             const char* description) {
  if (path.empty()) {
    return Status::InvalidArgument(std::string(description) + " is empty");
  }
  Status status = env->FileExists(path);
  if (status.ok()) {
    return Status::InvalidArgument(std::string(description) +
                                   " already exists: " + path);
  }
  if (status.IsNotFound()) {
    return Status::OK();
  }
  return status;
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

Status OpenCheckpointDB(DB* source,
                        const std::vector<ColumnFamilyHandle*>& source_cfs,
                        const std::string& checkpoint_directory,
                        OpenedCheckpoint* checkpoint) {
  std::vector<ColumnFamilyDescriptor> descriptors;
  descriptors.reserve(source_cfs.size() + 1);

  const uint32_t default_cf_id = source->DefaultColumnFamily()->GetID();
  ColumnFamilyDescriptor default_descriptor;
  Status status =
      source->DefaultColumnFamily()->GetDescriptor(&default_descriptor);
  if (!status.ok()) {
    return status;
  }
  default_descriptor.options.disable_auto_compactions = true;
  default_descriptor.options.cf_paths.clear();
  descriptors.push_back(std::move(default_descriptor));

  for (ColumnFamilyHandle* source_cf : source_cfs) {
    if (source_cf->GetID() == default_cf_id) {
      continue;
    }
    ColumnFamilyDescriptor descriptor;
    status = source_cf->GetDescriptor(&descriptor);
    if (!status.ok()) {
      return status;
    }
    descriptor.options.disable_auto_compactions = true;
    descriptor.options.cf_paths.clear();
    descriptors.push_back(std::move(descriptor));
  }

  DBOptions db_options = source->GetDBOptions();
  db_options.create_if_missing = false;
  db_options.create_missing_column_families = false;
  db_options.error_if_exists = false;
  db_options.db_paths.clear();
  db_options.wal_dir.clear();

  status = DB::Open(db_options, checkpoint_directory, descriptors,
                    &checkpoint->handles, &checkpoint->db);
  if (!status.ok()) {
    return status;
  }
  for (ColumnFamilyHandle* handle : checkpoint->handles) {
    checkpoint->handles_by_id.emplace(handle->GetID(), handle);
  }
  return Status::OK();
}

ColumnFamilyHandle* FindCheckpointColumnFamily(
    const OpenedCheckpoint& checkpoint, ColumnFamilyHandle* source_cf) {
  const auto it = checkpoint.handles_by_id.find(source_cf->GetID());
  return it == checkpoint.handles_by_id.end() ? nullptr : it->second;
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
    ColumnFamilyHandle* destination_cf, IngestExternalFileArg* arg,
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
  arg->options.snapshot_consistency = false;
  arg->options.allow_global_seqno = false;
  arg->options.allow_blocking_flush = false;
  arg->options.write_global_seqno = false;
  arg->options.link_files = true;
  arg->options.failed_move_fall_back_to_copy = true;
  arg->options.fill_cache = false;
  arg->options.verify_checksums_before_ingest = true;

  for (auto level = metadata.levels.rbegin(); level != metadata.levels.rend();
       ++level) {
    for (auto file = level->files.rbegin(); file != level->files.rend();
         ++file) {
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
  if (options.column_family_splits.empty()) {
    return Status::InvalidArgument("column_family_splits is empty");
  }

  Status status = CheckPathDoesNotExist(
      source->GetEnv(), options.destination_db_path, "destination DB path");
  if (!status.ok()) {
    return status;
  }

  std::unordered_set<uint32_t> source_cf_ids;
  std::vector<ColumnFamilyHandle*> source_cfs;
  source_cfs.reserve(options.column_family_splits.size());
  for (const ColumnFamilySplit& split : options.column_family_splits) {
    status = ValidateColumnFamilyHandle(source, split.source_cf, "source CF");
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
    source_cfs.push_back(split.source_cf);
  }

  ScopedDBDirectoryCleanup cleanup(source->GetEnv(),
                                   options.destination_db_path);
  status = CreateCheckpoint(source, source_cfs, options.destination_db_path,
                            &result->checkpoint_sequence);
  if (!status.ok()) {
    return status;
  }
  cleanup.Enable();

  {
    OpenedCheckpoint checkpoint;
    status = OpenCheckpointDB(source, source_cfs, options.destination_db_path,
                              &checkpoint);
    if (!status.ok()) {
      return status;
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

  cleanup.Release();
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
  if (options.verify_checksums) {
    status = source->VerifyChecksum(ReadOptions());
  }
  return status;
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

  Status status = CheckPathDoesNotExist(
      source->GetEnv(), options.checkpoint_directory, "checkpoint directory");
  if (!status.ok()) {
    return status;
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

  ScopedDBDirectoryCleanup cleanup(source->GetEnv(),
                                   options.checkpoint_directory);
  status = CreateCheckpoint(source, source_cfs, options.checkpoint_directory,
                            &result->checkpoint_sequence);
  if (!status.ok()) {
    return status;
  }
  cleanup.Enable();

  OpenedCheckpoint checkpoint;
  status = OpenCheckpointDB(source, source_cfs, options.checkpoint_directory,
                            &checkpoint);
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
                             merge.destination_cf, &arg,
                             &prepared_file_infos.back(), result);
    if (!status.ok()) {
      return status;
    }
    if (!arg.external_files.empty()) {
      ingestion_args.push_back(std::move(arg));
    }
  }

  if (options.verify_checksums) {
    status = checkpoint.db->VerifyChecksum(ReadOptions());
    if (!status.ok()) {
      return status;
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

}  // namespace ROCKSDB_NAMESPACE
