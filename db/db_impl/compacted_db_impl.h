//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE
#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"

namespace ROCKSDB_NAMESPACE {

// TODO: Share common structure with DBImplSecondary and DBImplReadOnly
class CompactedDBImpl : public DBImpl {
 public:
  CompactedDBImpl(const DBOptions& options, const std::string& dbname);
  // No copying allowed
  CompactedDBImpl(const CompactedDBImpl&) = delete;
  void operator=(const CompactedDBImpl&) = delete;

  ~CompactedDBImpl() override;

  static Status Open(const Options& options, const std::string& dbname,
                     DB** dbptr);

  // Implementations of the DB interface
  using DB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value) override;

  Status Get(const ReadOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, PinnableSlice* value,
             std::string* timestamp) override;

  using DB::MultiGet;
  // Note that CompactedDBImpl::MultiGet is not the optimized version of
  // MultiGet to use.
  // TODO: optimize CompactedDBImpl::MultiGet, see DBImpl::MultiGet for details.
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options, const std::vector<ColumnFamilyHandle*>&,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  std::vector<Status> MultiGet(const ReadOptions& options,
                               const std::vector<ColumnFamilyHandle*>&,
                               const std::vector<Slice>& keys,
                               std::vector<std::string>* values,
                               std::vector<std::string>* timestamps) override;

  using DBImpl::Put;
  virtual Status Put(const WriteOptions& /*options*/,
                     ColumnFamilyHandle* /*column_family*/,
                     const Slice& /*key*/, const Slice& /*value*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  using DBImpl::PutEntity;
  Status PutEntity(const WriteOptions& /* options */,
                   ColumnFamilyHandle* /* column_family */,
                   const Slice& /* key */,
                   const WideColumns& /* columns */) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  using DBImpl::Merge;
  virtual Status Merge(const WriteOptions& /*options*/,
                       ColumnFamilyHandle* /*column_family*/,
                       const Slice& /*key*/, const Slice& /*value*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  using DBImpl::Delete;
  virtual Status Delete(const WriteOptions& /*options*/,
                        ColumnFamilyHandle* /*column_family*/,
                        const Slice& /*key*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }
  virtual Status Write(const WriteOptions& /*options*/,
                       WriteBatch* /*updates*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }
  using DBImpl::CompactRange;
  virtual Status CompactRange(const CompactRangeOptions& /*options*/,
                              ColumnFamilyHandle* /*column_family*/,
                              const Slice* /*begin*/,
                              const Slice* /*end*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  virtual Status DisableFileDeletions() override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }
  virtual Status EnableFileDeletions(bool /*force*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }
  virtual Status GetLiveFiles(std::vector<std::string>& ret,
                              uint64_t* manifest_file_size,
                              bool /*flush_memtable*/) override {
    return DBImpl::GetLiveFiles(ret, manifest_file_size,
                                false /* flush_memtable */);
  }
  using DBImpl::Flush;
  virtual Status Flush(const FlushOptions& /*options*/,
                       ColumnFamilyHandle* /*column_family*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  virtual Status SyncWAL() override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  using DB::IngestExternalFile;
  virtual Status IngestExternalFile(
      ColumnFamilyHandle* /*column_family*/,
      const std::vector<std::string>& /*external_files*/,
      const IngestExternalFileOptions& /*ingestion_options*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }
  using DB::CreateColumnFamilyWithImport;
  virtual Status CreateColumnFamilyWithImport(
      const ColumnFamilyOptions& /*options*/,
      const std::string& /*column_family_name*/,
      const ImportColumnFamilyOptions& /*import_options*/,
      const ExportImportFilesMetaData& /*metadata*/,
      ColumnFamilyHandle** /*handle*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  // FIXME: some missing overrides for more "write" functions
  // Share with DBImplReadOnly?

 protected:
#ifndef ROCKSDB_LITE
  Status FlushForGetLiveFiles() override {
    // No-op for read-only DB
    return Status::OK();
  }
#endif  // !ROCKSDB_LITE

 private:
  friend class DB;
  inline size_t FindFile(const Slice& key);
  Status Init(const Options& options);

  ColumnFamilyData* cfd_;
  Version* version_;
  const Comparator* user_comparator_;
  LevelFilesBrief files_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
