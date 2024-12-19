//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"

namespace ROCKSDB_NAMESPACE {

// TODO: Share common structure with CompactedDBImpl and DBImplSecondary
class DBImplReadOnly : public DBImpl {
 public:
  DBImplReadOnly(const DBOptions& options, const std::string& dbname);
  // No copying allowed
  DBImplReadOnly(const DBImplReadOnly&) = delete;
  void operator=(const DBImplReadOnly&) = delete;

  virtual ~DBImplReadOnly();

  // Implementations of the DB interface
  using DBImpl::GetImpl;
  Status GetImpl(const ReadOptions& options, const Slice& key,
                 GetImplOptions& get_impl_options) override;

  // TODO: Implement ReadOnly MultiGet?

  using DBImpl::NewIterator;
  Iterator* NewIterator(const ReadOptions& _read_options,
                        ColumnFamilyHandle* column_family) override;

  Status NewIterators(const ReadOptions& options,
                      const std::vector<ColumnFamilyHandle*>& column_families,
                      std::vector<Iterator*>* iterators) override;

  using DBImpl::Put;
  Status Put(const WriteOptions& /*options*/,
             ColumnFamilyHandle* /*column_family*/, const Slice& /*key*/,
             const Slice& /*value*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  using DBImpl::PutEntity;
  Status PutEntity(const WriteOptions& /* options */,
                   ColumnFamilyHandle* /* column_family */,
                   const Slice& /* key */,
                   const WideColumns& /* columns */) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }
  Status PutEntity(const WriteOptions& /* options */, const Slice& /* key */,
                   const AttributeGroups& /* attribute_groups */) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  using DBImpl::Merge;
  Status Merge(const WriteOptions& /*options*/,
               ColumnFamilyHandle* /*column_family*/, const Slice& /*key*/,
               const Slice& /*value*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }
  using DBImpl::Delete;
  Status Delete(const WriteOptions& /*options*/,
                ColumnFamilyHandle* /*column_family*/,
                const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }
  using DBImpl::SingleDelete;
  Status SingleDelete(const WriteOptions& /*options*/,
                      ColumnFamilyHandle* /*column_family*/,
                      const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }
  Status Write(const WriteOptions& /*options*/,
               WriteBatch* /*updates*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }
  using DBImpl::CompactRange;
  Status CompactRange(const CompactRangeOptions& /*options*/,
                      ColumnFamilyHandle* /*column_family*/,
                      const Slice* /*begin*/, const Slice* /*end*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  using DBImpl::CompactFiles;
  Status CompactFiles(
      const CompactionOptions& /*compact_options*/,
      ColumnFamilyHandle* /*column_family*/,
      const std::vector<std::string>& /*input_file_names*/,
      const int /*output_level*/, const int /*output_path_id*/ = -1,
      std::vector<std::string>* const /*output_file_names*/ = nullptr,
      CompactionJobInfo* /*compaction_job_info*/ = nullptr) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  Status DisableFileDeletions() override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  Status EnableFileDeletions() override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }
  Status GetLiveFiles(std::vector<std::string>& ret,
                      uint64_t* manifest_file_size,
                      bool /*flush_memtable*/) override {
    return DBImpl::GetLiveFiles(ret, manifest_file_size,
                                false /* flush_memtable */);
  }

  using DBImpl::Flush;
  Status Flush(const FlushOptions& /*options*/,
               ColumnFamilyHandle* /*column_family*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  using DBImpl::SyncWAL;
  Status SyncWAL() override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  using DB::IngestExternalFile;
  Status IngestExternalFile(
      ColumnFamilyHandle* /*column_family*/,
      const std::vector<std::string>& /*external_files*/,
      const IngestExternalFileOptions& /*ingestion_options*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  using DB::CreateColumnFamilyWithImport;
  Status CreateColumnFamilyWithImport(
      const ColumnFamilyOptions& /*options*/,
      const std::string& /*column_family_name*/,
      const ImportColumnFamilyOptions& /*import_options*/,
      const ExportImportFilesMetaData& /*metadata*/,
      ColumnFamilyHandle** /*handle*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  Status CreateColumnFamilyWithImport(
      const ColumnFamilyOptions& /*options*/,
      const std::string& /*column_family_name*/,
      const ImportColumnFamilyOptions& /*import_options*/,
      const std::vector<const ExportImportFilesMetaData*>& /*metadatas*/,
      ColumnFamilyHandle** /*handle*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  using DB::ClipColumnFamily;
  Status ClipColumnFamily(ColumnFamilyHandle* /*column_family*/,
                          const Slice& /*begin*/,
                          const Slice& /*end*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  // FIXME: some missing overrides for more "write" functions

 protected:
  Status FlushForGetLiveFiles() override {
    // No-op for read-only DB
    return Status::OK();
  }

 private:
  // A "helper" function for DB::OpenForReadOnly without column families
  // to reduce unnecessary I/O
  // It has the same functionality as DB::OpenForReadOnly with column families
  // but does not check the existence of dbname in the file system
  static Status OpenForReadOnlyWithoutCheck(
      const DBOptions& db_options, const std::string& dbname,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
      bool error_if_wal_file_exists = false);
  friend class DB;
};
}  // namespace ROCKSDB_NAMESPACE
