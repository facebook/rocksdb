//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

// A wrapper class to hold log reader, log reporter, log status.
class LogReaderContainer {
 public:
  LogReaderContainer()
      : reader_(nullptr), reporter_(nullptr), status_(nullptr) {}
  LogReaderContainer(Env* env, std::shared_ptr<Logger> info_log,
                     std::string fname,
                     std::unique_ptr<SequentialFileReader>&& file_reader,
                     uint64_t log_number) {
    LogReporter* reporter = new LogReporter();
    status_ = new Status();
    reporter->env = env;
    reporter->info_log = info_log.get();
    reporter->fname = std::move(fname);
    reporter->status = status_;
    reporter_ = reporter;
    // We intentially make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    reader_ = new log::FragmentBufferedReader(info_log, std::move(file_reader),
                                              reporter, true /*checksum*/,
                                              log_number);
  }
  log::FragmentBufferedReader* reader_;
  log::Reader::Reporter* reporter_;
  Status* status_;
  ~LogReaderContainer() {
    delete reader_;
    delete reporter_;
    delete status_;
  }

 private:
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    std::string fname;
    Status* status;  // nullptr if immutable_db_options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      ROCKS_LOG_WARN(info_log, "%s%s: dropping %d bytes; %s",
                     (this->status == nullptr ? "(ignoring error) " : ""),
                     fname.c_str(), static_cast<int>(bytes),
                     s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) {
        *this->status = s;
      }
    }
  };
};

// The secondary instance shares access to the storage as the primary.
// The secondary is able to read and replay changes described in both the
// MANIFEST and the WAL files without coordination with the primary.
// The secondary instance can be opened using `DB::OpenAsSecondary`. After
// that, it can call `DBImplSecondary::TryCatchUpWithPrimary` to make best
// effort attempts to catch up with the primary.
// TODO: Share common structure with CompactedDBImpl and DBImplReadOnly
class DBImplSecondary : public DBImpl {
 public:
  DBImplSecondary(const DBOptions& options, const std::string& dbname,
                  std::string secondary_path);
  ~DBImplSecondary() override;

  // Recover by replaying MANIFEST and WAL. Also initialize manifest_reader_
  // and log_readers_ to facilitate future operations.
  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families,
                 bool read_only, bool error_if_wal_file_exists,
                 bool error_if_data_exists_in_wals, uint64_t* = nullptr,
                 RecoveryContext* recovery_ctx = nullptr) override;

  // Implementations of the DB interface.
  using DB::Get;
  // Can return IOError due to files being deleted by the primary. To avoid
  // IOError in this case, application can coordinate between primary and
  // secondaries so that primary will not delete files that are currently being
  // used by the secondaries. The application can also provide a custom FS/Env
  // implementation so that files will remain present until all primary and
  // secondaries indicate that they can be deleted. As a partial hacky
  // workaround, the secondaries can be opened with `max_open_files=-1` so that
  // it eagerly keeps all talbe files open and is able to access the contents of
  // deleted files via prior open fd.
  Status Get(const ReadOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, PinnableSlice* value) override;

  Status Get(const ReadOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, PinnableSlice* value,
             std::string* timestamp) override;

  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* column_family,
                 const Slice& key, PinnableSlice* value,
                 std::string* timestamp);

  using DBImpl::NewIterator;
  // Operations on the created iterators can return IOError due to files being
  // deleted by the primary. To avoid IOError in this case, application can
  // coordinate between primary and secondaries so that primary will not delete
  // files that are currently being used by the secondaries. The application can
  // also provide a custom FS/Env implementation so that files will remain
  // present until all primary and secondaries indicate that they can be
  // deleted. As a partial hacky workaround, the secondaries can be opened with
  // `max_open_files=-1` so that it eagerly keeps all talbe files open and is
  // able to access the contents of deleted files via prior open fd.
  Iterator* NewIterator(const ReadOptions&,
                        ColumnFamilyHandle* column_family) override;

  ArenaWrappedDBIter* NewIteratorImpl(const ReadOptions& read_options,
                                      ColumnFamilyData* cfd,
                                      SequenceNumber snapshot,
                                      ReadCallback* read_callback,
                                      bool expose_blob_index = false,
                                      bool allow_refresh = true);

  Status NewIterators(const ReadOptions& options,
                      const std::vector<ColumnFamilyHandle*>& column_families,
                      std::vector<Iterator*>* iterators) override;

  using DBImpl::Put;
  Status Put(const WriteOptions& /*options*/,
             ColumnFamilyHandle* /*column_family*/, const Slice& /*key*/,
             const Slice& /*value*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::PutEntity;
  Status PutEntity(const WriteOptions& /* options */,
                   ColumnFamilyHandle* /* column_family */,
                   const Slice& /* key */,
                   const WideColumns& /* columns */) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::Merge;
  Status Merge(const WriteOptions& /*options*/,
               ColumnFamilyHandle* /*column_family*/, const Slice& /*key*/,
               const Slice& /*value*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::Delete;
  Status Delete(const WriteOptions& /*options*/,
                ColumnFamilyHandle* /*column_family*/,
                const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::SingleDelete;
  Status SingleDelete(const WriteOptions& /*options*/,
                      ColumnFamilyHandle* /*column_family*/,
                      const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  Status Write(const WriteOptions& /*options*/,
               WriteBatch* /*updates*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::CompactRange;
  Status CompactRange(const CompactRangeOptions& /*options*/,
                      ColumnFamilyHandle* /*column_family*/,
                      const Slice* /*begin*/, const Slice* /*end*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::CompactFiles;
  Status CompactFiles(
      const CompactionOptions& /*compact_options*/,
      ColumnFamilyHandle* /*column_family*/,
      const std::vector<std::string>& /*input_file_names*/,
      const int /*output_level*/, const int /*output_path_id*/ = -1,
      std::vector<std::string>* const /*output_file_names*/ = nullptr,
      CompactionJobInfo* /*compaction_job_info*/ = nullptr) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  Status DisableFileDeletions() override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  Status EnableFileDeletions(bool /*force*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  Status GetLiveFiles(std::vector<std::string>&,
                      uint64_t* /*manifest_file_size*/,
                      bool /*flush_memtable*/ = true) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::Flush;
  Status Flush(const FlushOptions& /*options*/,
               ColumnFamilyHandle* /*column_family*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::SetDBOptions;
  Status SetDBOptions(const std::unordered_map<std::string, std::string>&
                      /*options_map*/) override {
    // Currently not supported because changing certain options may cause
    // flush/compaction.
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::SetOptions;
  Status SetOptions(
      ColumnFamilyHandle* /*cfd*/,
      const std::unordered_map<std::string, std::string>& /*options_map*/)
      override {
    // Currently not supported because changing certain options may cause
    // flush/compaction and/or write to MANIFEST.
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::SyncWAL;
  Status SyncWAL() override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DB::IngestExternalFile;
  Status IngestExternalFile(
      ColumnFamilyHandle* /*column_family*/,
      const std::vector<std::string>& /*external_files*/,
      const IngestExternalFileOptions& /*ingestion_options*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  // Try to catch up with the primary by reading as much as possible from the
  // log files until there is nothing more to read or encounters an error. If
  // the amount of information in the log files to process is huge, this
  // method can take long time due to all the I/O and CPU costs.
  Status TryCatchUpWithPrimary() override;

  // Try to find log reader using log_number from log_readers_ map, initialize
  // if it doesn't exist
  Status MaybeInitLogReader(uint64_t log_number,
                            log::FragmentBufferedReader** log_reader);

  // Check if all live files exist on file system and that their file sizes
  // matche to the in-memory records. It is possible that some live files may
  // have been deleted by the primary. In this case, CheckConsistency() does
  // not flag the missing file as inconsistency.
  Status CheckConsistency() override;

#ifndef NDEBUG
  Status TEST_CompactWithoutInstallation(const OpenAndCompactOptions& options,
                                         ColumnFamilyHandle* cfh,
                                         const CompactionServiceInput& input,
                                         CompactionServiceResult* result) {
    return CompactWithoutInstallation(options, cfh, input, result);
  }
#endif  // NDEBUG

 protected:
#ifndef ROCKSDB_LITE
  Status FlushForGetLiveFiles() override {
    // No-op for read-only DB
    return Status::OK();
  }
#endif  // !ROCKSDB_LITE

  // ColumnFamilyCollector is a write batch handler which does nothing
  // except recording unique column family IDs
  class ColumnFamilyCollector : public WriteBatch::Handler {
    std::unordered_set<uint32_t> column_family_ids_;

    Status AddColumnFamilyId(uint32_t column_family_id) {
      if (column_family_ids_.find(column_family_id) ==
          column_family_ids_.end()) {
        column_family_ids_.insert(column_family_id);
      }
      return Status::OK();
    }

   public:
    explicit ColumnFamilyCollector() {}

    ~ColumnFamilyCollector() override {}

    Status PutCF(uint32_t column_family_id, const Slice&,
                 const Slice&) override {
      return AddColumnFamilyId(column_family_id);
    }

    Status DeleteCF(uint32_t column_family_id, const Slice&) override {
      return AddColumnFamilyId(column_family_id);
    }

    Status SingleDeleteCF(uint32_t column_family_id, const Slice&) override {
      return AddColumnFamilyId(column_family_id);
    }

    Status DeleteRangeCF(uint32_t column_family_id, const Slice&,
                         const Slice&) override {
      return AddColumnFamilyId(column_family_id);
    }

    Status MergeCF(uint32_t column_family_id, const Slice&,
                   const Slice&) override {
      return AddColumnFamilyId(column_family_id);
    }

    Status PutBlobIndexCF(uint32_t column_family_id, const Slice&,
                          const Slice&) override {
      return AddColumnFamilyId(column_family_id);
    }

    Status MarkBeginPrepare(bool) override { return Status::OK(); }

    Status MarkEndPrepare(const Slice&) override { return Status::OK(); }

    Status MarkRollback(const Slice&) override { return Status::OK(); }

    Status MarkCommit(const Slice&) override { return Status::OK(); }

    Status MarkCommitWithTimestamp(const Slice&, const Slice&) override {
      return Status::OK();
    }

    Status MarkNoop(bool) override { return Status::OK(); }

    const std::unordered_set<uint32_t>& column_families() const {
      return column_family_ids_;
    }
  };

  Status CollectColumnFamilyIdsFromWriteBatch(
      const WriteBatch& batch, std::vector<uint32_t>* column_family_ids) {
    assert(column_family_ids != nullptr);
    column_family_ids->clear();
    ColumnFamilyCollector handler;
    Status s = batch.Iterate(&handler);
    if (s.ok()) {
      for (const auto& cf : handler.column_families()) {
        column_family_ids->push_back(cf);
      }
    }
    return s;
  }

  bool OwnTablesAndLogs() const override {
    // Currently, the secondary instance does not own the database files. It
    // simply opens the files of the primary instance and tracks their file
    // descriptors until they become obsolete. In the future, the secondary may
    // create links to database files. OwnTablesAndLogs will return true then.
    return false;
  }

 private:
  friend class DB;

  // No copying allowed
  DBImplSecondary(const DBImplSecondary&);
  void operator=(const DBImplSecondary&);

  using DBImpl::Recover;

  Status FindAndRecoverLogFiles(
      std::unordered_set<ColumnFamilyData*>* cfds_changed,
      JobContext* job_context);
  Status FindNewLogNumbers(std::vector<uint64_t>* logs);
  // After manifest recovery, replay WALs and refresh log_readers_ if necessary
  // REQUIRES: log_numbers are sorted in ascending order
  Status RecoverLogFiles(const std::vector<uint64_t>& log_numbers,
                         SequenceNumber* next_sequence,
                         std::unordered_set<ColumnFamilyData*>* cfds_changed,
                         JobContext* job_context);

  // Run compaction without installation, the output files will be placed in the
  // secondary DB path. The LSM tree won't be changed, the secondary DB is still
  // in read-only mode.
  Status CompactWithoutInstallation(const OpenAndCompactOptions& options,
                                    ColumnFamilyHandle* cfh,
                                    const CompactionServiceInput& input,
                                    CompactionServiceResult* result);

  std::unique_ptr<log::FragmentBufferedReader> manifest_reader_;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter_;
  std::unique_ptr<Status> manifest_reader_status_;

  // Cache log readers for each log number, used for continue WAL replay
  // after recovery
  std::map<uint64_t, std::unique_ptr<LogReaderContainer>> log_readers_;

  // Current WAL number replayed for each column family.
  std::unordered_map<ColumnFamilyData*, uint64_t> cfd_to_current_log_;

  const std::string secondary_path_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE
