//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <unordered_map>
#include <string>
#include <vector>
#include <atomic>

#include "db/memtable_list.h"
#include "db/table_cache.h"
#include "db/table_properties_collector.h"
#include "db/write_batch_internal.h"
#include "db/write_controller.h"
#include "options/cf_options.h"
#include "rocksdb/compaction_job_stats.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

class Version;
class VersionSet;
class VersionStorageInfo;
class MemTable;
class MemTableListVersion;
class CompactionPicker;
class Compaction;
class InternalKey;
class InternalStats;
class ColumnFamilyData;
class DBImpl;
class LogBuffer;
class InstrumentedMutex;
class InstrumentedMutexLock;
struct SuperVersionContext;
class BlobFileCache;

extern const double kIncSlowdownRatio;
// This file contains a list of data structures for managing column family
// level metadata.
//
// The basic relationships among classes declared here are illustrated as
// following:
//
//       +----------------------+    +----------------------+   +--------+
//   +---+ ColumnFamilyHandle 1 | +--+ ColumnFamilyHandle 2 |   | DBImpl |
//   |   +----------------------+ |  +----------------------+   +----+---+
//   | +--------------------------+                                  |
//   | |                               +-----------------------------+
//   | |                               |
//   | | +-----------------------------v-------------------------------+
//   | | |                                                             |
//   | | |                      ColumnFamilySet                        |
//   | | |                                                             |
//   | | +-------------+--------------------------+----------------+---+
//   | |               |                          |                |
//   | +-------------------------------------+    |                |
//   |                 |                     |    |                v
//   |   +-------------v-------------+ +-----v----v---------+
//   |   |                           | |                    |
//   |   |     ColumnFamilyData 1    | | ColumnFamilyData 2 |    ......
//   |   |                           | |                    |
//   +--->                           | |                    |
//       |                 +---------+ |                    |
//       |                 | MemTable| |                    |
//       |                 |  List   | |                    |
//       +--------+---+--+-+----+----+ +--------------------++
//                |   |  |      |
//                |   |  |      |
//                |   |  |      +-----------------------+
//                |   |  +-----------+                  |
//                v   +--------+     |                  |
//       +--------+--------+   |     |                  |
//       |                 |   |     |       +----------v----------+
// +---> |SuperVersion 1.a +----------------->                     |
//       |                 +------+  |       | MemTableListVersion |
//       +---+-------------+   |  |  |       |                     |
//           |                 |  |  |       +----+------------+---+
//           |      current    |  |  |            |            |
//           |   +-------------+  |  |mem         |            |
//           |   |                |  |            |            |
//         +-v---v-------+    +---v--v---+  +-----v----+  +----v-----+
//         |             |    |          |  |          |  |          |
//         | Version 1.a |    | memtable |  | memtable |  | memtable |
//         |             |    |   1.a    |  |   1.b    |  |   1.c    |
//         +-------------+    |          |  |          |  |          |
//                            +----------+  +----------+  +----------+
//
// DBImpl keeps a ColumnFamilySet, which references to all column families by
// pointing to respective ColumnFamilyData object of each column family.
// This is how DBImpl can list and operate on all the column families.
// ColumnFamilyHandle also points to ColumnFamilyData directly, so that
// when a user executes a query, it can directly find memtables and Version
// as well as SuperVersion to the column family, without going through
// ColumnFamilySet.
//
// ColumnFamilySet points to the latest view of the LSM-tree (list of memtables
// and SST files) indirectly, while ongoing operations may hold references
// to a current or an out-of-date SuperVersion, which in turn points to a
// point-in-time view of the LSM-tree. This guarantees the memtables and SST
// files being operated on will not go away, until the SuperVersion is
// unreferenced to 0 and destoryed.
//
// The following graph illustrates a possible referencing relationships:
//
// Column       +--------------+      current       +-----------+
// Family +---->+              +------------------->+           |
//  Data        | SuperVersion +----------+         | Version A |
//              |      3       |   imm    |         |           |
// Iter2 +----->+              |  +-------v------+  +-----------+
//              +-----+--------+  | MemtableList +----------------> Empty
//                    |           |   Version r  |  +-----------+
//                    |           +--------------+  |           |
//                    +------------------+   current| Version B |
//              +--------------+         |   +----->+           |
//              |              |         |   |      +-----+-----+
// Compaction +>+ SuperVersion +-------------+            ^
//    Job       |      2       +------+  |                |current
//              |              +----+ |  |     mem        |    +------------+
//              +--------------+    | |  +--------------------->            |
//                                  | +------------------------> MemTable a |
//                                  |          mem        |    |            |
//              +--------------+    |                     |    +------------+
//              |              +--------------------------+
//  Iter1 +-----> SuperVersion |    |                          +------------+
//              |      1       +------------------------------>+            |
//              |              +-+  |        mem               | MemTable b |
//              +--------------+ |  |                          |            |
//                               |  |    +--------------+      +-----^------+
//                               |  |imm | MemtableList |            |
//                               |  +--->+   Version s  +------------+
//                               |       +--------------+
//                               |       +--------------+
//                               |       | MemtableList |
//                               +------>+   Version t  +-------->  Empty
//                                 imm   +--------------+
//
// In this example, even if the current LSM-tree consists of Version A and
// memtable a, which is also referenced by SuperVersion, two older SuperVersion
// SuperVersion2 and Superversion1 still exist, and are referenced by a
// compaction job and an old iterator Iter1, respectively. SuperVersion2
// contains Version B, memtable a and memtable b; SuperVersion1 contains
// Version B and memtable b (mutable). As a result, Version B and memtable b
// are prevented from being destroyed or deleted.

// ColumnFamilyHandleImpl is the class that clients use to access different
// column families. It has non-trivial destructor, which gets called when client
// is done using the column family
class ColumnFamilyHandleImpl : public ColumnFamilyHandle {
 public:
  // create while holding the mutex
  ColumnFamilyHandleImpl(
      ColumnFamilyData* cfd, DBImpl* db, InstrumentedMutex* mutex);
  // destroy without mutex
  virtual ~ColumnFamilyHandleImpl();
  virtual ColumnFamilyData* cfd() const { return cfd_; }

  virtual uint32_t GetID() const override;
  virtual const std::string& GetName() const override;
  virtual Status GetDescriptor(ColumnFamilyDescriptor* desc) override;
  virtual const Comparator* GetComparator() const override;

 private:
  ColumnFamilyData* cfd_;
  DBImpl* db_;
  InstrumentedMutex* mutex_;
};

// Does not ref-count ColumnFamilyData
// We use this dummy ColumnFamilyHandleImpl because sometimes MemTableInserter
// calls DBImpl methods. When this happens, MemTableInserter need access to
// ColumnFamilyHandle (same as the client would need). In that case, we feed
// MemTableInserter dummy ColumnFamilyHandle and enable it to call DBImpl
// methods
class ColumnFamilyHandleInternal : public ColumnFamilyHandleImpl {
 public:
  ColumnFamilyHandleInternal()
      : ColumnFamilyHandleImpl(nullptr, nullptr, nullptr), internal_cfd_(nullptr) {}

  void SetCFD(ColumnFamilyData* _cfd) { internal_cfd_ = _cfd; }
  virtual ColumnFamilyData* cfd() const override { return internal_cfd_; }

 private:
  ColumnFamilyData* internal_cfd_;
};

// holds references to memtable, all immutable memtables and version
struct SuperVersion {
  // Accessing members of this class is not thread-safe and requires external
  // synchronization (ie db mutex held or on write thread).
  ColumnFamilyData* cfd;
  MemTable* mem;
  MemTableListVersion* imm;
  Version* current;
  MutableCFOptions mutable_cf_options;
  // Version number of the current SuperVersion
  uint64_t version_number;
  WriteStallCondition write_stall_condition;

  InstrumentedMutex* db_mutex;

  // should be called outside the mutex
  SuperVersion() = default;
  ~SuperVersion();
  SuperVersion* Ref();
  // If Unref() returns true, Cleanup() should be called with mutex held
  // before deleting this SuperVersion.
  bool Unref();

  // call these two methods with db mutex held
  // Cleanup unrefs mem, imm and current. Also, it stores all memtables
  // that needs to be deleted in to_delete vector. Unrefing those
  // objects needs to be done in the mutex
  void Cleanup();
  void Init(ColumnFamilyData* new_cfd, MemTable* new_mem,
            MemTableListVersion* new_imm, Version* new_current);

  // The value of dummy is not actually used. kSVInUse takes its address as a
  // mark in the thread local storage to indicate the SuperVersion is in use
  // by thread. This way, the value of kSVInUse is guaranteed to have no
  // conflict with SuperVersion object address and portable on different
  // platform.
  static int dummy;
  static void* const kSVInUse;
  static void* const kSVObsolete;

 private:
  std::atomic<uint32_t> refs;
  // We need to_delete because during Cleanup(), imm->Unref() returns
  // all memtables that we need to free through this vector. We then
  // delete all those memtables outside of mutex, during destruction
  autovector<MemTable*> to_delete;
};

extern Status CheckCompressionSupported(const ColumnFamilyOptions& cf_options);

extern Status CheckConcurrentWritesSupported(
    const ColumnFamilyOptions& cf_options);

extern Status CheckCFPathsSupported(const DBOptions& db_options,
                                    const ColumnFamilyOptions& cf_options);

extern ColumnFamilyOptions SanitizeOptions(const ImmutableDBOptions& db_options,
                                           const ColumnFamilyOptions& src);
// Wrap user defined table properties collector factories `from cf_options`
// into internal ones in int_tbl_prop_collector_factories. Add a system internal
// one too.
extern void GetIntTblPropCollectorFactory(
    const ImmutableCFOptions& ioptions,
    IntTblPropCollectorFactories* int_tbl_prop_collector_factories);

class ColumnFamilySet;

// This class keeps all the data that a column family needs.
// Most methods require DB mutex held, unless otherwise noted
class ColumnFamilyData {
 public:
  ~ColumnFamilyData();

  // thread-safe
  uint32_t GetID() const { return id_; }
  // thread-safe
  const std::string& GetName() const { return name_; }

  // Ref() can only be called from a context where the caller can guarantee
  // that ColumnFamilyData is alive (while holding a non-zero ref already,
  // holding a DB mutex, or as the leader in a write batch group).
  void Ref() { refs_.fetch_add(1); }

  // UnrefAndTryDelete() decreases the reference count and do free if needed,
  // return true if this is freed else false, UnrefAndTryDelete() can only
  // be called while holding a DB mutex, or during single-threaded recovery.
  // sv_under_cleanup is only provided when called from SuperVersion::Cleanup.
  bool UnrefAndTryDelete(SuperVersion* sv_under_cleanup = nullptr);

  // SetDropped() can only be called under following conditions:
  // 1) Holding a DB mutex,
  // 2) from single-threaded write thread, AND
  // 3) from single-threaded VersionSet::LogAndApply()
  // After dropping column family no other operation on that column family
  // will be executed. All the files and memory will be, however, kept around
  // until client drops the column family handle. That way, client can still
  // access data from dropped column family.
  // Column family can be dropped and still alive. In that state:
  // *) Compaction and flush is not executed on the dropped column family.
  // *) Client can continue reading from column family. Writes will fail unless
  // WriteOptions::ignore_missing_column_families is true
  // When the dropped column family is unreferenced, then we:
  // *) Remove column family from the linked list maintained by ColumnFamilySet
  // *) delete all memory associated with that column family
  // *) delete all the files associated with that column family
  void SetDropped();
  bool IsDropped() const { return dropped_.load(std::memory_order_relaxed); }

  // thread-safe
  int NumberLevels() const { return ioptions_.num_levels; }

  void SetLogNumber(uint64_t log_number) { log_number_ = log_number; }
  uint64_t GetLogNumber() const { return log_number_; }

  void SetFlushReason(FlushReason flush_reason) {
    flush_reason_ = flush_reason;
  }
  FlushReason GetFlushReason() const { return flush_reason_; }
  // thread-safe
  const FileOptions* soptions() const;
  const ImmutableOptions* ioptions() const { return &ioptions_; }
  // REQUIRES: DB mutex held
  // This returns the MutableCFOptions used by current SuperVersion
  // You should use this API to reference MutableCFOptions most of the time.
  const MutableCFOptions* GetCurrentMutableCFOptions() const {
    return &(super_version_->mutable_cf_options);
  }
  // REQUIRES: DB mutex held
  // This returns the latest MutableCFOptions, which may be not in effect yet.
  const MutableCFOptions* GetLatestMutableCFOptions() const {
    return &mutable_cf_options_;
  }

  // REQUIRES: DB mutex held
  // Build ColumnFamiliesOptions with immutable options and latest mutable
  // options.
  ColumnFamilyOptions GetLatestCFOptions() const;

  bool is_delete_range_supported() { return is_delete_range_supported_; }

  // Validate CF options against DB options
  static Status ValidateOptions(const DBOptions& db_options,
                                const ColumnFamilyOptions& cf_options);
#ifndef ROCKSDB_LITE
  // REQUIRES: DB mutex held
  Status SetOptions(
      const DBOptions& db_options,
      const std::unordered_map<std::string, std::string>& options_map);
#endif  // ROCKSDB_LITE

  InternalStats* internal_stats() { return internal_stats_.get(); }

  MemTableList* imm() { return &imm_; }
  MemTable* mem() { return mem_; }

  bool IsEmpty() {
    return mem()->GetFirstSequenceNumber() == 0 && imm()->NumNotFlushed() == 0;
  }

  Version* current() { return current_; }
  Version* dummy_versions() { return dummy_versions_; }
  void SetCurrent(Version* _current);
  uint64_t GetNumLiveVersions() const;  // REQUIRE: DB mutex held
  uint64_t GetTotalSstFilesSize() const;  // REQUIRE: DB mutex held
  uint64_t GetLiveSstFilesSize() const;   // REQUIRE: DB mutex held
  void SetMemtable(MemTable* new_mem) {
    uint64_t memtable_id = last_memtable_id_.fetch_add(1) + 1;
    new_mem->SetID(memtable_id);
    mem_ = new_mem;
  }

  // calculate the oldest log needed for the durability of this column family
  uint64_t OldestLogToKeep();

  // See Memtable constructor for explanation of earliest_seq param.
  MemTable* ConstructNewMemtable(const MutableCFOptions& mutable_cf_options,
                                 SequenceNumber earliest_seq);
  void CreateNewMemtable(const MutableCFOptions& mutable_cf_options,
                         SequenceNumber earliest_seq);

  TableCache* table_cache() const { return table_cache_.get(); }
  BlobFileCache* blob_file_cache() const { return blob_file_cache_.get(); }

  // See documentation in compaction_picker.h
  // REQUIRES: DB mutex held
  bool NeedsCompaction() const;
  // REQUIRES: DB mutex held
  Compaction* PickCompaction(const MutableCFOptions& mutable_options,
                             const MutableDBOptions& mutable_db_options,
                             LogBuffer* log_buffer);

  // Check if the passed range overlap with any running compactions.
  // REQUIRES: DB mutex held
  bool RangeOverlapWithCompaction(const Slice& smallest_user_key,
                                  const Slice& largest_user_key,
                                  int level) const;

  // Check if the passed ranges overlap with any unflushed memtables
  // (immutable or mutable).
  //
  // @param super_version A referenced SuperVersion that will be held for the
  //    duration of this function.
  //
  // Thread-safe
  Status RangesOverlapWithMemtables(const autovector<Range>& ranges,
                                    SuperVersion* super_version,
                                    bool allow_data_in_errors, bool* overlap);

  // A flag to tell a manual compaction is to compact all levels together
  // instead of a specific level.
  static const int kCompactAllLevels;
  // A flag to tell a manual compaction's output is base level.
  static const int kCompactToBaseLevel;
  // REQUIRES: DB mutex held
  Compaction* CompactRange(const MutableCFOptions& mutable_cf_options,
                           const MutableDBOptions& mutable_db_options,
                           int input_level, int output_level,
                           const CompactRangeOptions& compact_range_options,
                           const InternalKey* begin, const InternalKey* end,
                           InternalKey** compaction_end, bool* manual_conflict,
                           uint64_t max_file_num_to_ignore);

  CompactionPicker* compaction_picker() { return compaction_picker_.get(); }
  // thread-safe
  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
  // thread-safe
  const InternalKeyComparator& internal_comparator() const {
    return internal_comparator_;
  }

  const IntTblPropCollectorFactories* int_tbl_prop_collector_factories() const {
    return &int_tbl_prop_collector_factories_;
  }

  SuperVersion* GetSuperVersion() { return super_version_; }
  // thread-safe
  // Return a already referenced SuperVersion to be used safely.
  SuperVersion* GetReferencedSuperVersion(DBImpl* db);
  // thread-safe
  // Get SuperVersion stored in thread local storage. If it does not exist,
  // get a reference from a current SuperVersion.
  SuperVersion* GetThreadLocalSuperVersion(DBImpl* db);
  // Try to return SuperVersion back to thread local storage. Return true on
  // success and false on failure. It fails when the thread local storage
  // contains anything other than SuperVersion::kSVInUse flag.
  bool ReturnThreadLocalSuperVersion(SuperVersion* sv);
  // thread-safe
  uint64_t GetSuperVersionNumber() const {
    return super_version_number_.load();
  }
  // will return a pointer to SuperVersion* if previous SuperVersion
  // if its reference count is zero and needs deletion or nullptr if not
  // As argument takes a pointer to allocated SuperVersion to enable
  // the clients to allocate SuperVersion outside of mutex.
  // IMPORTANT: Only call this from DBImpl::InstallSuperVersion()
  void InstallSuperVersion(SuperVersionContext* sv_context,
                           InstrumentedMutex* db_mutex,
                           const MutableCFOptions& mutable_cf_options);
  void InstallSuperVersion(SuperVersionContext* sv_context,
                           InstrumentedMutex* db_mutex);

  void ResetThreadLocalSuperVersions();

  // Protected by DB mutex
  void set_queued_for_flush(bool value) { queued_for_flush_ = value; }
  void set_queued_for_compaction(bool value) { queued_for_compaction_ = value; }
  bool queued_for_flush() { return queued_for_flush_; }
  bool queued_for_compaction() { return queued_for_compaction_; }

  enum class WriteStallCause {
    kNone,
    kMemtableLimit,
    kL0FileCountLimit,
    kPendingCompactionBytes,
  };
  static std::pair<WriteStallCondition, WriteStallCause>
  GetWriteStallConditionAndCause(
      int num_unflushed_memtables, int num_l0_files,
      uint64_t num_compaction_needed_bytes,
      const MutableCFOptions& mutable_cf_options,
      const ImmutableCFOptions& immutable_cf_options);

  // Recalculate some small conditions, which are changed only during
  // compaction, adding new memtable and/or
  // recalculation of compaction score. These values are used in
  // DBImpl::MakeRoomForWrite function to decide, if it need to make
  // a write stall
  WriteStallCondition RecalculateWriteStallConditions(
      const MutableCFOptions& mutable_cf_options);

  void set_initialized() { initialized_.store(true); }

  bool initialized() const { return initialized_.load(); }

  const ColumnFamilyOptions& initial_cf_options() {
    return initial_cf_options_;
  }

  Env::WriteLifeTimeHint CalculateSSTWriteHint(int level);

  // created_dirs remembers directory created, so that we don't need to call
  // the same data creation operation again.
  Status AddDirectories(
      std::map<std::string, std::shared_ptr<FSDirectory>>* created_dirs);

  FSDirectory* GetDataDir(size_t path_id) const;

  // full_history_ts_low_ can only increase.
  void SetFullHistoryTsLow(std::string ts_low) {
    assert(!ts_low.empty());
    const Comparator* ucmp = user_comparator();
    assert(ucmp);
    if (full_history_ts_low_.empty() ||
        ucmp->CompareTimestamp(ts_low, full_history_ts_low_) > 0) {
      full_history_ts_low_ = std::move(ts_low);
    }
  }

  const std::string& GetFullHistoryTsLow() const {
    return full_history_ts_low_;
  }

  ThreadLocalPtr* TEST_GetLocalSV() { return local_sv_.get(); }

 private:
  friend class ColumnFamilySet;
  static const uint32_t kDummyColumnFamilyDataId;
  ColumnFamilyData(uint32_t id, const std::string& name,
                   Version* dummy_versions, Cache* table_cache,
                   WriteBufferManager* write_buffer_manager,
                   const ColumnFamilyOptions& options,
                   const ImmutableDBOptions& db_options,
                   const FileOptions& file_options,
                   ColumnFamilySet* column_family_set,
                   BlockCacheTracer* const block_cache_tracer,
                   const std::shared_ptr<IOTracer>& io_tracer,
                   const std::string& db_session_id);

  std::vector<std::string> GetDbPaths() const;

  uint32_t id_;
  const std::string name_;
  Version* dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;         // == dummy_versions->prev_

  std::atomic<int> refs_;      // outstanding references to ColumnFamilyData
  std::atomic<bool> initialized_;
  std::atomic<bool> dropped_;  // true if client dropped it

  const InternalKeyComparator internal_comparator_;
  IntTblPropCollectorFactories int_tbl_prop_collector_factories_;

  const ColumnFamilyOptions initial_cf_options_;
  const ImmutableOptions ioptions_;
  MutableCFOptions mutable_cf_options_;

  const bool is_delete_range_supported_;

  std::unique_ptr<TableCache> table_cache_;
  std::unique_ptr<BlobFileCache> blob_file_cache_;

  std::unique_ptr<InternalStats> internal_stats_;

  WriteBufferManager* write_buffer_manager_;

  MemTable* mem_;
  MemTableList imm_;
  SuperVersion* super_version_;

  // An ordinal representing the current SuperVersion. Updated by
  // InstallSuperVersion(), i.e. incremented every time super_version_
  // changes.
  std::atomic<uint64_t> super_version_number_;

  // Thread's local copy of SuperVersion pointer
  // This needs to be destructed before mutex_
  std::unique_ptr<ThreadLocalPtr> local_sv_;

  // pointers for a circular linked list. we use it to support iterations over
  // all column families that are alive (note: dropped column families can also
  // be alive as long as client holds a reference)
  ColumnFamilyData* next_;
  ColumnFamilyData* prev_;

  // This is the earliest log file number that contains data from this
  // Column Family. All earlier log files must be ignored and not
  // recovered from
  uint64_t log_number_;

  std::atomic<FlushReason> flush_reason_;

  // An object that keeps all the compaction stats
  // and picks the next compaction
  std::unique_ptr<CompactionPicker> compaction_picker_;

  ColumnFamilySet* column_family_set_;

  std::unique_ptr<WriteControllerToken> write_controller_token_;

  // If true --> this ColumnFamily is currently present in DBImpl::flush_queue_
  bool queued_for_flush_;

  // If true --> this ColumnFamily is currently present in
  // DBImpl::compaction_queue_
  bool queued_for_compaction_;

  uint64_t prev_compaction_needed_bytes_;

  // if the database was opened with 2pc enabled
  bool allow_2pc_;

  // Memtable id to track flush.
  std::atomic<uint64_t> last_memtable_id_;

  // Directories corresponding to cf_paths.
  std::vector<std::shared_ptr<FSDirectory>> data_dirs_;

  bool db_paths_registered_;

  std::string full_history_ts_low_;
};

// ColumnFamilySet has interesting thread-safety requirements
// * CreateColumnFamily() or RemoveColumnFamily() -- need to be protected by DB
// mutex AND executed in the write thread.
// CreateColumnFamily() should ONLY be called from VersionSet::LogAndApply() AND
// single-threaded write thread. It is also called during Recovery and in
// DumpManifest().
// RemoveColumnFamily() is only called from SetDropped(). DB mutex needs to be
// held and it needs to be executed from the write thread. SetDropped() also
// guarantees that it will be called only from single-threaded LogAndApply(),
// but this condition is not that important.
// * Iteration -- hold DB mutex, but you can release it in the body of
// iteration. If you release DB mutex in body, reference the column
// family before the mutex and unreference after you unlock, since the column
// family might get dropped when the DB mutex is released
// * GetDefault() -- thread safe
// * GetColumnFamily() -- either inside of DB mutex or from a write thread
// * GetNextColumnFamilyID(), GetMaxColumnFamily(), UpdateMaxColumnFamily(),
// NumberOfColumnFamilies -- inside of DB mutex
class ColumnFamilySet {
 public:
  // ColumnFamilySet supports iteration
  class iterator {
   public:
    explicit iterator(ColumnFamilyData* cfd)
        : current_(cfd) {}
    iterator& operator++() {
      // dropped column families might still be included in this iteration
      // (we're only removing them when client drops the last reference to the
      // column family).
      // dummy is never dead, so this will never be infinite
      do {
        current_ = current_->next_;
      } while (current_->refs_.load(std::memory_order_relaxed) == 0);
      return *this;
    }
    bool operator!=(const iterator& other) {
      return this->current_ != other.current_;
    }
    ColumnFamilyData* operator*() { return current_; }

   private:
    ColumnFamilyData* current_;
  };

  ColumnFamilySet(const std::string& dbname,
                  const ImmutableDBOptions* db_options,
                  const FileOptions& file_options, Cache* table_cache,
                  WriteBufferManager* _write_buffer_manager,
                  WriteController* _write_controller,
                  BlockCacheTracer* const block_cache_tracer,
                  const std::shared_ptr<IOTracer>& io_tracer,
                  const std::string& db_session_id);
  ~ColumnFamilySet();

  ColumnFamilyData* GetDefault() const;
  // GetColumnFamily() calls return nullptr if column family is not found
  ColumnFamilyData* GetColumnFamily(uint32_t id) const;
  ColumnFamilyData* GetColumnFamily(const std::string& name) const;
  // this call will return the next available column family ID. it guarantees
  // that there is no column family with id greater than or equal to the
  // returned value in the current running instance or anytime in RocksDB
  // instance history.
  uint32_t GetNextColumnFamilyID();
  uint32_t GetMaxColumnFamily();
  void UpdateMaxColumnFamily(uint32_t new_max_column_family);
  size_t NumberOfColumnFamilies() const;

  ColumnFamilyData* CreateColumnFamily(const std::string& name, uint32_t id,
                                       Version* dummy_version,
                                       const ColumnFamilyOptions& options);

  iterator begin() { return iterator(dummy_cfd_->next_); }
  iterator end() { return iterator(dummy_cfd_); }

  // REQUIRES: DB mutex held
  // Don't call while iterating over ColumnFamilySet
  void FreeDeadColumnFamilies();

  Cache* get_table_cache() { return table_cache_; }

  WriteBufferManager* write_buffer_manager() { return write_buffer_manager_; }

  WriteController* write_controller() { return write_controller_; }

 private:
  friend class ColumnFamilyData;
  // helper function that gets called from cfd destructor
  // REQUIRES: DB mutex held
  void RemoveColumnFamily(ColumnFamilyData* cfd);

  // column_families_ and column_family_data_ need to be protected:
  // * when mutating both conditions have to be satisfied:
  // 1. DB mutex locked
  // 2. thread currently in single-threaded write thread
  // * when reading, at least one condition needs to be satisfied:
  // 1. DB mutex locked
  // 2. accessed from a single-threaded write thread
  std::unordered_map<std::string, uint32_t> column_families_;
  std::unordered_map<uint32_t, ColumnFamilyData*> column_family_data_;

  uint32_t max_column_family_;
  ColumnFamilyData* dummy_cfd_;
  // We don't hold the refcount here, since default column family always exists
  // We are also not responsible for cleaning up default_cfd_cache_. This is
  // just a cache that makes common case (accessing default column family)
  // faster
  ColumnFamilyData* default_cfd_cache_;

  const std::string db_name_;
  const ImmutableDBOptions* const db_options_;
  const FileOptions file_options_;
  Cache* table_cache_;
  WriteBufferManager* write_buffer_manager_;
  WriteController* write_controller_;
  BlockCacheTracer* const block_cache_tracer_;
  std::shared_ptr<IOTracer> io_tracer_;
  std::string db_session_id_;
};

// We use ColumnFamilyMemTablesImpl to provide WriteBatch a way to access
// memtables of different column families (specified by ID in the write batch)
class ColumnFamilyMemTablesImpl : public ColumnFamilyMemTables {
 public:
  explicit ColumnFamilyMemTablesImpl(ColumnFamilySet* column_family_set)
      : column_family_set_(column_family_set), current_(nullptr) {}

  // Constructs a ColumnFamilyMemTablesImpl equivalent to one constructed
  // with the arguments used to construct *orig.
  explicit ColumnFamilyMemTablesImpl(ColumnFamilyMemTablesImpl* orig)
      : column_family_set_(orig->column_family_set_), current_(nullptr) {}

  // sets current_ to ColumnFamilyData with column_family_id
  // returns false if column family doesn't exist
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  bool Seek(uint32_t column_family_id) override;

  // Returns log number of the selected column family
  // REQUIRES: under a DB mutex OR from a write thread
  uint64_t GetLogNumber() const override;

  // REQUIRES: Seek() called first
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  virtual MemTable* GetMemTable() const override;

  // Returns column family handle for the selected column family
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  virtual ColumnFamilyHandle* GetColumnFamilyHandle() override;

  // Cannot be called while another thread is calling Seek().
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  virtual ColumnFamilyData* current() override { return current_; }

 private:
  ColumnFamilySet* column_family_set_;
  ColumnFamilyData* current_;
  ColumnFamilyHandleInternal handle_;
};

extern uint32_t GetColumnFamilyID(ColumnFamilyHandle* column_family);

extern const Comparator* GetColumnFamilyUserComparator(
    ColumnFamilyHandle* column_family);

}  // namespace ROCKSDB_NAMESPACE
