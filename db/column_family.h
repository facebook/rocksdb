//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <unordered_map>
#include <string>
#include <vector>
#include <atomic>

#include "rocksdb/options.h"
#include "rocksdb/env.h"
#include "db/memtable_list.h"
#include "db/write_batch_internal.h"
#include "db/table_cache.h"
#include "util/thread_local.h"

namespace rocksdb {

class Version;
class VersionSet;
class MemTable;
class MemTableListVersion;
class CompactionPicker;
class Compaction;
class InternalKey;
class InternalStats;
class ColumnFamilyData;
class DBImpl;

class ColumnFamilyHandleImpl : public ColumnFamilyHandle {
 public:
  // create while holding the mutex
  ColumnFamilyHandleImpl(ColumnFamilyData* cfd, DBImpl* db, port::Mutex* mutex);
  // destroy without mutex
  virtual ~ColumnFamilyHandleImpl();
  virtual ColumnFamilyData* cfd() const { return cfd_; }

  virtual uint32_t GetID() const override;

 private:
  ColumnFamilyData* cfd_;
  DBImpl* db_;
  port::Mutex* mutex_;
};

// does not ref-count cfd_
class ColumnFamilyHandleInternal : public ColumnFamilyHandleImpl {
 public:
  ColumnFamilyHandleInternal()
      : ColumnFamilyHandleImpl(nullptr, nullptr, nullptr) {}

  void SetCFD(ColumnFamilyData* cfd) { internal_cfd_ = cfd; }
  virtual ColumnFamilyData* cfd() const override { return internal_cfd_; }

 private:
  ColumnFamilyData* internal_cfd_;
};

// holds references to memtable, all immutable memtables and version
struct SuperVersion {
  MemTable* mem;
  MemTableListVersion* imm;
  Version* current;
  std::atomic<uint32_t> refs;
  // We need to_delete because during Cleanup(), imm->Unref() returns
  // all memtables that we need to free through this vector. We then
  // delete all those memtables outside of mutex, during destruction
  autovector<MemTable*> to_delete;
  // Version number of the current SuperVersion
  uint64_t version_number;
  port::Mutex* db_mutex;

  // should be called outside the mutex
  SuperVersion() = default;
  ~SuperVersion();
  SuperVersion* Ref();
  // Returns true if this was the last reference and caller should
  // call Clenaup() and delete the object
  bool Unref();

  // call these two methods with db mutex held
  // Cleanup unrefs mem, imm and current. Also, it stores all memtables
  // that needs to be deleted in to_delete vector. Unrefing those
  // objects needs to be done in the mutex
  void Cleanup();
  void Init(MemTable* new_mem, MemTableListVersion* new_imm,
            Version* new_current);

  // The value of dummy is not actually used. kSVInUse takes its address as a
  // mark in the thread local storage to indicate the SuperVersion is in use
  // by thread. This way, the value of kSVInUse is guaranteed to have no
  // conflict with SuperVersion object address and portable on different
  // platform.
  static int dummy;
  static void* const kSVInUse;
  static void* const kSVObsolete;
};

extern ColumnFamilyOptions SanitizeOptions(const InternalKeyComparator* icmp,
                                           const InternalFilterPolicy* ipolicy,
                                           const ColumnFamilyOptions& src);

class ColumnFamilySet;

// column family metadata. not thread-safe. should be protected by db_mutex
class ColumnFamilyData {
 public:
  ~ColumnFamilyData();

  uint32_t GetID() const { return id_; }
  const std::string& GetName() { return name_; }

  // DB mutex held for all these
  void Ref() { ++refs_; }
  // will just decrease reference count to 0, but will not delete it. returns
  // true if the ref count was decreased to zero and needs to be cleaned up by
  // the caller
  bool Unref() {
    assert(refs_ > 0);
    return --refs_ == 0;
  }
  bool Dead() { return refs_ == 0; }

  // SetDropped() and IsDropped() are thread-safe
  void SetDropped() {
    // can't drop default CF
    assert(id_ != 0);
    dropped_.store(true);
  }
  bool IsDropped() const { return dropped_.load(); }

  int NumberLevels() const { return options_.num_levels; }

  void SetLogNumber(uint64_t log_number) { log_number_ = log_number; }
  uint64_t GetLogNumber() const { return log_number_; }

  const ColumnFamilyOptions* options() const { return &options_; }
  const Options* full_options() const { return &full_options_; }
  InternalStats* internal_stats();

  MemTableList* imm() { return &imm_; }
  MemTable* mem() { return mem_; }
  Version* current() { return current_; }
  Version* dummy_versions() { return dummy_versions_; }
  void SetMemtable(MemTable* new_mem) { mem_ = new_mem; }
  void SetCurrent(Version* current);
  void CreateNewMemtable();

  TableCache* table_cache() const { return table_cache_.get(); }

  // See documentation in compaction_picker.h
  Compaction* PickCompaction(LogBuffer* log_buffer);
  Compaction* CompactRange(int input_level, int output_level,
                           const InternalKey* begin, const InternalKey* end,
                           InternalKey** compaction_end);

  CompactionPicker* compaction_picker() const {
    return compaction_picker_.get();
  }
  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
  const InternalKeyComparator& internal_comparator() const {
    return internal_comparator_;
  }

  SuperVersion* GetSuperVersion() const { return super_version_; }
  ThreadLocalPtr* GetThreadLocalSuperVersion() const { return local_sv_.get(); }
  uint64_t GetSuperVersionNumber() const {
    return super_version_number_.load();
  }
  // will return a pointer to SuperVersion* if previous SuperVersion
  // if its reference count is zero and needs deletion or nullptr if not
  // As argument takes a pointer to allocated SuperVersion to enable
  // the clients to allocate SuperVersion outside of mutex.
  SuperVersion* InstallSuperVersion(SuperVersion* new_superversion,
                                    port::Mutex* db_mutex);

  void ResetThreadLocalSuperVersions();
  // REQUIRED: db mutex held
  // Do not access column family after calling this method
  void DeleteSuperVersion();

  // A Flag indicating whether write needs to slowdown because of there are
  // too many number of level0 files.
  bool NeedSlowdownForNumLevel0Files() const {
    return need_slowdown_for_num_level0_files_;
  }

 private:
  friend class ColumnFamilySet;
  ColumnFamilyData(const std::string& dbname, uint32_t id,
                   const std::string& name, Version* dummy_versions,
                   Cache* table_cache, const ColumnFamilyOptions& options,
                   const DBOptions* db_options,
                   const EnvOptions& storage_options,
                   ColumnFamilySet* column_family_set);

  ColumnFamilyData* next() { return next_; }

  uint32_t id_;
  const std::string name_;
  Version* dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;         // == dummy_versions->prev_

  int refs_;                   // outstanding references to ColumnFamilyData
  std::atomic<bool> dropped_;  // true if client dropped it

  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;

  ColumnFamilyOptions const options_;
  Options const full_options_;

  std::unique_ptr<TableCache> table_cache_;

  std::unique_ptr<InternalStats> internal_stats_;

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

  // pointers for a circular linked list. we use it to support iterations
  // that can be concurrent with writes
  ColumnFamilyData* next_;
  ColumnFamilyData* prev_;

  // This is the earliest log file number that contains data from this
  // Column Family. All earlier log files must be ignored and not
  // recovered from
  uint64_t log_number_;

  // A flag indicating whether we should delay writes because
  // we have too many level 0 files
  bool need_slowdown_for_num_level0_files_;

  // An object that keeps all the compaction stats
  // and picks the next compaction
  std::unique_ptr<CompactionPicker> compaction_picker_;

  ColumnFamilySet* column_family_set_;
};

// Thread safe only for reading without a writer. All access should be
// locked when adding or dropping column family
class ColumnFamilySet {
 public:
  class iterator {
   public:
    explicit iterator(ColumnFamilyData* cfd)
        : current_(cfd) {}
    iterator& operator++() {
      // dummy is never dead, so this will never be infinite
      do {
        current_ = current_->next();
      } while (current_->Dead());
      return *this;
    }
    bool operator!=(const iterator& other) {
      return this->current_ != other.current_;
    }
    ColumnFamilyData* operator*() { return current_; }

   private:
    ColumnFamilyData* current_;
  };

  ColumnFamilySet(const std::string& dbname, const DBOptions* db_options,
                  const EnvOptions& storage_options, Cache* table_cache);
  ~ColumnFamilySet();

  ColumnFamilyData* GetDefault() const;
  // GetColumnFamily() calls return nullptr if column family is not found
  ColumnFamilyData* GetColumnFamily(uint32_t id) const;
  ColumnFamilyData* GetColumnFamily(const std::string& name) const;
  bool Exists(uint32_t id);
  bool Exists(const std::string& name);
  uint32_t GetID(const std::string& name);
  // this call will return the next available column family ID. it guarantees
  // that there is no column family with id greater than or equal to the
  // returned value in the current running instance or anytime in RocksDB
  // instance history.
  uint32_t GetNextColumnFamilyID();
  uint32_t GetMaxColumnFamily();
  void UpdateMaxColumnFamily(uint32_t new_max_column_family);

  ColumnFamilyData* CreateColumnFamily(const std::string& name, uint32_t id,
                                       Version* dummy_version,
                                       const ColumnFamilyOptions& options);
  void DropColumnFamily(ColumnFamilyData* cfd);

  iterator begin() { return iterator(dummy_cfd_->next()); }
  iterator end() { return iterator(dummy_cfd_); }

  // ColumnFamilySet has interesting thread-safety requirements
  // * CreateColumnFamily() or DropColumnFamily() -- need to protect by DB
  // mutex. Inside, column_family_data_ and column_families_ will be protected
  // by Lock() and Unlock()
  // * Iterate -- hold DB mutex, but you can release it in the body of
  // iteration. If you release DB mutex in body, reference the column
  // family before the mutex and unreference after you unlock, since the column
  // family might get dropped when you release the DB mutex.
  // * GetDefault(), GetColumnFamily(), Exists(), GetID() -- either inside of DB
  // mutex or call Lock()
  // * GetNextColumnFamilyID(), GetMaxColumnFamily(), UpdateMaxColumnFamily() --
  // inside of DB mutex
  void Lock();
  void Unlock();

 private:
  // when mutating: 1. DB mutex locked first, 2. spinlock locked second
  // when reading, either: 1. lock DB mutex, or 2. lock spinlock
  //  (if both, respect the ordering to avoid deadlock!)
  std::unordered_map<std::string, uint32_t> column_families_;
  std::unordered_map<uint32_t, ColumnFamilyData*> column_family_data_;
  uint32_t max_column_family_;
  ColumnFamilyData* dummy_cfd_;

  const std::string db_name_;
  const DBOptions* const db_options_;
  const EnvOptions storage_options_;
  Cache* table_cache_;
  std::atomic_flag spin_lock_;
};

class ColumnFamilyMemTablesImpl : public ColumnFamilyMemTables {
 public:
  explicit ColumnFamilyMemTablesImpl(ColumnFamilySet* column_family_set)
      : column_family_set_(column_family_set), current_(nullptr) {}

  // sets current_ to ColumnFamilyData with column_family_id
  // returns false if column family doesn't exist
  bool Seek(uint32_t column_family_id) override;

  // Returns log number of the selected column family
  uint64_t GetLogNumber() const override;

  // REQUIRES: Seek() called first
  virtual MemTable* GetMemTable() const override;

  // Returns options for selected column family
  // REQUIRES: Seek() called first
  virtual const Options* GetFullOptions() const override;

  // Returns column family handle for the selected column family
  virtual ColumnFamilyHandle* GetColumnFamilyHandle() override;

 private:
  ColumnFamilySet* column_family_set_;
  ColumnFamilyData* current_;
  ColumnFamilyHandleInternal handle_;
};

}  // namespace rocksdb
