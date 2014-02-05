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

#include "rocksdb/options.h"
#include "rocksdb/env.h"
#include "db/memtablelist.h"
#include "db/write_batch_internal.h"
#include "db/table_cache.h"

namespace rocksdb {

class Version;
class VersionSet;
class MemTable;
class MemTableListVersion;
class CompactionPicker;
class Compaction;
class InternalKey;
class InternalStats;

// holds references to memtable, all immutable memtables and version
struct SuperVersion {
  MemTable* mem;
  MemTableListVersion* imm;
  Version* current;
  std::atomic<uint32_t> refs;
  // We need to_delete because during Cleanup(), imm->Unref() returns
  // all memtables that we need to free through this vector. We then
  // delete all those memtables outside of mutex, during destruction
  std::vector<MemTable*> to_delete;

  // should be called outside the mutex
  SuperVersion();
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
};

extern ColumnFamilyOptions SanitizeOptions(const InternalKeyComparator* icmp,
                                           const InternalFilterPolicy* ipolicy,
                                           const ColumnFamilyOptions& src);

// column family metadata. not thread-safe. should be protected by db_mutex
class ColumnFamilyData {
 public:
  uint32_t GetID() const { return id_; }
  const std::string& GetName() { return name_; }

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
  Compaction* PickCompaction();
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
  uint64_t GetSuperVersionNumber() const {
    return super_version_number_.load();
  }
  // will return a pointer to SuperVersion* if previous SuperVersion
  // if its reference count is zero and needs deletion or nullptr if not
  // As argument takes a pointer to allocated SuperVersion to enable
  // the clients to allocate SuperVersion outside of mutex.
  SuperVersion* InstallSuperVersion(SuperVersion* new_superversion);

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
                   const EnvOptions& storage_options);
  ~ColumnFamilyData();

  ColumnFamilyData* next() { return next_.load(); }

  uint32_t id_;
  const std::string name_;
  Version* dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;         // == dummy_versions->prev_

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

  // pointers for a circular linked list. we use it to support iterations
  // that can be concurrent with writes
  std::atomic<ColumnFamilyData*> next_;
  std::atomic<ColumnFamilyData*> prev_;

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
      current_ = current_->next();
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
  bool Exists(uint32_t id);
  bool Exists(const std::string& name);
  uint32_t GetID(const std::string& name);
  // this call will return the next available column family ID. it guarantees
  // that there is no column family with id greater than or equal to the
  // returned value in the current running instance. It does not, however,
  // guarantee that the returned ID is unique accross RocksDB restarts.
  // For example, if a client adds a column family 6 and then drops it,
  // after a restart, we might reuse column family 6 ID.
  uint32_t GetNextColumnFamilyID();

  ColumnFamilyData* CreateColumnFamily(const std::string& name, uint32_t id,
                                       Version* dummy_version,
                                       const ColumnFamilyOptions& options);
  void DropColumnFamily(uint32_t id);

  iterator begin() { return iterator(dummy_cfd_->next()); }
  iterator end() { return iterator(dummy_cfd_); }

 private:
  std::unordered_map<std::string, uint32_t> column_families_;
  std::unordered_map<uint32_t, ColumnFamilyData*> column_family_data_;
  // we need to keep them alive because we still can't control the lifetime of
  // all of column family data members (options for example)
  std::vector<ColumnFamilyData*> droppped_column_families_;
  uint32_t max_column_family_;
  ColumnFamilyData* dummy_cfd_;

  const std::string db_name_;
  const DBOptions* const db_options_;
  const EnvOptions storage_options_;
  Cache* table_cache_;
};

class ColumnFamilyMemTablesImpl : public ColumnFamilyMemTables {
 public:
  explicit ColumnFamilyMemTablesImpl(ColumnFamilySet* column_family_set)
      : column_family_set_(column_family_set), log_number_(0) {}

  // If column_family_data->log_number is bigger than log_number,
  // the memtable will not be returned.
  // If log_number == 0, the memtable will be always returned
  void SetLogNumber(uint64_t log_number) { log_number_ = log_number; }

  // Returns the column families memtable if log_number == 0 || log_number <=
  // column_family_data->log_number.
  // If column family doesn't exist, it asserts
  virtual MemTable* GetMemTable(uint32_t column_family_id) override;

 private:
  ColumnFamilySet* column_family_set_;
  uint64_t log_number_;
};

}  // namespace rocksdb
