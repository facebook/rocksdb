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
#include "db/memtablelist.h"

namespace rocksdb {

class Version;
class VersionSet;
class MemTable;
class MemTableListVersion;

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
  explicit SuperVersion(const int num_memtables = 0);
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

// column family metadata
struct ColumnFamilyData {
  uint32_t id;
  std::string name;
  Version* dummy_versions; // Head of circular doubly-linked list of versions.
  Version* current;        // == dummy_versions->prev_
  ColumnFamilyOptions options;

  MemTable* mem;
  MemTableList imm;
  SuperVersion* super_version;

  ColumnFamilyData(uint32_t id, const std::string& name,
                   Version* dummy_versions, const ColumnFamilyOptions& options);
  ~ColumnFamilyData();

  void CreateNewMemtable();
};

class ColumnFamilySet {
 public:
  class iterator {
   public:
    explicit iterator(
        std::unordered_map<uint32_t, ColumnFamilyData*>::iterator itr)
        : itr_(itr) {}
    iterator& operator++() {
      ++itr_;
      return *this;
    }
    bool operator!=(const iterator& other) { return this->itr_ != other.itr_; }
    ColumnFamilyData* operator*() { return itr_->second; }

   private:
    std::unordered_map<uint32_t, ColumnFamilyData*>::iterator itr_;
  };

  ColumnFamilySet();
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

  iterator begin() { return iterator(column_family_data_.begin()); }
  iterator end() { return iterator(column_family_data_.end()); }

 private:
  std::unordered_map<std::string, uint32_t> column_families_;
  std::unordered_map<uint32_t, ColumnFamilyData*> column_family_data_;
  // we need to keep them alive because we still can't control the lifetime of
  // all of column family data members (options for example)
  std::vector<ColumnFamilyData*> droppped_column_families_;
  uint32_t max_column_family_;
};

}  // namespace rocksdb
