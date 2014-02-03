//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/column_family.h"

#include <vector>
#include <string>
#include <algorithm>

#include "db/version_set.h"
#include "db/compaction_picker.h"

namespace rocksdb {

SuperVersion::SuperVersion() {}

SuperVersion::~SuperVersion() {
  for (auto td : to_delete) {
    delete td;
  }
}

SuperVersion* SuperVersion::Ref() {
  refs.fetch_add(1, std::memory_order_relaxed);
  return this;
}

bool SuperVersion::Unref() {
  assert(refs > 0);
  // fetch_sub returns the previous value of ref
  return refs.fetch_sub(1, std::memory_order_relaxed) == 1;
}

void SuperVersion::Cleanup() {
  assert(refs.load(std::memory_order_relaxed) == 0);
  imm->Unref(&to_delete);
  MemTable* m = mem->Unref();
  if (m != nullptr) {
    to_delete.push_back(m);
  }
  current->Unref();
}

void SuperVersion::Init(MemTable* new_mem, MemTableListVersion* new_imm,
                        Version* new_current) {
  mem = new_mem;
  imm = new_imm;
  current = new_current;
  mem->Ref();
  imm->Ref();
  current->Ref();
  refs.store(1, std::memory_order_relaxed);
}

ColumnFamilyData::ColumnFamilyData(uint32_t id, const std::string& name,
                                   Version* dummy_versions,
                                   const ColumnFamilyOptions& options)
    : id_(id),
      name_(name),
      dummy_versions_(dummy_versions),
      current_(nullptr),
      options_(options),
      icmp_(options_.comparator),
      mem_(nullptr),
      imm_(options.min_write_buffer_number_to_merge),
      super_version_(nullptr),
      super_version_number_(0),
      next_(nullptr),
      prev_(nullptr),
      log_number_(0),
      need_slowdown_for_num_level0_files_(false) {
  if (options_.compaction_style == kCompactionStyleUniversal) {
    compaction_picker_.reset(new UniversalCompactionPicker(&options_, &icmp_));
  } else {
    compaction_picker_.reset(new LevelCompactionPicker(&options_, &icmp_));
  }
}

ColumnFamilyData::~ColumnFamilyData() {
  if (super_version_ != nullptr) {
    bool is_last_reference __attribute__((unused));
    is_last_reference = super_version_->Unref();
    assert(is_last_reference);
    super_version_->Cleanup();
    delete super_version_;
  }
  if (dummy_versions_ != nullptr) {
    // List must be empty
    assert(dummy_versions_->next_ == dummy_versions_);
    delete dummy_versions_;
  }

  if (mem_ != nullptr) {
    delete mem_->Unref();
  }
  std::vector<MemTable*> to_delete;
  imm_.current()->Unref(&to_delete);
  for (MemTable* m : to_delete) {
    delete m;
  }
}

void ColumnFamilyData::SetCurrent(Version* current) {
  current_ = current;
  need_slowdown_for_num_level0_files_ =
      (options_.level0_slowdown_writes_trigger >= 0 &&
       current_->NumLevelFiles(0) >= options_.level0_slowdown_writes_trigger);
}

void ColumnFamilyData::CreateNewMemtable() {
  assert(current_ != nullptr);
  if (mem_ != nullptr) {
    delete mem_->Unref();
  }
  mem_ = new MemTable(icmp_, options_);
  mem_->Ref();
}

Compaction* ColumnFamilyData::PickCompaction() {
  return compaction_picker_->PickCompaction(current_);
}

Compaction* ColumnFamilyData::CompactRange(int input_level, int output_level,
                                           const InternalKey* begin,
                                           const InternalKey* end,
                                           InternalKey** compaction_end) {
  return compaction_picker_->CompactRange(current_, input_level, output_level,
                                          begin, end, compaction_end);
}

SuperVersion* ColumnFamilyData::InstallSuperVersion(
    SuperVersion* new_superversion) {
  new_superversion->Init(mem_, imm_.current(), current_);
  SuperVersion* old_superversion = super_version_;
  super_version_ = new_superversion;
  ++super_version_number_;
  if (old_superversion != nullptr && old_superversion->Unref()) {
    old_superversion->Cleanup();
    return old_superversion;  // will let caller delete outside of mutex
  }
  return nullptr;
}

ColumnFamilySet::ColumnFamilySet()
    : max_column_family_(0),
      dummy_cfd_(new ColumnFamilyData(0, "", nullptr, ColumnFamilyOptions())) {
  // initialize linked list
  dummy_cfd_->prev_.store(dummy_cfd_);
  dummy_cfd_->next_.store(dummy_cfd_);
}

ColumnFamilySet::~ColumnFamilySet() {
  for (auto& cfd : column_family_data_) {
    delete cfd.second;
  }
  for (auto& cfd : droppped_column_families_) {
    delete cfd;
  }
  delete dummy_cfd_;
}

ColumnFamilyData* ColumnFamilySet::GetDefault() const {
  auto cfd = GetColumnFamily(0);
  // default column family should always exist
  assert(cfd != nullptr);
  return cfd;
}

ColumnFamilyData* ColumnFamilySet::GetColumnFamily(uint32_t id) const {
  auto cfd_iter = column_family_data_.find(id);
  if (cfd_iter != column_family_data_.end()) {
    return cfd_iter->second;
  } else {
    return nullptr;
  }
}

bool ColumnFamilySet::Exists(uint32_t id) {
  return column_family_data_.find(id) != column_family_data_.end();
}

bool ColumnFamilySet::Exists(const std::string& name) {
  return column_families_.find(name) != column_families_.end();
}

uint32_t ColumnFamilySet::GetID(const std::string& name) {
  auto cfd_iter = column_families_.find(name);
  assert(cfd_iter != column_families_.end());
  return cfd_iter->second;
}

uint32_t ColumnFamilySet::GetNextColumnFamilyID() {
  return ++max_column_family_;
}

ColumnFamilyData* ColumnFamilySet::CreateColumnFamily(
    const std::string& name, uint32_t id, Version* dummy_versions,
    const ColumnFamilyOptions& options) {
  assert(column_families_.find(name) == column_families_.end());
  column_families_.insert({name, id});
  ColumnFamilyData* new_cfd =
      new ColumnFamilyData(id, name, dummy_versions, options);
  column_family_data_.insert({id, new_cfd});
  max_column_family_ = std::max(max_column_family_, id);
  // add to linked list
  new_cfd->next_.store(dummy_cfd_);
  auto prev = dummy_cfd_->prev_.load();
  new_cfd->prev_.store(prev);
  prev->next_.store(new_cfd);
  dummy_cfd_->prev_.store(new_cfd);
  return new_cfd;
}

void ColumnFamilySet::DropColumnFamily(uint32_t id) {
  assert(id != 0);
  auto cfd_iter = column_family_data_.find(id);
  assert(cfd_iter != column_family_data_.end());
  auto cfd = cfd_iter->second;
  column_families_.erase(cfd->GetName());
  cfd->current()->Unref();
  droppped_column_families_.push_back(cfd);
  column_family_data_.erase(cfd_iter);
  // remove from linked list
  auto prev = cfd->prev_.load();
  auto next = cfd->next_.load();
  prev->next_.store(next);
  next->prev_.store(prev);
}

MemTable* ColumnFamilyMemTablesImpl::GetMemTable(uint32_t column_family_id) {
  auto cfd = column_family_set_->GetColumnFamily(column_family_id);
  // TODO(icanadi): this should not be asserting. Rather, it should somehow
  // return Corruption status back to the Iterator. This will require
  // API change in WriteBatch::Handler, which is a public API
  assert(cfd != nullptr);

  if (log_number_ == 0 || log_number_ >= cfd->GetLogNumber()) {
    return cfd->mem();
  } else {
    return nullptr;
  }
}

}  // namespace rocksdb
