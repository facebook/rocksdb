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

#include "db/db_impl.h"
#include "db/version_set.h"
#include "db/internal_stats.h"
#include "db/compaction_picker.h"
#include "db/table_properties_collector.h"
#include "util/autovector.h"
#include "util/hash_skiplist_rep.h"

namespace rocksdb {

ColumnFamilyHandleImpl::ColumnFamilyHandleImpl(ColumnFamilyData* cfd,
                                               DBImpl* db, port::Mutex* mutex)
    : cfd_(cfd), db_(db), mutex_(mutex) {
  if (cfd_ != nullptr) {
    cfd_->Ref();
  }
}

ColumnFamilyHandleImpl::~ColumnFamilyHandleImpl() {
  if (cfd_ != nullptr) {
    DBImpl::DeletionState deletion_state;
    mutex_->Lock();
    if (cfd_->Unref()) {
      delete cfd_;
    }
    db_->FindObsoleteFiles(deletion_state, false, true);
    mutex_->Unlock();
    db_->PurgeObsoleteFiles(deletion_state);
  }
}

namespace {
// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
}  // anonymous namespace

ColumnFamilyOptions SanitizeOptions(const InternalKeyComparator* icmp,
                                    const InternalFilterPolicy* ipolicy,
                                    const ColumnFamilyOptions& src) {
  ColumnFamilyOptions result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.write_buffer_size,
              ((size_t)64) << 10, ((size_t)64) << 30);
  // if user sets arena_block_size, we trust user to use this value. Otherwise,
  // calculate a proper value from writer_buffer_size;
  if (result.arena_block_size <= 0) {
    result.arena_block_size = result.write_buffer_size / 10;
  }
  result.min_write_buffer_number_to_merge =
      std::min(result.min_write_buffer_number_to_merge,
               result.max_write_buffer_number - 1);
  if (result.block_cache == nullptr && !result.no_block_cache) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  result.compression_per_level = src.compression_per_level;
  if (result.block_size_deviation < 0 || result.block_size_deviation > 100) {
    result.block_size_deviation = 0;
  }
  if (result.max_mem_compaction_level >= result.num_levels) {
    result.max_mem_compaction_level = result.num_levels - 1;
  }
  if (result.soft_rate_limit > result.hard_rate_limit) {
    result.soft_rate_limit = result.hard_rate_limit;
  }
  if (result.prefix_extractor) {
    // If a prefix extractor has been supplied and a HashSkipListRepFactory is
    // being used, make sure that the latter uses the former as its transform
    // function.
    auto factory =
        dynamic_cast<HashSkipListRepFactory*>(result.memtable_factory.get());
    if (factory && factory->GetTransform() != result.prefix_extractor) {
      result.memtable_factory = std::make_shared<SkipListFactory>();
    }
  }

  // -- Sanitize the table properties collector
  // All user defined properties collectors will be wrapped by
  // UserKeyTablePropertiesCollector since for them they only have the
  // knowledge of the user keys; internal keys are invisible to them.
  auto& collectors = result.table_properties_collectors;
  for (size_t i = 0; i < result.table_properties_collectors.size(); ++i) {
    assert(collectors[i]);
    collectors[i] =
        std::make_shared<UserKeyTablePropertiesCollector>(collectors[i]);
  }
  // Add collector to collect internal key statistics
  collectors.push_back(std::make_shared<InternalKeyPropertiesCollector>());

  return result;
}


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
  // fetch_sub returns the previous value of ref
  uint32_t previous_refs = refs.fetch_sub(1, std::memory_order_relaxed);
  assert(previous_refs > 0);
  return previous_refs == 1;
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

ColumnFamilyData::ColumnFamilyData(const std::string& dbname, uint32_t id,
                                   const std::string& name,
                                   Version* dummy_versions, Cache* table_cache,
                                   const ColumnFamilyOptions& options,
                                   const DBOptions* db_options,
                                   const EnvOptions& storage_options,
                                   ColumnFamilySet* column_family_set)
    : id_(id),
      name_(name),
      dummy_versions_(dummy_versions),
      current_(nullptr),
      refs_(0),
      dropped_(false),
      internal_comparator_(options.comparator),
      internal_filter_policy_(options.filter_policy),
      options_(SanitizeOptions(&internal_comparator_, &internal_filter_policy_,
                               options)),
      full_options_(*db_options, options_),
      mem_(nullptr),
      imm_(options.min_write_buffer_number_to_merge),
      super_version_(nullptr),
      super_version_number_(0),
      next_(nullptr),
      prev_(nullptr),
      log_number_(0),
      need_slowdown_for_num_level0_files_(false),
      column_family_set_(column_family_set) {
  Ref();

  // if dummy_versions is nullptr, then this is a dummy column family.
  if (dummy_versions != nullptr) {
    internal_stats_.reset(new InternalStats(options.num_levels, db_options->env,
                                            db_options->statistics.get()));
    table_cache_.reset(
        new TableCache(dbname, &full_options_, storage_options, table_cache));
    if (options_.compaction_style == kCompactionStyleUniversal) {
      compaction_picker_.reset(new UniversalCompactionPicker(
          &options_, &internal_comparator_, db_options->info_log.get()));
    } else {
      compaction_picker_.reset(new LevelCompactionPicker(
          &options_, &internal_comparator_, db_options->info_log.get()));
    }

    Log(full_options_.info_log, "Options for column family \"%s\":\n",
        name.c_str());
    options_.Dump(full_options_.info_log.get());
  }
}

// DB mutex held
ColumnFamilyData::~ColumnFamilyData() {
  assert(refs_ == 0);
  // remove from linked list
  auto prev = prev_;
  auto next = next_;
  prev->next_ = next;
  next->prev_ = prev;

  if (super_version_ != nullptr) {
    bool is_last_reference __attribute__((unused));
    is_last_reference = super_version_->Unref();
    assert(is_last_reference);
    super_version_->Cleanup();
    delete super_version_;
  }

  // it's nullptr for dummy CFD
  if (column_family_set_ != nullptr) {
    // remove from column_family_set
    column_family_set_->DropColumnFamily(this);
  }

  if (current_ != nullptr) {
    current_->Unref();
  }

  if (dummy_versions_ != nullptr) {
    // List must be empty
    assert(dummy_versions_->next_ == dummy_versions_);
    delete dummy_versions_;
  }

  if (mem_ != nullptr) {
    delete mem_->Unref();
  }
  autovector<MemTable*> to_delete;
  imm_.current()->Unref(&to_delete);
  for (MemTable* m : to_delete) {
    delete m;
  }
}

InternalStats* ColumnFamilyData::internal_stats() {
  return internal_stats_.get();
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
  mem_ = new MemTable(internal_comparator_, options_);
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

ColumnFamilySet::ColumnFamilySet(const std::string& dbname,
                                 const DBOptions* db_options,
                                 const EnvOptions& storage_options,
                                 Cache* table_cache)
    : max_column_family_(0),
      dummy_cfd_(new ColumnFamilyData(dbname, 0, "", nullptr, nullptr,
                                      ColumnFamilyOptions(), db_options,
                                      storage_options_, nullptr)),
      db_name_(dbname),
      db_options_(db_options),
      storage_options_(storage_options),
      table_cache_(table_cache),
      spin_lock_(ATOMIC_FLAG_INIT) {
  // initialize linked list
  dummy_cfd_->prev_ = dummy_cfd_;
  dummy_cfd_->next_ = dummy_cfd_;
}

ColumnFamilySet::~ColumnFamilySet() {
  while (column_family_data_.size() > 0) {
    // cfd destructor will delete itself from column_family_data_
    auto cfd = column_family_data_.begin()->second;
    cfd->Unref();
    delete cfd;
  }
  dummy_cfd_->Unref();
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

// under a DB mutex
ColumnFamilyData* ColumnFamilySet::CreateColumnFamily(
    const std::string& name, uint32_t id, Version* dummy_versions,
    const ColumnFamilyOptions& options) {
  assert(column_families_.find(name) == column_families_.end());
  ColumnFamilyData* new_cfd =
      new ColumnFamilyData(db_name_, id, name, dummy_versions, table_cache_,
                           options, db_options_, storage_options_, this);
  Lock();
  column_families_.insert({name, id});
  column_family_data_.insert({id, new_cfd});
  Unlock();
  max_column_family_ = std::max(max_column_family_, id);
  // add to linked list
  new_cfd->next_ = dummy_cfd_;
  auto prev = dummy_cfd_->prev_;
  new_cfd->prev_ = prev;
  prev->next_ = new_cfd;
  dummy_cfd_->prev_ = new_cfd;
  return new_cfd;
}

// under a DB mutex
void ColumnFamilySet::DropColumnFamily(ColumnFamilyData* cfd) {
  auto cfd_iter = column_family_data_.find(cfd->GetID());
  assert(cfd_iter != column_family_data_.end());
  Lock();
  column_family_data_.erase(cfd_iter);
  column_families_.erase(cfd->GetName());
  Unlock();
}

void ColumnFamilySet::Lock() {
  // spin lock
  while (spin_lock_.test_and_set(std::memory_order_acquire)) {
  }
}

void ColumnFamilySet::Unlock() { spin_lock_.clear(std::memory_order_release); }

bool ColumnFamilyMemTablesImpl::Seek(uint32_t column_family_id) {
  // maybe outside of db mutex, should lock
  column_family_set_->Lock();
  current_ = column_family_set_->GetColumnFamily(column_family_id);
  column_family_set_->Unlock();
  handle_.SetCFD(current_);
  return current_ != nullptr;
}

uint64_t ColumnFamilyMemTablesImpl::GetLogNumber() const {
  assert(current_ != nullptr);
  return current_->GetLogNumber();
}

MemTable* ColumnFamilyMemTablesImpl::GetMemTable() const {
  assert(current_ != nullptr);
  return current_->mem();
}

const Options* ColumnFamilyMemTablesImpl::GetFullOptions() const {
  assert(current_ != nullptr);
  return current_->full_options();
}

ColumnFamilyHandle* ColumnFamilyMemTablesImpl::GetColumnFamilyHandle() {
  assert(current_ != nullptr);
  return &handle_;
}

}  // namespace rocksdb
