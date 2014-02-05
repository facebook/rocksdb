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
#include "db/internal_stats.h"
#include "db/compaction_picker.h"
#include "db/table_properties_collector.h"
#include "util/hash_skiplist_rep.h"

namespace rocksdb {

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
                                   const Options* db_options,
                                   const EnvOptions& storage_options)
    : id_(id),
      name_(name),
      dummy_versions_(dummy_versions),
      current_(nullptr),
      internal_comparator_(options.comparator),
      internal_filter_policy_(options.filter_policy),
      options_(SanitizeOptions(&internal_comparator_, &internal_filter_policy_,
                               options)),
      full_options_(DBOptions(*db_options), options_),
      mem_(nullptr),
      imm_(options.min_write_buffer_number_to_merge),
      super_version_(nullptr),
      super_version_number_(0),
      next_(nullptr),
      prev_(nullptr),
      log_number_(0),
      need_slowdown_for_num_level0_files_(false) {
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
                                 const Options* db_options,
                                 const EnvOptions& storage_options,
                                 Cache* table_cache)
    : max_column_family_(0),
      dummy_cfd_(new ColumnFamilyData(dbname, 0, "", nullptr, nullptr,
                                      ColumnFamilyOptions(), db_options,
                                      storage_options_)),
      db_name_(dbname),
      db_options_(db_options),
      storage_options_(storage_options),
      table_cache_(table_cache) {
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
      new ColumnFamilyData(db_name_, id, name, dummy_versions, table_cache_,
                           options, db_options_, storage_options_);
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
