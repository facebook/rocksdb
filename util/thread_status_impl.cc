// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "port/likely.h"
#include "util/mutexlock.h"
#include "util/thread_status_impl.h"

namespace rocksdb {

ThreadStatusImpl thread_local_status;

#if ROCKSDB_USING_THREAD_STATUS
__thread ThreadStatusData* ThreadStatusImpl::thread_status_data_ = nullptr;
std::mutex ThreadStatusImpl::thread_list_mutex_;
std::unordered_set<ThreadStatusData*> ThreadStatusImpl::thread_data_set_;
std::unordered_map<const void*, ConstantColumnFamilyInfo*>
    ThreadStatusImpl::cf_info_map_;
std::unordered_map<const void*, std::unordered_set<const void*>>
    ThreadStatusImpl::db_key_map_;

ThreadStatusImpl::~ThreadStatusImpl() {
  std::lock_guard<std::mutex> lck(thread_list_mutex_);
  for (auto* thread_data : thread_data_set_) {
    assert(thread_data->thread_type == ThreadStatus::ThreadType::USER_THREAD);
    delete thread_data;
  }
  assert(thread_data_set_.size() == 0);
  thread_data_set_.clear();
}

void ThreadStatusImpl::UnregisterThread() {
  if (thread_status_data_ != nullptr) {
    std::lock_guard<std::mutex> lck(thread_list_mutex_);
    thread_data_set_.erase(thread_status_data_);
    delete thread_status_data_;
  }
}

void ThreadStatusImpl::SetThreadType(
    ThreadStatus::ThreadType ttype) {
  auto* data = InitAndGet();
  data->thread_type.store(ttype, std::memory_order_relaxed);
}

void ThreadStatusImpl::SetColumnFamilyInfoKey(
    const void* cf_key) {
  auto* data = InitAndGet();
  data->cf_key.store(cf_key, std::memory_order_relaxed);
}

void ThreadStatusImpl::SetEventInfoPtr(
    const ThreadEventInfo* event_info) {
  auto* data = InitAndGet();
  data->event_info.store(event_info, std::memory_order_relaxed);
}

Status ThreadStatusImpl::GetThreadList(
    std::vector<ThreadStatus>* thread_list) const {
  thread_list->clear();
  std::vector<std::shared_ptr<ThreadStatusData>> valid_list;

  std::lock_guard<std::mutex> lck(thread_list_mutex_);
  for (auto* thread_data : thread_data_set_) {
    assert(thread_data);
    auto thread_type = thread_data->thread_type.load(
        std::memory_order_relaxed);
    auto cf_key = thread_data->cf_key.load(
        std::memory_order_relaxed);
    auto iter = cf_info_map_.find(
        thread_data->cf_key.load(std::memory_order_relaxed));
    assert(cf_key == 0 || iter != cf_info_map_.end());
    auto* cf_info = iter != cf_info_map_.end() ?
        iter->second : nullptr;
    auto* event_info = thread_data->event_info.load(
        std::memory_order_relaxed);
    const std::string* db_name = nullptr;
    const std::string* cf_name = nullptr;
    const std::string* event_name = nullptr;
    if (cf_info != nullptr) {
      db_name = &cf_info->db_name;
      cf_name = &cf_info->cf_name;
      // display lower-level info only when higher-level info is available.
      if (event_info != nullptr) {
        event_name = &event_info->event_name;
      }
    }
    thread_list->emplace_back(
        thread_data->thread_id, thread_type,
        db_name ? *db_name : "",
        cf_name ? *cf_name : "",
        event_name ? *event_name : "");
  }

  return Status::OK();
}

ThreadStatusData* ThreadStatusImpl::InitAndGet() {
  if (UNLIKELY(thread_status_data_ == nullptr)) {
    thread_status_data_ = new ThreadStatusData();
    thread_status_data_->thread_id = reinterpret_cast<uint64_t>(
        thread_status_data_);
    std::lock_guard<std::mutex> lck(thread_list_mutex_);
    thread_data_set_.insert(thread_status_data_);
  }
  return thread_status_data_;
}

void ThreadStatusImpl::NewColumnFamilyInfo(
    const void* db_key, const std::string& db_name,
    const void* cf_key, const std::string& cf_name) {
  std::lock_guard<std::mutex> lck(thread_list_mutex_);

  cf_info_map_[cf_key] = new ConstantColumnFamilyInfo(db_key, db_name, cf_name);
  db_key_map_[db_key].insert(cf_key);
}

void ThreadStatusImpl::EraseColumnFamilyInfo(const void* cf_key) {
  std::lock_guard<std::mutex> lck(thread_list_mutex_);
  auto cf_pair = cf_info_map_.find(cf_key);
  assert(cf_pair != cf_info_map_.end());

  auto* cf_info = cf_pair->second;
  assert(cf_info);

  // Remove its entry from db_key_map_ by the following steps:
  // 1. Obtain the entry in db_key_map_ whose set contains cf_key
  // 2. Remove it from the set.
  auto db_pair = db_key_map_.find(cf_info->db_key);
  assert(db_pair != db_key_map_.end());
  size_t result __attribute__((unused)) = db_pair->second.erase(cf_key);
  assert(result);

  delete cf_info;
  result = cf_info_map_.erase(cf_key);
  assert(result);
}

void ThreadStatusImpl::EraseDatabaseInfo(const void* db_key) {
  std::lock_guard<std::mutex> lck(thread_list_mutex_);
  auto db_pair = db_key_map_.find(db_key);
  if (UNLIKELY(db_pair == db_key_map_.end())) {
    // In some occasional cases such as DB::Open fails, we won't
    // register ColumnFamilyInfo for a db.
    return;
  }

  size_t result __attribute__((unused)) = 0;
  for (auto cf_key : db_pair->second) {
    auto cf_pair = cf_info_map_.find(cf_key);
    assert(cf_pair != cf_info_map_.end());
    result = cf_info_map_.erase(cf_key);
    delete cf_pair->second;
    assert(result);
  }
  db_key_map_.erase(db_key);
}

#else

ThreadStatusImpl::~ThreadStatusImpl() {
}

void ThreadStatusImpl::UnregisterThread() {
}

void ThreadStatusImpl::SetThreadType(
    ThreadStatus::ThreadType ttype) {
}

void ThreadStatusImpl::SetColumnFamilyInfoKey(
    const void* cf_key) {
}

void ThreadStatusImpl::SetEventInfoPtr(
    const ThreadEventInfo* event_info) {
}

Status ThreadStatusImpl::GetThreadList(
    std::vector<ThreadStatus>* thread_list) const {
  return Status::NotSupported(
      "GetThreadList is not supported in the current running environment.");
}

void ThreadStatusImpl::NewColumnFamilyInfo(
    const void* db_key, const std::string& db_name,
    const void* cf_key, const std::string& cf_name) {
}

void ThreadStatusImpl::EraseColumnFamilyInfo(const void* cf_key) {
}

void ThreadStatusImpl::EraseDatabaseInfo(const void* db_key) {
}

#endif  // ROCKSDB_USING_THREAD_STATUS
}  // namespace rocksdb
