// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "rocksdb/env.h"
#include "util/thread_status_updater.h"
#include "util/thread_status_util.h"

namespace rocksdb {

#if ROCKSDB_USING_THREAD_STATUS
__thread ThreadStatusUpdater*
    ThreadStatusUtil::thread_updater_local_cache_ = nullptr;
__thread bool ThreadStatusUtil::thread_updater_initialized_ = false;

void ThreadStatusUtil::SetThreadType(
    const Env* env, ThreadStatus::ThreadType thread_type) {
  if (!MaybeInitThreadLocalUpdater(env)) {
    return;
  }
  assert(thread_updater_local_cache_);
  thread_updater_local_cache_->SetThreadType(thread_type);
}

void ThreadStatusUtil::UnregisterThread() {
  thread_updater_initialized_ = false;
  if (thread_updater_local_cache_ != nullptr) {
    thread_updater_local_cache_->UnregisterThread();
    thread_updater_local_cache_ = nullptr;
  }
}

void ThreadStatusUtil::SetColumnFamily(const ColumnFamilyData* cfd) {
  if (!MaybeInitThreadLocalUpdater(cfd->ioptions()->env)) {
    return;
  }
  assert(thread_updater_local_cache_);
  thread_updater_local_cache_->SetColumnFamilyInfoKey(cfd);
}

void ThreadStatusUtil::NewColumnFamilyInfo(
    const DB* db, const ColumnFamilyData* cfd) {
  if (!MaybeInitThreadLocalUpdater(cfd->ioptions()->env)) {
    return;
  }
  assert(thread_updater_local_cache_);
  if (thread_updater_local_cache_) {
    thread_updater_local_cache_->NewColumnFamilyInfo(
        db, db->GetName(), cfd, cfd->GetName());
  }
}

void ThreadStatusUtil::EraseColumnFamilyInfo(
    const ColumnFamilyData* cfd) {
  if (thread_updater_local_cache_ == nullptr) {
    return;
  }
  thread_updater_local_cache_->EraseColumnFamilyInfo(cfd);
}

void ThreadStatusUtil::EraseDatabaseInfo(const DB* db) {
  if (thread_updater_local_cache_ == nullptr) {
    return;
  }
  thread_updater_local_cache_->EraseDatabaseInfo(db);
}

bool ThreadStatusUtil::MaybeInitThreadLocalUpdater(const Env* env) {
  if (!thread_updater_initialized_ && env != nullptr) {
    thread_updater_initialized_ = true;
    thread_updater_local_cache_ = env->GetThreadStatusUpdater();
  }
  return (thread_updater_local_cache_ != nullptr);
}

#else

ThreadStatusUpdater* ThreadStatusUtil::thread_updater_local_cache_ = nullptr;
bool ThreadStatusUtil::thread_updater_initialized_ = false;

bool ThreadStatusUtil::MaybeInitThreadLocalUpdater(const Env* env) {
  return false;
}

void ThreadStatusUtil::SetColumnFamily(const ColumnFamilyData* cfd) {
}

void ThreadStatusUtil::NewColumnFamilyInfo(
    const DB* db, const ColumnFamilyData* cfd) {
}

void ThreadStatusUtil::EraseColumnFamilyInfo(
    const ColumnFamilyData* cfd) {
}

void ThreadStatusUtil::EraseDatabaseInfo(const DB* db) {
}

#endif  // ROCKSDB_USING_THREAD_STATUS

}  // namespace rocksdb
