// Copyright (c) 2021 Rockset.
#ifndef ROCKSDB_LITE

#include <cinttypes>

#include "cloud/cloud_env_impl.h"

namespace ROCKSDB_NAMESPACE {

namespace {
// static method to use as a callback from the cache.
static void DeleteEntry(const Slice& key, void* value) {
  std::string filename(key.data());
  CloudEnvImpl* cenv = reinterpret_cast<CloudEnvImpl*>(value);
  cenv->FileCacheDeleter(filename);
}

// These are used to retrieve all the values from the cache.
// Only used for unit tests.
static CloudEnvImpl* DecodeValue(void* v) {
  return static_cast<CloudEnvImpl*>(reinterpret_cast<CloudEnvImpl*>(v));
}
static std::vector<std::pair<CloudEnvImpl*, uint64_t>> callback_state;
static void callback(void* entry, size_t charge) {
  callback_state.push_back({DecodeValue(entry), charge});
}
static void clear_callback_state() { callback_state.clear(); }
}  // namespace

//
// Touch the file so that is the the most-recent LRU item in cache.
//
void CloudEnvImpl::FileCacheAccess(const std::string& fname) {
  if (!GetCloudEnvOptions().sst_file_cache) {
    return;
  }
  Slice key(fname);
  Cache::Handle* handle =
      GetCloudEnvOptions().sst_file_cache->get()->Lookup(key);
  if (handle) {
    GetCloudEnvOptions().sst_file_cache->get()->Release(handle);
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_, "[%s] File Cache access %s", Name(),
      fname.c_str());
}

//
// Record the file into the cache.
//
void CloudEnvImpl::FileCacheInsert(const std::string& fname,
                                   uint64_t filesize) {
  if (!GetCloudEnvOptions().sst_file_cache) {
    return;
  }

  // insert into cache, key is the file path.
  Slice key(fname);
  GetCloudEnvOptions().sst_file_cache->get()->Insert(key, this, filesize,
                                                     DeleteEntry);
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[%s] File Cache insert %s size %" PRIu64 "", Name(), fname.c_str(),
      filesize);
}

//
// Remove a specific entry from the cache.
//
void CloudEnvImpl::FileCacheErase(const std::string& fname) {
  if (!GetCloudEnvOptions().sst_file_cache) {
    return;
  }

  Slice key(fname);
  GetCloudEnvOptions().sst_file_cache->get()->Erase(key);

  Log(InfoLogLevel::INFO_LEVEL, info_log_, "[%s] File Cache Erase %s",
      Name(), fname.c_str());
}

//
// When the cache is full, delete files from local store
//
void CloudEnvImpl::FileCacheDeleter(const std::string& fname) {
  assert(GetCloudEnvOptions().sst_file_cache);

  Status st = base_env_->DeleteFile(fname);
  Log(InfoLogLevel::INFO_LEVEL, info_log_, "[%s] File Cache purging %s %s",
      Name(), fname.c_str(), st.ToString().c_str());
}

//
// Get total charge in the cache.
// This is not thread-safe and is used only for unit tests.
//
uint64_t CloudEnvImpl::FileCacheGetCharge() {
  assert(GetCloudEnvOptions().sst_file_cache);
  clear_callback_state();
  GetCloudEnvOptions().sst_file_cache->get()->ApplyToAllCacheEntries(callback,
                                                                     true);
  uint64_t total = 0;
  for (auto& it : callback_state) {
    total += it.second;
  }
  return total;
}

//
// Get total number of items in the cache.
// This is not thread-safe and is used only for unit tests.
//
uint64_t CloudEnvImpl::FileCacheGetNumItems() {
  assert(GetCloudEnvOptions().sst_file_cache);
  clear_callback_state();
  GetCloudEnvOptions().sst_file_cache->get()->ApplyToAllCacheEntries(callback,
                                                                     true);
  return callback_state.size();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
