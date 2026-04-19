// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/write_buffer_manager.h"

#include <jni.h>

#include <cassert>

#include "include/org_rocksdb_WriteBufferManager.h"
#include "rocksdb/cache.h"
#include "rocksjni/cplusplus_to_java_convert.h"

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    newWriteBufferManager
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_WriteBufferManager_newWriteBufferManager(
    JNIEnv* /*env*/, jclass /*jclazz*/, jlong jbuffer_size, jlong jcache_handle,
    jboolean allow_stall) {
  auto* cache_ptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(
          jcache_handle);
  auto* write_buffer_manager =
      new std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>(
          std::make_shared<ROCKSDB_NAMESPACE::WriteBufferManager>(
              jbuffer_size, *cache_ptr, allow_stall));
  return GET_CPLUSPLUS_POINTER(write_buffer_manager);
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    enabled
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteBufferManager_enabled(JNIEnv* /*env*/,
                                                      jclass /*jcls*/,
                                                      jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  return static_cast<jboolean>((*write_buffer_manager)->enabled());
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    costToCache
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteBufferManager_costToCache(JNIEnv* /*env*/,
                                                         jclass /*jcls*/,
                                                         jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  return static_cast<jboolean>((*write_buffer_manager)->cost_to_cache());
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    memoryUsage
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBufferManager_memoryUsage(JNIEnv* /*env*/,
                                                      jclass /*jcls*/,
                                                      jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  return static_cast<jlong>((*write_buffer_manager)->memory_usage());
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    mutableMemtableMemoryUsage
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBufferManager_mutableMemtableMemoryUsage(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  return static_cast<jlong>(
      (*write_buffer_manager)->mutable_memtable_memory_usage());
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    dummyEntriesInCacheUsage
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBufferManager_dummyEntriesInCacheUsage(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  return static_cast<jlong>(
      (*write_buffer_manager)->dummy_entries_in_cache_usage());
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    bufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBufferManager_bufferSize(JNIEnv* /*env*/,
                                                     jclass /*jcls*/,
                                                     jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  return static_cast<jlong>((*write_buffer_manager)->buffer_size());
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    setBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WriteBufferManager_setBufferSize(JNIEnv* /*env*/,
                                                       jclass /*jcls*/,
                                                       jlong jhandle,
                                                       jlong jbuffer_size) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  (*write_buffer_manager)->SetBufferSize(static_cast<size_t>(jbuffer_size));
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    setAllowStall
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteBufferManager_setAllowStall(JNIEnv* /*env*/,
                                                       jclass /*jcls*/,
                                                       jlong jhandle,
                                                       jboolean jallow_stall) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  (*write_buffer_manager)->SetAllowStall(static_cast<bool>(jallow_stall));
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    isStallActive
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteBufferManager_isStallActive(JNIEnv* /*env*/,
                                                           jclass /*jcls*/,
                                                           jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  return static_cast<jboolean>((*write_buffer_manager)->IsStallActive());
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    isStallThresholdExceeded
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteBufferManager_isStallThresholdExceeded(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  return static_cast<jboolean>(
      (*write_buffer_manager)->IsStallThresholdExceeded());
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBufferManager_disposeInternalJni(JNIEnv* /*env*/,
                                                            jclass /*jcls*/,
                                                            jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  delete write_buffer_manager;
}
