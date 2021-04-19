// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>

#include "include/org_rocksdb_WriteBufferManager.h"

#include "rocksdb/cache.h"
#include "rocksdb/write_buffer_manager.h"

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    newWriteBufferManager
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_WriteBufferManager_newWriteBufferManager(
        JNIEnv* /*env*/, jclass /*jclazz*/, jlong jbuffer_size, jlong jcache_handle) {
  auto* cache_ptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(
          jcache_handle);
  auto* write_buffer_manager =
      new std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>(
          std::make_shared<ROCKSDB_NAMESPACE::WriteBufferManager>(jbuffer_size,
                                                                  *cache_ptr));
  return reinterpret_cast<jlong>(write_buffer_manager);
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    newWriteBufferManager
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_WriteBufferManager_newWriteBufferManager(
    JNIEnv* /*env*/, jclass /*jclazz*/, jlong jbuffer_size) {
  auto* write_buffer_manager =
      new std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>(
          std::make_shared<ROCKSDB_NAMESPACE::WriteBufferManager>(jbuffer_size,
                                                                  nullptr));
  return reinterpret_cast<jlong>(write_buffer_manager);
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBufferManager_disposeInternal(
        JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  delete write_buffer_manager;
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    getMemoryUsage
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBufferManager_getMemoryUsage(JNIEnv* /*env*/,
                                                         jobject,
                                                         jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  return static_cast<jlong>(write_buffer_manager->get()->memory_usage());
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    getMutableMemtableMemoryUsage
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBufferManager_getMutableMemtableMemoryUsage(
    JNIEnv* /*env*/, jobject, jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  return static_cast<jlong>(
      write_buffer_manager->get()->mutable_memtable_memory_usage());
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    getBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBufferManager_getBufferSize(JNIEnv*, jobject,
                                                        jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  return static_cast<jlong>(write_buffer_manager->get()->buffer_size());
}