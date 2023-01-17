// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/write_buffer_manager.h"

#include <jni.h>

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
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBufferManager_disposeInternal(JNIEnv* /*env*/,
                                                         jobject /*jobj*/,
                                                         jlong jhandle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(
          jhandle);
  assert(write_buffer_manager != nullptr);
  delete write_buffer_manager;
}
