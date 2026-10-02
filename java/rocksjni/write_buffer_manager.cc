// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/write_buffer_manager.h"

#include <jni.h>

#include <cassert>
#include <string>

#include "include/org_rocksdb_WriteBufferManager.h"
#include "rocksdb/cache.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

namespace {

bool ParseFlushPolicy(jbyte value,
                      ROCKSDB_NAMESPACE::WriteBufferFlushPolicy* policy) {
  switch (value) {
    case 0:
      *policy = ROCKSDB_NAMESPACE::WriteBufferFlushPolicy::kFlushOldest;
      return true;
    case 1:
      *policy = ROCKSDB_NAMESPACE::WriteBufferFlushPolicy::kFlushLargest;
      return true;
    case 2:
      *policy =
          ROCKSDB_NAMESPACE::WriteBufferFlushPolicy::kFlushLargestAcrossDBs;
      return true;
    default:
      return false;
  }
}

std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>* GetWriteBufferManager(
    jlong handle) {
  return reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>*>(handle);
}

}  // namespace

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
 * Method:    newWriteBufferManagerWithFlushPolicy
 * Signature: (JJZB)J
 */
jlong Java_org_rocksdb_WriteBufferManager_newWriteBufferManagerWithFlushPolicy(
    JNIEnv* env, jclass /*jclazz*/, jlong jbuffer_size, jlong jcache_handle,
    jboolean allow_stall, jbyte jflush_policy) {
  ROCKSDB_NAMESPACE::WriteBufferFlushPolicy flush_policy;
  if (!ParseFlushPolicy(jflush_policy, &flush_policy)) {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(
        env, std::string("Unknown write buffer manager flush policy"));
    return 0;
  }
  auto* cache_ptr =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(
          jcache_handle);
  auto* write_buffer_manager =
      new std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>(
          std::make_shared<ROCKSDB_NAMESPACE::WriteBufferManager>(
              jbuffer_size, *cache_ptr, allow_stall, flush_policy));
  return GET_CPLUSPLUS_POINTER(write_buffer_manager);
}

jbyte Java_org_rocksdb_WriteBufferManager_flushPolicy(JNIEnv* /*env*/,
                                                      jclass /*jclazz*/,
                                                      jlong jhandle) {
  const std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>* const manager =
      GetWriteBufferManager(jhandle);
  return static_cast<jbyte>((*manager)->flush_policy());
}

void Java_org_rocksdb_WriteBufferManager_setFlushPolicy(JNIEnv* env,
                                                        jclass /*jclazz*/,
                                                        jlong jhandle,
                                                        jbyte jflush_policy) {
  ROCKSDB_NAMESPACE::WriteBufferFlushPolicy flush_policy;
  if (!ParseFlushPolicy(jflush_policy, &flush_policy)) {
    ROCKSDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(
        env, std::string("Unknown write buffer manager flush policy"));
    return;
  }
  std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager>* const manager =
      GetWriteBufferManager(jhandle);
  (*manager)->SetFlushPolicy(flush_policy);
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBufferManager_disposeInternalJni(JNIEnv* /*env*/,
                                                            jclass /*jcls*/,
                                                            jlong jhandle) {
  auto* write_buffer_manager = GetWriteBufferManager(jhandle);
  assert(write_buffer_manager != nullptr);
  delete write_buffer_manager;
}
