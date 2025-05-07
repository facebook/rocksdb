//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/concurrent_task_limiter.h"

#include <jni.h>

#include <memory>
#include <string>

#include "include/org_rocksdb_ConcurrentTaskLimiterImpl.h"
#include "rocksjni/portal/jni_util.h"

/*
 * Class:     org_rocksdb_ConcurrentTaskLimiterImpl
 * Method:    newConcurrentTaskLimiterImpl0
 * Signature: (Ljava/lang/String;I)J
 */
jlong Java_org_rocksdb_ConcurrentTaskLimiterImpl_newConcurrentTaskLimiterImpl0(
    JNIEnv* env, jclass, jstring jname, jint limit) {
  jboolean has_exception = JNI_FALSE;
  std::string name =
      ROCKSDB_NAMESPACE::JniUtil::copyStdString(env, jname, &has_exception);
  if (JNI_TRUE == has_exception) {
    return 0;
  }

  auto* ptr = new std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter>(
      ROCKSDB_NAMESPACE::NewConcurrentTaskLimiter(name, limit));

  return GET_CPLUSPLUS_POINTER(ptr);
}

/*
 * Class:     org_rocksdb_ConcurrentTaskLimiterImpl
 * Method:    name
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_ConcurrentTaskLimiterImpl_name(JNIEnv* env, jclass,
                                                        jlong handle) {
  const auto& limiter = *reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter>*>(handle);
  return ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &limiter->GetName());
}

/*
 * Class:     org_rocksdb_ConcurrentTaskLimiterImpl
 * Method:    setMaxOutstandingTask
 * Signature: (JI)V
 */
void Java_org_rocksdb_ConcurrentTaskLimiterImpl_setMaxOutstandingTask(
    JNIEnv*, jclass, jlong handle, jint max_outstanding_task) {
  const auto& limiter = *reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter>*>(handle);
  limiter->SetMaxOutstandingTask(static_cast<int32_t>(max_outstanding_task));
}

/*
 * Class:     org_rocksdb_ConcurrentTaskLimiterImpl
 * Method:    resetMaxOutstandingTask
 * Signature: (J)V
 */
void Java_org_rocksdb_ConcurrentTaskLimiterImpl_resetMaxOutstandingTask(
    JNIEnv*, jclass, jlong handle) {
  const auto& limiter = *reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter>*>(handle);
  limiter->ResetMaxOutstandingTask();
}

/*
 * Class:     org_rocksdb_ConcurrentTaskLimiterImpl
 * Method:    outstandingTask
 * Signature: (J)I
 */
jint Java_org_rocksdb_ConcurrentTaskLimiterImpl_outstandingTask(JNIEnv*, jclass,
                                                                jlong handle) {
  const auto& limiter = *reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter>*>(handle);
  return static_cast<jint>(limiter->GetOutstandingTask());
}

/*
 * Class:     org_rocksdb_ConcurrentTaskLimiterImpl
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ConcurrentTaskLimiterImpl_disposeInternalJni(
    JNIEnv*, jclass, jlong jhandle) {
  auto* ptr = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter>*>(jhandle);
  delete ptr;  // delete std::shared_ptr
}
