#include <jni.h>

#include <memory>
#include <string>

#include "include/org_rocksdb_limiter_ConcurrentTaskLimiterImpl.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_limiter_ConcurrentTaskLimiterImpl
 * Method:    newConcurrentTaskLimiterImpl0
 * Signature: (Ljava/lang/String;I)J
 */
jlong Java_org_rocksdb_limiter_ConcurrentTaskLimiterImpl_newConcurrentTaskLimiterImpl0(
    JNIEnv *env, jclass, jstring jname, jint limit) {
  jboolean has_exception;
  std::string name = ROCKSDB_NAMESPACE::JniUtil::copyStdString(env, jname, &has_exception);
  if (JNI_TRUE == has_exception) {
    return 0;
  }

  auto* ptr = new std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter>(
      ROCKSDB_NAMESPACE::NewConcurrentTaskLimiter(name, limit));

  return reinterpret_cast<jlong>(ptr);
}

/*
 * Class:     org_rocksdb_limiter_ConcurrentTaskLimiterImpl
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_limiter_ConcurrentTaskLimiterImpl_disposeInternal(
    JNIEnv *, jobject, jlong jhandle) {
  auto* ptr = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::ConcurrentTaskLimiter>*>(jhandle);
  delete ptr;  // delete std::shared_ptr
}