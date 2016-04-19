//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// JNI Callbacks from C++ to sub-classes or org.rocksdb.RocksCallbackObject

#include <assert.h>
#include "rocksjni/jnicallback.h"

namespace rocksdb {
JniCallback::JniCallback(JNIEnv* env, jobject jcallback_obj) {
  // Note: jcallback_obj may be accessed by multiple threads,
  // so we ref the jvm not the env
  const jint rs = env->GetJavaVM(&m_jvm);
  if(rs != JNI_OK) {
    // exception thrown
    return;
  }

  // Note: we may want to access the Java callback object instance
  // across multiple method calls, so we create a global ref
  assert(jcallback_obj != nullptr);
  m_jcallback_obj = env->NewGlobalRef(jcallback_obj);
  if(jcallback_obj == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
}

JNIEnv* JniCallback::getJniEnv() const {
  JNIEnv *env;
  jint rs __attribute__((unused)) =
      m_jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL);
  assert(rs == JNI_OK);
  return env;
}

void JniCallback::releaseJniEnv() const {
  m_jvm->DetachCurrentThread();
}

JniCallback::~JniCallback() {
  JNIEnv* m_env = getJniEnv();
  m_env->DeleteGlobalRef(m_jcallback_obj);

  // Note: do not need to explicitly detach, as this function is effectively
  // called from the Java class's disposeInternal method, and so already
  // has an attached thread, getJniEnv above is just a no-op Attach to get
  // the env jvm->DetachCurrentThread();
}
}  // namespace rocksdb