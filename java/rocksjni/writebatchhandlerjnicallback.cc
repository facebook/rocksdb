// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Comparator.

#include "rocksjni/writebatchhandlerjnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
WriteBatchHandlerJniCallback::WriteBatchHandlerJniCallback(
    JNIEnv* env, jobject jWriteBatchHandler) {

  // Note: WriteBatchHandler methods may be accessed by multiple threads,
  // so we ref the jvm not the env
  const jint rs = env->GetJavaVM(&m_jvm);
  assert(rs == JNI_OK);

  // Note: we want to access the Java WriteBatchHandler instance
  // across multiple method calls, so we create a global ref
  m_jWriteBatchHandler = env->NewGlobalRef(jWriteBatchHandler);

  m_jPutMethodId = WriteBatchHandlerJni::getPutMethodId(env);
  m_jMergeMethodId = WriteBatchHandlerJni::getMergeMethodId(env);
  m_jDeleteMethodId = WriteBatchHandlerJni::getDeleteMethodId(env);
  m_jLogDataMethodId = WriteBatchHandlerJni::getLogDataMethodId(env);
  m_jContinueMethodId = WriteBatchHandlerJni::getContinueMethodId(env);
}

/**
 * Attach/Get a JNIEnv for the current native thread
 */
JNIEnv* WriteBatchHandlerJniCallback::getJniEnv() const {
  JNIEnv *env;
  jint rs = m_jvm->AttachCurrentThread(reinterpret_cast<void **>(&env), NULL);
  assert(rs == JNI_OK);
  return env;
}

void WriteBatchHandlerJniCallback::Put(const Slice& key, const Slice& value) {
  getJniEnv()->CallVoidMethod(
      m_jWriteBatchHandler,
      m_jPutMethodId,
      sliceToJArray(key),
      sliceToJArray(value));
}

void WriteBatchHandlerJniCallback::Merge(const Slice& key, const Slice& value) {
  getJniEnv()->CallVoidMethod(
      m_jWriteBatchHandler,
      m_jMergeMethodId,
      sliceToJArray(key),
      sliceToJArray(value));
}

void WriteBatchHandlerJniCallback::Delete(const Slice& key) {
  getJniEnv()->CallVoidMethod(
      m_jWriteBatchHandler,
      m_jDeleteMethodId,
      sliceToJArray(key));
}

void WriteBatchHandlerJniCallback::LogData(const Slice& blob) {
  getJniEnv()->CallVoidMethod(
      m_jWriteBatchHandler,
      m_jLogDataMethodId,
      sliceToJArray(blob));
}

bool WriteBatchHandlerJniCallback::Continue() {
  jboolean jContinue = getJniEnv()->CallBooleanMethod(
      m_jWriteBatchHandler,
      m_jContinueMethodId);

  return static_cast<bool>(jContinue == JNI_TRUE);
}

jbyteArray WriteBatchHandlerJniCallback::sliceToJArray(const Slice& s) {
  jbyteArray ja = getJniEnv()->NewByteArray(s.size());
  getJniEnv()->SetByteArrayRegion(
      ja, 0, s.size(),
      reinterpret_cast<const jbyte*>(s.data()));
  return ja;
}

WriteBatchHandlerJniCallback::~WriteBatchHandlerJniCallback() {
  JNIEnv* m_env = getJniEnv();

  m_env->DeleteGlobalRef(m_jWriteBatchHandler);

  // Note: do not need to explicitly detach, as this function is effectively
  // called from the Java class's disposeInternal method, and so already
  // has an attached thread, getJniEnv above is just a no-op Attach to get
  // the env jvm->DetachCurrentThread();
}
}  // namespace rocksdb
