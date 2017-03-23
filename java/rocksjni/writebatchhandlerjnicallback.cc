// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
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
    JNIEnv* env, jobject jWriteBatchHandler)
    : m_env(env) {

  // Note: we want to access the Java WriteBatchHandler instance
  // across multiple method calls, so we create a global ref
  assert(jWriteBatchHandler != nullptr);
  m_jWriteBatchHandler = env->NewGlobalRef(jWriteBatchHandler);
  if(m_jWriteBatchHandler == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  m_jPutMethodId = WriteBatchHandlerJni::getPutMethodId(env);
  if(m_jPutMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jMergeMethodId = WriteBatchHandlerJni::getMergeMethodId(env);
  if(m_jMergeMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jDeleteMethodId = WriteBatchHandlerJni::getDeleteMethodId(env);
  if(m_jDeleteMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jDeleteRangeMethodId = WriteBatchHandlerJni::getDeleteRangeMethodId(env);
  if (m_jDeleteRangeMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jLogDataMethodId = WriteBatchHandlerJni::getLogDataMethodId(env);
  if(m_jLogDataMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jContinueMethodId = WriteBatchHandlerJni::getContinueMethodId(env);
  if(m_jContinueMethodId == nullptr) {
    // exception thrown
    return;
  }
}

void WriteBatchHandlerJniCallback::Put(const Slice& key, const Slice& value) {
  const jbyteArray j_key = sliceToJArray(key);
  if(j_key == nullptr) {
    // exception thrown
    if(m_env->ExceptionCheck()) {
      m_env->ExceptionDescribe();
    }
    return;
  }

  const jbyteArray j_value = sliceToJArray(value);
  if(j_value == nullptr) {
    // exception thrown
    if(m_env->ExceptionCheck()) {
      m_env->ExceptionDescribe();
    }
    if(j_key != nullptr) {
      m_env->DeleteLocalRef(j_key);
    }
    return;
  }

  m_env->CallVoidMethod(
      m_jWriteBatchHandler,
      m_jPutMethodId,
      j_key,
      j_value);
  if(m_env->ExceptionCheck()) {
    // exception thrown
    m_env->ExceptionDescribe();
    if(j_value != nullptr) {
      m_env->DeleteLocalRef(j_value);
    }
    if(j_key != nullptr) {
      m_env->DeleteLocalRef(j_key);
    }
    return;
  }

  if(j_value != nullptr) {
    m_env->DeleteLocalRef(j_value);
  }
  if(j_key != nullptr) {
    m_env->DeleteLocalRef(j_key);
  }
}

void WriteBatchHandlerJniCallback::Merge(const Slice& key, const Slice& value) {
  const jbyteArray j_key = sliceToJArray(key);
  if(j_key == nullptr) {
    // exception thrown
    if(m_env->ExceptionCheck()) {
      m_env->ExceptionDescribe();
    }
    return;
  }

  const jbyteArray j_value = sliceToJArray(value);
  if(j_value == nullptr) {
    // exception thrown
    if(m_env->ExceptionCheck()) {
      m_env->ExceptionDescribe();
    }
    if(j_key != nullptr) {
      m_env->DeleteLocalRef(j_key);
    }
    return;
  }

  m_env->CallVoidMethod(
      m_jWriteBatchHandler,
      m_jMergeMethodId,
      j_key,
      j_value);
  if(m_env->ExceptionCheck()) {
    // exception thrown
    m_env->ExceptionDescribe();
    if(j_value != nullptr) {
      m_env->DeleteLocalRef(j_value);
    }
    if(j_key != nullptr) {
      m_env->DeleteLocalRef(j_key);
    }
    return;
  }

  if(j_value != nullptr) {
    m_env->DeleteLocalRef(j_value);
  }
  if(j_key != nullptr) {
    m_env->DeleteLocalRef(j_key);
  }
}

void WriteBatchHandlerJniCallback::Delete(const Slice& key) {
  const jbyteArray j_key = sliceToJArray(key);
  if(j_key == nullptr) {
    // exception thrown
    if(m_env->ExceptionCheck()) {
      m_env->ExceptionDescribe();
    }
    return;
  }

  m_env->CallVoidMethod(
      m_jWriteBatchHandler,
      m_jDeleteMethodId,
      j_key);
  if(m_env->ExceptionCheck()) {
    // exception thrown
    m_env->ExceptionDescribe();
    if(j_key != nullptr) {
      m_env->DeleteLocalRef(j_key);
    }
    return;
  }

  if(j_key != nullptr) {
    m_env->DeleteLocalRef(j_key);
  }
}

void WriteBatchHandlerJniCallback::DeleteRange(const Slice& beginKey,
                                               const Slice& endKey) {
  const jbyteArray j_beginKey = sliceToJArray(beginKey);
  if (j_beginKey == nullptr) {
    // exception thrown
    if (m_env->ExceptionCheck()) {
      m_env->ExceptionDescribe();
    }
    return;
  }

  const jbyteArray j_endKey = sliceToJArray(beginKey);
  if (j_endKey == nullptr) {
    // exception thrown
    if (m_env->ExceptionCheck()) {
      m_env->ExceptionDescribe();
    }
    return;
  }

  m_env->CallVoidMethod(m_jWriteBatchHandler, m_jDeleteRangeMethodId,
                        j_beginKey, j_endKey);
  if (m_env->ExceptionCheck()) {
    // exception thrown
    m_env->ExceptionDescribe();
    if (j_beginKey != nullptr) {
      m_env->DeleteLocalRef(j_beginKey);
    }
    if (j_endKey != nullptr) {
      m_env->DeleteLocalRef(j_endKey);
    }
    return;
  }

  if (j_beginKey != nullptr) {
    m_env->DeleteLocalRef(j_beginKey);
  }

  if (j_endKey != nullptr) {
    m_env->DeleteLocalRef(j_endKey);
  }
}

void WriteBatchHandlerJniCallback::LogData(const Slice& blob) {
  const jbyteArray j_blob = sliceToJArray(blob);
  if(j_blob == nullptr) {
    // exception thrown
    if(m_env->ExceptionCheck()) {
      m_env->ExceptionDescribe();
    }
    return;
  }

  m_env->CallVoidMethod(
      m_jWriteBatchHandler,
      m_jLogDataMethodId,
      j_blob);
  if(m_env->ExceptionCheck()) {
    // exception thrown
    m_env->ExceptionDescribe();
    if(j_blob != nullptr) {
      m_env->DeleteLocalRef(j_blob);
    }
    return;
  }

  if(j_blob != nullptr) {
    m_env->DeleteLocalRef(j_blob);
  }
}

bool WriteBatchHandlerJniCallback::Continue() {
  jboolean jContinue = m_env->CallBooleanMethod(
      m_jWriteBatchHandler,
      m_jContinueMethodId);
  if(m_env->ExceptionCheck()) {
    // exception thrown
    m_env->ExceptionDescribe();
  }

  return static_cast<bool>(jContinue == JNI_TRUE);
}

/*
 * Creates a Java Byte Array from the data in a Slice
 *
 * When calling this function
 * you must remember to call env->DeleteLocalRef
 * on the result after you have finished with it
 *
 * @param s A Slice to convery to a Java byte array
 *
 * @return A reference to a Java byte array, or a nullptr if an
 *     exception occurs
 */
jbyteArray WriteBatchHandlerJniCallback::sliceToJArray(const Slice& s) {
  jbyteArray ja = m_env->NewByteArray(static_cast<jsize>(s.size()));
  if(ja == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  m_env->SetByteArrayRegion(
      ja, 0, static_cast<jsize>(s.size()),
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(s.data())));
  if(m_env->ExceptionCheck()) {
    if(ja != nullptr) {
      m_env->DeleteLocalRef(ja);
    }
    // exception thrown: ArrayIndexOutOfBoundsException
    return nullptr;
  }

  return ja;
}

WriteBatchHandlerJniCallback::~WriteBatchHandlerJniCallback() {
  if(m_jWriteBatchHandler != nullptr) {
    m_env->DeleteGlobalRef(m_jWriteBatchHandler);
  }
}
}  // namespace rocksdb
