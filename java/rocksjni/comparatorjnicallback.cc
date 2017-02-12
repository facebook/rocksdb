// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Comparator.

#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
BaseComparatorJniCallback::BaseComparatorJniCallback(
    JNIEnv* env, jobject jComparator,
    const ComparatorJniCallbackOptions* copt)
    : mtx_compare(new port::Mutex(copt->use_adaptive_mutex)),
    mtx_findShortestSeparator(new port::Mutex(copt->use_adaptive_mutex)) {
  // Note: Comparator methods may be accessed by multiple threads,
  // so we ref the jvm not the env
  const jint rs __attribute__((unused)) = env->GetJavaVM(&m_jvm);
  assert(rs == JNI_OK);

  // Note: we want to access the Java Comparator instance
  // across multiple method calls, so we create a global ref
  m_jComparator = env->NewGlobalRef(jComparator);

  // Note: The name of a Comparator will not change during it's lifetime,
  // so we cache it in a global var
  jmethodID jNameMethodId = AbstractComparatorJni::getNameMethodId(env);
  jstring jsName = (jstring)env->CallObjectMethod(m_jComparator, jNameMethodId);
  m_name = JniUtil::copyString(env, jsName);  // also releases jsName

  m_jCompareMethodId = AbstractComparatorJni::getCompareMethodId(env);
  m_jFindShortestSeparatorMethodId =
    AbstractComparatorJni::getFindShortestSeparatorMethodId(env);
  m_jFindShortSuccessorMethodId =
    AbstractComparatorJni::getFindShortSuccessorMethodId(env);
}

const char* BaseComparatorJniCallback::Name() const {
  return m_name.c_str();
}

int BaseComparatorJniCallback::Compare(const Slice& a, const Slice& b) const {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = JniUtil::getJniEnv(m_jvm, &attached_thread);
  assert(env != nullptr);

  // TODO(adamretter): slice objects can potentially be cached using thread
  // local variables to avoid locking. Could make this configurable depending on
  // performance.
  mtx_compare->Lock();

  AbstractSliceJni::setHandle(env, m_jSliceA, &a, JNI_FALSE);
  AbstractSliceJni::setHandle(env, m_jSliceB, &b, JNI_FALSE);
  jint result =
    env->CallIntMethod(m_jComparator, m_jCompareMethodId, m_jSliceA,
      m_jSliceB);

  mtx_compare->Unlock();

  JniUtil::releaseJniEnv(m_jvm, attached_thread);

  return result;
}

void BaseComparatorJniCallback::FindShortestSeparator(
  std::string* start, const Slice& limit) const {
  if (start == nullptr) {
    return;
  }

  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = JniUtil::getJniEnv(m_jvm, &attached_thread);
  assert(env != nullptr);

  const char* startUtf = start->c_str();
  jstring jsStart = env->NewStringUTF(startUtf);

  // TODO(adamretter): slice object can potentially be cached using thread local
  // variable to avoid locking. Could make this configurable depending on
  // performance.
  mtx_findShortestSeparator->Lock();

  AbstractSliceJni::setHandle(env, m_jSliceLimit, &limit, JNI_FALSE);
  jstring jsResultStart =
    (jstring)env->CallObjectMethod(m_jComparator,
      m_jFindShortestSeparatorMethodId, jsStart, m_jSliceLimit);

  mtx_findShortestSeparator->Unlock();

  env->DeleteLocalRef(jsStart);

  if (jsResultStart != nullptr) {
    // update start with result
    *start =
      JniUtil::copyString(env, jsResultStart);  // also releases jsResultStart
  }

  JniUtil::releaseJniEnv(m_jvm, attached_thread);
}

void BaseComparatorJniCallback::FindShortSuccessor(std::string* key) const {
  if (key == nullptr) {
    return;
  }

  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = JniUtil::getJniEnv(m_jvm, &attached_thread);
  assert(env != nullptr);

  const char* keyUtf = key->c_str();
  jstring jsKey = env->NewStringUTF(keyUtf);

  jstring jsResultKey =
    (jstring)env->CallObjectMethod(m_jComparator,
      m_jFindShortSuccessorMethodId, jsKey);

  env->DeleteLocalRef(jsKey);

  if (jsResultKey != nullptr) {
    // updates key with result, also releases jsResultKey.
    *key = JniUtil::copyString(env, jsResultKey);
  }

  JniUtil::releaseJniEnv(m_jvm, attached_thread);
}

BaseComparatorJniCallback::~BaseComparatorJniCallback() {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = JniUtil::getJniEnv(m_jvm, &attached_thread);
  assert(env != nullptr);

  env->DeleteGlobalRef(m_jComparator);

  JniUtil::releaseJniEnv(m_jvm, attached_thread);
}

ComparatorJniCallback::ComparatorJniCallback(
    JNIEnv* env, jobject jComparator,
    const ComparatorJniCallbackOptions* copt) :
    BaseComparatorJniCallback(env, jComparator, copt) {
  m_jSliceA = env->NewGlobalRef(SliceJni::construct0(env));
  m_jSliceB = env->NewGlobalRef(SliceJni::construct0(env));
  m_jSliceLimit = env->NewGlobalRef(SliceJni::construct0(env));
}

ComparatorJniCallback::~ComparatorJniCallback() {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = JniUtil::getJniEnv(m_jvm, &attached_thread);
  assert(env != nullptr);

  env->DeleteGlobalRef(m_jSliceA);
  env->DeleteGlobalRef(m_jSliceB);
  env->DeleteGlobalRef(m_jSliceLimit);

  JniUtil::releaseJniEnv(m_jvm, attached_thread);
}

DirectComparatorJniCallback::DirectComparatorJniCallback(
    JNIEnv* env, jobject jComparator,
    const ComparatorJniCallbackOptions* copt) :
    BaseComparatorJniCallback(env, jComparator, copt) {
  m_jSliceA = env->NewGlobalRef(DirectSliceJni::construct0(env));
  m_jSliceB = env->NewGlobalRef(DirectSliceJni::construct0(env));
  m_jSliceLimit = env->NewGlobalRef(DirectSliceJni::construct0(env));
}

DirectComparatorJniCallback::~DirectComparatorJniCallback() {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = JniUtil::getJniEnv(m_jvm, &attached_thread);
  assert(env != nullptr);

  env->DeleteGlobalRef(m_jSliceA);
  env->DeleteGlobalRef(m_jSliceB);
  env->DeleteGlobalRef(m_jSliceLimit);

  JniUtil::releaseJniEnv(m_jvm, attached_thread);
}
}  // namespace rocksdb
