//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksjni/associative_merge_operator_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {

  AssociativeMergeOperatorJniCallback::AssociativeMergeOperatorJniCallback(JNIEnv* env, jobject jMergeOperator)
    : JniCallback(env, jMergeOperator) {

    // Note: The name of a MergeOperator will not change during it's lifetime,
    // so we cache it in a global var
    jmethodID jNameMethodId = AssociativeMergeOperatorJni::getNameMethodId(env);
    if (jNameMethodId == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return;
    }
    jstring jsName = (jstring)env->CallObjectMethod(m_jcallback_obj, jNameMethodId);
    if (env->ExceptionCheck()) {
      // exception thrown
      return;
    }
    jboolean has_exception = JNI_FALSE;
    m_name = JniUtil::copyString(env, jsName,
                                 &has_exception);  // also releases jsName
    if (has_exception == JNI_TRUE) {
      // exception thrown
      return;
    }

    mergeMethodId = AssociativeMergeOperatorJni::getMergeMethodId(env);

    jclass returnTypeClassTmp = env->FindClass("org/rocksdb/ReturnType");
    if (returnTypeClassTmp == 0)
      rocksdb::RocksDBExceptionJni::ThrowNew(env, "unable to find object org.rocksdb.ReturnType");
    else
      returnTypeClass = reinterpret_cast<jclass>(env->NewGlobalRef(returnTypeClassTmp));

    returnTypeInitMethodId = env->GetMethodID(returnTypeClass, "<init>", "()V");
    if (returnTypeInitMethodId == 0)
      rocksdb::RocksDBExceptionJni::ThrowNew(env, "unable to find field ReturnType.<init>");

    returnTypeFieldId = env->GetFieldID(returnTypeClass, "isArgumentReference", "Z");
    if (returnTypeFieldId == 0)
      rocksdb::RocksDBExceptionJni::ThrowNew(env, "unable to find field ReturnType.isArgumentReference");

    catchAndLog(env);
  }

  bool AssociativeMergeOperatorJniCallback::Merge(const Slice &key,
                                                  const Slice *existing_value,
                                                  const Slice &value,
                                                  std::string *new_value,
                                                  Logger* /*logger*/) const {
    jboolean attached_thread = JNI_FALSE;
    JNIEnv *env = getJniEnv(&attached_thread);
    if (env == NULL)
      return false;

    jbyteArray jb0, jb1, jb2;
    jbyte *buf0;
    jbyte *buf1;
    jbyte *buf2;

    size_t _s0 = key.size() * sizeof(char);
    const jsize s0 = static_cast<jsize>(_s0);
    buf0 = (jbyte *) key.data();
    jb0 = env->NewByteArray(s0);
    env->SetByteArrayRegion(jb0, 0, s0, buf0);

    if (existing_value != NULL) {
      size_t _s1 = existing_value->size() * sizeof(char);
      const jsize s1 = static_cast<jsize>(_s1);
      buf1 = (jbyte *) existing_value->data();
      jb1 = env->NewByteArray(s1);
      env->SetByteArrayRegion(jb1, 0, s1, buf1);
    } else {
      buf1 = NULL;
      jb1 = NULL;
    }

    size_t _s2 = value.size() * sizeof(char);
    const jsize s2 = static_cast<jsize>(_s2);
    buf2 = (jbyte *) value.data();
    jb2 = env->NewByteArray(s2);
    env->SetByteArrayRegion(jb2, 0, s2, buf2);

    jobject rtobject = env->NewObject(returnTypeClass, returnTypeInitMethodId);
    jbyteArray jresult = (jbyteArray) env->CallObjectMethod(m_jcallback_obj,
                                                            mergeMethodId,
                                                            jb0, jb1, jb2, rtobject);
    jthrowable ex = env->ExceptionOccurred();
    catchAndLog(env);
    env->DeleteLocalRef(jb0);
    catchAndLog(env);
    if (jb1 != NULL)
      env->DeleteLocalRef(jb1);
    env->DeleteLocalRef(jb2);
    catchAndLog(env);
    jboolean rtr = env->GetBooleanField(rtobject, returnTypeFieldId);
    env->DeleteLocalRef(rtobject);

    if (ex) {
      if (jresult != nullptr) {
        env->DeleteLocalRef(jresult);
      }
      env->Throw(ex);
      releaseJniEnv(attached_thread);
      return false;
    } else {
      int len = env->GetArrayLength(jresult) / sizeof(char);
      char *result = (char *) env->GetByteArrayElements(jresult, 0);
      new_value->clear();
      new_value->assign(result, len);
      env->ReleaseByteArrayElements(jresult, (jbyte *) result, rtr ? JNI_COMMIT : JNI_ABORT);
      env->DeleteLocalRef(jresult);
      releaseJniEnv(attached_thread);
      return true;
    }
  }

  const char *AssociativeMergeOperatorJniCallback::Name() const {
    return m_name.get();
  }
}
