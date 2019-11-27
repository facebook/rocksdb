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
    if (mergeMethodId == nullptr)
      rocksdb::RocksDBExceptionJni::ThrowNew(env, "unable to find merge method id");

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

  AssociativeMergeOperatorJniCallback::~AssociativeMergeOperatorJniCallback() {
    jboolean attached_thread = JNI_FALSE;
    JNIEnv *env = getJniEnv(&attached_thread);
    if (env == nullptr)
      return;
    env->DeleteGlobalRef(returnTypeClass);
  }

  bool AssociativeMergeOperatorJniCallback::GetByteArray(JNIEnv *env, size_t size, const char* value, jbyteArray& jb) const {
    const jsize size_ = static_cast<jsize>(size);
    jbyte *buf = (jbyte *)value;
    jb = env->NewByteArray(size_);
    if (jb == nullptr) {
      // exception thrown: OutOfMemoryError
      return false;
    }
    env->SetByteArrayRegion(jb, 0, size_, buf);
    if (env->ExceptionCheck()) {
      catchAndLog(env);
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jb);
      return false;
    }
    return true;
  }

  bool AssociativeMergeOperatorJniCallback::Merge(const Slice &key,
                                                  const Slice *existing_value,
                                                  const Slice &value,
                                                  std::string *new_value,
                                                  Logger* /*logger*/) const {
    jboolean attached_thread = JNI_FALSE;
    JNIEnv *env = getJniEnv(&attached_thread);
    if (env == nullptr)
      return false;

    jbyteArray jb0, jb1, jb2;

    if (GetByteArray(env, key.size(), key.data(), jb0) == false)
      return false;

    if (existing_value != nullptr) {
      if (GetByteArray(env, existing_value->size(), existing_value->data(), jb1) == false) {
        env->DeleteLocalRef(jb0);
        return false;
      }
    } else {
      jb1 = nullptr;
    }

    if (GetByteArray(env, value.size(), value.data(), jb2) == false) {
      env->DeleteLocalRef(jb0);
      if (jb1 != nullptr)
        env->DeleteLocalRef(jb1);
      return false;
    }

    jobject rtobject = env->NewObject(returnTypeClass, returnTypeInitMethodId);
    jbyteArray jresult = nullptr;
    if (rtobject != nullptr)
      jresult = (jbyteArray) env->CallObjectMethod(m_jcallback_obj,
                                                   mergeMethodId,
                                                   jb0, jb1, jb2, rtobject);

    jthrowable ex = env->ExceptionOccurred();
    catchAndLog(env);
    env->DeleteLocalRef(jb0);
    if (jb1 != nullptr)
      env->DeleteLocalRef(jb1);
    env->DeleteLocalRef(jb2);
    catchAndLog(env);

    if (jresult == nullptr)
      return false;

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
      if (len == 0)
        return false;
      char *result = (char *)env->GetByteArrayElements(jresult, 0);
      if (result == nullptr)
        return false;
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
