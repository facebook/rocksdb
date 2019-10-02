//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksjni/merge_operator_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {

  MergeOperatorJniCallback::MergeOperatorJniCallback(JNIEnv* env,
                                                     jobject jMergeOperator,
                                                     jboolean _allowSingleOperand,
                                                     jboolean _allowShouldMerge,
                                                     jboolean _allowPartialMultiMerge)
      : JniCallback(env, jMergeOperator)
  {
    allowSingleOperand = _allowSingleOperand;
    allowShouldMerge = _allowShouldMerge;
    allowPartialMultiMerge = _allowPartialMultiMerge;

    // Note: The name of a MergeOperator will not change during it's lifetime,
    // so we cache it in a global var
    jmethodID jNameMethodId = MergeOperatorJni::getNameMethodId(env);
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

    fullMergeMethodId = MergeOperatorJni::getFullMergeMethodId(env);
    if (fullMergeMethodId == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return;
    }

    partialMultiMergeMethodId = MergeOperatorJni::getPartialMultiMergeMethodId(env);
    if (partialMultiMergeMethodId == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return;
    }

    partialMergeMethodId = MergeOperatorJni::getPartialMergeMethodId(env);
    if (partialMergeMethodId == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return;
    }

    shouldMergeMethodId = MergeOperatorJni::getShouldMergeMethodId(env);
    if (shouldMergeMethodId == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return;
    }

    jclass cls = MergeOperatorJni::getJClass(env);
    jclass a = ByteJni::getArrayJClass(env);
    if (a == 0) {
      cls = env->FindClass("java/lang/Error");
      env->ThrowNew(cls, "unable to find object []");
    } else
      byteArrayClass = reinterpret_cast<jclass>(env->NewGlobalRef(a));

    catchAndLog(env);
  }

  bool MergeOperatorJniCallback::GetByteArray(JNIEnv *env, int size, const char* data, jbyteArray& jb) const {
    size_t _size = size * sizeof(char);
    const jsize s = static_cast<jsize>(_size);
    jbyte *buf = (jbyte *)data;
    jb = env->NewByteArray(s);
    if (jb == nullptr) {
      return false;
    }
    env->SetByteArrayRegion(jb, 0, s, buf);
    if (env->ExceptionCheck()) {
      catchAndLog(env);
      env->DeleteLocalRef(jb);
      return false;
    }
    return true;
  }

  bool MergeOperatorJniCallback::GetNewValue(JNIEnv *env, jbyteArray &jresult, std::string *new_value) const
  {
    int len = env->GetArrayLength(jresult) / sizeof(char);
    if (len == 0) {
      return false;
    }
    char *result = (char *)env->GetByteArrayElements(jresult, 0);
    if (result == nullptr) {
      return false;
    }
    new_value->clear();
    new_value->assign(result, len);
    env->ReleaseByteArrayElements(jresult, (jbyte *)result, JNI_ABORT);
    return true;
  }

  bool MergeOperatorJniCallback::PartialMerge(const Slice &key,
                                              const Slice &lop,
                                              const Slice &rop,
                                              std::string *new_value,
                                              Logger* /*logger*/) const
  {
    jboolean attached_thread = JNI_FALSE;
    JNIEnv *env = getJniEnv(&attached_thread);
    if (env == nullptr) return false;
    jbyteArray jb0, jb1, jb2;

    bool jb0_init = GetByteArray(env, key.size(), key.data(), jb0);
    bool jb1_init = GetByteArray(env, lop.size(), lop.data(), jb1);
    bool jb2_init = GetByteArray(env, rop.size(), rop.data(), jb2);

    jbyteArray jresult = nullptr;
    if (jb0_init && jb1_init && jb2_init) {
      jresult = (jbyteArray) env->CallObjectMethod(m_jcallback_obj, partialMergeMethodId, jb0, jb1, jb2);
    }

    if (jb0_init) env->DeleteLocalRef(jb0);
    if (jb1_init) env->DeleteLocalRef(jb1);
    if (jb2_init) env->DeleteLocalRef(jb2);

    bool success = true;
    jthrowable ex = env->ExceptionOccurred();
    if (jresult == nullptr || ex) {
      if (jresult != nullptr) {
        env->DeleteLocalRef(jresult);
      }
      env->Throw(ex);
      success = false;
    } else {
      success = GetNewValue(env, jresult, new_value);
      env->DeleteLocalRef(jresult);
    }
    releaseJniEnv(attached_thread);
    return success;
  }


  bool MergeOperatorJniCallback::PartialMergeMulti(const Slice &key,
                                                   const std::deque <Slice> &operands,
                                                   std::string *new_value,
                                                   Logger* /*logger*/) const
  {
    if (!allowPartialMultiMerge) return false;

    jboolean attached_thread = JNI_FALSE;
    JNIEnv *env = getJniEnv(&attached_thread);
    if (env == NULL) return false;

    jbyteArray jbr;
    if (GetByteArray(env, key.size(), key.data(), jbr) == false)
      return false;

    size_t _opSize = operands.size();
    const jsize opSize = static_cast<jsize>(_opSize);
    jobjectArray oa = env->NewObjectArray(opSize, byteArrayClass, NULL);
    if (oa == nullptr) {
      env->DeleteLocalRef(jbr);
    }
    Slice slice;
    jbyteArray jb[_opSize];
    bool initOk = true;
    for (jsize i = 0; i < opSize; i++) {
      slice = operands.at(i);
      if (GetByteArray(env, slice.size(), slice.data(), jb[i]) == false) {
        initOk = false;
        break;
      }
      env->SetObjectArrayElement(oa, i, jb[i]);
    }
    if (initOk == false) {
      env->DeleteLocalRef(jbr);
      for (size_t i = 0; i < _opSize; i++) {
        if (jb[i] != nullptr)
          env->DeleteLocalRef(jb[i]);
      }
      return false;
    }

    jbyteArray jresult = (jbyteArray)env->CallObjectMethod(m_jcallback_obj, partialMultiMergeMethodId, jbr, oa);
    jthrowable ex = env->ExceptionOccurred();

    for (size_t i = 0; i < _opSize; i++) {
      env->DeleteLocalRef(jb[i]);
    }
    env->DeleteLocalRef(oa);
    env->DeleteLocalRef(jbr);

    bool success = true;
    if (jresult == nullptr || ex) {
      if (jresult != nullptr) {
        env->DeleteLocalRef(jresult);
      }
      env->Throw(ex);
      success = false;
    } else {
      success = GetNewValue(env, jresult, new_value);
      env->DeleteLocalRef(jresult);
    }
    releaseJniEnv(attached_thread);
    return success;
  }

  bool MergeOperatorJniCallback::ShouldMerge(const std::vector<Slice> &operands) const
  {
    if (!allowShouldMerge) {
      return false;
    }

    jboolean attached_thread = JNI_FALSE;
    JNIEnv *env = getJniEnv(&attached_thread);

    if (env == nullptr) {
      return false;
    }    
    catchAndLog(env);

    size_t _opSize = operands.size();
    const jsize opSize = static_cast<jsize>(_opSize);
    jobjectArray oa = env->NewObjectArray(opSize, byteArrayClass, NULL);
    if (oa == nullptr) {
      releaseJniEnv(attached_thread);
      return false;
    }
    Slice slice;
    jbyteArray jb[_opSize];
    bool initOk = true;
    for (jsize i = 0; i < opSize; i++) {
      slice = operands.at(i);
      if (GetByteArray(env, slice.size(), slice.data(), jb[i]) == false) {
        initOk = false;
        break;
      }
      env->SetObjectArrayElement(oa, i, jb[i]);
    }
    if (initOk == false) {
      for (size_t i = 0; i < _opSize; i++) {
        if (jb[i] != nullptr)
          env->DeleteLocalRef(jb[i]);
      }
      releaseJniEnv(attached_thread);
      return false;
    }

    jboolean jresult = env->CallBooleanMethod(m_jcallback_obj, shouldMergeMethodId, oa);
    jthrowable ex = env->ExceptionOccurred();

    for (jsize i = 0; i < opSize; i++) {
      env->DeleteLocalRef(jb[i]);
    }
    env->DeleteLocalRef(oa);

    bool success = (bool)jresult;
    if (ex) {
      env->ExceptionDescribe();
      return false;
    }
    releaseJniEnv(attached_thread);
    return success;
  }

  bool MergeOperatorJniCallback::FullMergeV2(const MergeOperationInput &merge_in,
                                             MergeOperationOutput *merge_out) const
  {
    jboolean attached_thread = JNI_FALSE;
    JNIEnv *env = getJniEnv(&attached_thread);
    if (env == NULL) {
      return false;
    }
    jbyteArray jb0, jb1;

    Slice key = merge_in.key;
    if (GetByteArray(env, key.size(), key.data(), jb0) == false) {
      return false;
    }

    const Slice *existing_value = merge_in.existing_value;
    if (existing_value != nullptr) {
      if (GetByteArray(env, existing_value->size(), existing_value->data(), jb1) == false) {
        env->DeleteLocalRef(jb0);
        return false;
      }
    } else {
      jb1 = nullptr;
    }

    std::vector <rocksdb::Slice> operands = merge_in.operand_list;

    size_t _opSize = operands.size();
    const jsize opSize = static_cast<jsize>(_opSize);
    jobjectArray oa = env->NewObjectArray(opSize, byteArrayClass, NULL);
    if (oa == nullptr) {
      env->DeleteLocalRef(jb0);
      if (jb1 != nullptr)
        env->DeleteLocalRef(jb1);
      return false;
    }
    Slice slice;
    jbyteArray jb[_opSize];
    bool initOk = true;
    for (jsize i = 0; i < opSize; i++) {
      slice = operands.at(i);
      if (GetByteArray(env, slice.size(), slice.data(), jb[i]) == false) {
        initOk = false;
        break;
      }
      env->SetObjectArrayElement(oa, i, jb[i]);
    }
    if (initOk == false) {
      env->DeleteLocalRef(jb0);
      if (jb1 != nullptr)
        env->DeleteLocalRef(jb1);
      for (size_t i = 0; i < _opSize; i++) {
        if (jb[i] != nullptr)
          env->DeleteLocalRef(jb[i]);
      }
      return false;
    }

    jbyteArray jresult = (jbyteArray) env->CallObjectMethod(m_jcallback_obj, fullMergeMethodId, jb0, jb1, oa);
    jthrowable ex = env->ExceptionOccurred();

    env->DeleteLocalRef(jb0);
    if (jb1 != nullptr)
      env->DeleteLocalRef(jb1);
    for (size_t i = 0; i < _opSize; i++) {
      env->DeleteLocalRef(jb[i]);
    }
    env->DeleteLocalRef(oa);

    bool success = true;
    if (jresult == nullptr || ex) {
      if (jresult != nullptr) {
        env->DeleteLocalRef(jresult);
      }
      env->Throw(ex);
      success = false;
    } else {
      success = GetNewValue(env, jresult, &(merge_out->new_value));
      env->DeleteLocalRef(jresult);
      success = true;
    }
    releaseJniEnv(attached_thread);
    return success;
  }


  const char *MergeOperatorJniCallback::Name() const {
    return m_name.get();
  }

  bool MergeOperatorJniCallback::AllowSingleOperand() const { return allowSingleOperand; }
}
