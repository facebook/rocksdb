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
    if(jNameMethodId == nullptr) {
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

    partialMultiMergeMethodId = MergeOperatorJni::getPartialMultiMergeMethodId(env);

    partialMergeMethodId = MergeOperatorJni::getPartialMergeMethodId(env);

    shouldMergeMethodId = MergeOperatorJni::getShouldMergeMethodId(env);

    jclass cls = MergeOperatorJni::getJClass(env);
    jclass a = ByteJni::getArrayJClass(env);
    if (a == 0) {
      cls = env->FindClass("java/lang/Error");
      env->ThrowNew(cls, "unable to find object []");
    } else
      byteArrayClass = reinterpret_cast<jclass>(env->NewGlobalRef(a));

    catchAndLog(env);
  }

  bool MergeOperatorJniCallback::PartialMerge(const Slice &key,
                                              const Slice &lop,
                                              const Slice &rop,
                                              std::string *new_value,
                                              Logger* /*logger*/) const
  {
    jboolean attached_thread = JNI_FALSE;
    JNIEnv *env = getJniEnv(&attached_thread);
    if (env == NULL) return false;
    jbyteArray jb0, jb1, jb2;
    jbyte *buf0;
    jbyte *buf1;
    jbyte *buf2;

    size_t _s0 = key.size() * sizeof(char);
    const jsize s0 = static_cast<jsize>(_s0);
    buf0 = (jbyte *) key.data();
    jb0 = env->NewByteArray(s0);
    env->SetByteArrayRegion(jb0, 0, s0, buf0);


    size_t _s1 = lop.size() * sizeof(char);
    const jsize s1 = static_cast<jsize>(_s1);
    buf1 = (jbyte *) lop.data();
    jb1 = env->NewByteArray(s1);

    env->SetByteArrayRegion(jb1, 0, s1, buf1);


    size_t _s2 = rop.size() * sizeof(char);
    const jsize s2 = static_cast<jsize>(_s2);
    buf2 = (jbyte *) rop.data();
    jb2 = env->NewByteArray(s2);
    env->SetByteArrayRegion(jb2, 0, s2, buf2);


    jbyteArray jresult = (jbyteArray) env->CallObjectMethod(m_jcallback_obj, partialMergeMethodId, jb0, jb1, jb2);
    jthrowable ex = env->ExceptionOccurred();

    env->DeleteLocalRef(jb0);
    env->DeleteLocalRef(jb1);
    env->DeleteLocalRef(jb2);

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
      env->ReleaseByteArrayElements(jresult, (jbyte *) result, JNI_ABORT);

      releaseJniEnv(attached_thread);
      return true;
    }
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

    size_t _opSize = operands.size();
    const jsize opSize = static_cast<jsize>(_opSize);
    jobjectArray oa = env->NewObjectArray(opSize, byteArrayClass, NULL);
    Slice slice;
    jbyteArray jb[_opSize];
    for (jsize i = 0; i < opSize; i++) {
      slice = operands.at(i);
      const jsize sliceSize = static_cast<jsize>(slice.size());
      jb[i] = env->NewByteArray(sliceSize);
      env->SetByteArrayRegion(jb[i], 0, sliceSize, (jbyte *) slice.data());
      env->SetObjectArrayElement(oa, i, jb[i]);
    }
    size_t _s0 = key.size() * sizeof(char);
    const jsize s0 = static_cast<jsize>(_s0);
    jbyteArray jbr = env->NewByteArray(s0);
    jbyte *buf = (jbyte *) key.data();
    env->SetByteArrayRegion(jbr, 0, s0, buf);


    jbyteArray jresult = (jbyteArray) env->CallObjectMethod(m_jcallback_obj, partialMultiMergeMethodId, jbr, oa);
    jthrowable ex = env->ExceptionOccurred();

    for (size_t i = 0; i < _opSize; i++) {
      env->DeleteLocalRef(jb[i]);
    }
    env->DeleteLocalRef(oa);

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
      env->ReleaseByteArrayElements(jresult, (jbyte *) result, JNI_ABORT);
      releaseJniEnv(attached_thread);
      return true;
    }
  }


  bool MergeOperatorJniCallback::ShouldMerge(const std::vector <Slice> &operands) const
  {
    if (!allowShouldMerge) return false;

    jboolean attached_thread = JNI_FALSE;
    JNIEnv *env = getJniEnv(&attached_thread);

    if (env == NULL) return false;
    size_t _opSize = operands.size();
    const jsize opSize = static_cast<jsize>(_opSize);
    jobjectArray oa = env->NewObjectArray(opSize, byteArrayClass, NULL);
    Slice slice;
    jbyteArray jb[opSize];
    for (jsize i = 0; i < opSize; i++) {
      slice = operands.at(i);
      const jsize sliceSize = static_cast<jsize>(slice.size());
      jb[i] = env->NewByteArray(sliceSize);
      env->SetByteArrayRegion(jb[i], 0, sliceSize, (jbyte *) slice.data());
      env->SetObjectArrayElement(oa, i, jb[i]);
    }

    jboolean jresult = env->CallBooleanMethod(m_jcallback_obj, shouldMergeMethodId, oa);
    jthrowable ex = env->ExceptionOccurred();

    for (jsize i = 0; i < opSize; i++) {
      env->DeleteLocalRef(jb[i]);
    }

    env->DeleteLocalRef(oa);

    if (ex) {
      env->Throw(ex);
      releaseJniEnv(attached_thread);
      return false;
    } else {
      releaseJniEnv(attached_thread);
      return (bool) jresult;
    }
  }

  bool MergeOperatorJniCallback::FullMergeV2(const MergeOperationInput &merge_in,
                                             MergeOperationOutput *merge_out) const
  {
    jboolean attached_thread = JNI_FALSE;
    JNIEnv *env = getJniEnv(&attached_thread);
    if (env == NULL) return false;
    jbyteArray jb0, jb1;
    jbyte *buf0;
    jbyte *buf1;

    Slice key = merge_in.key;
    size_t _s0 = key.size() * sizeof(char);
    const jsize s0 = static_cast<jsize>(_s0);
    buf0 = (jbyte *) key.data();
    jb0 = env->NewByteArray(s0);
    env->SetByteArrayRegion(jb0, 0, s0, buf0);
    const Slice *existing_value = merge_in.existing_value;

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

    std::vector <rocksdb::Slice> operands = merge_in.operand_list;
    size_t _opSize = operands.size();
    const jsize opSize = static_cast<jsize>(_opSize);
    jobjectArray oa = env->NewObjectArray(opSize, byteArrayClass, NULL);
    Slice slice;
    jbyteArray jb[_opSize];
    for (jsize i = 0; i < opSize; i++) {
      slice = operands.at(i);
      const jsize sliceSize = static_cast<jsize>(slice.size());
      jb[i] = env->NewByteArray(sliceSize);
      env->SetByteArrayRegion(jb[i], 0, sliceSize, (jbyte *) slice.data());
      env->SetObjectArrayElement(oa, i, jb[i]);
    }

    jbyteArray jresult = (jbyteArray) env->CallObjectMethod(m_jcallback_obj, fullMergeMethodId, jb0, jb1, oa);
    jthrowable ex = env->ExceptionOccurred();
    env->DeleteLocalRef(jb0);

    if (existing_value != NULL)
      env->DeleteLocalRef(jb1);

    for (size_t i = 0; i < _opSize; i++) {
      env->DeleteLocalRef(jb[i]);
    }
    env->DeleteLocalRef(oa);

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
      merge_out->new_value.clear();
      merge_out->new_value.assign(result, len);
      env->ReleaseByteArrayElements(jresult, (jbyte *) result, JNI_ABORT);
      env->DeleteLocalRef(jresult);
      releaseJniEnv(attached_thread);
      return true;
    }
  }


  const char *MergeOperatorJniCallback::Name() const {
    return m_name.get();
  }

  bool MergeOperatorJniCallback::AllowSingleOperand() const { return allowSingleOperand; }
}
