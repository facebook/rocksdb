// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::ThreadStatus methods from Java side.

#include <jni.h>

#include "portal.h"
#include "include/org_rocksdb_ThreadStatus.h"
#include "rocksdb/thread_status.h"

/*
 * Class:     org_rocksdb_ThreadStatus
 * Method:    getThreadTypeName
 * Signature: (B)Ljava/lang/String;
 */
jstring Java_org_rocksdb_ThreadStatus_getThreadTypeName(
    JNIEnv* env, jclass, jbyte jthread_type_value) {
  auto name = ROCKSDB_NAMESPACE::ThreadStatus::GetThreadTypeName(
      ROCKSDB_NAMESPACE::ThreadTypeJni::toCppThreadType(jthread_type_value));
  return ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &name, true);
}

/*
 * Class:     org_rocksdb_ThreadStatus
 * Method:    getOperationName
 * Signature: (B)Ljava/lang/String;
 */
jstring Java_org_rocksdb_ThreadStatus_getOperationName(
    JNIEnv* env, jclass, jbyte joperation_type_value) {
  auto name = ROCKSDB_NAMESPACE::ThreadStatus::GetOperationName(
      ROCKSDB_NAMESPACE::OperationTypeJni::toCppOperationType(
          joperation_type_value));
  return ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &name, true);
}

/*
 * Class:     org_rocksdb_ThreadStatus
 * Method:    microsToStringNative
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_ThreadStatus_microsToStringNative(
    JNIEnv* env, jclass, jlong jmicros) {
  auto str = ROCKSDB_NAMESPACE::ThreadStatus::MicrosToString(
      static_cast<uint64_t>(jmicros));
  return ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &str, true);
}

/*
 * Class:     org_rocksdb_ThreadStatus
 * Method:    getOperationStageName
 * Signature: (B)Ljava/lang/String;
 */
jstring Java_org_rocksdb_ThreadStatus_getOperationStageName(
    JNIEnv* env, jclass, jbyte joperation_stage_value) {
  auto name = ROCKSDB_NAMESPACE::ThreadStatus::GetOperationStageName(
      ROCKSDB_NAMESPACE::OperationStageJni::toCppOperationStage(
          joperation_stage_value));
  return ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &name, true);
}

/*
 * Class:     org_rocksdb_ThreadStatus
 * Method:    getOperationPropertyName
 * Signature: (BI)Ljava/lang/String;
 */
jstring Java_org_rocksdb_ThreadStatus_getOperationPropertyName(
    JNIEnv* env, jclass, jbyte joperation_type_value, jint jindex) {
  auto name = ROCKSDB_NAMESPACE::ThreadStatus::GetOperationPropertyName(
      ROCKSDB_NAMESPACE::OperationTypeJni::toCppOperationType(
          joperation_type_value),
      static_cast<int>(jindex));
  return ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &name, true);
}

/*
 * Class:     org_rocksdb_ThreadStatus
 * Method:    interpretOperationProperties
 * Signature: (B[J)Ljava/util/Map;
 */
jobject Java_org_rocksdb_ThreadStatus_interpretOperationProperties(
    JNIEnv* env, jclass, jbyte joperation_type_value,
    jlongArray joperation_properties) {

  //convert joperation_properties
  const jsize len = env->GetArrayLength(joperation_properties);
  const std::unique_ptr<uint64_t[]> op_properties(new uint64_t[len]);
  jlong* jop = env->GetLongArrayElements(joperation_properties, nullptr);
  if (jop == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  for (jsize i = 0; i < len; i++) {
    op_properties[i] = static_cast<uint64_t>(jop[i]);
  }
  env->ReleaseLongArrayElements(joperation_properties, jop, JNI_ABORT);

  // call the function
  auto result = ROCKSDB_NAMESPACE::ThreadStatus::InterpretOperationProperties(
      ROCKSDB_NAMESPACE::OperationTypeJni::toCppOperationType(
          joperation_type_value),
      op_properties.get());
  jobject jresult = ROCKSDB_NAMESPACE::HashMapJni::fromCppMap(env, &result);
  if (env->ExceptionCheck()) {
    // exception occurred
    return nullptr;
  }

  return jresult;
}

/*
 * Class:     org_rocksdb_ThreadStatus
 * Method:    getStateName
 * Signature: (B)Ljava/lang/String;
 */
jstring Java_org_rocksdb_ThreadStatus_getStateName(
  JNIEnv* env, jclass, jbyte jstate_type_value) {
  auto name = ROCKSDB_NAMESPACE::ThreadStatus::GetStateName(
      ROCKSDB_NAMESPACE::StateTypeJni::toCppStateType(jstate_type_value));
  return ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &name, true);
}
