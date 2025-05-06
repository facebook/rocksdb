// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#pragma once

#include <jni.h>

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksjni/portal/common.h"
#include "rocksjni/portal/rocks_d_b_native_class.h"

namespace ROCKSDB_NAMESPACE {
// The portal class for org.rocksdb.AbstractEventListener
class AbstractEventListenerJni
    : public RocksDBNativeClass<
          const ROCKSDB_NAMESPACE::EventListenerJniCallback*,
          AbstractEventListenerJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractEventListener
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
                                         "org/rocksdb/AbstractEventListener");
  }

  /**
   * Get the Java Method: AbstractEventListener#onFlushCompletedProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFlushCompletedProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onFlushCompletedProxy",
                                            "(JLorg/rocksdb/FlushJobInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFlushBeginProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFlushBeginProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onFlushBeginProxy",
                                            "(JLorg/rocksdb/FlushJobInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onTableFileDeleted
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnTableFileDeletedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onTableFileDeleted", "(Lorg/rocksdb/TableFileDeletionInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onCompactionBeginProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnCompactionBeginProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid =
        env->GetMethodID(jclazz, "onCompactionBeginProxy",
                         "(JLorg/rocksdb/CompactionJobInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onCompactionCompletedProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnCompactionCompletedProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid =
        env->GetMethodID(jclazz, "onCompactionCompletedProxy",
                         "(JLorg/rocksdb/CompactionJobInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onTableFileCreated
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnTableFileCreatedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onTableFileCreated", "(Lorg/rocksdb/TableFileCreationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onTableFileCreationStarted
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnTableFileCreationStartedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid =
        env->GetMethodID(jclazz, "onTableFileCreationStarted",
                         "(Lorg/rocksdb/TableFileCreationBriefInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onMemTableSealed
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnMemTableSealedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onMemTableSealed",
                                            "(Lorg/rocksdb/MemTableInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method:
   * AbstractEventListener#onColumnFamilyHandleDeletionStarted
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnColumnFamilyHandleDeletionStartedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid =
        env->GetMethodID(jclazz, "onColumnFamilyHandleDeletionStarted",
                         "(Lorg/rocksdb/ColumnFamilyHandle;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onExternalFileIngestedProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnExternalFileIngestedProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid =
        env->GetMethodID(jclazz, "onExternalFileIngestedProxy",
                         "(JLorg/rocksdb/ExternalFileIngestionInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onBackgroundError
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnBackgroundErrorProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onBackgroundErrorProxy",
                                            "(BLorg/rocksdb/Status;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onStallConditionsChanged
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnStallConditionsChangedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onStallConditionsChanged",
                                            "(Lorg/rocksdb/WriteStallInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileReadFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileReadFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileReadFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileWriteFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileWriteFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileWriteFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileFlushFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileFlushFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileFlushFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileSyncFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileSyncFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileSyncFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileRangeSyncFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileRangeSyncFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileRangeSyncFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileTruncateFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileTruncateFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileTruncateFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onFileCloseFinish
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnFileCloseFinishMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(
        jclazz, "onFileCloseFinish", "(Lorg/rocksdb/FileOperationInfo;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#shouldBeNotifiedOnFileIO
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getShouldBeNotifiedOnFileIOMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid =
        env->GetMethodID(jclazz, "shouldBeNotifiedOnFileIO", "()Z");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onErrorRecoveryBeginProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnErrorRecoveryBeginProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onErrorRecoveryBeginProxy",
                                            "(BLorg/rocksdb/Status;)Z");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractEventListener#onErrorRecoveryCompleted
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID
   */
  static jmethodID getOnErrorRecoveryCompletedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID mid = env->GetMethodID(jclazz, "onErrorRecoveryCompleted",
                                            "(Lorg/rocksdb/Status;)V");
    assert(mid != nullptr);
    return mid;
  }
};

}  // namespace ROCKSDB_NAMESPACE
