//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::EventListener.

#ifndef JAVA_ROCKSJNI_EVENT_LISTENER_JNICALLBACK_H_
#define JAVA_ROCKSJNI_EVENT_LISTENER_JNICALLBACK_H_

#include <jni.h>

#include <memory>
#include <set>

#include "rocksjni/portal.h"
#include "rocksdb/listener.h"
#include "rocksjni/jnicallback.h"

namespace ROCKSDB_NAMESPACE {


class EventListenerJniCallback : public JniCallback, public EventListener {
 public:
  EventListenerJniCallback(
      JNIEnv* env, jobject jevent_listener,
      const std::set<EnabledEventCallback>& enabled_event_callbacks);
  virtual ~EventListenerJniCallback();
  virtual void OnFlushCompleted(DB* db, const FlushJobInfo& flush_job_info);
  virtual void OnFlushBegin(DB* db, const FlushJobInfo& flush_job_info);
  virtual void OnTableFileDeleted(const TableFileDeletionInfo& info);
  virtual void OnCompactionBegin(DB* db, const CompactionJobInfo& ci);
  virtual void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci);
  virtual void OnTableFileCreated(const TableFileCreationInfo& info);
  virtual void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& info);
  virtual void OnMemTableSealed(const MemTableInfo& info);
  virtual void OnColumnFamilyHandleDeletionStarted(ColumnFamilyHandle* handle);
  virtual void OnExternalFileIngested(DB* db,
                                      const ExternalFileIngestionInfo& info);
  virtual void OnBackgroundError(BackgroundErrorReason reason,
                                 Status* bg_error);
  virtual void OnStallConditionsChanged(const WriteStallInfo& info);
  virtual void OnFileReadFinish(const FileOperationInfo& info);
  virtual void OnFileWriteFinish(const FileOperationInfo& info);
  virtual void OnFileFlushFinish(const FileOperationInfo& info);
  virtual void OnFileSyncFinish(const FileOperationInfo& info);
  virtual void OnFileRangeSyncFinish(const FileOperationInfo& info);
  virtual void OnFileTruncateFinish(const FileOperationInfo& info);
  virtual void OnFileCloseFinish(const FileOperationInfo& info);
  virtual bool ShouldBeNotifiedOnFileIO();
  virtual void OnErrorRecoveryBegin(BackgroundErrorReason reason,
                                    Status bg_error, bool* auto_recovery);
  virtual void OnErrorRecoveryCompleted(Status old_bg_error);

 private:
  inline void InitCallbackMethodId(jmethodID& mid, EnabledEventCallback eec,
                                   JNIEnv* env,
                                   jmethodID (*get_id)(JNIEnv* env));
  template <class T>
  inline jobject SetupCallbackInvocation(
      JNIEnv*& env, jboolean& attached_thread, const T& cpp_obj,
      jobject (*convert)(JNIEnv* env, const T* cpp_obj));
  inline void CleanupCallbackInvocation(JNIEnv* env, jboolean attached_thread,
                                        std::initializer_list<jobject*> refs);
  inline void OnFileOperation(const jmethodID& mid,
                              const FileOperationInfo& info);

  const std::set<EnabledEventCallback> m_enabled_event_callbacks;
  jmethodID m_on_flush_completed_proxy_mid;
  jmethodID m_on_flush_begin_proxy_mid;
  jmethodID m_on_table_file_deleted_mid;
  jmethodID m_on_compaction_begin_proxy_mid;
  jmethodID m_on_compaction_completed_proxy_mid;
  jmethodID m_on_table_file_created_mid;
  jmethodID m_on_table_file_creation_started_mid;
  jmethodID m_on_mem_table_sealed_mid;
  jmethodID m_on_column_family_handle_deletion_started_mid;
  jmethodID m_on_external_file_ingested_proxy_mid;
  jmethodID m_on_background_error_proxy_mid;
  jmethodID m_on_stall_conditions_changed_mid;
  jmethodID m_on_file_read_finish_mid;
  jmethodID m_on_file_write_finish_mid;
  jmethodID m_on_file_flush_finish_mid;
  jmethodID m_on_file_sync_finish_mid;
  jmethodID m_on_file_range_sync_finish_mid;
  jmethodID m_on_file_truncate_finish_mid;
  jmethodID m_on_file_close_finish_mid;
  jmethodID m_should_be_notified_on_file_io;
  jmethodID m_on_error_recovery_begin_proxy_mid;
  jmethodID m_on_error_recovery_completed_mid;

  std::unique_ptr<ROCKSDB_NAMESPACE::FlushJobInfoJni> flushJobInfoJniConverter = nullptr;
  std::unique_ptr<ROCKSDB_NAMESPACE::CompactionJobInfoJni> compactionJobInfoJniConverter = nullptr;

};

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



#endif  // JAVA_ROCKSJNI_EVENT_LISTENER_JNICALLBACK_H_
