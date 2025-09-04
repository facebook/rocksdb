//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::EventListener.

#include "rocksjni/event_listener_jnicallback.h"

#include "rocksjni/portal.h"

namespace ROCKSDB_NAMESPACE {
/**
 * Create new instance of event listener.
 * @param env JNIEnv
 * @param jevent_listener Instance of AbstractEventListener
 * @param enabled_event_callbacks set of subscribed events.
 * @throw JniException when JNI initialization calls fail.
 */
EventListenerJniCallback::EventListenerJniCallback(
    JNIEnv* env, jobject jevent_listener,
    const std::set<EnabledEventCallback>& enabled_event_callbacks)
    : JniCallback(env, jevent_listener),
      m_enabled_event_callbacks(enabled_event_callbacks) {
  InitCallbackMethodId(
      m_on_flush_completed_proxy_mid, EnabledEventCallback::ON_FLUSH_COMPLETED,
      env, AbstractEventListenerJni::getOnFlushCompletedProxyMethodId);

  InitCallbackMethodId(m_on_flush_begin_proxy_mid,
                       EnabledEventCallback::ON_FLUSH_BEGIN, env,
                       AbstractEventListenerJni::getOnFlushBeginProxyMethodId);

  InitCallbackMethodId(m_on_table_file_deleted_mid,
                       EnabledEventCallback::ON_TABLE_FILE_DELETED, env,
                       AbstractEventListenerJni::getOnTableFileDeletedMethodId);

  InitCallbackMethodId(
      m_on_compaction_begin_proxy_mid,
      EnabledEventCallback::ON_COMPACTION_BEGIN, env,
      AbstractEventListenerJni::getOnCompactionBeginProxyMethodId);

  InitCallbackMethodId(
      m_on_compaction_completed_proxy_mid,
      EnabledEventCallback::ON_COMPACTION_COMPLETED, env,
      AbstractEventListenerJni::getOnCompactionCompletedProxyMethodId);

  InitCallbackMethodId(m_on_table_file_created_mid,
                       EnabledEventCallback::ON_TABLE_FILE_CREATED, env,
                       AbstractEventListenerJni::getOnTableFileCreatedMethodId);

  InitCallbackMethodId(
      m_on_table_file_creation_started_mid,
      EnabledEventCallback::ON_TABLE_FILE_CREATION_STARTED, env,
      AbstractEventListenerJni::getOnTableFileCreationStartedMethodId);

  InitCallbackMethodId(m_on_mem_table_sealed_mid,
                       EnabledEventCallback::ON_MEMTABLE_SEALED, env,
                       AbstractEventListenerJni::getOnMemTableSealedMethodId);

  InitCallbackMethodId(
      m_on_column_family_handle_deletion_started_mid,
      EnabledEventCallback::ON_COLUMN_FAMILY_HANDLE_DELETION_STARTED, env,
      AbstractEventListenerJni::getOnColumnFamilyHandleDeletionStartedMethodId);

  InitCallbackMethodId(
      m_on_external_file_ingested_proxy_mid,
      EnabledEventCallback::ON_EXTERNAL_FILE_INGESTED, env,
      AbstractEventListenerJni::getOnExternalFileIngestedProxyMethodId);

  InitCallbackMethodId(
      m_on_background_error_proxy_mid,
      EnabledEventCallback::ON_BACKGROUND_ERROR, env,
      AbstractEventListenerJni::getOnBackgroundErrorProxyMethodId);

  InitCallbackMethodId(
      m_on_stall_conditions_changed_mid,
      EnabledEventCallback::ON_STALL_CONDITIONS_CHANGED, env,
      AbstractEventListenerJni::getOnStallConditionsChangedMethodId);

  InitCallbackMethodId(m_on_file_read_finish_mid,
                       EnabledEventCallback::ON_FILE_READ_FINISH, env,
                       AbstractEventListenerJni::getOnFileReadFinishMethodId);

  InitCallbackMethodId(m_on_file_write_finish_mid,
                       EnabledEventCallback::ON_FILE_WRITE_FINISH, env,
                       AbstractEventListenerJni::getOnFileWriteFinishMethodId);

  InitCallbackMethodId(m_on_file_flush_finish_mid,
                       EnabledEventCallback::ON_FILE_FLUSH_FINISH, env,
                       AbstractEventListenerJni::getOnFileFlushFinishMethodId);

  InitCallbackMethodId(m_on_file_sync_finish_mid,
                       EnabledEventCallback::ON_FILE_SYNC_FINISH, env,
                       AbstractEventListenerJni::getOnFileSyncFinishMethodId);

  InitCallbackMethodId(
      m_on_file_range_sync_finish_mid,
      EnabledEventCallback::ON_FILE_RANGE_SYNC_FINISH, env,
      AbstractEventListenerJni::getOnFileRangeSyncFinishMethodId);

  InitCallbackMethodId(
      m_on_file_truncate_finish_mid,
      EnabledEventCallback::ON_FILE_TRUNCATE_FINISH, env,
      AbstractEventListenerJni::getOnFileTruncateFinishMethodId);

  InitCallbackMethodId(m_on_file_close_finish_mid,
                       EnabledEventCallback::ON_FILE_CLOSE_FINISH, env,
                       AbstractEventListenerJni::getOnFileCloseFinishMethodId);

  InitCallbackMethodId(
      m_should_be_notified_on_file_io,
      EnabledEventCallback::SHOULD_BE_NOTIFIED_ON_FILE_IO, env,
      AbstractEventListenerJni::getShouldBeNotifiedOnFileIOMethodId);

  InitCallbackMethodId(
      m_on_error_recovery_begin_proxy_mid,
      EnabledEventCallback::ON_ERROR_RECOVERY_BEGIN, env,
      AbstractEventListenerJni::getOnErrorRecoveryBeginProxyMethodId);

  InitCallbackMethodId(
      m_on_error_recovery_completed_mid,
      EnabledEventCallback::ON_ERROR_RECOVERY_COMPLETED, env,
      AbstractEventListenerJni::getOnErrorRecoveryCompletedMethodId);

  flushJobInfoJniConverter = std::make_unique<rocksdb::FlushJobInfoJni>(env);
  compactionJobInfoJniConverter = std::make_unique<CompactionJobInfoJni>(env);
  tableFileCreationInfoJniConverter =
      std::make_unique<TableFileCreationInfoJni>(env);
  tableFileCreationBriefInfoJniConterter =
      std::make_unique<TableFileCreationBriefInfoJni>(env);
  memTableInfoJniConverter = std::make_unique<MemTableInfoJni>(env);
  columnFamilyHandleJniConverter = std::make_unique<ColumnFamilyHandleJni>(env);
  externalFileIngestionInfoJniConverter =
      std::make_unique<ExternalFileIngestionInfoJni>(env);
  writeStallInfoJniConverter = std::make_unique<WriteStallInfoJni>(env);
  fileOperationInfoJniConverter = std::make_unique<FileOperationInfoJni>(env);
  tableFileDeletionInfoJniConverter =
      std::make_unique<TableFileDeletionInfoJni>(env);

  statusJClazz = StatusJni::getJClass(env);
  ROCKSDB_NAMESPACE::JniException::checkAndThrowException(env);

  statusJClazz = static_cast<jclass>(env->NewGlobalRef(statusJClazz));
  ROCKSDB_NAMESPACE::JniException::ThrowIfTrue(
      env, "Unable to obtain global reference for Status class",
      statusJClazz == nullptr);

  statusCtor = StatusJni::getConstructorMethodId(env, statusJClazz);
  ROCKSDB_NAMESPACE::JniException::checkAndThrowException(env);
}

EventListenerJniCallback::~EventListenerJniCallback() {
  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);
  env->DeleteGlobalRef(statusJClazz);

  releaseJniEnv(attached_thread);
}

void EventListenerJniCallback::OnFlushCompleted(
    DB* db, const FlushJobInfo& flush_job_info) {
  if (m_on_flush_completed_proxy_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jflush_job_info =
      flushJobInfoJniConverter->fromCppFlushJobInfo(env, &flush_job_info);

  if (jflush_job_info != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, m_on_flush_completed_proxy_mid,
                        reinterpret_cast<jlong>(db), jflush_job_info);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jflush_job_info});
}

void EventListenerJniCallback::OnFlushBegin(
    DB* db, const FlushJobInfo& flush_job_info) {
  if (m_on_flush_begin_proxy_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jflush_job_info =
      flushJobInfoJniConverter->fromCppFlushJobInfo(env, &flush_job_info);

  if (jflush_job_info != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, m_on_flush_begin_proxy_mid,
                        reinterpret_cast<jlong>(db), jflush_job_info);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jflush_job_info});
}

void EventListenerJniCallback::OnTableFileDeleted(
    const TableFileDeletionInfo& info) {
  if (m_on_table_file_deleted_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jdeletion_info =
      tableFileDeletionInfoJniConverter->fromCppTableFileDeletionInfo(env,
                                                                      &info);

  if (jdeletion_info != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, m_on_table_file_deleted_mid,
                        jdeletion_info);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jdeletion_info});
}

void EventListenerJniCallback::OnCompactionBegin(DB* db,
                                                 const CompactionJobInfo& ci) {
  if (m_on_compaction_begin_proxy_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jcompaction_job_info =
      compactionJobInfoJniConverter->fromCppCompactionJobInfo(env, &ci);

  if (jcompaction_job_info != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, m_on_compaction_begin_proxy_mid,
                        reinterpret_cast<jlong>(db), jcompaction_job_info);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jcompaction_job_info});
}

void EventListenerJniCallback::OnCompactionCompleted(
    DB* db, const CompactionJobInfo& ci) {
  if (m_on_compaction_completed_proxy_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jcompaction_job_info =
      compactionJobInfoJniConverter->fromCppCompactionJobInfo(env, &ci);

  if (jcompaction_job_info != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, m_on_compaction_completed_proxy_mid,
                        reinterpret_cast<jlong>(db), jcompaction_job_info);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jcompaction_job_info});
}

void EventListenerJniCallback::OnTableFileCreated(
    const TableFileCreationInfo& info) {
  if (m_on_table_file_created_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jfile_creation_info =
      tableFileCreationInfoJniConverter->fromCppTableFileCreationInfo(env,
                                                                      &info);

  if (jfile_creation_info != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, m_on_table_file_created_mid,
                        jfile_creation_info);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jfile_creation_info});
}

void EventListenerJniCallback::OnTableFileCreationStarted(
    const TableFileCreationBriefInfo& info) {
  if (m_on_table_file_creation_started_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jcreation_brief_info =
      tableFileCreationBriefInfoJniConterter->fromCppTableFileCreationBriefInfo(
          env, &info);

  if (jcreation_brief_info != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, m_on_table_file_creation_started_mid,
                        jcreation_brief_info);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jcreation_brief_info});
}

void EventListenerJniCallback::OnMemTableSealed(const MemTableInfo& info) {
  if (m_on_mem_table_sealed_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jmem_table_info =
      memTableInfoJniConverter->fromCppMemTableInfo(env, &info);

  if (jmem_table_info != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, m_on_mem_table_sealed_mid,
                        jmem_table_info);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jmem_table_info});
}

void EventListenerJniCallback::OnColumnFamilyHandleDeletionStarted(
    ColumnFamilyHandle* handle) {
  if (m_on_column_family_handle_deletion_started_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jcf_handle =
      columnFamilyHandleJniConverter->fromCppColumnFamilyHandle(env, handle);

  if (jcf_handle != nullptr) {
    env->CallVoidMethod(m_jcallback_obj,
                        m_on_column_family_handle_deletion_started_mid,
                        jcf_handle);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jcf_handle});
}

void EventListenerJniCallback::OnExternalFileIngested(
    DB* db, const ExternalFileIngestionInfo& info) {
  if (m_on_external_file_ingested_proxy_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jingestion_info =
      externalFileIngestionInfoJniConverter->fromCppExternalFileIngestionInfo(
          env, &info);

  if (jingestion_info != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, m_on_external_file_ingested_proxy_mid,
                        reinterpret_cast<jlong>(db), jingestion_info);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jingestion_info});
}

void EventListenerJniCallback::OnBackgroundError(BackgroundErrorReason reason,
                                                 Status* bg_error) {
  if (m_on_background_error_proxy_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jstatus =
      StatusJni::construct(env, *bg_error, statusJClazz, statusCtor);

  if (jstatus != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, m_on_background_error_proxy_mid,
                        static_cast<jbyte>(reason), jstatus);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jstatus});
}

void EventListenerJniCallback::OnStallConditionsChanged(
    const WriteStallInfo& info) {
  if (m_on_stall_conditions_changed_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jwrite_stall_info =
      writeStallInfoJniConverter->fromCppWriteStallInfo(env, &info);

  if (jwrite_stall_info != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, m_on_stall_conditions_changed_mid,
                        jwrite_stall_info);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jwrite_stall_info});
}

void EventListenerJniCallback::OnFileReadFinish(const FileOperationInfo& info) {
  OnFileOperation(m_on_file_read_finish_mid, info);
}

void EventListenerJniCallback::OnFileWriteFinish(
    const FileOperationInfo& info) {
  OnFileOperation(m_on_file_write_finish_mid, info);
}

void EventListenerJniCallback::OnFileFlushFinish(
    const FileOperationInfo& info) {
  OnFileOperation(m_on_file_flush_finish_mid, info);
}

void EventListenerJniCallback::OnFileSyncFinish(const FileOperationInfo& info) {
  OnFileOperation(m_on_file_sync_finish_mid, info);
}

void EventListenerJniCallback::OnFileRangeSyncFinish(
    const FileOperationInfo& info) {
  OnFileOperation(m_on_file_range_sync_finish_mid, info);
}

void EventListenerJniCallback::OnFileTruncateFinish(
    const FileOperationInfo& info) {
  OnFileOperation(m_on_file_truncate_finish_mid, info);
}

void EventListenerJniCallback::OnFileCloseFinish(
    const FileOperationInfo& info) {
  OnFileOperation(m_on_file_close_finish_mid, info);
}

bool EventListenerJniCallback::ShouldBeNotifiedOnFileIO() {
  if (m_should_be_notified_on_file_io == nullptr) {
    return false;
  }

  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jboolean jshould_be_notified =
      env->CallBooleanMethod(m_jcallback_obj, m_should_be_notified_on_file_io);

  CleanupCallbackInvocation(env, attached_thread, {});

  return static_cast<bool>(jshould_be_notified);
}

void EventListenerJniCallback::OnErrorRecoveryBegin(
    BackgroundErrorReason reason, Status bg_error, bool* auto_recovery) {
  if (m_on_error_recovery_begin_proxy_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jbg_error =
      StatusJni::construct(env, bg_error, statusJClazz, statusCtor);

  if (jbg_error != nullptr) {
    jboolean jauto_recovery = env->CallBooleanMethod(
        m_jcallback_obj, m_on_error_recovery_begin_proxy_mid,
        static_cast<jbyte>(reason), jbg_error);
    *auto_recovery = jauto_recovery == JNI_TRUE;
  }

  CleanupCallbackInvocation(env, attached_thread, {&jbg_error});
}

void EventListenerJniCallback::OnErrorRecoveryCompleted(Status old_bg_error) {
  if (m_on_error_recovery_completed_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jold_bg_error =
      StatusJni::construct(env, old_bg_error, statusJClazz, statusCtor);

  if (jold_bg_error != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, m_on_error_recovery_completed_mid,
                        jold_bg_error);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jold_bg_error});
}

void EventListenerJniCallback::InitCallbackMethodId(
    jmethodID& mid, EnabledEventCallback eec, JNIEnv* env,
    jmethodID (*get_id)(JNIEnv* env)) {
  if (m_enabled_event_callbacks.count(eec) == 1) {
    mid = get_id(env);
  } else {
    mid = nullptr;
  }
}

template <class T>
jobject EventListenerJniCallback::SetupCallbackInvocation(
    JNIEnv*& env, jboolean& attached_thread, const T& cpp_obj,
    jobject (*convert)(JNIEnv* env, const T* cpp_obj)) {
  attached_thread = JNI_FALSE;
  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  return convert(env, &cpp_obj);
}

void EventListenerJniCallback::CleanupCallbackInvocation(
    JNIEnv* env, jboolean attached_thread,
    std::initializer_list<jobject*> refs) {
  for (auto* ref : refs) {
    if (*ref == nullptr) continue;
    env->DeleteLocalRef(*ref);
  }

  if (env->ExceptionCheck()) {
    // exception thrown from CallVoidMethod
    env->ExceptionDescribe();  // print out exception to stderr
  }

  releaseJniEnv(attached_thread);
}

void EventListenerJniCallback::OnFileOperation(const jmethodID& mid,
                                               const FileOperationInfo& info) {
  if (mid == nullptr) {
    return;
  }
  JNIEnv* env;
  jboolean attached_thread = JNI_FALSE;

  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jop_info =
      fileOperationInfoJniConverter->fromCppFileOperationInfo(env, &info);

  if (jop_info != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, mid, jop_info);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jop_info});
}
}  // namespace ROCKSDB_NAMESPACE
