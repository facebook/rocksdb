//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::EventListener.

#include "rocksjni/event_listener_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
EventListenerJniCallback::EventListenerJniCallback(JNIEnv* env,
    jobject jevent_listener,
    const std::set<EnabledEventCallback>& enabled_event_callbacks)
        : JniCallback(env, jevent_listener),
        m_enabled_event_callbacks(enabled_event_callbacks) {

  InitCallbackMethodId(m_on_flush_completed_proxy_mid,
      EnabledEventCallback::ON_FLUSH_COMPLETED,
      env,
      AbstractEventListenerJni::getOnFlushCompletedProxyMethodId);

  InitCallbackMethodId(m_on_flush_begin_proxy_mid,
      EnabledEventCallback::ON_FLUSH_BEGIN,
      env,
      AbstractEventListenerJni::getOnFlushBeginProxyMethodId);

  InitCallbackMethodId(m_on_table_file_deleted_mid,
      EnabledEventCallback::ON_TABLE_FILE_DELETED,
      env,
      AbstractEventListenerJni::getOnTableFileDeletedMethodId);

  InitCallbackMethodId(m_on_compaction_begin_proxy_mid,
      EnabledEventCallback::ON_COMPACTION_BEGIN,
      env,
      AbstractEventListenerJni::getOnCompactionBeginProxyMethodId);

  InitCallbackMethodId(m_on_compaction_completed_proxy_mid,
      EnabledEventCallback::ON_COMPACTION_COMPLETED,
      env,
      AbstractEventListenerJni::getOnCompactionCompletedProxyMethodId);

  InitCallbackMethodId(m_on_table_file_created_mid,
      EnabledEventCallback::ON_TABLE_FILE_CREATED,
      env,
      AbstractEventListenerJni::getOnTableFileCreatedMethodId);

  InitCallbackMethodId(m_on_table_file_creation_started_mid,
      EnabledEventCallback::ON_TABLE_FILE_CREATION_STARTED,
      env,
      AbstractEventListenerJni::getOnTableFileCreationStartedMethodId);

  InitCallbackMethodId(m_on_mem_table_sealed_mid,
      EnabledEventCallback::ON_MEMTABLE_SEALED,
      env,
      AbstractEventListenerJni::getOnMemTableSealedMethodId);

  InitCallbackMethodId(m_on_column_family_handle_deletion_started_mid,
      EnabledEventCallback::ON_COLUMN_FAMILY_HANDLE_DELETION_STARTED,
      env,
      AbstractEventListenerJni::getOnColumnFamilyHandleDeletionStartedMethodId);

  InitCallbackMethodId(m_on_external_file_ingested_proxy_mid,
      EnabledEventCallback::ON_EXTERNAL_FILE_INGESTED,
      env,
      AbstractEventListenerJni::getOnExternalFileIngestedProxyMethodId);

  InitCallbackMethodId(m_on_background_error_proxy_mid,
      EnabledEventCallback::ON_BACKGROUND_ERROR,
      env,
      AbstractEventListenerJni::getOnBackgroundErrorProxyMethodId);

  InitCallbackMethodId(m_on_stall_conditions_changed_mid,
      EnabledEventCallback::ON_STALL_CONDITIONS_CHANGED,
      env,
      AbstractEventListenerJni::getOnStallConditionsChangedMethodId);

  InitCallbackMethodId(m_on_file_read_finish_mid,
      EnabledEventCallback::ON_FILE_READ_FINISH,
      env,
      AbstractEventListenerJni::getOnFileReadFinishMethodId);

  InitCallbackMethodId(m_on_file_write_finish_mid,
      EnabledEventCallback::ON_FILE_WRITE_FINISH,
      env,
      AbstractEventListenerJni::getOnFileWriteFinishMethodId);

  InitCallbackMethodId(m_should_be_notified_on_file_io,
      EnabledEventCallback::SHOULD_BE_NOTIFIED_ON_FILE_IO,
      env,
      AbstractEventListenerJni::getShouldBeNotifiedOnFileIOMethodId);

  InitCallbackMethodId(m_on_error_recovery_begin_proxy_mid,
      EnabledEventCallback::ON_ERROR_RECOVERY_BEGIN,
      env,
      AbstractEventListenerJni::getOnErrorRecoveryBeginProxyMethodId);

  InitCallbackMethodId(m_on_error_recovery_completed_mid,
      EnabledEventCallback::ON_ERROR_RECOVERY_COMPLETED,
      env,
      AbstractEventListenerJni::getOnErrorRecoveryCompletedMethodId);
}

EventListenerJniCallback::~EventListenerJniCallback() {

}

void EventListenerJniCallback::OnFlushCompleted(DB* db,
    const FlushJobInfo& flush_job_info) {

  JNIEnv *env;
  jboolean attached_thread;
  jobject jflush_job_info = SetupCallbackInvocation<FlushJobInfo>(env, attached_thread,
      m_on_flush_completed_proxy_mid, flush_job_info, FlushJobInfoJni::fromCppFlushJobInfo);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_flush_completed_proxy_mid,
      reinterpret_cast<jlong>(db),
      jflush_job_info);

  CleanEnv(env, attached_thread, { &jflush_job_info });
}

void EventListenerJniCallback::OnFlushBegin(DB* db,
    const FlushJobInfo& flush_job_info) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jflush_job_info = SetupCallbackInvocation<FlushJobInfo>(env, attached_thread,
      m_on_flush_begin_proxy_mid, flush_job_info, FlushJobInfoJni::fromCppFlushJobInfo);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_flush_begin_proxy_mid,
      reinterpret_cast<jlong>(db),
      jflush_job_info);

  CleanEnv(env, attached_thread, { &jflush_job_info });
}

void EventListenerJniCallback::OnTableFileDeleted(const TableFileDeletionInfo& info) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jdeletion_info = SetupCallbackInvocation<TableFileDeletionInfo>(env, attached_thread,
      m_on_table_file_deleted_mid, info, TableFileDeletionInfoJni::fromCppTableFileDeletionInfo);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_table_file_deleted_mid,
      jdeletion_info);

  CleanEnv(env, attached_thread, { &jdeletion_info });
}

void EventListenerJniCallback::OnCompactionBegin(DB* db, const CompactionJobInfo& ci) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jcompaction_job_info = SetupCallbackInvocation<CompactionJobInfo>(env, attached_thread,
      m_on_compaction_begin_proxy_mid, ci, CompactionJobInfoJni::fromCppCompactionJobInfo);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_compaction_begin_proxy_mid,
      reinterpret_cast<jlong>(db),
      jcompaction_job_info);

  CleanEnv(env, attached_thread, { &jcompaction_job_info });
}

void EventListenerJniCallback::OnCompactionCompleted(DB* db, const CompactionJobInfo& ci) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jcompaction_job_info = SetupCallbackInvocation<CompactionJobInfo>(env, attached_thread,
      m_on_compaction_completed_proxy_mid, ci, CompactionJobInfoJni::fromCppCompactionJobInfo);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_compaction_completed_proxy_mid,
      reinterpret_cast<jlong>(db),
      jcompaction_job_info);

  CleanEnv(env, attached_thread, { &jcompaction_job_info });
}

void EventListenerJniCallback::OnTableFileCreated(const TableFileCreationInfo& info) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jfile_creation_info = SetupCallbackInvocation<TableFileCreationInfo>(env, attached_thread,
      m_on_table_file_created_mid, info, TableFileCreationInfoJni::fromCppTableFileCreationInfo);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_table_file_created_mid,
      jfile_creation_info);

  CleanEnv(env, attached_thread, { &jfile_creation_info });
}

void EventListenerJniCallback::OnTableFileCreationStarted(const TableFileCreationBriefInfo& info) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jcreation_brief_info = SetupCallbackInvocation<TableFileCreationBriefInfo>(env, attached_thread,
      m_on_table_file_creation_started_mid, info, TableFileCreationBriefInfoJni::fromCppTableFileCreationBriefInfo);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_table_file_creation_started_mid,
      jcreation_brief_info);

  CleanEnv(env, attached_thread, { &jcreation_brief_info });
}

void EventListenerJniCallback::OnMemTableSealed(const MemTableInfo& info) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jmem_table_info = SetupCallbackInvocation<MemTableInfo>(env, attached_thread,
      m_on_mem_table_sealed_mid, info, MemTableInfoJni::fromCppMemTableInfo);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_mem_table_sealed_mid,
      jmem_table_info);

  CleanEnv(env, attached_thread, { &jmem_table_info });
}

void EventListenerJniCallback::OnColumnFamilyHandleDeletionStarted(ColumnFamilyHandle* handle) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jcf_handle = SetupCallbackInvocation<ColumnFamilyHandle>(env, attached_thread,
      m_on_column_family_handle_deletion_started_mid, *handle, ColumnFamilyHandleJni::fromCppColumnFamilyHandle);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_column_family_handle_deletion_started_mid,
      jcf_handle);

  CleanEnv(env, attached_thread, { &jcf_handle });
}

void EventListenerJniCallback::OnExternalFileIngested(DB* db, const ExternalFileIngestionInfo& info) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jingestion_info = SetupCallbackInvocation<ExternalFileIngestionInfo>(env, attached_thread,
      m_on_external_file_ingested_proxy_mid, info, ExternalFileIngestionInfoJni::fromCppExternalFileIngestionInfo);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_external_file_ingested_proxy_mid,
      reinterpret_cast<jlong>(db),
      jingestion_info);

  CleanEnv(env, attached_thread, { &jingestion_info });
}

void EventListenerJniCallback::OnBackgroundError(BackgroundErrorReason reason, Status* bg_error) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jstatus = SetupCallbackInvocation<Status>(env, attached_thread,
      m_on_background_error_proxy_mid, *bg_error, StatusJni::construct);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_background_error_proxy_mid,
      static_cast<jbyte>(reason),
      jstatus);

  CleanEnv(env, attached_thread, { &jstatus });
}

void EventListenerJniCallback::OnStallConditionsChanged(const WriteStallInfo& info) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jwrite_stall_info = SetupCallbackInvocation<WriteStallInfo>(env, attached_thread,
      m_on_stall_conditions_changed_mid, info, WriteStallInfoJni::fromCppWriteStallInfo);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_stall_conditions_changed_mid,
      jwrite_stall_info);

  CleanEnv(env, attached_thread, { &jwrite_stall_info });
}

void EventListenerJniCallback::OnFileReadFinish(const FileOperationInfo& info) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jop_info = SetupCallbackInvocation<FileOperationInfo>(env, attached_thread,
      m_on_file_read_finish_mid, info, FileOperationInfoJni::fromCppFileOperationInfo);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_file_read_finish_mid,
      jop_info);

  CleanEnv(env, attached_thread, { &jop_info });
}

void EventListenerJniCallback::OnFileWriteFinish(const FileOperationInfo& info) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jop_info = SetupCallbackInvocation<FileOperationInfo>(env, attached_thread,
      m_on_file_write_finish_mid, info, FileOperationInfoJni::fromCppFileOperationInfo);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_file_write_finish_mid,
      jop_info);

  CleanEnv(env, attached_thread, { &jop_info });
}

bool EventListenerJniCallback::ShouldBeNotifiedOnFileIO() {
  if (m_should_be_notified_on_file_io == nullptr) {
    return false;
  }

  jboolean attached_thread = JNI_FALSE;
  JNIEnv *env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jboolean jshould_be_notified = env->CallBooleanMethod(m_jcallback_obj,
      m_should_be_notified_on_file_io);

  CleanEnv(env, attached_thread, { });

  return static_cast<bool>(jshould_be_notified);
}

void EventListenerJniCallback::OnErrorRecoveryBegin(BackgroundErrorReason reason,
    Status bg_error, bool* auto_recovery) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jbg_error = SetupCallbackInvocation<Status>(env, attached_thread,
      m_on_error_recovery_begin_proxy_mid, bg_error, StatusJni::construct);

  jboolean jauto_recovery = env->CallBooleanMethod(m_jcallback_obj,
      m_on_error_recovery_begin_proxy_mid,
      static_cast<jbyte>(reason),
      jbg_error);

  CleanEnv(env, attached_thread, { &jbg_error });

  *auto_recovery = static_cast<bool>(jauto_recovery);
}

void EventListenerJniCallback::OnErrorRecoveryCompleted(Status old_bg_error) {
  JNIEnv *env;
  jboolean attached_thread;
  jobject jold_bg_error = SetupCallbackInvocation<Status>(env, attached_thread,
      m_on_error_recovery_completed_mid, old_bg_error, StatusJni::construct);

  env->CallVoidMethod(m_jcallback_obj,
      m_on_error_recovery_completed_mid,
      jold_bg_error);

  CleanEnv(env, attached_thread, { &jold_bg_error });
}

void EventListenerJniCallback::InitCallbackMethodId(jmethodID& mid, EnabledEventCallback eec,
    JNIEnv *env,
    jmethodID (*get_id)(JNIEnv *env)) {
  if (m_enabled_event_callbacks.count(eec) == 1) {
    mid = get_id(env);
  } else {
    mid = nullptr;
  }
}

template <class T>
jobject EventListenerJniCallback::SetupCallbackInvocation(JNIEnv*& env, jboolean& attached_thread,
      const jmethodID& mid,
      const T& cpp_obj,
      jobject (*convert)(JNIEnv *env, const T* cpp_obj)) {
  if (mid == nullptr) {
    return nullptr;
  }

  attached_thread = JNI_FALSE;
  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jobj = convert(env, &cpp_obj);
  if (jobj == nullptr) {
    // exception thrown from fromCppFlushJobInfo
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
  }

  return jobj;
}

void EventListenerJniCallback::CleanEnv(JNIEnv *env,
    jboolean attached_thread, std::initializer_list<jobject*> refs) {

  for (auto* ref : refs) {
    env->DeleteLocalRef(*ref);
  }

  if(env->ExceptionCheck()) {
    // exception thrown from CallVoidMethod
    env->ExceptionDescribe();  // print out exception to stderr
  }

  releaseJniEnv(attached_thread);
}
}  // namespace rocksdb