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

  InitCallbackMethodId(m_on_external_file_ingested_mid,
      EnabledEventCallback::ON_EXTERNAL_FILE_INGESTED,
      env,
      AbstractEventListenerJni::getOnExternalFileIngestedProxyMethodId);

  InitCallbackMethodId(m_on_background_error_mid,
      EnabledEventCallback::ON_BACKGROUND_ERROR,
      env,
      AbstractEventListenerJni::getOnBackgroundErrorMethodId);

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

  InitCallbackMethodId(m_on_error_recovery_begin_mid,
      EnabledEventCallback::ON_ERROR_RECOVERY_BEGIN,
      env,
      AbstractEventListenerJni::getOnErrorRecoveryBeginMethodId);

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

void EventListenerJniCallback::OnTableFileCreationStarted(const TableFileCreationBriefInfo& /*info*/) {}
void EventListenerJniCallback::OnMemTableSealed(const MemTableInfo& /*info*/) {}
void EventListenerJniCallback::OnColumnFamilyHandleDeletionStarted(ColumnFamilyHandle* /*handle*/) {}
void EventListenerJniCallback::OnExternalFileIngested(DB* /*db*/, const ExternalFileIngestionInfo& /*info*/) {}
void EventListenerJniCallback::OnBackgroundError(BackgroundErrorReason /* reason */, Status* /* bg_error */) {}
void EventListenerJniCallback::OnStallConditionsChanged(const WriteStallInfo& /*info*/) {}
void EventListenerJniCallback::OnFileReadFinish(const FileOperationInfo& /* info */) {}
void EventListenerJniCallback::OnFileWriteFinish(const FileOperationInfo& /* info */) {}
bool EventListenerJniCallback::ShouldBeNotifiedOnFileIO() { return false; }
void EventListenerJniCallback::OnErrorRecoveryBegin(BackgroundErrorReason /* reason */, Status /* bg_error */, bool* /* auto_recovery */) {}
void EventListenerJniCallback::OnErrorRecoveryCompleted(Status /* old_bg_error */) {}

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

  jobject jflush_job_info = convert(env, &cpp_obj);
  if (jflush_job_info == nullptr) {
    // exception thrown from fromCppFlushJobInfo
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
  }

  return jflush_job_info;
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