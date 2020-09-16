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

  if (enabled_event_callbacks.count(
      EnabledEventCallback::ON_FLUSH_COMPLETED) == 1) {
    m_on_flush_completed_proxy_mid =
        AbstractEventListenerJni::getOnFlushCompletedProxyMethodId(env);
    if(m_on_flush_completed_proxy_mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return;
    }
  } else {
    m_on_flush_completed_proxy_mid = nullptr;
  }

  if (enabled_event_callbacks.count(
      EnabledEventCallback::ON_TABLE_FILE_DELETED) == 1) {
    m_on_table_file_deleted_mid =
        AbstractEventListenerJni::getOnTableFileDeletedMethodId(env);
    if(m_on_table_file_deleted_mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return;
    }
  } else {
    m_on_table_file_deleted_mid = nullptr;
  }
}

EventListenerJniCallback::~EventListenerJniCallback() {

}

void EventListenerJniCallback::OnFlushCompleted(DB* db,
    const FlushJobInfo& flush_job_info) {
  if (m_on_flush_completed_proxy_mid == nullptr) {
    return;
  }

  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jobject jflush_job_info =
      FlushJobInfoJni::fromCppFlushJobInfo(env, &flush_job_info);
  if (jflush_job_info == nullptr) {
    // exception thrown from fromCppFlushJobInfo
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return;
  }

  env->CallVoidMethod(m_jcallback_obj,
      m_on_flush_completed_proxy_mid,
      reinterpret_cast<jlong>(db),
      jflush_job_info);

  env->DeleteLocalRef(jflush_job_info);

  if(env->ExceptionCheck()) {
    // exception thrown from CallVoidMethod
    env->ExceptionDescribe();  // print out exception to stderr
  }

  releaseJniEnv(attached_thread);
}

void EventListenerJniCallback::OnFlushBegin(DB* /*db*/, const FlushJobInfo& /*flush_job_info*/) {}

void EventListenerJniCallback::OnTableFileDeleted(const TableFileDeletionInfo& info) {
  if (m_on_table_file_deleted_mid == nullptr) {
    return;
  }

  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  if (env == nullptr) {
    return;
  }

  jobject jdeletion_info =
      TableFileDeletionInfoJni::fromCppTableFileDeletionInfo(env, &info);
  if (jdeletion_info == nullptr) {
    // exception thrown from fromCppFlushJobInfo
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return;
  }

  env->CallVoidMethod(m_jcallback_obj,
      m_on_table_file_deleted_mid,
      jdeletion_info);

  env->DeleteLocalRef(jdeletion_info);

  if(env->ExceptionCheck()) {
    // exception thrown from CallVoidMethod
    env->ExceptionDescribe();  // print out exception to stderr
  }

  releaseJniEnv(attached_thread);
}

void EventListenerJniCallback::OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& /*ci*/) {}
void EventListenerJniCallback::OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& /*ci*/) {}
void EventListenerJniCallback::OnTableFileCreated(const TableFileCreationInfo& /*info*/) {}
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
}  // namespace rocksdb