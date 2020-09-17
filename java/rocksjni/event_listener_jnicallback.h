//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::EventListener.

#ifndef JAVA_ROCKSJNI_EVENT_LISTENER_JNICALLBACK_H_
#define JAVA_ROCKSJNI_EVENT_LISTENER_JNICALLBACK_H_

#include <jni.h>
#include <memory>
#include <set>

#include "rocksdb/listener.h"
#include "rocksjni/jnicallback.h"

namespace rocksdb {

enum EnabledEventCallback {
  ON_FLUSH_COMPLETED = 0x0,
  ON_FLUSH_BEGIN = 0x1,
  ON_TABLE_FILE_DELETED = 0x2,
  ON_COMPACTION_BEGIN = 0x3,
  ON_COMPACTION_COMPLETED = 0x4,
  ON_TABLE_FILE_CREATED = 0x5,
  ON_TABLE_FILE_CREATION_STARTED = 0x6,
  ON_MEMTABLE_SEALED = 0x7,
  ON_COLUMN_FAMILY_HANDLE_DELETION_STARTED = 0x8,
  ON_EXTERNAL_FILE_INGESTED = 0x9,
  ON_BACKGROUND_ERROR = 0xA,
  ON_STALL_CONDITIONS_CHANGED = 0xB,
  ON_FILE_READ_FINISH = 0xC,
  ON_FILE_WRITE_FINISH = 0xD,
  SHOULD_BE_NOTIFIED_ON_FILE_IO = 0xE,
  ON_ERROR_RECOVERY_BEGIN = 0xF,
  ON_ERROR_RECOVERY_COMPLETED = 0x10,

  NUM_ENABLED_EVENT_CALLBACK = 0x11,
};

class EventListenerJniCallback : public JniCallback, public EventListener {
 public:
  EventListenerJniCallback(JNIEnv* env, jobject jevent_listener,
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
  virtual bool ShouldBeNotifiedOnFileIO();
  virtual void OnErrorRecoveryBegin(BackgroundErrorReason reason,
      Status bg_error, bool* auto_recovery);
  virtual void OnErrorRecoveryCompleted(Status old_bg_error);

 private:
  void InitCallbackMethodId(jmethodID& mid, EnabledEventCallback eec,
      JNIEnv *env,
      jmethodID (*get_id)(JNIEnv *env));
  template <class T> jobject SetupCallbackInvocation(JNIEnv*& env, jboolean& attached_thread,
      const jmethodID& mid, const T& cpp_obj,
      jobject (*convert)(JNIEnv *env, const T* cpp_obj));
  void CleanEnv(JNIEnv *env,
      jboolean attached_thread, std::initializer_list<jobject*> refs);

  const std::set<EnabledEventCallback> m_enabled_event_callbacks;
  jmethodID m_on_flush_completed_proxy_mid;
  jmethodID m_on_flush_begin_proxy_mid;
  jmethodID m_on_table_file_deleted_mid;
  jmethodID m_on_compaction_begin_proxy_mid;
  jmethodID m_on_compaction_completed_proxy_mid;
};

}  //namespace rocksdb

#endif  // JAVA_ROCKSJNI_EVENT_LISTENER_JNICALLBACK_H_