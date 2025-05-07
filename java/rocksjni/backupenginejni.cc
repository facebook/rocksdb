// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ ROCKSDB_NAMESPACE::BackupEngine methods from the Java side.

#include <jni.h>

#include <vector>

#include "include/org_rocksdb_BackupEngine.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksjni/loggerjnicallback.h"
#include "rocksjni/portal/backup_info_jni.h"
#include "rocksjni/portal/backup_info_list_jni.h"
#include "rocksjni/portal/jni_util.h"
#include "rocksjni/portal/list_jni.h"
#include "rocksjni/portal/logger_jni.h"
#include "rocksjni/portal/rocks_d_b_exception_jni.h"

/*
 * Class:     org_rocksdb_BackupEngine
 * Method:    open
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_BackupEngine_open(JNIEnv* env, jclass /*jcls*/,
                                         jlong env_handle,
                                         jlong backup_engine_options_handle) {
  auto* rocks_env = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(env_handle);
  auto* backup_engine_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngineOptions*>(
          backup_engine_options_handle);
  ROCKSDB_NAMESPACE::BackupEngine* backup_engine;
  auto status = ROCKSDB_NAMESPACE::BackupEngine::Open(
      rocks_env, *backup_engine_options, &backup_engine);

  if (status.ok()) {
    return GET_CPLUSPLUS_POINTER(backup_engine);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
    return 0;
  }
}

/*
 * Class:     org_rocksdb_BackupEngine
 * Method:    createNewBackup
 * Signature: (JJZ)V
 */
void Java_org_rocksdb_BackupEngine_createNewBackup(
    JNIEnv* env, jclass /*jbe*/, jlong jbe_handle, jlong db_handle,
    jboolean jflush_before_backup) {
  auto* db = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(db_handle);
  auto* backup_engine =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngine*>(jbe_handle);
  auto status = backup_engine->CreateNewBackup(
      db, static_cast<bool>(jflush_before_backup));

  if (status.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
}

/*
 * Class:     org_rocksdb_BackupEngine
 * Method:    createNewBackupWithMetadata
 * Signature: (JJLjava/lang/String;Z)V
 */
void Java_org_rocksdb_BackupEngine_createNewBackupWithMetadata(
    JNIEnv* env, jclass /*jbe*/, jlong jbe_handle, jlong db_handle,
    jstring japp_metadata, jboolean jflush_before_backup) {
  auto* db = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(db_handle);
  auto* backup_engine =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngine*>(jbe_handle);

  jboolean has_exception = JNI_FALSE;
  std::string app_metadata = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
      env, japp_metadata, &has_exception);
  if (has_exception == JNI_TRUE) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, "Could not copy jstring to std::string");
    return;
  }

  auto status = backup_engine->CreateNewBackupWithMetadata(
      db, app_metadata, static_cast<bool>(jflush_before_backup));

  if (status.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
}

/*
 * Class:     org_rocksdb_BackupEngine
 * Method:    getBackupInfo
 * Signature: (J)Ljava/util/List;
 */
jobject Java_org_rocksdb_BackupEngine_getBackupInfo(JNIEnv* env,
                                                    jclass /*jcls*/,
                                                    jlong jbe_handle) {
  auto* backup_engine =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngine*>(jbe_handle);
  std::vector<ROCKSDB_NAMESPACE::BackupInfo> backup_infos;
  backup_engine->GetBackupInfo(&backup_infos);
  return ROCKSDB_NAMESPACE::BackupInfoListJni::getBackupInfo(env, backup_infos);
}

/*
 * Class:     org_rocksdb_BackupEngine
 * Method:    getCorruptedBackups
 * Signature: (J)[I
 */
jintArray Java_org_rocksdb_BackupEngine_getCorruptedBackups(JNIEnv* env,
                                                            jclass /*jcls*/,
                                                            jlong jbe_handle) {
  auto* backup_engine =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngine*>(jbe_handle);
  std::vector<ROCKSDB_NAMESPACE::BackupID> backup_ids;
  backup_engine->GetCorruptedBackups(&backup_ids);
  // store backupids in int array
  std::vector<jint> int_backup_ids(backup_ids.begin(), backup_ids.end());

  // Store ints in java array
  // Its ok to loose precision here (64->32)
  jsize ret_backup_ids_size = static_cast<jsize>(backup_ids.size());
  jintArray ret_backup_ids = env->NewIntArray(ret_backup_ids_size);
  if (ret_backup_ids == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  env->SetIntArrayRegion(ret_backup_ids, 0, ret_backup_ids_size,
                         int_backup_ids.data());
  return ret_backup_ids;
}

/*
 * Class:     org_rocksdb_BackupEngine
 * Method:    garbageCollect
 * Signature: (J)V
 */
void Java_org_rocksdb_BackupEngine_garbageCollect(JNIEnv* env, jclass /*jbe*/,
                                                  jlong jbe_handle) {
  auto* backup_engine =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngine*>(jbe_handle);
  auto status = backup_engine->GarbageCollect();

  if (status.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
}

/*
 * Class:     org_rocksdb_BackupEngine
 * Method:    purgeOldBackups
 * Signature: (JI)V
 */
void Java_org_rocksdb_BackupEngine_purgeOldBackups(JNIEnv* env, jclass /*jbe*/,
                                                   jlong jbe_handle,
                                                   jint jnum_backups_to_keep) {
  auto* backup_engine =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngine*>(jbe_handle);
  auto status = backup_engine->PurgeOldBackups(
      static_cast<uint32_t>(jnum_backups_to_keep));

  if (status.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
}

/*
 * Class:     org_rocksdb_BackupEngine
 * Method:    deleteBackup
 * Signature: (JI)V
 */
void Java_org_rocksdb_BackupEngine_deleteBackup(JNIEnv* env, jclass /*jbe*/,
                                                jlong jbe_handle,
                                                jint jbackup_id) {
  auto* backup_engine =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngine*>(jbe_handle);
  auto status = backup_engine->DeleteBackup(
      static_cast<ROCKSDB_NAMESPACE::BackupID>(jbackup_id));

  if (status.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
}

/*
 * Class:     org_rocksdb_BackupEngine
 * Method:    restoreDbFromBackup
 * Signature: (JILjava/lang/String;Ljava/lang/String;J)V
 */
void Java_org_rocksdb_BackupEngine_restoreDbFromBackup(
    JNIEnv* env, jclass /*jbe*/, jlong jbe_handle, jint jbackup_id,
    jstring jdb_dir, jstring jwal_dir, jlong jrestore_options_handle) {
  auto* backup_engine =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngine*>(jbe_handle);
  const char* db_dir = env->GetStringUTFChars(jdb_dir, nullptr);
  if (db_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  const char* wal_dir = env->GetStringUTFChars(jwal_dir, nullptr);
  if (wal_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    env->ReleaseStringUTFChars(jdb_dir, db_dir);
    return;
  }
  auto* restore_options = reinterpret_cast<ROCKSDB_NAMESPACE::RestoreOptions*>(
      jrestore_options_handle);
  auto status = backup_engine->RestoreDBFromBackup(
      static_cast<ROCKSDB_NAMESPACE::BackupID>(jbackup_id), db_dir, wal_dir,
      *restore_options);

  env->ReleaseStringUTFChars(jwal_dir, wal_dir);
  env->ReleaseStringUTFChars(jdb_dir, db_dir);

  if (status.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
}

/*
 * Class:     org_rocksdb_BackupEngine
 * Method:    restoreDbFromLatestBackup
 * Signature: (JLjava/lang/String;Ljava/lang/String;J)V
 */
void Java_org_rocksdb_BackupEngine_restoreDbFromLatestBackup(
    JNIEnv* env, jclass /*jbe*/, jlong jbe_handle, jstring jdb_dir,
    jstring jwal_dir, jlong jrestore_options_handle) {
  auto* backup_engine =
      reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngine*>(jbe_handle);
  const char* db_dir = env->GetStringUTFChars(jdb_dir, nullptr);
  if (db_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  const char* wal_dir = env->GetStringUTFChars(jwal_dir, nullptr);
  if (wal_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    env->ReleaseStringUTFChars(jdb_dir, db_dir);
    return;
  }
  auto* restore_options = reinterpret_cast<ROCKSDB_NAMESPACE::RestoreOptions*>(
      jrestore_options_handle);
  auto status = backup_engine->RestoreDBFromLatestBackup(db_dir, wal_dir,
                                                         *restore_options);

  env->ReleaseStringUTFChars(jwal_dir, wal_dir);
  env->ReleaseStringUTFChars(jdb_dir, db_dir);

  if (status.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
}

/*
 * Class:     org_rocksdb_BackupEngine
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_BackupEngine_disposeInternalJni(JNIEnv* /*env*/,
                                                      jclass /*jcls*/,
                                                      jlong jbe_handle) {
  auto* be = reinterpret_cast<ROCKSDB_NAMESPACE::BackupEngine*>(jbe_handle);
  assert(be != nullptr);
  delete be;
}
