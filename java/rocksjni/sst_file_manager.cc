// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ ROCKSDB_NAMESPACE::SstFileManager methods
// from Java side.

#include "rocksdb/sst_file_manager.h"

#include <jni.h>

#include <memory>

#include "include/org_rocksdb_SstFileManager.h"
#include "rocksjni/portal/hash_map_jni.h"
#include "rocksjni/portal/long_jni.h"
#include "rocksjni/portal/rocks_d_b_exception_jni.h"

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    newSstFileManager
 * Signature: (JJJDJ)J
 */
jlong Java_org_rocksdb_SstFileManager_newSstFileManager(
    JNIEnv* jnienv, jclass /*jcls*/, jlong jenv_handle, jlong jlogger_handle,
    jlong jrate_bytes, jdouble jmax_trash_db_ratio,
    jlong jmax_delete_chunk_bytes) {
  auto* env = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jenv_handle);
  ROCKSDB_NAMESPACE::Status s;
  ROCKSDB_NAMESPACE::SstFileManager* sst_file_manager = nullptr;

  if (jlogger_handle != 0) {
    auto* sptr_logger =
        reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Logger>*>(
            jlogger_handle);
    sst_file_manager = ROCKSDB_NAMESPACE::NewSstFileManager(
        env, *sptr_logger, "", jrate_bytes, true, &s, jmax_trash_db_ratio,
        jmax_delete_chunk_bytes);
  } else {
    sst_file_manager = ROCKSDB_NAMESPACE::NewSstFileManager(
        env, nullptr, "", jrate_bytes, true, &s, jmax_trash_db_ratio,
        jmax_delete_chunk_bytes);
  }

  if (!s.ok()) {
    if (sst_file_manager != nullptr) {
      delete sst_file_manager;
    }
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(jnienv, s);
  }
  auto* sptr_sst_file_manager =
      new std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>(sst_file_manager);

  return GET_CPLUSPLUS_POINTER(sptr_sst_file_manager);
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    setMaxAllowedSpaceUsage
 * Signature: (JJ)V
 */
void Java_org_rocksdb_SstFileManager_setMaxAllowedSpaceUsage(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle, jlong jmax_allowed_space) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jhandle);
  sptr_sst_file_manager->get()->SetMaxAllowedSpaceUsage(jmax_allowed_space);
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    setCompactionBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_SstFileManager_setCompactionBufferSize(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle,
    jlong jcompaction_buffer_size) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jhandle);
  sptr_sst_file_manager->get()->SetCompactionBufferSize(
      jcompaction_buffer_size);
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    isMaxAllowedSpaceReached
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_SstFileManager_isMaxAllowedSpaceReached(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jhandle);
  return sptr_sst_file_manager->get()->IsMaxAllowedSpaceReached();
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    isMaxAllowedSpaceReachedIncludingCompactions
 * Signature: (J)Z
 */
jboolean
Java_org_rocksdb_SstFileManager_isMaxAllowedSpaceReachedIncludingCompactions(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jhandle);
  return sptr_sst_file_manager->get()
      ->IsMaxAllowedSpaceReachedIncludingCompactions();
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    getTotalSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileManager_getTotalSize(JNIEnv* /*env*/,
                                                   jclass /*jcls*/,
                                                   jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jhandle);
  return sptr_sst_file_manager->get()->GetTotalSize();
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    getTrackedFiles
 * Signature: (J)Ljava/util/Map;
 */
jobject Java_org_rocksdb_SstFileManager_getTrackedFiles(JNIEnv* env,
                                                        jclass /*jcls*/,
                                                        jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jhandle);
  auto tracked_files = sptr_sst_file_manager->get()->GetTrackedFiles();

  // TODO(AR) could refactor to share code with
  // ROCKSDB_NAMESPACE::HashMapJni::fromCppMap(env, tracked_files);

  const jobject jtracked_files = ROCKSDB_NAMESPACE::HashMapJni::construct(
      env, static_cast<uint32_t>(tracked_files.size()));
  if (jtracked_files == nullptr) {
    // exception occurred
    return nullptr;
  }

  const ROCKSDB_NAMESPACE::HashMapJni::FnMapKV<const std::string,
                                               const uint64_t, jobject, jobject>
      fn_map_kv =
          [env](const std::pair<const std::string, const uint64_t>& pair) {
            const jstring jtracked_file_path =
                env->NewStringUTF(pair.first.c_str());
            if (jtracked_file_path == nullptr) {
              // an error occurred
              return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
            }
            const jobject jtracked_file_size =
                ROCKSDB_NAMESPACE::LongJni::valueOf(env, pair.second);
            if (jtracked_file_size == nullptr) {
              // an error occurred
              return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
            }
            return std::unique_ptr<std::pair<jobject, jobject>>(
                new std::pair<jobject, jobject>(jtracked_file_path,
                                                jtracked_file_size));
          };

  if (!ROCKSDB_NAMESPACE::HashMapJni::putAll(env, jtracked_files,
                                             tracked_files.begin(),
                                             tracked_files.end(), fn_map_kv)) {
    // exception occcurred
    return nullptr;
  }

  return jtracked_files;
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    getDeleteRateBytesPerSecond
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileManager_getDeleteRateBytesPerSecond(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jhandle);
  return sptr_sst_file_manager->get()->GetDeleteRateBytesPerSecond();
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    setDeleteRateBytesPerSecond
 * Signature: (JJ)V
 */
void Java_org_rocksdb_SstFileManager_setDeleteRateBytesPerSecond(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle, jlong jdelete_rate) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jhandle);
  sptr_sst_file_manager->get()->SetDeleteRateBytesPerSecond(jdelete_rate);
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    getMaxTrashDBRatio
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_SstFileManager_getMaxTrashDBRatio(JNIEnv* /*env*/,
                                                           jclass /*jcls*/,
                                                           jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jhandle);
  return sptr_sst_file_manager->get()->GetMaxTrashDBRatio();
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    setMaxTrashDBRatio
 * Signature: (JD)V
 */
void Java_org_rocksdb_SstFileManager_setMaxTrashDBRatio(JNIEnv* /*env*/,
                                                        jclass /*jcls*/,
                                                        jlong jhandle,
                                                        jdouble jratio) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jhandle);
  sptr_sst_file_manager->get()->SetMaxTrashDBRatio(jratio);
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_SstFileManager_disposeInternalJni(JNIEnv* /*env*/,
                                                        jclass /*cls*/,
                                                        jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::SstFileManager>*>(
          jhandle);
  delete sptr_sst_file_manager;
}
