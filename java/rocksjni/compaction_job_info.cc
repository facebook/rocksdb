// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::CompactionJobInfo.

#include <jni.h>

#include "include/org_rocksdb_CompactionJobInfo.h"
#include "rocksdb/listener.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    newCompactionJobInfo
 * Signature: ()J
 */
jlong Java_org_rocksdb_CompactionJobInfo_newCompactionJobInfo(
    JNIEnv*, jclass) {
  auto* compact_job_info = new ROCKSDB_NAMESPACE::CompactionJobInfo();
  return reinterpret_cast<jlong>(compact_job_info);
}

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_CompactionJobInfo_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* compact_job_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(jhandle);
  delete compact_job_info;
}

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    columnFamilyName
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_CompactionJobInfo_columnFamilyName(
    JNIEnv* env, jclass, jlong jhandle) {
  auto* compact_job_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(jhandle);
  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, compact_job_info->cf_name);
}

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    status
 * Signature: (J)Lorg/rocksdb/Status;
 */
jobject Java_org_rocksdb_CompactionJobInfo_status(
    JNIEnv* env, jclass, jlong jhandle) {
  auto* compact_job_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(jhandle);
  return ROCKSDB_NAMESPACE::StatusJni::construct(env, compact_job_info->status);
}

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    threadId
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobInfo_threadId(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(jhandle);
  return static_cast<jlong>(compact_job_info->thread_id);
}

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    jobId
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompactionJobInfo_jobId(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(jhandle);
  return static_cast<jint>(compact_job_info->job_id);
}

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    baseInputLevel
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompactionJobInfo_baseInputLevel(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(jhandle);
  return static_cast<jint>(compact_job_info->base_input_level);
}

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    outputLevel
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompactionJobInfo_outputLevel(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(jhandle);
  return static_cast<jint>(compact_job_info->output_level);
}

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    inputFiles
 * Signature: (J)[Ljava/lang/String;
 */
jobjectArray Java_org_rocksdb_CompactionJobInfo_inputFiles(
    JNIEnv* env, jclass, jlong jhandle) {
  auto* compact_job_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(jhandle);
  return ROCKSDB_NAMESPACE::JniUtil::toJavaStrings(
      env, &compact_job_info->input_files);
}

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    outputFiles
 * Signature: (J)[Ljava/lang/String;
 */
jobjectArray Java_org_rocksdb_CompactionJobInfo_outputFiles(
    JNIEnv* env, jclass, jlong jhandle) {
  auto* compact_job_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(jhandle);
  return ROCKSDB_NAMESPACE::JniUtil::toJavaStrings(
      env, &compact_job_info->output_files);
}

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    tableProperties
 * Signature: (J)Ljava/util/Map;
 */
jobject Java_org_rocksdb_CompactionJobInfo_tableProperties(
    JNIEnv* env, jclass, jlong jhandle) {
  auto* compact_job_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(jhandle);
  auto* map = &compact_job_info->table_properties;

  jobject jhash_map = ROCKSDB_NAMESPACE::HashMapJni::construct(
      env, static_cast<uint32_t>(map->size()));
  if (jhash_map == nullptr) {
    // exception occurred
    return nullptr;
  }

  const ROCKSDB_NAMESPACE::HashMapJni::FnMapKV<
      const std::string,
      std::shared_ptr<const ROCKSDB_NAMESPACE::TableProperties>, jobject,
      jobject>
      fn_map_kv =
          [env](const std::pair<
                const std::string,
                std::shared_ptr<const ROCKSDB_NAMESPACE::TableProperties>>&
                    kv) {
            jstring jkey = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
                env, &(kv.first), false);
            if (env->ExceptionCheck()) {
              // an error occurred
              return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
            }

            jobject jtable_properties =
                ROCKSDB_NAMESPACE::TablePropertiesJni::fromCppTableProperties(
                    env, *(kv.second.get()));
            if (env->ExceptionCheck()) {
              // an error occurred
              env->DeleteLocalRef(jkey);
              return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
            }

            return std::unique_ptr<std::pair<jobject, jobject>>(
                new std::pair<jobject, jobject>(static_cast<jobject>(jkey),
                                                jtable_properties));
          };

  if (!ROCKSDB_NAMESPACE::HashMapJni::putAll(env, jhash_map, map->begin(),
                                             map->end(), fn_map_kv)) {
    // exception occurred
    return nullptr;
  }

  return jhash_map;
}

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    compactionReason
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_CompactionJobInfo_compactionReason(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(jhandle);
  return ROCKSDB_NAMESPACE::CompactionReasonJni::toJavaCompactionReason(
      compact_job_info->compaction_reason);
}

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    compression
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_CompactionJobInfo_compression(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(jhandle);
  return ROCKSDB_NAMESPACE::CompressionTypeJni::toJavaCompressionType(
      compact_job_info->compression);
}

/*
 * Class:     org_rocksdb_CompactionJobInfo
 * Method:    stats
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobInfo_stats(
    JNIEnv *, jclass, jlong jhandle) {
  auto* compact_job_info =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(jhandle);
  auto* stats = new ROCKSDB_NAMESPACE::CompactionJobStats();
  stats->Add(compact_job_info->stats);
  return reinterpret_cast<jlong>(stats);
}
