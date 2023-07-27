//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "include/org_rocksdb_ExportImportFilesMetaData.h"
#include "include/org_rocksdb_LiveFileMetaData.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_ExportImportFilesMetaData
 * Method:    newExportImportFilesMetaDataHandle
 * Signature: ([BI[J)J
 */
jlong Java_org_rocksdb_ExportImportFilesMetaData_newExportImportFilesMetaDataHandle(
    JNIEnv* env, jobject, jbyteArray j_db_comparator_name,
    jint j_db_comparator_name_len, jlongArray j_live_file_meta_data_array) {
  std::string db_comparator_name;
  jboolean has_exception = JNI_FALSE;

  if (j_db_comparator_name_len > 0) {
    db_comparator_name = ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
        env, j_db_comparator_name, j_db_comparator_name_len,
        [](const char* str, const size_t len) { return std::string(str, len); },
        &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      return 0;
    }
  }

  std::vector<ROCKSDB_NAMESPACE::LiveFileMetaData> live_file_metas;
  jlong* ptr_live_file_meta_data_array =
      env->GetLongArrayElements(j_live_file_meta_data_array, nullptr);
  if (ptr_live_file_meta_data_array == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  const jsize array_size = env->GetArrayLength(j_live_file_meta_data_array);
  for (jsize i = 0; i < array_size; ++i) {
    auto* ptr_level_file_meta =
        reinterpret_cast<ROCKSDB_NAMESPACE::LiveFileMetaData*>(
            ptr_live_file_meta_data_array[i]);
    live_file_metas.push_back(*ptr_level_file_meta);
  }

  env->ReleaseLongArrayElements(j_live_file_meta_data_array,
                                ptr_live_file_meta_data_array, JNI_ABORT);
  auto* export_import_files_meta_data =
      new ROCKSDB_NAMESPACE::ExportImportFilesMetaData;
  export_import_files_meta_data->db_comparator_name = db_comparator_name;
  export_import_files_meta_data->files = live_file_metas;
  return GET_CPLUSPLUS_POINTER(export_import_files_meta_data);
}

/*
 * Class:     org_rocksdb_LiveFileMetaData
 * Method:    newLiveFileMetaDataHandle
 * Signature: ([BIILjava/lang/String;Ljava/lang/String;JJJ[BI[BIJZJJ)J
 */
jlong Java_org_rocksdb_LiveFileMetaData_newLiveFileMetaDataHandle(
    JNIEnv* env, jobject, jbyteArray j_column_family_name,
    jint j_column_family_name_len, jint j_level, jstring j_file_name,
    jstring j_path, jlong j_size, jlong j_smallest_seqno, jlong j_largest_seqno,
    jbyteArray j_smallest_key, jint j_smallest_key_len,
    jbyteArray j_largest_key, jint j_largest_key_len, jlong j_num_read_sampled,
    jboolean j_being_compacted, jlong j_num_entries, jlong j_num_deletions) {
  std::string column_family_name;
  jboolean has_exception = JNI_FALSE;

  if (j_column_family_name_len > 0) {
    column_family_name = ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
        env, j_column_family_name, j_column_family_name_len,
        [](const char* str, const size_t len) { return std::string(str, len); },
        &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      return 0;
    }
  }

  const char* file_name = env->GetStringUTFChars(j_file_name, nullptr);
  if (file_name == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  const char* path = env->GetStringUTFChars(j_path, nullptr);
  if (path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  std::string smallest_key;
  if (j_smallest_key_len > 0) {
    smallest_key = ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
        env, j_smallest_key, j_smallest_key_len,
        [](const char* str, const size_t len) { return std::string(str, len); },
        &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      return 0;
    }
  }

  std::string largest_key;
  if (j_largest_key_len > 0) {
    largest_key = ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
        env, j_largest_key, j_largest_key_len,
        [](const char* str, const size_t len) { return std::string(str, len); },
        &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      return 0;
    }
  }
  auto* live_file_meta = new ROCKSDB_NAMESPACE::LiveFileMetaData;
  live_file_meta->column_family_name = column_family_name;
  live_file_meta->level = static_cast<int>(j_level);
  live_file_meta->db_path = path;
  live_file_meta->name = file_name;
  live_file_meta->size = j_size;
  live_file_meta->smallest_seqno = j_smallest_seqno;
  live_file_meta->largest_seqno = j_largest_seqno;
  live_file_meta->smallestkey = smallest_key;
  live_file_meta->largestkey = largest_key;
  live_file_meta->num_reads_sampled = j_num_read_sampled;
  live_file_meta->being_compacted = j_being_compacted;
  live_file_meta->num_entries = j_num_entries;
  live_file_meta->num_deletions = j_num_deletions;
  return GET_CPLUSPLUS_POINTER(live_file_meta);
}
