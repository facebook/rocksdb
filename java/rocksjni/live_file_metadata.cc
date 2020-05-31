// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::FilterPolicy.

#include <jni.h>

#include "include/org_rocksdb_LiveFileMetaData.h"
#include "rocksdb/metadata.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_LiveFileMetaData
 * Method:    newLiveFileMetaData
 * Signature: ()J
 */
jlong Java_org_rocksdb_LiveFileMetaData_newLiveFileMetaData(JNIEnv*, jclass) {
  auto* metadata = new ROCKSDB_NAMESPACE::LiveFileMetaData();
  return reinterpret_cast<jlong>(metadata);
}

/*
 * Class:     org_rocksdb_LiveFileMetaData
 * Method:    newLiveFileMetaData
 * Signature: ([BIILjava/lang/String;Ljava/lang/String;JJJ[BI[BIJZJJ)J
 */
jlong Java_org_rocksdb_LiveFileMetaData_newLiveFileMetaData__3BIILjava_lang_String_Ljava_lang_String_JJJ3BI3BIJZJJ(
    JNIEnv*, jclass, jbyteArray jcolumn_family_name,
    jint jcolumn_family_name_len, jint jlevel, jstring jfile_name,
    jstring jpath, jlong jsize, jlong jsmallest_seqno, jlong jlargest_seqno,
    jbyteArray jsmallest_key, jint jsmallest_key_len, jbyteArray jlargest_key,
    jint jlargest_key_len, jlong jnum_reads_sampled, jboolean jbeing_compacted,
    jlong jnum_entries, jlong jnum_deletions) {
  auto* metadata = new ROCKSDB_NAMESPACE::LiveFileMetaData();

  jboolean has_exception = JNI_FALSE;
  const std::string column_family_name =
      ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
          env, jcolumn_family_name, jcolumn_family_name_len,
          [](const char* str, const size_t len) {
            return std::string(str, len);
          },
          &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }

  has_exception = JNI_FALSE;
  std::string file_name = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
      env, jfile_name, &has_exception);
  if (has_exception == JNI_TRUE) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, "Could not copy jstring to std::string");
    return;
  }

  has_exception = JNI_FALSE;
  std::string path =
      ROCKSDB_NAMESPACE::JniUtil::copyStdString(env, jpath, &has_exception);
  if (has_exception == JNI_TRUE) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, "Could not copy jstring to std::string");
    return;
  }

  has_exception = JNI_FALSE;
  const std::string smallestkey =
      ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
          env, jsmallest_key, jsmallest_key_len,
          [](const char* str, const size_t len) {
            return std::string(str, len);
          },
          &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }

  has_exception = JNI_FALSE;
  const std::string largestkey =
      ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
          env, jlargest_key, jlargest_key_len,
          [](const char* str, const size_t len) {
            return std::string(str, len);
          },
          &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }

  metadata->column_family_name = column_family_name;
  metadata->level = static_cast<int>(jlevel);
  metadata->name = file_name;
  metadata->db_path = path;
  metadata->size = static_cast<size_t>(jsize);
  metadata->smallest_seqno =
      static_cast<ROCKSDB_NAMESPACE::SequenceNumber>(jsmallest_seqno);
  metadata->largest_seqno =
      static_cast<ROCKSDB_NAMESPACE::SequenceNumber>(jlargest_seqno);
  metadata->smallestkey = smallestkey;
  metadata->largestkey = largestkey;
  metadata->num_reads_sampled = static_cast<uint64_t>(jnum_reads_sampled);
  metadata->being_compacted = (jbeing_compacted == NI_TRUE);
  metadata->num_entries = static_cast<uint64_t>(jnum_entries);
  metadata->num_deletions = static_cast<uint64_t>(jnum_deletions);

  return reinterpret_cast<jlong>(metadata);
}

/*
 * Class:     org_rocksdb_LiveFileMetaData
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_LiveFileMetaData_disposeInternal(JNIEnv*, jobject,
                                                       jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::LiveFileMetaData*>(jhandle);
  delete metadata;
}

/*
 * Class:     org_rocksdb_LiveFileMetaData
 * Method:    columnFamilyName
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_LiveFileMetaData_columnFamilyName(JNIEnv*, jobject,
                                                              jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::LiveFileMetaData*>(jhandle);
  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env,
                                               metadata->column_family_name);
}

/*
 * Class:     org_rocksdb_LiveFileMetaData
 * Method:    level
 * Signature: (J)I
 */
jint Java_org_rocksdb_LiveFileMetaData_level(JNIEnv*, jobject, jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::LiveFileMetaData*>(jhandle);
  return static_cast<jint>(metadata->level);
}
