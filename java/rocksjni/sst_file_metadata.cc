// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::FilterPolicy.

#include <jni.h>

#include "include/org_rocksdb_SstFileMetaData.h"
#include "rocksdb/metadata.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    newSstFileMetaData
 * Signature: ()J
 */
jlong Java_org_rocksdb_SstFileMetaData_newSstFileMetaData(JNIEnv*, jclass) {
  auto* metadata = new ROCKSDB_NAMESPACE::SstFileMetaData();
  return reinterpret_cast<jlong>(metadata);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    newSstFileMetaData
 * Signature: (Ljava/lang/String;Ljava/lang/String;JJJ[BI[BIJZJJ)J
 */
jlong Java_org_rocksdb_SstFileMetaData_newSstFileMetaData__Ljava_lang_String_Ljava_lang_String_JJJ3BI3BIJZJJ(
    JNIEnv*, jclass, jstring jfile_name, jstring jpath, jlong jsize,
    jlong jsmallest_seqno, jlong jlargest_seqno, jbyteArray jsmallest_key,
    jint jsmallest_key_len, jbyteArray jlargest_key, jint jlargest_key_len,
    jlong jnum_reads_sampled, jboolean jbeing_compacted, jlong jnum_entries,
    jlong jnum_deletions) {
  auto* metadata = new ROCKSDB_NAMESPACE::LiveFileMetaData();

  jboolean has_exception = JNI_FALSE;
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
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_SstFileMetaData_disposeInternal(JNIEnv*, jobject,
                                                      jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  delete metadata;
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    fileName
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_SstFileMetaData_fileName(JNIEnv*, jobject,
                                                  jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &metadata->name, false);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    path
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_SstFileMetaData_path(JNIEnv*, jobject, jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &metadata->db_path,
                                                  false);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    size
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileMetaData_size(JNIEnv*, jobject, jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jlong>(metadata->size);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    smallestSeqno
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileMetaData_smallestSeqno(JNIEnv*, jobject,
                                                     jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jlong>(metadata->smallest_seqno);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    largestSeqno
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileMetaData_largestSeqno(JNIEnv*, jobject,
                                                    jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jlong>(metadata->largest_seqno);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    smallestKey
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_SstFileMetaData_smallestKey(JNIEnv*, jobject,
                                                        jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, metadata->smallestkey);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    largestKey
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_SstFileMetaData_largestKey(JNIEnv*, jobject,
                                                       jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, metadata->largestkey);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    numReadsSampled
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileMetaData_numReadsSampled(JNIEnv*, jobject,
                                                       jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jlong>(metadata->num_reads_sampled);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    beingCompacted
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_SstFileMetaData_beingCompacted(JNIEnv*, jobject,
                                                         jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jboolean>(metadata->being_compacted);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    numEntries
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileMetaData_numEntries(JNIEnv*, jobject,
                                                  jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jlong>(metadata->num_entries);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    numDeletions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileMetaData_numDeletions(JNIEnv*, jobject,
                                                    jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jlong>(metadata->num_deletions);
}