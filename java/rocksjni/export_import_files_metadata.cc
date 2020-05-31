// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::FilterPolicy.

#include <jni.h>

#include "include/org_rocksdb_ExportImportFilesMetaData.h"
#include "rocksdb/metadata.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_ExportImportFilesMetaData
 * Method:    newExportImportFilesMetaData
 * Signature: ()J
 */
jlong Java_org_rocksdb_ExportImportFilesMetaData_newExportImportFilesMetaData(
    JNIEnv*, jclass) {
  auto* metadata = new ROCKSDB_NAMESPACE::ExportImportFilesMetaData();
  return reinterpret_cast<jlong>(metadata);
}

/*
 * Class:     org_rocksdb_ExportImportFilesMetaData
 * Method:    setDbComparatorName
 * Signature: (Ljava/lang/String;[J)J
 */
jlong Java_org_rocksdb_ExportImportFilesMetaData_newExportImportFilesMetaData__Ljava_lang_String_3J(
    JNIEnv*, jclass, jstring jdb_comparator_name, jlongArray jfile_handles) {
  auto* metadata = new ROCKSDB_NAMESPACE::ExportImportFilesMetaData();

  jboolean has_exception = JNI_FALSE;
  std::string db_comparator_name = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
      env, jdb_comparator_name, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }

  metadata->db_comparator_name = db_comparator_name;

  jlong* jfh = env->GetLongArrayElements(jfile_handles, nullptr);
  if (jfh == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  const jsize jlen = env->GetArrayLength(jfile_handles);
  for (jsize i = 0; i < jlen; i++) {
    auto* file_handle =
        reinterpret_cast<ROCKSDB_NAMESPACE::LiveFileMetaData*>(jfh[i]);
    metadata->files.push_back(*file_handle);
  }
  env->ReleaseLongArrayElements(jfile_handles, jfh, JNI_ABORT);

  return reinterpret_cast<jlong>(metadata);
}

/*
 * Class:     org_rocksdb_ExportImportFilesMetaData
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ExportImportFilesMetaData_disposeInternal(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::ExportImportFilesMetaData*>(jhandle);
  delete metadata;
}

/*
 * Class:     org_rocksdb_ExportImportFilesMetaData
 * Method:    dbComparatorName
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_ExportImportFilesMetaData_dbComparatorName(
    JNIEnv*, jobject, jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::ExportImportFilesMetaData*>(jhandle);
  return ROCKSDB_NAMESPACE::JniUtil::toJavaString(
      env, &metadata->db_comparator_name, false);
}

/*
 * Class:     org_rocksdb_ExportImportFilesMetaData
 * Method:    files
 * Signature: (J)[Lorg/rocksdb/LiveFileMetaData;
 */
jobjectArray Java_org_rocksdb_ExportImportFilesMetaData_files(JNIEnv*, jobject,
                                                              jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::ExportImportFilesMetaData*>(jhandle);

  const jsize jlen = static_cast<jsize>(metadata->files.size());
  jobjectArray jfiles =
      env->NewObjectArray(jlen, LiveFileMetaDataJni::getJClass(env), nullptr);
  if (jfiles == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  jsize i = 0;
  for (auto it = metadata->files.begin(); it != metadata->files.end(); ++it) {
    jobject jfile = LiveFileMetaDataJni::fromCppLiveFileMetaData(env, &(*it));
    if (jfile == nullptr) {
      // exception occurred
      env->DeleteLocalRef(jfiles);
      return nullptr;
    }

    env->SetObjectArrayElement(jfiles, i++, jfile);
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(jfile);
      env->DeleteLocalRef(jfiles);
      return nullptr;
    }

    env->DeleteLocalRef(jfile);
  }

  return jfiles;
}
