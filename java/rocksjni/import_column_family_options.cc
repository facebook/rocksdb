// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::FilterPolicy.

#include <jni.h>

#include "include/org_rocksdb_ImportColumnFamilyOptions.h"
#include "rocksdb/options.h"

/*
 * Class:     org_rocksdb_ImportColumnFamilyOptions
 * Method:    newImportColumnFamilyOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_ImportColumnFamilyOptions_newImportColumnFamilyOptions(
    JNIEnv*, jclass) {
  auto* options = new ROCKSDB_NAMESPACE::ImportColumnFamilyOptions();
  return reinterpret_cast<jlong>(options);
}

/*
 * Class:     org_rocksdb_newImportColumnFamilyOptions
 * Method:    newImportColumnFamilyOptions
 * Signature: (Z)J
 */
jlong Java_org_rocksdb_ImportColumnFamilyOptions_newImportColumnFamilyOptions__Z(
    JNIEnv*, jclass, jboolean jmove_files) {
  auto* options = new ROCKSDB_NAMESPACE::ImportColumnFamilyOptions();
  options->move_files = static_cast<bool>(jmove_files);
  return reinterpret_cast<jlong>(options);
}

/*
 * Class:     org_rocksdb_ImportColumnFamilyOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ImportColumnFamilyOptions_disposeInternal(JNIEnv*,
                                                                jobject,
                                                                jlong jhandle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ImportColumnFamilyOptions*>(jhandle);
  delete options;
}

/*
 * Class:     org_rocksdb_ImportColumnFamilyOptions
 * Method:    moveFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ImportColumnFamilyOptions_moveFiles(JNIEnv*, jobject,
                                                              jlong jhandle) {
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ImportColumnFamilyOptions*>(jhandle);
  return static_cast<jboolean>(options->move_files);
}
