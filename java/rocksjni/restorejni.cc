// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::RestoreBackupableDB and rocksdb::RestoreOptions methods
// from Java side.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>

#include "include/org_rocksdb_RestoreOptions.h"
#include "rocksjni/portal.h"
#include "rocksdb/utilities/backupable_db.h"
/*
 * Class:     org_rocksdb_RestoreOptions
 * Method:    newRestoreOptions
 * Signature: (Z)J
 */
jlong Java_org_rocksdb_RestoreOptions_newRestoreOptions(JNIEnv* env,
    jclass jcls, jboolean keep_log_files) {
  auto ropt = new rocksdb::RestoreOptions(keep_log_files);
  return reinterpret_cast<jlong>(ropt);
}

/*
 * Class:     org_rocksdb_RestoreOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RestoreOptions_disposeInternal(JNIEnv* env, jobject jobj,
    jlong jhandle) {
  auto ropt = reinterpret_cast<rocksdb::RestoreOptions*>(jhandle);
  assert(ropt);
  delete ropt;
}
