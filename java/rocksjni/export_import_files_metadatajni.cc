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
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ExportImportFilesMetaData_disposeInternal(
    JNIEnv* /*env*/, jobject /*jopt*/, jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::ExportImportFilesMetaData*>(jhandle);
  assert(metadata != nullptr);
  delete metadata;
}
