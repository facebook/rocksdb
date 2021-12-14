// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::ColumnFamilyHandle.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include "api_rocksnative.h"
#include "include/org_rocksdb_RocksNative.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_api_RocksDB
 * Method:    open
 * Signature: (JLjava/lang/String;[[B[J)[J
 */
jlongArray Java_org_rocksdb_api_RocksDB_open__JLjava_lang_String_2_3_3B_3J(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path,
    jobjectArray jcolumn_names, jlongArray jcolumn_options) {
  //
  jlongArray rocksdb_open_helper(
      env, jopt_handle, jdb_path, jcolumn_names, jcolumn_options,
      (ROCKSDB_NAMESPACE::Status(*)(
          const ROCKSDB_NAMESPACE::DBOptions&, const std::string&,
          const std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor>&,
          std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>*,
          ROCKSDB_NAMESPACE::DB**)) &
          ROCKSDB_NAMESPACE::DB::Open);
}

api_rocksdb::
