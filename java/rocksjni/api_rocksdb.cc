// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::ColumnFamilyHandle.

#include "api_rocksdb.h"

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include "api_columnfamilyhandle.h"
#include "api_rocksdb.h"
#include "api_rocksnative.h"
#include "org_rocksdb_api_RocksDB.h"
#include "rocksjni/portal.h"

// TODO AP - put this extern into a header, and/or refactor
jlong rocksdb_open_helper(JNIEnv* env, jlong jopt_handle, jstring jdb_path,
                          std::function<ROCKSDB_NAMESPACE::Status(
                              const ROCKSDB_NAMESPACE::Options&,
                              const std::string&, ROCKSDB_NAMESPACE::DB**)>
                              open_fn);
/*
 * Class:     org_rocksdb_api_RocksDB
 * Method:    open
 * Signature: (JLjava/lang/String;[[B[J)[J
 */
jlongArray Java_org_rocksdb_api_RocksDB_open(JNIEnv* env, jclass,
                                             jlong jopt_handle,
                                             jstring jdb_path,
                                             jobjectArray jcolumn_names,
                                             jlongArray jcolumn_options) {
  jlongArray jresult_handles = rocksdb_open_helper(
      env, jopt_handle, jdb_path, jcolumn_names, jcolumn_options,
      (ROCKSDB_NAMESPACE::Status(*)(
          const ROCKSDB_NAMESPACE::DBOptions&, const std::string&,
          const std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor>&,
          std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>*,
          ROCKSDB_NAMESPACE::DB**)) &
          ROCKSDB_NAMESPACE::DB::Open);

  if (jresult_handles == nullptr) {
    return nullptr;
  }

  const jsize len_results = env->GetArrayLength(jresult_handles);
  jlong* jresults = env->GetLongArrayElements(jresult_handles, nullptr);
  // TODO AP - there is no error checking, nullptr return, or JVM
  // element release implemented

  std::shared_ptr<ROCKSDB_NAMESPACE::DB> db(
      reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(jresults[0]));
  std::unique_ptr<APIRocksDB> apiRocksDB(new APIRocksDB(db));
  jresults[0] = reinterpret_cast<jlong>(apiRocksDB.get());
  // TODO AP - there is no error checking, nullptr return, or JVM
  // element release implemented

  for (int i = 1; i < len_results; i++) {
    std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh(
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jresults[i]));
    apiRocksDB->columnFamilyHandles.push_back(cfh);
    std::unique_ptr<APIColumnFamilyHandle> apiColumnFamilyHandle(db, cfh);
    jresults[i] = reinterpret_cast<jlong>(apiColumnFamilyHandle.get());
  }

  env->ReleaseLongArrayElements(jresults, nullptr);
  return jresult_handles;
}
