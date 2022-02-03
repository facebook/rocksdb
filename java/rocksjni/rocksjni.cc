// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::DB methods from Java side.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "api_columnfamilyhandle.h"
#include "api_iterator.h"
#include "api_rocksdb.h"
#include "include/org_rocksdb_RocksDB.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "rocksdb/version.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

#ifdef min
#undef min
#endif

jlong rocksdb_open_helper(JNIEnv* env, jlong jopt_handle, jstring jdb_path,
                          std::function<ROCKSDB_NAMESPACE::Status(
                              const ROCKSDB_NAMESPACE::Options&,
                              const std::string&, ROCKSDB_NAMESPACE::DB**)>
                              open_fn) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jopt_handle);
  ROCKSDB_NAMESPACE::DB* db = nullptr;
  ROCKSDB_NAMESPACE::Status s = open_fn(*opt, db_path, &db);

  env->ReleaseStringUTFChars(jdb_path, db_path);

  if (s.ok()) {
    return GET_CPLUSPLUS_POINTER(db);
    std::shared_ptr<ROCKSDB_NAMESPACE::DB> dbShared(db);
    std::unique_ptr<APIRocksDB> dbAPI(new APIRocksDB(dbShared));
    return reinterpret_cast<jlong>(dbAPI.release());
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return 0;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    open
 * Signature: (JLjava/lang/String;)J
 */
jlong Java_org_rocksdb_RocksDB_open__JLjava_lang_String_2(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path) {
  return rocksdb_open_helper(env, jopt_handle, jdb_path,
                             (ROCKSDB_NAMESPACE::Status(*)(
                                 const ROCKSDB_NAMESPACE::Options&,
                                 const std::string&, ROCKSDB_NAMESPACE::DB**)) &
                                 ROCKSDB_NAMESPACE::DB::Open);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    openROnly
 * Signature: (JLjava/lang/String;Z)J
 */
jlong Java_org_rocksdb_RocksDB_openROnly__JLjava_lang_String_2Z(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path,
    jboolean jerror_if_wal_file_exists) {
  const bool error_if_wal_file_exists = jerror_if_wal_file_exists == JNI_TRUE;
  return rocksdb_open_helper(
      env, jopt_handle, jdb_path,
      [error_if_wal_file_exists](const ROCKSDB_NAMESPACE::Options& options,
                                 const std::string& db_path,
                                 ROCKSDB_NAMESPACE::DB** db) {
        return ROCKSDB_NAMESPACE::DB::OpenForReadOnly(options, db_path, db,
                                                      error_if_wal_file_exists);
      });
}

jlongArray rocksdb_open_helper(
    JNIEnv* env, jlong jopt_handle, jstring jdb_path,
    jobjectArray jcolumn_names, jlongArray jcolumn_options,
    std::function<ROCKSDB_NAMESPACE::Status(
        const ROCKSDB_NAMESPACE::DBOptions&, const std::string&,
        const std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor>&,
        std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>*,
        ROCKSDB_NAMESPACE::DB**)>
        open_fn) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  const jsize len_cols = env->GetArrayLength(jcolumn_names);
  jlong* jco = env->GetLongArrayElements(jcolumn_options, nullptr);
  if (jco == nullptr) {
    // exception thrown: OutOfMemoryError
    env->ReleaseStringUTFChars(jdb_path, db_path);
    return nullptr;
  }

  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor> column_families;
  jboolean has_exception = JNI_FALSE;
  ROCKSDB_NAMESPACE::JniUtil::byteStrings<std::string>(
      env, jcolumn_names,
      [](const char* str_data, const size_t str_len) {
        return std::string(str_data, str_len);
      },
      [&jco, &column_families](size_t idx, std::string cf_name) {
        ROCKSDB_NAMESPACE::ColumnFamilyOptions* cf_options =
            reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jco[idx]);
        column_families.push_back(
            ROCKSDB_NAMESPACE::ColumnFamilyDescriptor(cf_name, *cf_options));
      },
      &has_exception);

  env->ReleaseLongArrayElements(jcolumn_options, jco, JNI_ABORT);

  if (has_exception == JNI_TRUE) {
    // exception occurred
    env->ReleaseStringUTFChars(jdb_path, db_path);
    return nullptr;
  }

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jopt_handle);
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  ROCKSDB_NAMESPACE::DB* db = nullptr;
  ROCKSDB_NAMESPACE::Status s =
      open_fn(*opt, db_path, column_families, &cf_handles, &db);

  // we have now finished with db_path
  env->ReleaseStringUTFChars(jdb_path, db_path);

  // check if open operation was successful
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  std::shared_ptr<ROCKSDB_NAMESPACE::DB> dbShared(db);
  std::unique_ptr<APIRocksDB> dbAPI(new APIRocksDB(dbShared));

  const jsize resultsLen = 1 + len_cols;  // db handle + column family handles
  std::unique_ptr<jlong[]> results =
      std::unique_ptr<jlong[]>(new jlong[resultsLen]);
  for (int i = 1; i <= len_cols; i++) {
    std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfShared =
        APIColumnFamilyHandle::createSharedPtr(cf_handles[i - 1]);
    std::unique_ptr<APIColumnFamilyHandle> cfhAPI(
        new APIColumnFamilyHandle(dbShared, cfShared));
    dbAPI->columnFamilyHandles.push_back(cfShared);
    results[i] = GET_CPLUSPLUS_POINTER(cfhAPI.release());
  }
  results[0] = GET_CPLUSPLUS_POINTER(dbAPI.release());

  jlongArray jresults = env->NewLongArray(resultsLen);
  if (jresults == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  env->SetLongArrayRegion(jresults, 0, resultsLen, results.get());
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresults);
    return nullptr;
  }

  return jresults;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    openROnly
 * Signature: (JLjava/lang/String;[[B[JZ)[J
 */
jlongArray Java_org_rocksdb_RocksDB_openROnly__JLjava_lang_String_2_3_3B_3JZ(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path,
    jobjectArray jcolumn_names, jlongArray jcolumn_options,
    jboolean jerror_if_wal_file_exists) {
  const bool error_if_wal_file_exists = jerror_if_wal_file_exists == JNI_TRUE;
  return rocksdb_open_helper(
      env, jopt_handle, jdb_path, jcolumn_names, jcolumn_options,
      [error_if_wal_file_exists](
          const ROCKSDB_NAMESPACE::DBOptions& options,
          const std::string& db_path,
          const std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor>&
              column_families,
          std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>* handles,
          ROCKSDB_NAMESPACE::DB** db) {
        return ROCKSDB_NAMESPACE::DB::OpenForReadOnly(
            options, db_path, column_families, handles, db,
            error_if_wal_file_exists);
      });
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    open
 * Signature: (JLjava/lang/String;[[B[J)[J
 */
jlongArray Java_org_rocksdb_RocksDB_open__JLjava_lang_String_2_3_3B_3J(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path,
    jobjectArray jcolumn_names, jlongArray jcolumn_options) {
  return rocksdb_open_helper(
      env, jopt_handle, jdb_path, jcolumn_names, jcolumn_options,
      (ROCKSDB_NAMESPACE::Status(*)(
          const ROCKSDB_NAMESPACE::DBOptions&, const std::string&,
          const std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor>&,
          std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>*,
          ROCKSDB_NAMESPACE::DB**)) &
          ROCKSDB_NAMESPACE::DB::Open);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    openAsSecondary
 * Signature: (JLjava/lang/String;Ljava/lang/String;)J
 */
jlong Java_org_rocksdb_RocksDB_openAsSecondary__JLjava_lang_String_2Ljava_lang_String_2(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path,
    jstring jsecondary_db_path) {
  const char* secondary_db_path =
      env->GetStringUTFChars(jsecondary_db_path, nullptr);
  if (secondary_db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  jlong db_handle = rocksdb_open_helper(
      env, jopt_handle, jdb_path,
      [secondary_db_path](const ROCKSDB_NAMESPACE::Options& options,
                          const std::string& db_path,
                          ROCKSDB_NAMESPACE::DB** db) {
        return ROCKSDB_NAMESPACE::DB::OpenAsSecondary(options, db_path,
                                                      secondary_db_path, db);
      });

  // we have now finished with secondary_db_path
  env->ReleaseStringUTFChars(jsecondary_db_path, secondary_db_path);

  return db_handle;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    openAsSecondary
 * Signature: (JLjava/lang/String;Ljava/lang/String;[[B[J)[J
 */
jlongArray
Java_org_rocksdb_RocksDB_openAsSecondary__JLjava_lang_String_2Ljava_lang_String_2_3_3B_3J(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path,
    jstring jsecondary_db_path, jobjectArray jcolumn_names,
    jlongArray jcolumn_options) {
  const char* secondary_db_path =
      env->GetStringUTFChars(jsecondary_db_path, nullptr);
  if (secondary_db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  jlongArray jhandles = rocksdb_open_helper(
      env, jopt_handle, jdb_path, jcolumn_names, jcolumn_options,
      [secondary_db_path](
          const ROCKSDB_NAMESPACE::DBOptions& options,
          const std::string& db_path,
          const std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor>&
              column_families,
          std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>* handles,
          ROCKSDB_NAMESPACE::DB** db) {
        return ROCKSDB_NAMESPACE::DB::OpenAsSecondary(
            options, db_path, secondary_db_path, column_families, handles, db);
      });

  // we have now finished with secondary_db_path
  env->ReleaseStringUTFChars(jsecondary_db_path, secondary_db_path);

  return jhandles;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    nativeClose
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_nativeClose(JNIEnv*, jobject, jlong jhandle) {
  std::unique_ptr<APIRocksDB> dbAPI(reinterpret_cast<APIRocksDB*>(jhandle));
  dbAPI->check("nativeClose()");
  // Now the unique_ptr destructor will delete() referenced shared_ptr contents
  // in the API object.
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    isLastReference
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_RocksDB_isLastReference(JNIEnv*, jobject,
                                                  jlong jhandle) {
  std::unique_ptr<APIRocksDB> dbAPI(reinterpret_cast<APIRocksDB*>(jhandle));
  dbAPI->check("isLastReference()");
  const bool result = (dbAPI->db.use_count() == 1);
  dbAPI.release();
  return result;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    closeDatabase
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_closeDatabase(
    JNIEnv* env, jclass, jlong jhandle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jhandle);
  ROCKSDB_NAMESPACE::Status s = dbAPI->Close();
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    listColumnFamilies
 * Signature: (JLjava/lang/String;)[[B
 */
jobjectArray Java_org_rocksdb_RocksDB_listColumnFamilies(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path) {
  std::vector<std::string> column_family_names;
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jopt_handle);
  ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::DB::ListColumnFamilies(
      *opt, db_path, &column_family_names);

  env->ReleaseStringUTFChars(jdb_path, db_path);

  jobjectArray jcolumn_family_names =
      ROCKSDB_NAMESPACE::JniUtil::stringsBytes(env, column_family_names);

  return jcolumn_family_names;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    createColumnFamily
 * Signature: (J[BIJ)J
 */
jlong Java_org_rocksdb_RocksDB_createColumnFamily(
    JNIEnv* env, jobject, jlong jhandle, jbyteArray jcf_name,
    jint jcf_name_len, jlong jcf_options_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jhandle);
  jboolean has_exception = JNI_FALSE;
  const std::string cf_name =
      ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
          env, jcf_name, jcf_name_len,
          [](const char* str, const size_t len) {
            return std::string(str, len);
          },
          &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return 0;
  }
  auto* cf_options = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(
      jcf_options_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  ROCKSDB_NAMESPACE::Status s =
      dbAPI->CreateColumnFamily(*cf_options, cf_name, &cf_handle);
  if (!s.ok()) {
    // error occurred
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return 0;
  }
  std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh =
      APIColumnFamilyHandle::createSharedPtr(cf_handle);
  std::unique_ptr<APIColumnFamilyHandle> cfhAPI(
      new APIColumnFamilyHandle(dbAPI.db, cfh));
  dbAPI.columnFamilyHandles.push_back(cfh);
  return GET_CPLUSPLUS_POINTER(cfhAPI.release());
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    createColumnFamilies
 * Signature: (JJ[[B)[J
 */
jlongArray Java_org_rocksdb_RocksDB_createColumnFamilies__JJ_3_3B(
    JNIEnv* env, jobject, jlong jhandle, jlong jcf_options_handle,
    jobjectArray jcf_names) {
  auto* dbAPI = reinterpret_cast<APIRocksDB*>(jhandle);
  auto* cf_options = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(
      jcf_options_handle);
  jboolean has_exception = JNI_FALSE;
  std::vector<std::string> cf_names;
  ROCKSDB_NAMESPACE::JniUtil::byteStrings<std::string>(
      env, jcf_names,
      [](const char* str, const size_t len) { return std::string(str, len); },
      [&cf_names](const size_t, std::string str) { cf_names.push_back(str); },
      &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return nullptr;
  }

  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  ROCKSDB_NAMESPACE::Status s =
      (*dbAPI)->CreateColumnFamilies(*cf_options, cf_names, &cf_handles);
  if (!s.ok()) {
    // error occurred
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  std::vector<APIColumnFamilyHandle*> cfhAPIs;
  for (auto* cf_handle : cf_handles) {
    std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh =
        APIColumnFamilyHandle::createSharedPtr(cf_handle);
    std::unique_ptr<APIColumnFamilyHandle> cfhAPI(
        new APIColumnFamilyHandle(dbAPI->db, cfh));
    dbAPI->columnFamilyHandles.push_back(cfh);
    cfhAPIs.push_back(cfhAPI.release());
  }

  jlongArray jcf_handles =
      ROCKSDB_NAMESPACE::JniUtil::toJPointers<APIColumnFamilyHandle>(
          env, cfhAPIs, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return nullptr;
  }
  return jcf_handles;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    createColumnFamilies
 * Signature: (J[J[[B)[J
 */
jlongArray Java_org_rocksdb_RocksDB_createColumnFamilies__J_3J_3_3B(
    JNIEnv* env, jobject, jlong jhandle, jlongArray jcf_options_handles,
    jobjectArray jcf_names) {
  auto* dbAPI = reinterpret_cast<APIRocksDB*>(jhandle);
  const jsize jlen = env->GetArrayLength(jcf_options_handles);
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor> cf_descriptors;
  cf_descriptors.reserve(jlen);

  jlong* jcf_options_handles_elems =
      env->GetLongArrayElements(jcf_options_handles, nullptr);
  if(jcf_options_handles_elems == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
  }

  // extract the column family descriptors
  jboolean has_exception = JNI_FALSE;
  for (jsize i = 0; i < jlen; i++) {
    auto* cf_options =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(
            jcf_options_handles_elems[i]);
    jbyteArray jcf_name = static_cast<jbyteArray>(
        env->GetObjectArrayElement(jcf_names, i));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseLongArrayElements(jcf_options_handles, jcf_options_handles_elems, JNI_ABORT);
      return nullptr;
    }
    const std::string cf_name =
        ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
            env, jcf_name,
            [](const char* str, const size_t len) {
              return std::string(str, len);
            },
            &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      env->DeleteLocalRef(jcf_name);
      env->ReleaseLongArrayElements(jcf_options_handles, jcf_options_handles_elems, JNI_ABORT);
      return nullptr;
    }

    cf_descriptors.push_back(
        ROCKSDB_NAMESPACE::ColumnFamilyDescriptor(cf_name, *cf_options));

    env->DeleteLocalRef(jcf_name);
  }

  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  ROCKSDB_NAMESPACE::Status s =
      (*dbAPI)->CreateColumnFamilies(cf_descriptors, &cf_handles);

  env->ReleaseLongArrayElements(jcf_options_handles, jcf_options_handles_elems, JNI_ABORT);

  if (!s.ok()) {
    // error occurred
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  std::vector<APIColumnFamilyHandle*> cfhAPIs;
  for (auto* cf_handle : cf_handles) {
    std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh =
        APIColumnFamilyHandle::createSharedPtr(cf_handle);
    std::unique_ptr<APIColumnFamilyHandle> cfhAPI(
        new APIColumnFamilyHandle(dbAPI->db, cfh));
    dbAPI->columnFamilyHandles.push_back(cfh);
    cfhAPIs.push_back(cfhAPI.release());
  }

  jlongArray jcf_handles =
      ROCKSDB_NAMESPACE::JniUtil::toJPointers<APIColumnFamilyHandle>(
          env, cfhAPIs, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return nullptr;
  }
  return jcf_handles;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    dropColumnFamily
 * Signature: (JJ)V;
 */
void Java_org_rocksdb_RocksDB_dropColumnFamily(
    JNIEnv* env, jobject, jlong jdb_handle,
    jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto cfhPtr = APIColumnFamilyHandle::lock(env, jcf_handle);
  if (!cfhPtr) {
    // CFH exception
    return;
  }
  ROCKSDB_NAMESPACE::Status s = dbAPI->DropColumnFamily(cfhPtr.get());
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }

  const auto& it = std::find(dbAPI.columnFamilyHandles.begin(),
                             dbAPI.columnFamilyHandles.end(), cfhPtr);
  if (it != dbAPI.columnFamilyHandles.end()) {
    dbAPI.columnFamilyHandles.erase(it);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    dropColumnFamilies
 * Signature: (J[J)V
 */
void Java_org_rocksdb_RocksDB_dropColumnFamilies(
    JNIEnv* env, jobject, jlong jdb_handle,
    jlongArray jcolumn_family_handles) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);

  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  std::vector<std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>> cfhPtrs;
  if (jcolumn_family_handles != nullptr) {
    const jsize len_cols = env->GetArrayLength(jcolumn_family_handles);

    jlong* jcfh = env->GetLongArrayElements(jcolumn_family_handles, nullptr);
    if (jcfh == nullptr) {
      // exception thrown: OutOfMemoryError
      return;
    }

    for (jsize i = 0; i < len_cols; i++) {
      const auto cfhPtr = APIColumnFamilyHandle::lock(env, jcfh[i]);
      if (!cfhPtr) {
        // CFH exception
        env->ReleaseLongArrayElements(jcolumn_family_handles, jcfh, JNI_ABORT);
        return;
      }
      cf_handles.push_back(cfhPtr.get());
      cfhPtrs.push_back(cfhPtr);
    }
    env->ReleaseLongArrayElements(jcolumn_family_handles, jcfh, JNI_ABORT);
  }

  ROCKSDB_NAMESPACE::Status s = dbAPI->DropColumnFamilies(cf_handles);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return;
  }

  for (const auto& cfhPtr : cfhPtrs) {
    const auto& it = std::find(dbAPI.columnFamilyHandles.begin(),
                               dbAPI.columnFamilyHandles.end(), cfhPtr);
    if (it != dbAPI.columnFamilyHandles.end()) {
      dbAPI.columnFamilyHandles.erase(it);
    }
  }
}

//////////////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::DB::Put

/**
 * @return true if the put succeeded, false if a Java Exception was thrown
 */
bool rocksdb_put_helper(JNIEnv* env, APIRocksDB& dbAPI,
                        const ROCKSDB_NAMESPACE::WriteOptions& write_options,
                        APIColumnFamilyHandle* cfhAPI, jbyteArray jkey,
                        jint jkey_off, jint jkey_len, jbyteArray jval,
                        jint jval_off, jint jval_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return false;
  }

  jbyte* value = new jbyte[jval_len];
  env->GetByteArrayRegion(jval, jval_off, jval_len, value);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] value;
    delete[] key;
    return false;
  }

  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
  ROCKSDB_NAMESPACE::Slice value_slice(reinterpret_cast<char*>(value),
                                       jval_len);

  ROCKSDB_NAMESPACE::Status s;
  if (cfhAPI != nullptr) {
    auto cf_handle = cfhAPI->cfhLock(env);
    if (!cf_handle) {
      return false;
    }
    s = dbAPI->Put(write_options, cf_handle.get(), key_slice, value_slice);
  } else {
    // backwards compatibility
    s = dbAPI->Put(write_options, key_slice, value_slice);
  }

  // cleanup
  delete[] value;
  delete[] key;

  if (s.ok()) {
    return true;
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return false;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    put
 * Signature: (J[BII[BII)V
 */
void Java_org_rocksdb_RocksDB_put__J_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len) {
  auto* dbAPI = reinterpret_cast<APIRocksDB*>(jdb_handle);
  static const ROCKSDB_NAMESPACE::WriteOptions default_write_options =
      ROCKSDB_NAMESPACE::WriteOptions();
  rocksdb_put_helper(env, *dbAPI, default_write_options, nullptr, jkey,
                     jkey_off, jkey_len, jval, jval_off, jval_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    put
 * Signature: (J[BII[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_put__J_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcf_handle) {
  auto* dbAPI = reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* cfhAPI = reinterpret_cast<APIColumnFamilyHandle*>(jcf_handle);
  static const ROCKSDB_NAMESPACE::WriteOptions default_write_options =
      ROCKSDB_NAMESPACE::WriteOptions();
  if (cfhAPI != nullptr) {
    rocksdb_put_helper(env, *dbAPI, default_write_options, cfhAPI, jkey,
                       jkey_off, jkey_len, jval, jval_off, jval_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::InvalidColumnFamily());
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    put
 * Signature: (JJ[BII[BII)V
 */
void Java_org_rocksdb_RocksDB_put__JJ_3BII_3BII(JNIEnv* env, jobject,
                                                jlong jdb_handle,
                                                jlong jwrite_options_handle,
                                                jbyteArray jkey, jint jkey_off,
                                                jint jkey_len, jbyteArray jval,
                                                jint jval_off, jint jval_len) {
  auto* dbAPI = reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  rocksdb_put_helper(env, *dbAPI, *write_options, nullptr, jkey, jkey_off,
                     jkey_len, jval, jval_off, jval_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    put
 * Signature: (JJ[BII[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_put__JJ_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcf_handle) {
  auto* dbAPI = reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto* cfhAPI = reinterpret_cast<APIColumnFamilyHandle*>(jcf_handle);
  if (cfhAPI != nullptr) {
    rocksdb_put_helper(env, *dbAPI, *write_options, cfhAPI, jkey, jkey_off,
                       jkey_len, jval, jval_off, jval_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::InvalidColumnFamily());
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    putDirect
 * Signature: (JJLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;II)V
 */
void Java_org_rocksdb_RocksDB_putDirect__JJLjava_nio_ByteBuffer_2IILjava_nio_ByteBuffer_2II(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jwrite_options_handle,
    jobject jkey, jint jkey_off, jint jkey_len, jobject jval, jint jval_off,
    jint jval_len) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto put = [&env, &dbAPI, &write_options](ROCKSDB_NAMESPACE::Slice& key,
                                            ROCKSDB_NAMESPACE::Slice& value) {
    ROCKSDB_NAMESPACE::Status s = dbAPI->Put(*write_options, key, value);
    if (s.ok()) {
      return;
    }
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  };
  ROCKSDB_NAMESPACE::JniUtil::kv_op_direct(put, env, jkey, jkey_off, jkey_len,
                                           jval, jval_off, jval_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    putDirect
 * Signature: (JJLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_rocksdb_RocksDB_putDirect__JJLjava_nio_ByteBuffer_2IILjava_nio_ByteBuffer_2IIJ(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jwrite_options_handle,
    jobject jkey, jint jkey_off, jint jkey_len, jobject jval, jint jval_off,
    jint jval_len, jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto cfhPtr = APIColumnFamilyHandle::lock(env, jcf_handle);
  if (!cfhPtr) {
    return;
  }
  auto put = [&env, &dbAPI, &cfhPtr, &write_options](
                 ROCKSDB_NAMESPACE::Slice& key,
                 ROCKSDB_NAMESPACE::Slice& value) {
    ROCKSDB_NAMESPACE::Status s =
        dbAPI->Put(*write_options, cfhPtr.get(), key, value);
    if (s.ok()) {
      return;
    }
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  };
  ROCKSDB_NAMESPACE::JniUtil::kv_op_direct(put, env, jkey, jkey_off, jkey_len,
                                           jval, jval_off, jval_len);
}

//////////////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::DB::Delete()

/**
 * @return true if the delete succeeded, false if a Java Exception was thrown
 */
bool rocksdb_delete_helper(JNIEnv* env, ROCKSDB_NAMESPACE::DB* db,
                           const ROCKSDB_NAMESPACE::WriteOptions& write_options,
                           ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle,
                           jbyteArray jkey, jint jkey_off, jint jkey_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return false;
  }
  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  ROCKSDB_NAMESPACE::Status s;
  if (cf_handle != nullptr) {
    s = db->Delete(write_options, cf_handle, key_slice);
  } else {
    // backwards compatibility
    s = db->Delete(write_options, key_slice);
  }

  // cleanup
  delete[] key;

  if (s.ok()) {
    return true;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    delete
 * Signature: (J[BII)V
 */
void Java_org_rocksdb_RocksDB_delete__J_3BII(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  static const ROCKSDB_NAMESPACE::WriteOptions default_write_options =
      ROCKSDB_NAMESPACE::WriteOptions();
  rocksdb_delete_helper(env, dbAPI.get(), default_write_options, nullptr, jkey,
                        jkey_off, jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    delete
 * Signature: (J[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_delete__J_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jlong jcf_handle) {
  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  static const ROCKSDB_NAMESPACE::WriteOptions default_write_options =
      ROCKSDB_NAMESPACE::WriteOptions();
  const auto cfhPtr = APIColumnFamilyHandle::lock(env, jcf_handle);
  if (!cfhPtr) {
    // CFH exception
    return;
  }
  rocksdb_delete_helper(env, dbAPI.get(), default_write_options, cfhPtr.get(),
                        jkey, jkey_off, jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    delete
 * Signature: (JJ[BII)V
 */
void Java_org_rocksdb_RocksDB_delete__JJ_3BII(
    JNIEnv* env, jobject,
    jlong jdb_handle,
    jlong jwrite_options,
    jbyteArray jkey, jint jkey_off, jint jkey_len) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options);
  rocksdb_delete_helper(env, dbAPI.get(), *write_options, nullptr, jkey,
                        jkey_off, jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    delete
 * Signature: (JJ[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_delete__JJ_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jlong jcf_handle) {
  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options);
  const auto cfhPtr = APIColumnFamilyHandle::lock(env, jcf_handle);
  if (!cfhPtr) {
    // CFH exception
    return;
  }
  rocksdb_delete_helper(env, dbAPI.get(), *write_options, cfhPtr.get(), jkey,
                        jkey_off, jkey_len);
}

//////////////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::DB::SingleDelete()
/**
 * @return true if the single delete succeeded, false if a Java Exception
 *     was thrown
 */
bool rocksdb_single_delete_helper(
    JNIEnv* env, ROCKSDB_NAMESPACE::DB* db,
    const ROCKSDB_NAMESPACE::WriteOptions& write_options,
    ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle, jbyteArray jkey,
    jint jkey_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, 0, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return false;
  }
  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  ROCKSDB_NAMESPACE::Status s;
  if (cf_handle != nullptr) {
    s = db->SingleDelete(write_options, cf_handle, key_slice);
  } else {
    // backwards compatibility
    s = db->SingleDelete(write_options, key_slice);
  }

  delete[] key;

  if (s.ok()) {
    return true;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    singleDelete
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_RocksDB_singleDelete__J_3BI(
    JNIEnv* env, jobject,
    jlong jdb_handle,
    jbyteArray jkey,
    jint jkey_len) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  static const ROCKSDB_NAMESPACE::WriteOptions default_write_options =
      ROCKSDB_NAMESPACE::WriteOptions();
  rocksdb_single_delete_helper(env, dbAPI.get(), default_write_options, nullptr,
                               jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    singleDelete
 * Signature: (J[BIJ)V
 */
void Java_org_rocksdb_RocksDB_singleDelete__J_3BIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  static const ROCKSDB_NAMESPACE::WriteOptions default_write_options =
      ROCKSDB_NAMESPACE::WriteOptions();
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_single_delete_helper(env, dbAPI.get(), default_write_options,
                                 cf_handle, jkey, jkey_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::InvalidColumnFamily());
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    singleDelete
 * Signature: (JJ[BIJ)V
 */
void Java_org_rocksdb_RocksDB_singleDelete__JJ_3BI(
    JNIEnv* env, jobject, jlong jdb_handle,
    jlong jwrite_options,
    jbyteArray jkey,
    jint jkey_len) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options);
  rocksdb_single_delete_helper(env, dbAPI.get(), *write_options, nullptr, jkey,
                               jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    singleDelete
 * Signature: (JJ[BIJ)V
 */
void Java_org_rocksdb_RocksDB_singleDelete__JJ_3BIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options,
    jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_single_delete_helper(env, dbAPI.get(), *write_options, cf_handle,
                                 jkey, jkey_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::InvalidColumnFamily());
  }
}

//////////////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::DB::DeleteRange()
/**
 * @return true if the delete range succeeded, false if a Java Exception
 *     was thrown
 */
bool rocksdb_delete_range_helper(
    JNIEnv* env, ROCKSDB_NAMESPACE::DB* db,
    const ROCKSDB_NAMESPACE::WriteOptions& write_options,
    ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle, jbyteArray jbegin_key,
    jint jbegin_key_off, jint jbegin_key_len, jbyteArray jend_key,
    jint jend_key_off, jint jend_key_len) {
  jbyte* begin_key = new jbyte[jbegin_key_len];
  env->GetByteArrayRegion(jbegin_key, jbegin_key_off, jbegin_key_len,
      begin_key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] begin_key;
    return false;
  }
  ROCKSDB_NAMESPACE::Slice begin_key_slice(reinterpret_cast<char*>(begin_key),
                                           jbegin_key_len);

  jbyte* end_key = new jbyte[jend_key_len];
  env->GetByteArrayRegion(jend_key, jend_key_off, jend_key_len, end_key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] begin_key;
    delete[] end_key;
    return false;
  }
  ROCKSDB_NAMESPACE::Slice end_key_slice(reinterpret_cast<char*>(end_key),
                                         jend_key_len);

  ROCKSDB_NAMESPACE::Status s =
      db->DeleteRange(write_options, cf_handle, begin_key_slice, end_key_slice);

  // cleanup
  delete[] begin_key;
  delete[] end_key;

  if (s.ok()) {
    return true;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteRange
 * Signature: (J[BII[BII)V
 */
void Java_org_rocksdb_RocksDB_deleteRange__J_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jbegin_key, jint jbegin_key_off, jint jbegin_key_len,
    jbyteArray jend_key, jint jend_key_off, jint jend_key_len) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  static const ROCKSDB_NAMESPACE::WriteOptions default_write_options =
      ROCKSDB_NAMESPACE::WriteOptions();
  rocksdb_delete_range_helper(env, dbAPI.get(), default_write_options, nullptr,
                              jbegin_key, jbegin_key_off, jbegin_key_len,
                              jend_key, jend_key_off, jend_key_len);
}

jint rocksdb_get_helper_direct(
    JNIEnv* env, ROCKSDB_NAMESPACE::DB* db,
    const ROCKSDB_NAMESPACE::ReadOptions& read_options,
    ROCKSDB_NAMESPACE::ColumnFamilyHandle* column_family_handle, jobject jkey,
    jint jkey_off, jint jkey_len, jobject jval, jint jval_off, jint jval_len,
    bool* has_exception) {
  static const int kNotFound = -1;
  static const int kStatusError = -2;
  static const int kArgumentError = -3;

  char* key = reinterpret_cast<char*>(env->GetDirectBufferAddress(jkey));
  if (key == nullptr) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env,
        "Invalid key argument (argument is not a valid direct ByteBuffer)");
    *has_exception = true;
    return kArgumentError;
  }
  if (env->GetDirectBufferCapacity(jkey) < (jkey_off + jkey_len)) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env,
        "Invalid key argument. Capacity is less than requested region (offset "
        "+ length).");
    *has_exception = true;
    return kArgumentError;
  }

  char* value = reinterpret_cast<char*>(env->GetDirectBufferAddress(jval));
  if (value == nullptr) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env,
        "Invalid value argument (argument is not a valid direct ByteBuffer)");
    *has_exception = true;
    return kArgumentError;
  }

  if (env->GetDirectBufferCapacity(jval) < (jval_off + jval_len)) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env,
        "Invalid value argument. Capacity is less than requested region "
        "(offset + length).");
    *has_exception = true;
    return kArgumentError;
  }

  key += jkey_off;
  value += jval_off;

  ROCKSDB_NAMESPACE::Slice key_slice(key, jkey_len);

  // TODO(yhchiang): we might save one memory allocation here by adding
  // a DB::Get() function which takes preallocated jbyte* as input.
  std::string cvalue;
  ROCKSDB_NAMESPACE::Status s;
  if (column_family_handle != nullptr) {
    s = db->Get(read_options, column_family_handle, key_slice, &cvalue);
  } else {
    // backwards compatibility
    s = db->Get(read_options, key_slice, &cvalue);
  }

  if (s.IsNotFound()) {
    *has_exception = false;
    return kNotFound;
  } else if (!s.ok()) {
    *has_exception = true;
    // Here since we are throwing a Java exception from c++ side.
    // As a result, c++ does not know calling this function will in fact
    // throwing an exception.  As a result, the execution flow will
    // not stop here, and codes after this throw will still be
    // executed.
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);

    // Return a dummy const value to avoid compilation error, although
    // java side might not have a chance to get the return value :)
    return kStatusError;
  }

  const jint cvalue_len = static_cast<jint>(cvalue.size());
  const jint length = std::min(jval_len, cvalue_len);

  memcpy(value, cvalue.c_str(), length);

  *has_exception = false;
  return cvalue_len;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteRange
 * Signature: (J[BII[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_deleteRange__J_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jbegin_key, jint jbegin_key_off, jint jbegin_key_len,
    jbyteArray jend_key, jint jend_key_off, jint jend_key_len,
    jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  static const ROCKSDB_NAMESPACE::WriteOptions default_write_options =
      ROCKSDB_NAMESPACE::WriteOptions();
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_delete_range_helper(
        env, dbAPI.get(), default_write_options, cf_handle, jbegin_key,
        jbegin_key_off, jbegin_key_len, jend_key, jend_key_off, jend_key_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::InvalidColumnFamily());
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteRange
 * Signature: (JJ[BII[BII)V
 */
void Java_org_rocksdb_RocksDB_deleteRange__JJ_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options,
    jbyteArray jbegin_key, jint jbegin_key_off, jint jbegin_key_len,
    jbyteArray jend_key, jint jend_key_off, jint jend_key_len) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options);
  rocksdb_delete_range_helper(env, dbAPI.get(), *write_options, nullptr,
                              jbegin_key, jbegin_key_off, jbegin_key_len,
                              jend_key, jend_key_off, jend_key_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteRange
 * Signature: (JJ[BII[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_deleteRange__JJ_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options,
    jbyteArray jbegin_key, jint jbegin_key_off, jint jbegin_key_len,
    jbyteArray jend_key, jint jend_key_off, jint jend_key_len,
    jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_delete_range_helper(env, dbAPI.get(), *write_options, cf_handle,
                                jbegin_key, jbegin_key_off, jbegin_key_len,
                                jend_key, jend_key_off, jend_key_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::InvalidColumnFamily());
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getDirect
 * Signature: (JJLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJ)I
 */
jint Java_org_rocksdb_RocksDB_getDirect(JNIEnv* env, jobject /*jdb*/,
                                        jlong jdb_handle, jlong jropt_handle,
                                        jobject jkey, jint jkey_off,
                                        jint jkey_len, jobject jval,
                                        jint jval_off, jint jval_len,
                                        jlong jcf_handle) {
  auto* db_handle = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(jdb_handle);
  auto* ro_opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jropt_handle);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  bool has_exception = false;
  return rocksdb_get_helper_direct(
      env, db_handle,
      ro_opt == nullptr ? ROCKSDB_NAMESPACE::ReadOptions() : *ro_opt, cf_handle,
      jkey, jkey_off, jkey_len, jval, jval_off, jval_len, &has_exception);
}

//////////////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::DB::Merge

/**
 * @return true if the merge succeeded, false if a Java Exception was thrown
 */
bool rocksdb_merge_helper(JNIEnv* env, ROCKSDB_NAMESPACE::DB* db,
                          const ROCKSDB_NAMESPACE::WriteOptions& write_options,
                          ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle,
                          jbyteArray jkey, jint jkey_off, jint jkey_len,
                          jbyteArray jval, jint jval_off, jint jval_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return false;
  }
  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  jbyte* value = new jbyte[jval_len];
  env->GetByteArrayRegion(jval, jval_off, jval_len, value);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] value;
    delete[] key;
    return false;
  }
  ROCKSDB_NAMESPACE::Slice value_slice(reinterpret_cast<char*>(value),
                                       jval_len);

  ROCKSDB_NAMESPACE::Status s;
  if (cf_handle != nullptr) {
    s = db->Merge(write_options, cf_handle, key_slice, value_slice);
  } else {
    s = db->Merge(write_options, key_slice, value_slice);
  }

  // cleanup
  delete[] value;
  delete[] key;

  if (s.ok()) {
    return true;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    merge
 * Signature: (J[BII[BII)V
 */
void Java_org_rocksdb_RocksDB_merge__J_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  static const ROCKSDB_NAMESPACE::WriteOptions default_write_options =
      ROCKSDB_NAMESPACE::WriteOptions();
  rocksdb_merge_helper(env, dbAPI.get(), default_write_options, nullptr, jkey,
                       jkey_off, jkey_len, jval, jval_off, jval_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    merge
 * Signature: (J[BII[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_merge__J_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  static const ROCKSDB_NAMESPACE::WriteOptions default_write_options =
      ROCKSDB_NAMESPACE::WriteOptions();
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_merge_helper(env, dbAPI.get(), default_write_options, cf_handle,
                         jkey, jkey_off, jkey_len, jval, jval_off, jval_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::InvalidColumnFamily());
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    merge
 * Signature: (JJ[BII[BII)V
 */
void Java_org_rocksdb_RocksDB_merge__JJ_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  rocksdb_merge_helper(env, dbAPI.get(), *write_options, nullptr, jkey,
                       jkey_off, jkey_len, jval, jval_off, jval_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    merge
 * Signature: (JJ[BII[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_merge__JJ_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len, jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_merge_helper(env, dbAPI.get(), *write_options, cf_handle, jkey,
                         jkey_off, jkey_len, jval, jval_off, jval_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::InvalidColumnFamily());
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteDirect
 * Signature: (JJLjava/nio/ByteBuffer;II)V
 */
void Java_org_rocksdb_RocksDB_deleteDirect__JJLjava_nio_ByteBuffer_2II(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jwrite_options,
    jobject jkey, jint jkey_offset, jint jkey_len) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options);
  auto remove = [&env, &dbAPI, &write_options](ROCKSDB_NAMESPACE::Slice& key) {
    ROCKSDB_NAMESPACE::Status s = dbAPI->Delete(*write_options, key);
    if (s.ok()) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
      return;
    }
  };
  ROCKSDB_NAMESPACE::JniUtil::k_op_direct(remove, env, jkey, jkey_offset,
                                          jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteDirect
 * Signature: (JJLjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_rocksdb_RocksDB_deleteDirect__JJLjava_nio_ByteBuffer_2IIJ(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jwrite_options,
    jobject jkey, jint jkey_offset, jint jkey_len, jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options);
  const auto cfhPtr = APIColumnFamilyHandle::lock(env, jcf_handle);
  if (!cfhPtr) {
    // CFH exception
    return;
  }
  auto remove = [&env, &dbAPI, &write_options,
                 &cfhPtr](ROCKSDB_NAMESPACE::Slice& key) {
    ROCKSDB_NAMESPACE::Status s =
        dbAPI->Delete(*write_options, cfhPtr.get(), key);
    if (s.ok()) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
      return;
    }
  };
  ROCKSDB_NAMESPACE::JniUtil::k_op_direct(remove, env, jkey, jkey_offset,
                                          jkey_len);
}

//////////////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::DB::Write
/*
 * Class:     org_rocksdb_RocksDB
 * Method:    write0
 * Signature: (JJJ)V
 */
void Java_org_rocksdb_RocksDB_write0(
    JNIEnv* env, jobject, jlong jdb_handle,
    jlong jwrite_options_handle, jlong jwb_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);

  ROCKSDB_NAMESPACE::Status s = dbAPI->Write(*write_options, wb);

  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    write1
 * Signature: (JJJ)V
 */
void Java_org_rocksdb_RocksDB_write1(
    JNIEnv* env, jobject, jlong jdb_handle,
    jlong jwrite_options_handle, jlong jwbwi_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* wb = wbwi->GetWriteBatch();

  ROCKSDB_NAMESPACE::Status s = dbAPI->Write(*write_options, wb);

  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

//////////////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::DB::Get

jbyteArray rocksdb_get_helper(JNIEnv* env, APIRocksDB& dbAPI,
                              const ROCKSDB_NAMESPACE::ReadOptions& read_opt,
                              APIColumnFamilyHandle* cfhAPI, jbyteArray jkey,
                              jint jkey_off, jint jkey_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return nullptr;
  }

  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  std::string value;
  ROCKSDB_NAMESPACE::Status s;
  if (cfhAPI != nullptr) {
    auto cfh = cfhAPI->cfhLockDBCheck(env, dbAPI);
    if (!cfh) {
      delete[] key;
      return nullptr;
    }
    s = dbAPI->Get(read_opt, cfh.get(), key_slice, &value);
  } else {
    // backwards compatibility
    s = dbAPI->Get(read_opt, key_slice, &value);
  }

  // cleanup
  delete[] key;

  if (s.IsNotFound()) {
    return nullptr;
  }

  if (s.ok()) {
    jbyteArray jret_value = ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, value);
    if (jret_value == nullptr) {
      // exception occurred
      return nullptr;
    }
    return jret_value;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  return nullptr;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (J[BII)[B
 */
jbyteArray Java_org_rocksdb_RocksDB_get__J_3BII(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len) {
  return rocksdb_get_helper(env, *reinterpret_cast<APIRocksDB*>(jdb_handle),
                            ROCKSDB_NAMESPACE::ReadOptions(), nullptr, jkey,
                            jkey_off, jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (J[BIIJ)[B
 */
jbyteArray Java_org_rocksdb_RocksDB_get__J_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jlong jcf_handle) {
  auto* dbAPI = reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* cfAPI = reinterpret_cast<APIColumnFamilyHandle*>(jcf_handle);
  if (cfAPI != nullptr) {
    return rocksdb_get_helper(env, *dbAPI, ROCKSDB_NAMESPACE::ReadOptions(),
                              cfAPI, jkey, jkey_off, jkey_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::InvalidColumnFamily());
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (JJ[BII)[B
 */
jbyteArray Java_org_rocksdb_RocksDB_get__JJ_3BII(
    JNIEnv* env, jobject,
    jlong jdb_handle, jlong jropt_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len) {
  return rocksdb_get_helper(
      env, *reinterpret_cast<APIRocksDB*>(jdb_handle),
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jropt_handle), nullptr,
      jkey, jkey_off, jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (JJ[BIIJ)[B
 */
jbyteArray Java_org_rocksdb_RocksDB_get__JJ_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jropt_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jlong jcf_handle) {
  auto* dbAPI = reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto& ro_opt =
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jropt_handle);
  auto* cfhAPI = reinterpret_cast<APIColumnFamilyHandle*>(jcf_handle);
  if (cfhAPI != nullptr) {
    return rocksdb_get_helper(env, *dbAPI, ro_opt, cfhAPI, jkey, jkey_off,
                              jkey_len);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::InvalidColumnFamily());
    return nullptr;
  }
}

jint rocksdb_get_helper(JNIEnv* env, APIRocksDB& dbAPI,
                        const ROCKSDB_NAMESPACE::ReadOptions& read_options,
                        APIColumnFamilyHandle* cfhAPI, jbyteArray jkey,
                        jint jkey_off, jint jkey_len, jbyteArray jval,
                        jint jval_off, jint jval_len, bool* has_exception) {
  static const int kNotFound = -1;
  static const int kStatusError = -2;

  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    delete[] key;
    *has_exception = true;
    return kStatusError;
  }
  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  // TODO(yhchiang): we might save one memory allocation here by adding
  // a DB::Get() function which takes preallocated jbyte* as input.
  std::string cvalue;
  ROCKSDB_NAMESPACE::Status s;
  if (cfhAPI) {
    auto cfh = cfhAPI->cfhLockDBCheck(env, dbAPI);
    if (!cfh) {
      delete[] key;
      *has_exception = true;
      return kStatusError;
    }
    s = dbAPI->Get(read_options, cfh.get(), key_slice, &cvalue);
  } else {
    // backwards compatibility
    s = dbAPI->Get(read_options, key_slice, &cvalue);
  }

  // cleanup
  delete[] key;

  if (s.IsNotFound()) {
    *has_exception = false;
    return kNotFound;
  } else if (!s.ok()) {
    *has_exception = true;
    // Here since we are throwing a Java exception from c++ side.
    // As a result, c++ does not know calling this function will in fact
    // throwing an exception.  As a result, the execution flow will
    // not stop here, and codes after this throw will still be
    // executed.
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);

    // Return a dummy const value to avoid compilation error, although
    // java side might not have a chance to get the return value :)
    return kStatusError;
  }

  const jint cvalue_len = static_cast<jint>(cvalue.size());
  const jint length = std::min(jval_len, cvalue_len);

  env->SetByteArrayRegion(
      jval, jval_off, length,
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(cvalue.c_str())));
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    *has_exception = true;
    return kStatusError;
  }

  *has_exception = false;
  return cvalue_len;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (J[BII[BII)I
 */
jint Java_org_rocksdb_RocksDB_get__J_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len) {
  bool has_exception = false;
  return rocksdb_get_helper(env, *reinterpret_cast<APIRocksDB*>(jdb_handle),
                            ROCKSDB_NAMESPACE::ReadOptions(), nullptr, jkey,
                            jkey_off, jkey_len, jval, jval_off, jval_len,
                            &has_exception);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (J[BII[BIIJ)I
 */
jint Java_org_rocksdb_RocksDB_get__J_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcf_handle) {
  auto* dbAPI = reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* cfhAPI = reinterpret_cast<APIColumnFamilyHandle*>(jcf_handle);
  if (cfhAPI != nullptr) {
    bool has_exception = false;
    return rocksdb_get_helper(env, *dbAPI, ROCKSDB_NAMESPACE::ReadOptions(),
                              cfhAPI, jkey, jkey_off, jkey_len, jval, jval_off,
                              jval_len, &has_exception);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::InvalidColumnFamily());
    // will never be evaluated
    return 0;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (JJ[BII[BII)I
 */
jint Java_org_rocksdb_RocksDB_get__JJ_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jropt_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len) {
  bool has_exception = false;
  return rocksdb_get_helper(
      env, *reinterpret_cast<APIRocksDB*>(jdb_handle),
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jropt_handle), nullptr,
      jkey, jkey_off, jkey_len, jval, jval_off, jval_len, &has_exception);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (JJ[BII[BIIJ)I
 */
jint Java_org_rocksdb_RocksDB_get__JJ_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jropt_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcf_handle) {
  auto* dbAPI = reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto& ro_opt =
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jropt_handle);
  auto* cfhAPI = reinterpret_cast<APIColumnFamilyHandle*>(jcf_handle);
  if (cfhAPI != nullptr) {
    bool has_exception = false;
    return rocksdb_get_helper(env, *dbAPI, ro_opt, cfhAPI, jkey, jkey_off,
                              jkey_len, jval, jval_off, jval_len,
                              &has_exception);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::InvalidColumnFamily());
    // will never be evaluated
    return 0;
  }
}

inline void multi_get_helper_release_keys(std::vector<jbyte*>& keys_to_free) {
  auto end = keys_to_free.end();
  for (auto it = keys_to_free.begin(); it != end; ++it) {
    delete[] * it;
  }
  keys_to_free.clear();
}

/**
 * @brief fill a native array of cf handles from java handles
 *
 * @param env
 * @param cf_handles to fill from the java variants
 * @param jcolumn_family_handles
 * @return true if the copy succeeds
 * @return false if a JNI exception is generated
 */
inline bool cf_handles_from_jcf_handles(
    JNIEnv* env, APIRocksDB& dbAPI,
    std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>& cf_handles,
    jlongArray jcolumn_family_handles) {
  if (jcolumn_family_handles != nullptr) {
    const jsize len_cols = env->GetArrayLength(jcolumn_family_handles);

    jlong* jcfh = env->GetLongArrayElements(jcolumn_family_handles, nullptr);
    if (jcfh == nullptr) {
      // exception thrown: OutOfMemoryError
      jclass exception_cls = (env)->FindClass("java/lang/OutOfMemoryError");
      (env)->ThrowNew(exception_cls,
                      "Insufficient Memory for CF handle array.");
      return false;
    }

    for (jsize i = 0; i < len_cols; i++) {
      auto* cfhAPI = reinterpret_cast<APIColumnFamilyHandle*>(jcfh[i]);
      auto cfh = cfhAPI->cfhLockDBCheck(env, dbAPI);
      if (!cfh) {
        cf_handles.clear();
        return false;
      }
      cf_handles.push_back(cfh.get());
    }
    env->ReleaseLongArrayElements(jcolumn_family_handles, jcfh, JNI_ABORT);
  }
  return true;
}

/**
 * @brief copy keys from JNI into vector of slices for Rocks API
 *
 * @param keys to instantiate
 * @param jkeys
 * @param jkey_offs
 * @param jkey_lens
 * @return true if the copy succeeds
 * @return false if a JNI exception is raised
 */
inline bool keys_from_jkeys(JNIEnv* env,
                            std::vector<ROCKSDB_NAMESPACE::Slice>& keys,
                            std::vector<jbyte*>& keys_to_free,
                            jobjectArray jkeys, jintArray jkey_offs,
                            jintArray jkey_lens) {
  jint* jkey_off = env->GetIntArrayElements(jkey_offs, nullptr);
  if (jkey_off == nullptr) {
    // exception thrown: OutOfMemoryError
    jclass exception_cls = (env)->FindClass("java/lang/OutOfMemoryError");
    (env)->ThrowNew(exception_cls, "Insufficient Memory for key offset array.");
    return false;
  }

  jint* jkey_len = env->GetIntArrayElements(jkey_lens, nullptr);
  if (jkey_len == nullptr) {
    // exception thrown: OutOfMemoryError
    env->ReleaseIntArrayElements(jkey_offs, jkey_off, JNI_ABORT);
    jclass exception_cls = (env)->FindClass("java/lang/OutOfMemoryError");
    (env)->ThrowNew(exception_cls, "Insufficient Memory for key length array.");
    return false;
  }

  const jsize len_keys = env->GetArrayLength(jkeys);
  for (jsize i = 0; i < len_keys; i++) {
    jobject jkey = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseIntArrayElements(jkey_lens, jkey_len, JNI_ABORT);
      env->ReleaseIntArrayElements(jkey_offs, jkey_off, JNI_ABORT);
      multi_get_helper_release_keys(keys_to_free);
      jclass exception_cls = (env)->FindClass("java/lang/OutOfMemoryError");
      (env)->ThrowNew(exception_cls,
                      "Insufficient Memory for key object array.");
      return false;
    }

    jbyteArray jkey_ba = reinterpret_cast<jbyteArray>(jkey);

    const jint len_key = jkey_len[i];
    jbyte* key = new jbyte[len_key];
    env->GetByteArrayRegion(jkey_ba, jkey_off[i], len_key, key);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      delete[] key;
      env->DeleteLocalRef(jkey);
      env->ReleaseIntArrayElements(jkey_lens, jkey_len, JNI_ABORT);
      env->ReleaseIntArrayElements(jkey_offs, jkey_off, JNI_ABORT);
      multi_get_helper_release_keys(keys_to_free);
      jclass exception_cls =
          (env)->FindClass("java/lang/ArrayIndexOutOfBoundsException");
      (env)->ThrowNew(exception_cls, "Invalid byte array region index.");
      return false;
    }

    ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), len_key);
    keys.push_back(key_slice);

    env->DeleteLocalRef(jkey);
    keys_to_free.push_back(key);
  }

  // cleanup jkey_off and jken_len
  env->ReleaseIntArrayElements(jkey_lens, jkey_len, JNI_ABORT);
  env->ReleaseIntArrayElements(jkey_offs, jkey_off, JNI_ABORT);

  return true;
}

inline bool keys_from_bytebuffers(JNIEnv* env,
                                  std::vector<ROCKSDB_NAMESPACE::Slice>& keys,
                                  jobjectArray jkeys, jintArray jkey_offs,
                                  jintArray jkey_lens) {
  jint* jkey_off = env->GetIntArrayElements(jkey_offs, nullptr);
  if (jkey_off == nullptr) {
    // exception thrown: OutOfMemoryError
    jclass exception_cls = (env)->FindClass("java/lang/OutOfMemoryError");
    (env)->ThrowNew(exception_cls, "Insufficient Memory for key offset array.");
    return false;
  }

  jint* jkey_len = env->GetIntArrayElements(jkey_lens, nullptr);
  if (jkey_len == nullptr) {
    // exception thrown: OutOfMemoryError
    env->ReleaseIntArrayElements(jkey_offs, jkey_off, JNI_ABORT);
    jclass exception_cls = (env)->FindClass("java/lang/OutOfMemoryError");
    (env)->ThrowNew(exception_cls, "Insufficient Memory for key length array.");
    return false;
  }

  const jsize len_keys = env->GetArrayLength(jkeys);
  for (jsize i = 0; i < len_keys; i++) {
    jobject jkey = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      return false;
    }
    char* key = reinterpret_cast<char*>(env->GetDirectBufferAddress(jkey));
    ROCKSDB_NAMESPACE::Slice key_slice(key + jkey_off[i], jkey_len[i]);
    keys.push_back(key_slice);

    env->DeleteLocalRef(jkey);
  }
  return true;
}

/**
 * cf multi get
 *
 * @return byte[][] of values or nullptr if an
 * exception occurs
 */
jobjectArray multi_get_helper(JNIEnv* env, jobject, APIRocksDB& dbAPI,
                              const ROCKSDB_NAMESPACE::ReadOptions& rOpt,
                              jobjectArray jkeys, jintArray jkey_offs,
                              jintArray jkey_lens,
                              jlongArray jcolumn_family_handles) {
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  if (!cf_handles_from_jcf_handles(env, dbAPI, cf_handles,
                                   jcolumn_family_handles)) {
    return nullptr;
  }

  std::vector<ROCKSDB_NAMESPACE::Slice> keys;
  std::vector<jbyte*> keys_to_free;
  if (!keys_from_jkeys(env, keys, keys_to_free, jkeys, jkey_offs, jkey_lens)) {
    return nullptr;
  }

  std::vector<std::string> values;
  std::vector<ROCKSDB_NAMESPACE::Status> s;
  if (cf_handles.size() == 0) {
    s = dbAPI->MultiGet(rOpt, keys, &values);
  } else {
    s = dbAPI->MultiGet(rOpt, cf_handles, keys, &values);
  }

  // free up allocated byte arrays
  multi_get_helper_release_keys(keys_to_free);

  // prepare the results
  jobjectArray jresults = ROCKSDB_NAMESPACE::ByteJni::new2dByteArray(
      env, static_cast<jsize>(s.size()));
  if (jresults == nullptr) {
    // exception occurred
    jclass exception_cls = (env)->FindClass("java/lang/OutOfMemoryError");
    (env)->ThrowNew(exception_cls, "Insufficient Memory for results.");
    return nullptr;
  }

  // add to the jresults
  for (std::vector<ROCKSDB_NAMESPACE::Status>::size_type i = 0; i != s.size();
       i++) {
    if (s[i].ok()) {
      std::string* value = &values[i];
      const jsize jvalue_len = static_cast<jsize>(value->size());
      jbyteArray jentry_value = env->NewByteArray(jvalue_len);
      if (jentry_value == nullptr) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      env->SetByteArrayRegion(
          jentry_value, 0, static_cast<jsize>(jvalue_len),
          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value->c_str())));
      if (env->ExceptionCheck()) {
        // exception thrown:
        // ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jentry_value);
        return nullptr;
      }

      env->SetObjectArrayElement(jresults, static_cast<jsize>(i), jentry_value);
      if (env->ExceptionCheck()) {
        // exception thrown:
        // ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jentry_value);
        return nullptr;
      }

      env->DeleteLocalRef(jentry_value);
    }
  }

  return jresults;
}

/**
 * cf multi get
 *
 * fill supplied native buffers, or raise JNI
 * exception on a problem
 */

/**
 * @brief multi_get_helper_direct for fast-path multiget (io_uring) on Linux
 *
 * @param env
 * @param db
 * @param rOpt read options
 * @param jcolumn_family_handles 0, 1, or n column family handles
 * @param jkeys
 * @param jkey_offsets
 * @param jkey_lengths
 * @param jvalues byte buffers to receive values
 * @param jvalue_sizes returned actual sizes of data values for keys
 * @param jstatuses returned java RocksDB status values for per key
 */
void multi_get_helper_direct(JNIEnv* env, jobject, APIRocksDB& dbAPI,
                             const ROCKSDB_NAMESPACE::ReadOptions& rOpt,
                             jlongArray jcolumn_family_handles,
                             jobjectArray jkeys, jintArray jkey_offsets,
                             jintArray jkey_lengths, jobjectArray jvalues,
                             jintArray jvalue_sizes, jobjectArray jstatuses) {
  const jsize num_keys = env->GetArrayLength(jkeys);

  std::vector<ROCKSDB_NAMESPACE::Slice> keys;
  if (!keys_from_bytebuffers(env, keys, jkeys, jkey_offsets, jkey_lengths)) {
    return;
  }

  std::vector<ROCKSDB_NAMESPACE::PinnableSlice> values(num_keys);

  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  if (!cf_handles_from_jcf_handles(env, dbAPI, cf_handles,
                                   jcolumn_family_handles)) {
    return;
  }

  std::vector<ROCKSDB_NAMESPACE::Status> s(num_keys);
  if (cf_handles.size() == 0) {
    // we can use the more efficient call here
    auto cf_handle = dbAPI->DefaultColumnFamily();
    dbAPI->MultiGet(rOpt, cf_handle, num_keys, keys.data(), values.data(),
                    s.data());
  } else if (cf_handles.size() == 1) {
    // we can use the more efficient call here
    auto cf_handle = cf_handles[0];
    dbAPI->MultiGet(rOpt, cf_handle, num_keys, keys.data(), values.data(),
                    s.data());
  } else {
    // multiple CFs version
    dbAPI->MultiGet(rOpt, num_keys, cf_handles.data(), keys.data(),
                    values.data(), s.data());
  }

  // prepare the results
  jobjectArray jresults = ROCKSDB_NAMESPACE::ByteJni::new2dByteArray(
      env, static_cast<jsize>(s.size()));
  if (jresults == nullptr) {
    // exception occurred
    jclass exception_cls = (env)->FindClass("java/lang/OutOfMemoryError");
    (env)->ThrowNew(exception_cls, "Insufficient Memory for results.");
    return;
  }

  std::vector<jint> value_size;
  for (int i = 0; i < num_keys; i++) {
    auto jstatus = ROCKSDB_NAMESPACE::StatusJni::construct(env, s[i]);
    if (jstatus == nullptr) {
      // exception in context
      return;
    }
    env->SetObjectArrayElement(jstatuses, i, jstatus);

    if (s[i].ok()) {
      jobject jvalue_bytebuf = env->GetObjectArrayElement(jvalues, i);
      if (env->ExceptionCheck()) {
        // ArrayIndexOutOfBoundsException is thrown
        return;
      }
      jlong jvalue_capacity = env->GetDirectBufferCapacity(jvalue_bytebuf);
      if (jvalue_capacity == -1) {
        ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
            env,
            "Invalid value(s) argument (argument is not a valid direct "
            "ByteBuffer)");
        return;
      }
      void* jvalue_address = env->GetDirectBufferAddress(jvalue_bytebuf);
      if (jvalue_address == nullptr) {
        ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
            env,
            "Invalid value(s) argument (argument is not a valid direct "
            "ByteBuffer)");
        return;
      }

      // record num returned, push back that number, which may be bigger then
      // the ByteBuffer supplied. then copy as much as fits in the ByteBuffer.
      value_size.push_back(static_cast<jint>(values[i].size()));
      auto copy_bytes =
          std::min(static_cast<jlong>(values[i].size()), jvalue_capacity);
      memcpy(jvalue_address, values[i].data(), copy_bytes);
    } else {
      // bad status for this
      value_size.push_back(0);
    }
  }

  env->SetIntArrayRegion(jvalue_sizes, 0, num_keys, value_size.data());
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    multiGet
 * Signature: (J[[B[I[I)[[B
 */
jobjectArray Java_org_rocksdb_RocksDB_multiGet__J_3_3B_3I_3I(
    JNIEnv* env, jobject jdb, jlong jdb_handle, jobjectArray jkeys,
    jintArray jkey_offs, jintArray jkey_lens) {
  return multi_get_helper(env, jdb, *reinterpret_cast<APIRocksDB*>(jdb_handle),
                          ROCKSDB_NAMESPACE::ReadOptions(), jkeys, jkey_offs,
                          jkey_lens, nullptr);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    multiGet
 * Signature: (J[[B[I[I[J)[[B
 */
jobjectArray Java_org_rocksdb_RocksDB_multiGet__J_3_3B_3I_3I_3J(
    JNIEnv* env, jobject jdb, jlong jdb_handle, jobjectArray jkeys,
    jintArray jkey_offs, jintArray jkey_lens,
    jlongArray jcolumn_family_handles) {
  return multi_get_helper(env, jdb, *reinterpret_cast<APIRocksDB*>(jdb_handle),
                          ROCKSDB_NAMESPACE::ReadOptions(), jkeys, jkey_offs,
                          jkey_lens, jcolumn_family_handles);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    multiGet
 * Signature: (JJ[[B[I[I)[[B
 */
jobjectArray Java_org_rocksdb_RocksDB_multiGet__JJ_3_3B_3I_3I(
    JNIEnv* env, jobject jdb, jlong jdb_handle, jlong jropt_handle,
    jobjectArray jkeys, jintArray jkey_offs, jintArray jkey_lens) {
  return multi_get_helper(
      env, jdb, *reinterpret_cast<APIRocksDB*>(jdb_handle),
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jropt_handle), jkeys,
      jkey_offs, jkey_lens, nullptr);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    multiGet
 * Signature: (JJ[[B[I[I[J)[[B
 */
jobjectArray Java_org_rocksdb_RocksDB_multiGet__JJ_3_3B_3I_3I_3J(
    JNIEnv* env, jobject jdb, jlong jdb_handle, jlong jropt_handle,
    jobjectArray jkeys, jintArray jkey_offs, jintArray jkey_lens,
    jlongArray jcolumn_family_handles) {
  return multi_get_helper(
      env, jdb, *reinterpret_cast<APIRocksDB*>(jdb_handle),
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jropt_handle), jkeys,
      jkey_offs, jkey_lens, jcolumn_family_handles);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    multiGet
 * Signature:
 * (JJ[J[Ljava/nio/ByteBuffer;[I[I[Ljava/nio/ByteBuffer;[I[Lorg/rocksdb/Status;)V
 */
void Java_org_rocksdb_RocksDB_multiGet__JJ_3J_3Ljava_nio_ByteBuffer_2_3I_3I_3Ljava_nio_ByteBuffer_2_3I_3Lorg_rocksdb_Status_2(
    JNIEnv* env, jobject jdb, jlong jdb_handle, jlong jropt_handle,
    jlongArray jcolumn_family_handles, jobjectArray jkeys,
    jintArray jkey_offsets, jintArray jkey_lengths, jobjectArray jvalues,
    jintArray jvalues_sizes, jobjectArray jstatus_objects) {
  return multi_get_helper_direct(
      env, jdb, *reinterpret_cast<APIRocksDB*>(jdb_handle),
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jropt_handle),
      jcolumn_family_handles, jkeys, jkey_offsets, jkey_lengths, jvalues,
      jvalues_sizes, jstatus_objects);
}
// private native void
// multiGet(final long dbHandle, final long rOptHandle,
//        final long[] columnFamilyHandles, final ByteBuffer[] keysArray,
//        final ByteBuffer[] valuesArray);

//////////////////////////////////////////////////////////////////////////////
// ROCKSDB_NAMESPACE::DB::KeyMayExist
bool key_may_exist_helper(JNIEnv* env, jlong jdb_handle, jlong jcf_handle,
                          jlong jread_opts_handle, jbyteArray jkey,
                          jint jkey_offset, jint jkey_len, bool* has_exception,
                          std::string* value, bool* value_found) {
  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  const auto cfhPtr =
      APIColumnFamilyHandle::lockCFHOrDefault(env, jcf_handle, dbAPI);
  if (!cfhPtr) {
    // CFH exception
    *has_exception = true;
    return false;
  }

  ROCKSDB_NAMESPACE::ReadOptions read_opts =
      jread_opts_handle == 0
          ? ROCKSDB_NAMESPACE::ReadOptions()
          : *(reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(
                jread_opts_handle));

  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_offset, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    *has_exception = true;
    return false;
  }
  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  const bool exists = dbAPI->KeyMayExist(read_opts, cfhPtr.get(), key_slice,
                                         value, value_found);

  // cleanup
  delete[] key;

  return exists;
}

bool key_may_exist_direct_helper(JNIEnv* env, jlong jdb_handle,
                                 jlong jcf_handle, jlong jread_opts_handle,
                                 jobject jkey, jint jkey_offset, jint jkey_len,
                                 bool* has_exception, std::string* value,
                                 bool* value_found) {
  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = dbAPI->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  ROCKSDB_NAMESPACE::ReadOptions read_opts =
      jread_opts_handle == 0
          ? ROCKSDB_NAMESPACE::ReadOptions()
          : *(reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(
                jread_opts_handle));

  char* key = reinterpret_cast<char*>(env->GetDirectBufferAddress(jkey));
  if (key == nullptr) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env,
        "Invalid key argument (argument is not a valid direct ByteBuffer)");
    *has_exception = true;
    return false;
  }
  if (env->GetDirectBufferCapacity(jkey) < (jkey_offset + jkey_len)) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env,
        "Invalid key argument. Capacity is less than requested region (offset "
        "+ length).");
    *has_exception = true;
    return false;
  }

  ROCKSDB_NAMESPACE::Slice key_slice(key, jkey_len);

  const bool exists =
      dbAPI->KeyMayExist(read_opts, cf_handle, key_slice, value, value_found);

  return exists;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    keyMayExist
 * Signature: (JJJ[BII)Z
 */
jboolean Java_org_rocksdb_RocksDB_keyMayExist(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jlong jread_opts_handle,
    jbyteArray jkey, jint jkey_offset, jint jkey_len) {

  bool has_exception = false;
  std::string value;
  bool value_found = false;

  const bool exists = key_may_exist_helper(
      env, jdb_handle, jcf_handle, jread_opts_handle,
      jkey, jkey_offset, jkey_len,
      &has_exception, &value, &value_found);

  if (has_exception) {
    // java exception already raised
    return false;
  }

  return static_cast<jboolean>(exists);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    keyMayExistDirect
 * Signature: (JJJLjava/nio/ByteBuffer;II)Z
 */
jboolean Java_org_rocksdb_RocksDB_keyMayExistDirect(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jlong jread_opts_handle, jobject jkey, jint jkey_offset, jint jkey_len) {
  bool has_exception = false;
  std::string value;
  bool value_found = false;

  const bool exists = key_may_exist_direct_helper(
      env, jdb_handle, jcf_handle, jread_opts_handle, jkey, jkey_offset,
      jkey_len, &has_exception, &value, &value_found);
  if (has_exception) {
    // java exception already raised
    return false;
  }

  return static_cast<jboolean>(exists);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    keyMayExistDirectFoundValue
 * Signature:
 * (JJJLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;II)[J
 */
jintArray Java_org_rocksdb_RocksDB_keyMayExistDirectFoundValue(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jlong jread_opts_handle, jobject jkey, jint jkey_offset, jint jkey_len,
    jobject jval, jint jval_offset, jint jval_len) {
  char* val_buffer = reinterpret_cast<char*>(env->GetDirectBufferAddress(jval));
  if (val_buffer == nullptr) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env,
        "Invalid value argument (argument is not a valid direct ByteBuffer)");
    return nullptr;
  }

  if (env->GetDirectBufferCapacity(jval) < (jval_offset + jval_len)) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env,
        "Invalid value argument. Capacity is less than requested region "
        "(offset + length).");
    return nullptr;
  }

  bool has_exception = false;
  std::string cvalue;
  bool value_found = false;

  const bool exists = key_may_exist_direct_helper(
      env, jdb_handle, jcf_handle, jread_opts_handle, jkey, jkey_offset,
      jkey_len, &has_exception, &cvalue, &value_found);

  if (has_exception) {
    // java exception already raised
    return nullptr;
  }

  const jint cvalue_len = static_cast<jint>(cvalue.size());
  const jint length = std::min(jval_len, cvalue_len);
  memcpy(val_buffer + jval_offset, cvalue.c_str(), length);

  // keep consistent with java KeyMayExistEnum.values()
  const int kNotExist = 0;
  const int kExistsWithoutValue = 1;
  const int kExistsWithValue = 2;

  // TODO fix return value/type
  // exists/value_found/neither
  // cvalue_len
  jintArray jresult = env->NewIntArray(2);
  const jint jexists =
      exists ? (value_found ? kExistsWithValue : kExistsWithoutValue)
             : kNotExist;

  env->SetIntArrayRegion(jresult, 0, 1, &jexists);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresult);
    return nullptr;
  }
  env->SetIntArrayRegion(jresult, 1, 1, &cvalue_len);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresult);
    return nullptr;
  }

  return jresult;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    keyMayExistFoundValue
 * Signature: (JJJ[BII)[[B
 */
jobjectArray Java_org_rocksdb_RocksDB_keyMayExistFoundValue(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jlong jread_opts_handle, jbyteArray jkey, jint jkey_offset, jint jkey_len) {
  bool has_exception = false;
  std::string value;
  bool value_found = false;

  const bool exists = key_may_exist_helper(
      env, jdb_handle, jcf_handle, jread_opts_handle, jkey, jkey_offset,
      jkey_len, &has_exception, &value, &value_found);

  if (has_exception) {
    // java exception already raised
    return nullptr;
  }

  jbyte result_flags[1];
  if (!exists) {
    result_flags[0] = 0;
  } else if (!value_found) {
    result_flags[0] = 1;
  } else {
    // found
    result_flags[0] = 2;
  }

  jobjectArray jresults = ROCKSDB_NAMESPACE::ByteJni::new2dByteArray(env, 2);
  if (jresults == nullptr) {
    // exception occurred
    return nullptr;
  }

  // prepare the result flag
  jbyteArray jresult_flags = env->NewByteArray(1);
  if (jresult_flags == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  env->SetByteArrayRegion(jresult_flags, 0, 1, result_flags);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresult_flags);
    return nullptr;
  }

  env->SetObjectArrayElement(jresults, 0, jresult_flags);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresult_flags);
    return nullptr;
  }

  env->DeleteLocalRef(jresult_flags);

  if (result_flags[0] == 2) {
    // set the value
    const jsize jvalue_len = static_cast<jsize>(value.size());
    jbyteArray jresult_value = env->NewByteArray(jvalue_len);
    if (jresult_value == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }
    env->SetByteArrayRegion(
        jresult_value, 0, jvalue_len,
        const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value.data())));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jresult_value);
      return nullptr;
    }
    env->SetObjectArrayElement(jresults, 1, jresult_value);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jresult_value);
      return nullptr;
    }

    env->DeleteLocalRef(jresult_value);
  }

  return jresults;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    iterator
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RocksDB_iterator__J(
      JNIEnv*, jobject, jlong db_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(db_handle);
  auto dbIterator =
      dbAPI.newIterator(dbAPI->NewIterator(ROCKSDB_NAMESPACE::ReadOptions()),
                        dbAPI.defaultColumnFamilyHandle);
  return reinterpret_cast<jlong>(dbIterator.release());
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    iterator
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_RocksDB_iterator__JJ(
    JNIEnv*, jobject, jlong db_handle, jlong jread_options_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(db_handle);
  auto& read_options =
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  auto dbIterator = dbAPI.newIterator(dbAPI->NewIterator(read_options),
                                      dbAPI.defaultColumnFamilyHandle);
  return reinterpret_cast<jlong>(dbIterator.release());
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    iteratorCF
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_RocksDB_iteratorCF__JJ(JNIEnv* env, jobject,
                                              jlong db_handle,
                                              jlong jcf_handle) {
  static const int kNotFound = -1;

  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(db_handle);
  auto cfhPtr = APIColumnFamilyHandle::lock(env, jcf_handle);
  if (!cfhPtr) {
    // CFH exception
    return kNotFound;
  }
  auto dbIterator = dbAPI.newIterator(
      dbAPI->NewIterator(ROCKSDB_NAMESPACE::ReadOptions(), cfhPtr.get()),
      cfhPtr);
  return reinterpret_cast<jlong>(dbIterator.release());
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    iteratorCF
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_RocksDB_iteratorCF__JJJ(JNIEnv* env, jobject,
                                               jlong db_handle,
                                               jlong jcf_handle,
                                               jlong jread_options_handle) {
  static const int kNotFound = -1;

  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(db_handle);
  auto cfhPtr = APIColumnFamilyHandle::lock(env, jcf_handle);
  if (!cfhPtr) {
    // CFH exception
    return kNotFound;
  }
  auto& read_options =
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  auto dbIterator =
      dbAPI.newIterator(dbAPI->NewIterator(read_options, cfhPtr.get()), cfhPtr);
  return reinterpret_cast<jlong>(dbIterator.release());
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    iterators
 * Signature: (J[JJ)[J
 */
jlongArray Java_org_rocksdb_RocksDB_iterators(
    JNIEnv* env, jobject, jlong db_handle,
    jlongArray jcolumn_family_handles,
    jlong jread_options_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(db_handle);
  auto& read_options =
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle);

  std::vector<std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>> cfhs;
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  if (jcolumn_family_handles != nullptr) {
    const jsize len_cols = env->GetArrayLength(jcolumn_family_handles);
    jlong* jcfh = env->GetLongArrayElements(jcolumn_family_handles, nullptr);
    if (jcfh == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    for (jsize i = 0; i < len_cols; i++) {
      const auto& cfhPtr = APIColumnFamilyHandle::lock(env, jcfh[i]);
      if (!cfhPtr) {
        // CFH exception
        return nullptr;
      }
      cfhs.push_back(cfhPtr);
      cf_handles.push_back(cfhPtr.get());
    }

    env->ReleaseLongArrayElements(jcolumn_family_handles, jcfh, JNI_ABORT);
  }

  std::vector<ROCKSDB_NAMESPACE::Iterator*> iterators;
  ROCKSDB_NAMESPACE::Status s =
      dbAPI->NewIterators(read_options, cf_handles, &iterators);
  if (s.ok()) {
    jlongArray jLongArray =
        env->NewLongArray(static_cast<jsize>(iterators.size()));
    if (jLongArray == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    std::vector<APIIterator*> itAPIs;
    for (std::vector<ROCKSDB_NAMESPACE::Iterator*>::size_type i = 0;
         i < iterators.size(); i++) {
      std::unique_ptr<APIIterator> itAPI =
          dbAPI.newIterator(iterators[i], cfhs[i]);
      itAPIs.push_back(itAPI.release());
    }
    env->SetLongArrayRegion(jLongArray, 0, static_cast<jsize>(iterators.size()),
                            reinterpret_cast<const jlong*>(itAPIs.data()));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jLongArray);
      return nullptr;
    }

    return jLongArray;
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
}

/*
 * Method:    getSnapshot
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RocksDB_getSnapshot(
    JNIEnv*, jobject, jlong db_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(db_handle);
  const ROCKSDB_NAMESPACE::Snapshot* snapshot = dbAPI->GetSnapshot();
  return GET_CPLUSPLUS_POINTER(snapshot);
}

/*
 * Method:    releaseSnapshot
 * Signature: (JJ)V
 */
void Java_org_rocksdb_RocksDB_releaseSnapshot(
    JNIEnv*, jobject, jlong db_handle,
    jlong snapshot_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(db_handle);
  auto* snapshot =
      reinterpret_cast<ROCKSDB_NAMESPACE::Snapshot*>(snapshot_handle);
  dbAPI->ReleaseSnapshot(snapshot);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getProperty
 * Signature: (JJLjava/lang/String;I)Ljava/lang/String;
 */
jstring Java_org_rocksdb_RocksDB_getProperty(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jstring jproperty, jint jproperty_len) {
  const char* property = env->GetStringUTFChars(jproperty, nullptr);
  if (property == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  ROCKSDB_NAMESPACE::Slice property_name(property, jproperty_len);

  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  const auto& cfhPtr =
      APIColumnFamilyHandle::lockCFHOrDefault(env, jcf_handle, dbAPI);
  if (!cfhPtr) {
    return nullptr;
  }

  std::string property_value;
  bool retCode =
      dbAPI->GetProperty(cfhPtr.get(), property_name, &property_value);
  env->ReleaseStringUTFChars(jproperty, property);

  if (retCode) {
    return env->NewStringUTF(property_value.c_str());
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
      env, ROCKSDB_NAMESPACE::Status::NotFound());
  return nullptr;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getMapProperty
 * Signature: (JJLjava/lang/String;I)Ljava/util/Map;
 */
jobject Java_org_rocksdb_RocksDB_getMapProperty(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jstring jproperty, jint jproperty_len) {
    const char* property = env->GetStringUTFChars(jproperty, nullptr);
  if (property == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  ROCKSDB_NAMESPACE::Slice property_name(property, jproperty_len);

  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto cfhPtr = APIColumnFamilyHandle::lockCFHOrDefault(env, jcf_handle, dbAPI);
  if (!cfhPtr) {
    return nullptr;
  }

  std::map<std::string, std::string> property_value;
  bool retCode =
      dbAPI->GetMapProperty(cfhPtr.get(), property_name, &property_value);
  env->ReleaseStringUTFChars(jproperty, property);

  if (retCode) {
    return ROCKSDB_NAMESPACE::HashMapJni::fromCppMap(env, &property_value);
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
      env, ROCKSDB_NAMESPACE::Status::NotFound());
  return nullptr;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getLongProperty
 * Signature: (JJLjava/lang/String;I)J
 */
jlong Java_org_rocksdb_RocksDB_getLongProperty(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jstring jproperty, jint jproperty_len) {
  const char* property = env->GetStringUTFChars(jproperty, nullptr);
  if (property == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  ROCKSDB_NAMESPACE::Slice property_name(property, jproperty_len);

  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto cfhPtr = APIColumnFamilyHandle::lockCFHOrDefault(env, jcf_handle, dbAPI);
  if (!cfhPtr) {
    return 0;
  }

  uint64_t property_value;
  bool retCode =
      dbAPI->GetIntProperty(cfhPtr.get(), property_name, &property_value);
  env->ReleaseStringUTFChars(jproperty, property);

  if (retCode) {
    return property_value;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
      env, ROCKSDB_NAMESPACE::Status::NotFound());
  return 0;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    resetStats
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_resetStats(
    JNIEnv *, jobject, jlong jdb_handle) {
  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  dbAPI->ResetStats();
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getAggregatedLongProperty
 * Signature: (JLjava/lang/String;I)J
 */
jlong Java_org_rocksdb_RocksDB_getAggregatedLongProperty(
    JNIEnv* env, jobject, jlong db_handle,
    jstring jproperty, jint jproperty_len) {
  const char* property = env->GetStringUTFChars(jproperty, nullptr);
  if (property == nullptr) {
    return 0;
  }
  ROCKSDB_NAMESPACE::Slice property_name(property, jproperty_len);
  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(db_handle);
  uint64_t property_value = 0;
  bool retCode =
      dbAPI->GetAggregatedIntProperty(property_name, &property_value);
  env->ReleaseStringUTFChars(jproperty, property);

  if (retCode) {
    return property_value;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
      env, ROCKSDB_NAMESPACE::Status::NotFound());
  return 0;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getApproximateSizes
 * Signature: (JJ[JB)[J
 */
jlongArray Java_org_rocksdb_RocksDB_getApproximateSizes(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jlongArray jrange_slice_handles, jbyte jinclude_flags) {
  const jsize jlen = env->GetArrayLength(jrange_slice_handles);
  const size_t range_count = jlen / 2;

  jlong* jranges = env->GetLongArrayElements(jrange_slice_handles, nullptr);
  if (jranges == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  auto ranges = std::unique_ptr<ROCKSDB_NAMESPACE::Range[]>(
      new ROCKSDB_NAMESPACE::Range[range_count]);
  size_t range_offset = 0;
  for (jsize i = 0; i < jlen; ++i) {
    auto* start = reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(jranges[i]);
    auto* limit = reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(jranges[++i]);
    ranges.get()[range_offset++] = ROCKSDB_NAMESPACE::Range(*start, *limit);
  }

  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = dbAPI->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }

  auto sizes = std::unique_ptr<uint64_t[]>(new uint64_t[range_count]);

  ROCKSDB_NAMESPACE::DB::SizeApproximationFlags include_flags =
      ROCKSDB_NAMESPACE::DB::SizeApproximationFlags::NONE;
  if (jinclude_flags & 1) {
    include_flags =
        ROCKSDB_NAMESPACE::DB::SizeApproximationFlags::INCLUDE_MEMTABLES;
  }
  if (jinclude_flags & 2) {
    include_flags =
        (include_flags |
         ROCKSDB_NAMESPACE::DB::SizeApproximationFlags::INCLUDE_FILES);
  }

  dbAPI->GetApproximateSizes(cf_handle, ranges.get(),
                          static_cast<int>(range_count), sizes.get(),
                          include_flags);

  // release LongArrayElements
  env->ReleaseLongArrayElements(jrange_slice_handles, jranges, JNI_ABORT);

  // prepare results
  auto results = std::unique_ptr<jlong[]>(new jlong[range_count]);
  for (size_t i = 0; i < range_count; ++i) {
    results.get()[i] = static_cast<jlong>(sizes.get()[i]);
  }

  const jsize jrange_count = jlen / 2;
  jlongArray jresults = env->NewLongArray(jrange_count);
  if (jresults == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  env->SetLongArrayRegion(jresults, 0, jrange_count, results.get());
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresults);
    return nullptr;
  }

  return jresults;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getApproximateMemTableStats
 * Signature: (JJJJ)[J
 */
jlongArray Java_org_rocksdb_RocksDB_getApproximateMemTableStats(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jlong jstartHandle, jlong jlimitHandle) {
  auto* start = reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(jstartHandle);
  auto* limit = reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(jlimitHandle);
  const ROCKSDB_NAMESPACE::Range range(*start, *limit);

  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = dbAPI->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }

  uint64_t count = 0;
  uint64_t sizes = 0;
  dbAPI->GetApproximateMemTableStats(cf_handle, range, &count, &sizes);

  // prepare results
  jlong results[2] = {
      static_cast<jlong>(count),
      static_cast<jlong>(sizes)};

  jlongArray jsizes = env->NewLongArray(2);
  if (jsizes == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  env->SetLongArrayRegion(jsizes, 0, 2, results);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jsizes);
    return nullptr;
  }

  return jsizes;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    compactRange
 * Signature: (J[BI[BIJJ)V
 */
void Java_org_rocksdb_RocksDB_compactRange(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jbegin, jint jbegin_len,
    jbyteArray jend, jint jend_len,
    jlong jcompact_range_opts_handle,
    jlong jcf_handle) {
  jboolean has_exception = JNI_FALSE;

  std::string str_begin;
  if (jbegin_len > 0) {
    str_begin = ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
        env, jbegin, jbegin_len,
        [](const char* str, const size_t len) { return std::string(str, len); },
        &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      return;
    }
  }

  std::string str_end;
  if (jend_len > 0) {
    str_end = ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
        env, jend, jend_len,
        [](const char* str, const size_t len) { return std::string(str, len); },
        &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      return;
    }
  }

  ROCKSDB_NAMESPACE::CompactRangeOptions* compact_range_opts = nullptr;
  if (jcompact_range_opts_handle == 0) {
    // NOTE: we DO own the pointer!
    compact_range_opts = new ROCKSDB_NAMESPACE::CompactRangeOptions();
  } else {
    // NOTE: we do NOT own the pointer!
    compact_range_opts =
        reinterpret_cast<ROCKSDB_NAMESPACE::CompactRangeOptions*>(
            jcompact_range_opts_handle);
  }

  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);

  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = dbAPI->DefaultColumnFamily();
  } else {
    const auto& cfhPtr = APIColumnFamilyHandle::lock(env, jcf_handle);
    if (!cfhPtr) {
      // CFH exception
      return;
    }
    cf_handle = cfhPtr.get();
  }

  ROCKSDB_NAMESPACE::Status s;
  if (jbegin_len > 0 || jend_len > 0) {
    const ROCKSDB_NAMESPACE::Slice begin(str_begin);
    const ROCKSDB_NAMESPACE::Slice end(str_end);
    s = dbAPI->CompactRange(*compact_range_opts, cf_handle, &begin, &end);
  } else {
    s = dbAPI->CompactRange(*compact_range_opts, cf_handle, nullptr, nullptr);
  }

  if (jcompact_range_opts_handle == 0) {
    delete compact_range_opts;
  }
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    setOptions
 * Signature: (JJ[Ljava/lang/String;[Ljava/lang/String;)V
 */
void Java_org_rocksdb_RocksDB_setOptions(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jobjectArray jkeys, jobjectArray jvalues) {
  const jsize len = env->GetArrayLength(jkeys);
  assert(len == env->GetArrayLength(jvalues));

  std::unordered_map<std::string, std::string> options_map;
  for (jsize i = 0; i < len; i++) {
    jobject jobj_key = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      return;
    }

    jobject jobj_value = env->GetObjectArrayElement(jvalues, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jobj_key);
      return;
    }

    jboolean has_exception = JNI_FALSE;
    std::string s_key = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
        env, reinterpret_cast<jstring>(jobj_key), &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      env->DeleteLocalRef(jobj_value);
      env->DeleteLocalRef(jobj_key);
      return;
    }

    std::string s_value = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
        env, reinterpret_cast<jstring>(jobj_value), &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      env->DeleteLocalRef(jobj_value);
      env->DeleteLocalRef(jobj_key);
      return;
    }

    options_map[s_key] = s_value;

    env->DeleteLocalRef(jobj_key);
    env->DeleteLocalRef(jobj_value);
  }

  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle == nullptr) {
    cf_handle = dbAPI->DefaultColumnFamily();
  }
  auto s = dbAPI->SetOptions(cf_handle, options_map);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    setDBOptions
 * Signature: (J[Ljava/lang/String;[Ljava/lang/String;)V
 */
void Java_org_rocksdb_RocksDB_setDBOptions(
    JNIEnv* env, jobject, jlong jdb_handle,
    jobjectArray jkeys, jobjectArray jvalues) {
  const jsize len = env->GetArrayLength(jkeys);
  assert(len == env->GetArrayLength(jvalues));

  std::unordered_map<std::string, std::string> options_map;
    for (jsize i = 0; i < len; i++) {
    jobject jobj_key = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      return;
    }

    jobject jobj_value = env->GetObjectArrayElement(jvalues, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jobj_key);
      return;
    }

    jboolean has_exception = JNI_FALSE;
    std::string s_key = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
        env, reinterpret_cast<jstring>(jobj_key), &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      env->DeleteLocalRef(jobj_value);
      env->DeleteLocalRef(jobj_key);
      return;
    }

    std::string s_value = ROCKSDB_NAMESPACE::JniUtil::copyStdString(
        env, reinterpret_cast<jstring>(jobj_value), &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      env->DeleteLocalRef(jobj_value);
      env->DeleteLocalRef(jobj_key);
      return;
    }

    options_map[s_key] = s_value;

    env->DeleteLocalRef(jobj_key);
    env->DeleteLocalRef(jobj_value);
  }

  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto s = dbAPI->SetDBOptions(options_map);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getOptions
 * Signature: (JJ)Ljava/lang/String;
 */
jstring Java_org_rocksdb_RocksDB_getOptions(JNIEnv* env, jobject,
                                            jlong jdb_handle,
                                            jlong jcf_handle) {
  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  const auto& cfhPtr =
      APIColumnFamilyHandle::lockCFHOrDefault(env, jcf_handle, dbAPI);
  if (!cfhPtr) {
    // exception raised
    return nullptr;
  }

  auto options = dbAPI->GetOptions(cfhPtr.get());
  std::string options_as_string;
  ROCKSDB_NAMESPACE::Status s =
      GetStringFromColumnFamilyOptions(&options_as_string, options);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
  return env->NewStringUTF(options_as_string.c_str());
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getDBOptions
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_RocksDB_getDBOptions(JNIEnv* env, jobject,
                                              jlong jdb_handle) {
  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);

  auto options = dbAPI->GetDBOptions();
  std::string options_as_string;
  ROCKSDB_NAMESPACE::Status s =
      GetStringFromDBOptions(&options_as_string, options);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
  return env->NewStringUTF(options_as_string.c_str());
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    compactFiles
 * Signature: (JJJ[Ljava/lang/String;IIJ)[Ljava/lang/String;
 */
jobjectArray Java_org_rocksdb_RocksDB_compactFiles(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcompaction_opts_handle,
    jlong jcf_handle, jobjectArray jinput_file_names, jint joutput_level,
    jint joutput_path_id, jlong jcompaction_job_info_handle) {
  jboolean has_exception = JNI_FALSE;
  const std::vector<std::string> input_file_names =
      ROCKSDB_NAMESPACE::JniUtil::copyStrings(env, jinput_file_names,
                                              &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return nullptr;
  }

  auto* compaction_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptions*>(
          jcompaction_opts_handle);
  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = dbAPI->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }

  ROCKSDB_NAMESPACE::CompactionJobInfo* compaction_job_info = nullptr;
  if (jcompaction_job_info_handle != 0) {
    compaction_job_info =
        reinterpret_cast<ROCKSDB_NAMESPACE::CompactionJobInfo*>(
            jcompaction_job_info_handle);
  }

  std::vector<std::string> output_file_names;
  auto s = dbAPI->CompactFiles(*compaction_opts, cf_handle, input_file_names,
                               static_cast<int>(joutput_level),
                               static_cast<int>(joutput_path_id),
                               &output_file_names, compaction_job_info);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  return ROCKSDB_NAMESPACE::JniUtil::toJavaStrings(env, &output_file_names);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    cancelAllBackgroundWork
 * Signature: (JZ)V
 */
void Java_org_rocksdb_RocksDB_cancelAllBackgroundWork(
        JNIEnv*, jobject, jlong jdb_handle, jboolean jwait) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::CancelAllBackgroundWork(dbAPI.get(), jwait);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    pauseBackgroundWork
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_pauseBackgroundWork(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto s = dbAPI->PauseBackgroundWork();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    continueBackgroundWork
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_continueBackgroundWork(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto s = dbAPI->ContinueBackgroundWork();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    enableAutoCompaction
 * Signature: (J[J)V
 */
void Java_org_rocksdb_RocksDB_enableAutoCompaction(
    JNIEnv* env, jobject, jlong jdb_handle, jlongArray jcf_handles) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  jboolean has_exception = JNI_FALSE;
  const std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles =
      ROCKSDB_NAMESPACE::JniUtil::fromJPointers<
          ROCKSDB_NAMESPACE::ColumnFamilyHandle>(env, jcf_handles,
                                                 &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }
  dbAPI->EnableAutoCompaction(cf_handles);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    numberLevels
 * Signature: (JJ)I
 */
jint Java_org_rocksdb_RocksDB_numberLevels(
    JNIEnv*, jobject, jlong jdb_handle, jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = dbAPI->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  return static_cast<jint>(dbAPI->NumberLevels(cf_handle));
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    maxMemCompactionLevel
 * Signature: (JJ)I
 */
jint Java_org_rocksdb_RocksDB_maxMemCompactionLevel(
    JNIEnv*, jobject, jlong jdb_handle, jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = dbAPI->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  return static_cast<jint>(dbAPI->MaxMemCompactionLevel(cf_handle));
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    level0StopWriteTrigger
 * Signature: (JJ)I
 */
jint Java_org_rocksdb_RocksDB_level0StopWriteTrigger(
    JNIEnv*, jobject, jlong jdb_handle, jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = dbAPI->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  return static_cast<jint>(dbAPI->Level0StopWriteTrigger(cf_handle));
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getName
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_RocksDB_getName(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  std::string name = dbAPI->GetName();
  return ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &name, false);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getEnv
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RocksDB_getEnv(
    JNIEnv*, jobject, jlong jdb_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  return GET_CPLUSPLUS_POINTER(dbAPI->GetEnv());
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    flush
 * Signature: (JJ[J)V
 */
void Java_org_rocksdb_RocksDB_flush(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jflush_opts_handle,
    jlongArray jcf_handles) {
  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* flush_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::FlushOptions*>(jflush_opts_handle);
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  if (jcf_handles == nullptr) {
    cf_handles.push_back(dbAPI->DefaultColumnFamily());
  } else {
    jboolean has_exception = JNI_FALSE;
    std::vector<APIColumnFamilyHandle*> cfhAPIs =
        ROCKSDB_NAMESPACE::JniUtil::fromJPointers<APIColumnFamilyHandle>(
            env, jcf_handles, &has_exception);
    if (has_exception) {
      // exception occurred
      return;
    }
    for (auto& cfhAPI : cfhAPIs) {
      auto cfhPtr = cfhAPI->cfhLock(env);
      if (!cfhPtr) {
        // CFH expired - exception raised
        return;
      }
      cf_handles.push_back(cfhPtr.get());
    }
  }
  auto s = dbAPI->Flush(*flush_opts, cf_handles);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    flushWal
 * Signature: (JZ)V
 */
void Java_org_rocksdb_RocksDB_flushWal(
    JNIEnv* env, jobject, jlong jdb_handle, jboolean jsync) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto s = dbAPI->FlushWAL(jsync == JNI_TRUE);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    syncWal
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_syncWal(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto s = dbAPI->SyncWAL();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getLatestSequenceNumber
 * Signature: (J)V
 */
jlong Java_org_rocksdb_RocksDB_getLatestSequenceNumber(
    JNIEnv*, jobject, jlong jdb_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  return dbAPI->GetLatestSequenceNumber();
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    disableFileDeletions
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_disableFileDeletions(JNIEnv* env, jobject,
                                                   jlong jdb_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::Status s = dbAPI->DisableFileDeletions();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    enableFileDeletions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_RocksDB_enableFileDeletions(JNIEnv* env, jobject,
                                                  jlong jdb_handle,
                                                  jboolean jforce) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::Status s = dbAPI->EnableFileDeletions(jforce);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getLiveFiles
 * Signature: (JZ)[Ljava/lang/String;
 */
jobjectArray Java_org_rocksdb_RocksDB_getLiveFiles(JNIEnv* env, jobject,
                                                   jlong jdb_handle,
                                                   jboolean jflush_memtable) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  auto s = dbAPI->GetLiveFiles(live_files, &manifest_file_size,
                               jflush_memtable == JNI_TRUE);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  // append the manifest_file_size to the vector
  // for passing back to java
  live_files.push_back(std::to_string(manifest_file_size));

  return ROCKSDB_NAMESPACE::JniUtil::toJavaStrings(env, &live_files);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getSortedWalFiles
 * Signature: (J)[Lorg/rocksdb/LogFile;
 */
jobjectArray Java_org_rocksdb_RocksDB_getSortedWalFiles(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  std::vector<std::unique_ptr<ROCKSDB_NAMESPACE::LogFile>> sorted_wal_files;
  auto s = dbAPI->GetSortedWalFiles(sorted_wal_files);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  // convert to Java type
  const jsize jlen = static_cast<jsize>(sorted_wal_files.size());
  jobjectArray jsorted_wal_files = env->NewObjectArray(
      jlen, ROCKSDB_NAMESPACE::LogFileJni::getJClass(env), nullptr);
  if(jsorted_wal_files == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  jsize i = 0;
  for (auto it = sorted_wal_files.begin(); it != sorted_wal_files.end(); ++it) {
    jobject jlog_file =
        ROCKSDB_NAMESPACE::LogFileJni::fromCppLogFile(env, it->get());
    if (jlog_file == nullptr) {
      // exception occurred
      env->DeleteLocalRef(jsorted_wal_files);
      return nullptr;
    }

    env->SetObjectArrayElement(jsorted_wal_files, i++, jlog_file);
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(jlog_file);
      env->DeleteLocalRef(jsorted_wal_files);
      return nullptr;
    }

    env->DeleteLocalRef(jlog_file);
  }

  return jsorted_wal_files;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getUpdatesSince
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_RocksDB_getUpdatesSince(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jsequence_number) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::SequenceNumber sequence_number =
      static_cast<ROCKSDB_NAMESPACE::SequenceNumber>(jsequence_number);
  std::unique_ptr<ROCKSDB_NAMESPACE::TransactionLogIterator> iter;
  ROCKSDB_NAMESPACE::Status s = dbAPI->GetUpdatesSince(sequence_number, &iter);
  if (s.ok()) {
    return GET_CPLUSPLUS_POINTER(iter.release());
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  return 0;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteFile
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_RocksDB_deleteFile(
    JNIEnv* env, jobject, jlong jdb_handle, jstring jname) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  jboolean has_exception = JNI_FALSE;
  std::string name =
      ROCKSDB_NAMESPACE::JniUtil::copyStdString(env, jname, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }
  dbAPI->DeleteFile(name);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getLiveFilesMetaData
 * Signature: (J)[Lorg/rocksdb/LiveFileMetaData;
 */
jobjectArray Java_org_rocksdb_RocksDB_getLiveFilesMetaData(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  std::vector<ROCKSDB_NAMESPACE::LiveFileMetaData> live_files_meta_data;
  dbAPI->GetLiveFilesMetaData(&live_files_meta_data);

  // convert to Java type
  const jsize jlen = static_cast<jsize>(live_files_meta_data.size());
  jobjectArray jlive_files_meta_data = env->NewObjectArray(
      jlen, ROCKSDB_NAMESPACE::LiveFileMetaDataJni::getJClass(env), nullptr);
  if(jlive_files_meta_data == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  jsize i = 0;
  for (auto it = live_files_meta_data.begin(); it != live_files_meta_data.end(); ++it) {
    jobject jlive_file_meta_data =
        ROCKSDB_NAMESPACE::LiveFileMetaDataJni::fromCppLiveFileMetaData(env,
                                                                        &(*it));
    if (jlive_file_meta_data == nullptr) {
      // exception occurred
      env->DeleteLocalRef(jlive_files_meta_data);
      return nullptr;
    }

    env->SetObjectArrayElement(jlive_files_meta_data, i++, jlive_file_meta_data);
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(jlive_file_meta_data);
      env->DeleteLocalRef(jlive_files_meta_data);
      return nullptr;
    }

    env->DeleteLocalRef(jlive_file_meta_data);
  }

  return jlive_files_meta_data;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getColumnFamilyMetaData
 * Signature: (JJ)Lorg/rocksdb/ColumnFamilyMetaData;
 */
jobject Java_org_rocksdb_RocksDB_getColumnFamilyMetaData(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = dbAPI->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  ROCKSDB_NAMESPACE::ColumnFamilyMetaData cf_metadata;
  dbAPI->GetColumnFamilyMetaData(cf_handle, &cf_metadata);
  return ROCKSDB_NAMESPACE::ColumnFamilyMetaDataJni::
      fromCppColumnFamilyMetaData(env, &cf_metadata);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    ingestExternalFile
 * Signature: (JJ[Ljava/lang/String;IJ)V
 */
void Java_org_rocksdb_RocksDB_ingestExternalFile(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jobjectArray jfile_path_list, jint jfile_path_list_len,
    jlong jingest_external_file_options_handle) {
  jboolean has_exception = JNI_FALSE;
  std::vector<std::string> file_path_list =
      ROCKSDB_NAMESPACE::JniUtil::copyStrings(
          env, jfile_path_list, jfile_path_list_len, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }

  const auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  const auto& cfhPtr = APIColumnFamilyHandle::lock(env, jcf_handle);
  if (!cfhPtr) {
    // CFH exception
    return;
  }
  auto* ifo = reinterpret_cast<ROCKSDB_NAMESPACE::IngestExternalFileOptions*>(
      jingest_external_file_options_handle);
  ROCKSDB_NAMESPACE::Status s =
      dbAPI->IngestExternalFile(cfhPtr.get(), file_path_list, *ifo);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    verifyChecksum
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_verifyChecksum(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto s = dbAPI->VerifyChecksum();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getDefaultColumnFamily
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RocksDB_getDefaultColumnFamily(
    JNIEnv*, jobject, jlong jdb_handle) {
  auto* db_handle = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(jdb_handle);
  auto* cf_handle = db_handle->DefaultColumnFamily();
  return GET_CPLUSPLUS_POINTER(cf_handle);
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  return reinterpret_cast<jlong>(
      new APIColumnFamilyHandle(dbAPI.db, dbAPI.defaultColumnFamilyHandle));
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getPropertiesOfAllTables
 * Signature: (JJ)Ljava/util/Map;
 */
jobject Java_org_rocksdb_RocksDB_getPropertiesOfAllTables(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = dbAPI->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  ROCKSDB_NAMESPACE::TablePropertiesCollection table_properties_collection;
  auto s =
      dbAPI->GetPropertiesOfAllTables(cf_handle, &table_properties_collection);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }

  // convert to Java type
  jobject jhash_map = ROCKSDB_NAMESPACE::HashMapJni::construct(
      env, static_cast<uint32_t>(table_properties_collection.size()));
  if (jhash_map == nullptr) {
    // exception occurred
    return nullptr;
  }

  const ROCKSDB_NAMESPACE::HashMapJni::FnMapKV<
      const std::string,
      const std::shared_ptr<const ROCKSDB_NAMESPACE::TableProperties>, jobject,
      jobject>
      fn_map_kv =
          [env](const std::pair<const std::string,
                                const std::shared_ptr<
                                    const ROCKSDB_NAMESPACE::TableProperties>>&
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
            if (jtable_properties == nullptr) {
              // an error occurred
              env->DeleteLocalRef(jkey);
              return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
            }

            return std::unique_ptr<std::pair<jobject, jobject>>(
                new std::pair<jobject, jobject>(
                    static_cast<jobject>(jkey),
                    static_cast<jobject>(jtable_properties)));
          };

  if (!ROCKSDB_NAMESPACE::HashMapJni::putAll(
          env, jhash_map, table_properties_collection.begin(),
          table_properties_collection.end(), fn_map_kv)) {
    // exception occurred
    return nullptr;
  }

  return jhash_map;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getPropertiesOfTablesInRange
 * Signature: (JJ[J)Ljava/util/Map;
 */
jobject Java_org_rocksdb_RocksDB_getPropertiesOfTablesInRange(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jlongArray jrange_slice_handles) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = dbAPI->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  const jsize jlen = env->GetArrayLength(jrange_slice_handles);
  jlong* jrange_slice_handle =
      env->GetLongArrayElements(jrange_slice_handles, nullptr);
  if (jrange_slice_handle == nullptr) {
    // exception occurred
    return nullptr;
  }

  const size_t ranges_len = static_cast<size_t>(jlen / 2);
  auto ranges = std::unique_ptr<ROCKSDB_NAMESPACE::Range[]>(
      new ROCKSDB_NAMESPACE::Range[ranges_len]);
  for (jsize i = 0, j = 0; i < jlen; ++i) {
    auto* start =
        reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(jrange_slice_handle[i]);
    auto* limit =
        reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(jrange_slice_handle[++i]);
    ranges[j++] = ROCKSDB_NAMESPACE::Range(*start, *limit);
  }

  ROCKSDB_NAMESPACE::TablePropertiesCollection table_properties_collection;
  auto s = dbAPI->GetPropertiesOfTablesInRange(
      cf_handle, ranges.get(), ranges_len, &table_properties_collection);
  if (!s.ok()) {
    // error occurred
    env->ReleaseLongArrayElements(jrange_slice_handles, jrange_slice_handle, JNI_ABORT);
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  // cleanup
  env->ReleaseLongArrayElements(jrange_slice_handles, jrange_slice_handle, JNI_ABORT);

  return jrange_slice_handles;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    suggestCompactRange
 * Signature: (JJ)[J
 */
jlongArray Java_org_rocksdb_RocksDB_suggestCompactRange(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = dbAPI->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  auto* begin = new ROCKSDB_NAMESPACE::Slice();
  auto* end = new ROCKSDB_NAMESPACE::Slice();
  auto s = dbAPI->SuggestCompactRange(cf_handle, begin, end);
  if (!s.ok()) {
    // error occurred
    delete begin;
    delete end;
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  jlongArray jslice_handles = env->NewLongArray(2);
  if (jslice_handles == nullptr) {
    // exception thrown: OutOfMemoryError
    delete begin;
    delete end;
    return nullptr;
  }

  jlong slice_handles[2];
  slice_handles[0] = GET_CPLUSPLUS_POINTER(begin);
  slice_handles[1] = GET_CPLUSPLUS_POINTER(end);
  env->SetLongArrayRegion(jslice_handles, 0, 2, slice_handles);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete begin;
    delete end;
    env->DeleteLocalRef(jslice_handles);
    return nullptr;
  }

  return jslice_handles;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    promoteL0
 * Signature: (JJI)V
 */
void Java_org_rocksdb_RocksDB_promoteL0(
    JNIEnv*, jobject, jlong jdb_handle, jlong jcf_handle, jint jtarget_level) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = dbAPI->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  dbAPI->PromoteL0(cf_handle, static_cast<int>(jtarget_level));
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    startTrace
 * Signature: (JJJ)V
 */
void Java_org_rocksdb_RocksDB_startTrace(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jmax_trace_file_size,
    jlong jtrace_writer_jnicallback_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  ROCKSDB_NAMESPACE::TraceOptions trace_options;
  trace_options.max_trace_file_size =
      static_cast<uint64_t>(jmax_trace_file_size);
  // transfer ownership of trace writer from Java to C++
  auto trace_writer =
      std::unique_ptr<ROCKSDB_NAMESPACE::TraceWriterJniCallback>(
          reinterpret_cast<ROCKSDB_NAMESPACE::TraceWriterJniCallback*>(
              jtrace_writer_jnicallback_handle));
  auto s = dbAPI->StartTrace(trace_options, std::move(trace_writer));
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    endTrace
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_endTrace(JNIEnv* env, jobject, jlong jdb_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto s = dbAPI->EndTrace();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    tryCatchUpWithPrimary
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_tryCatchUpWithPrimary(JNIEnv* env, jobject,
                                                    jlong jdb_handle) {
  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto s = dbAPI->TryCatchUpWithPrimary();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    destroyDB
 * Signature: (Ljava/lang/String;J)V
 */
void Java_org_rocksdb_RocksDB_destroyDB(
    JNIEnv* env, jclass, jstring jdb_path, jlong joptions_handle) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(joptions_handle);
  if (options == nullptr) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::Status::InvalidArgument("Invalid Options."));
  }

  ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::DestroyDB(db_path, *options);
  env->ReleaseStringUTFChars(jdb_path, db_path);

  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

bool get_slice_helper(JNIEnv* env, jobjectArray ranges, jsize index,
                      std::unique_ptr<ROCKSDB_NAMESPACE::Slice>& slice,
                      std::vector<std::unique_ptr<jbyte[]>>& ranges_to_free) {
  jobject jArray = env->GetObjectArrayElement(ranges, index);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    return false;
  }

  if (jArray == nullptr) {
    return true;
  }

  jbyteArray jba = reinterpret_cast<jbyteArray>(jArray);
  jsize len_ba = env->GetArrayLength(jba);
  ranges_to_free.push_back(std::unique_ptr<jbyte[]>(new jbyte[len_ba]));
  env->GetByteArrayRegion(jba, 0, len_ba, ranges_to_free.back().get());
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jArray);
    return false;
  }
  env->DeleteLocalRef(jArray);
  slice.reset(new ROCKSDB_NAMESPACE::Slice(
      reinterpret_cast<char*>(ranges_to_free.back().get()), len_ba));
  return true;
}
/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteFilesInRanges
 * Signature: (JJLjava/util/List;Z)V
 */
void Java_org_rocksdb_RocksDB_deleteFilesInRanges(JNIEnv* env, jobject /*jdb*/,
                                                  jlong jdb_handle,
                                                  jlong jcf_handle,
                                                  jobjectArray ranges,
                                                  jboolean include_end) {
  jsize length = env->GetArrayLength(ranges);

  std::vector<ROCKSDB_NAMESPACE::RangePtr> rangesVector;
  std::vector<std::unique_ptr<ROCKSDB_NAMESPACE::Slice>> slices;
  std::vector<std::unique_ptr<jbyte[]>> ranges_to_free;
  for (jsize i = 0; (i + 1) < length; i += 2) {
    slices.push_back(std::unique_ptr<ROCKSDB_NAMESPACE::Slice>());
    if (!get_slice_helper(env, ranges, i, slices.back(), ranges_to_free)) {
      // exception thrown
      return;
    }

    slices.push_back(std::unique_ptr<ROCKSDB_NAMESPACE::Slice>());
    if (!get_slice_helper(env, ranges, i + 1, slices.back(), ranges_to_free)) {
      // exception thrown
      return;
    }

    rangesVector.push_back(ROCKSDB_NAMESPACE::RangePtr(
        slices[slices.size() - 2].get(), slices[slices.size() - 1].get()));
  }

  auto& dbAPI = *reinterpret_cast<APIRocksDB*>(jdb_handle);
  auto* column_family =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);

  ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::DeleteFilesInRanges(
      dbAPI.get(),
      column_family == nullptr ? dbAPI->DefaultColumnFamily() : column_family,
      rangesVector.data(), rangesVector.size(), include_end);

  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    createWeakDB
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RocksDB_createWeakDB(JNIEnv* env, jobject,
                                            jlong handle) {
  auto* dbAPI = reinterpret_cast<APIRocksDB*>(handle);
  std::unique_ptr<APIWeakDB> weakDBAPI(new APIWeakDB(dbAPI->db));
  return reinterpret_cast<jlong>(weakDBAPI.release());
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
      env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::InvalidColumnFamily());
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    version
 * Signature: ()I
 */
jint Java_org_rocksdb_RocksDB_version(JNIEnv*, jclass) {
  uint32_t encodedVersion = (ROCKSDB_MAJOR & 0xff) << 16;
  encodedVersion |= (ROCKSDB_MINOR & 0xff) << 8;
  encodedVersion |= (ROCKSDB_PATCH & 0xff);
  return static_cast<jint>(encodedVersion);
}
