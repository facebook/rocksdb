// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::DB methods from Java side.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "include/org_rocksdb_RocksDB.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "rocksjni/portal.h"

#ifdef min
#undef min
#endif

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::Open
jlong rocksdb_open_helper(
    JNIEnv* env, jlong jopt_handle, jstring jdb_path,
    std::function<rocksdb::Status(const rocksdb::Options&, const std::string&,
                                  rocksdb::DB**)>
        open_fn) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* opt = reinterpret_cast<rocksdb::Options*>(jopt_handle);
  rocksdb::DB* db = nullptr;
  rocksdb::Status s = open_fn(*opt, db_path, &db);

  env->ReleaseStringUTFChars(jdb_path, db_path);

  if (s.ok()) {
    return reinterpret_cast<jlong>(db);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
    return 0;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    open
 * Signature: (JLjava/lang/String;)J
 */
jlong Java_org_rocksdb_RocksDB_open__JLjava_lang_String_2(JNIEnv* env,
                                                          jclass /*jcls*/,
                                                          jlong jopt_handle,
                                                          jstring jdb_path) {
  return rocksdb_open_helper(
      env, jopt_handle, jdb_path,
      (rocksdb::Status(*)(const rocksdb::Options&, const std::string&,
                          rocksdb::DB**)) &
          rocksdb::DB::Open);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    openROnly
 * Signature: (JLjava/lang/String;)J
 */
jlong Java_org_rocksdb_RocksDB_openROnly__JLjava_lang_String_2(
    JNIEnv* env, jclass /*jcls*/, jlong jopt_handle, jstring jdb_path) {
  return rocksdb_open_helper(env, jopt_handle, jdb_path,
                             [](const rocksdb::Options& options,
                                const std::string& db_path, rocksdb::DB** db) {
                               return rocksdb::DB::OpenForReadOnly(options,
                                                                   db_path, db);
                             });
}

jlongArray rocksdb_open_helper(
    JNIEnv* env, jlong jopt_handle, jstring jdb_path,
    jobjectArray jcolumn_names, jlongArray jcolumn_options,
    std::function<rocksdb::Status(
        const rocksdb::DBOptions&, const std::string&,
        const std::vector<rocksdb::ColumnFamilyDescriptor>&,
        std::vector<rocksdb::ColumnFamilyHandle*>*, rocksdb::DB**)>
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

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  jboolean has_exception = JNI_FALSE;
  rocksdb::JniUtil::byteStrings<std::string>(
      env, jcolumn_names,
      [](const char* str_data, const size_t str_len) {
        return std::string(str_data, str_len);
      },
      [&jco, &column_families](size_t idx, std::string cf_name) {
        rocksdb::ColumnFamilyOptions* cf_options =
            reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jco[idx]);
        column_families.push_back(
            rocksdb::ColumnFamilyDescriptor(cf_name, *cf_options));
      },
      &has_exception);

  env->ReleaseLongArrayElements(jcolumn_options, jco, JNI_ABORT);

  if (has_exception == JNI_TRUE) {
    // exception occurred
    env->ReleaseStringUTFChars(jdb_path, db_path);
    return nullptr;
  }

  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jopt_handle);
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::DB* db = nullptr;
  rocksdb::Status s = open_fn(*opt, db_path, column_families, &handles, &db);

  // we have now finished with db_path
  env->ReleaseStringUTFChars(jdb_path, db_path);

  // check if open operation was successful
  if (s.ok()) {
    const jsize resultsLen = 1 + len_cols;  // db handle + column family handles
    std::unique_ptr<jlong[]> results =
        std::unique_ptr<jlong[]>(new jlong[resultsLen]);
    results[0] = reinterpret_cast<jlong>(db);
    for (int i = 1; i <= len_cols; i++) {
      results[i] = reinterpret_cast<jlong>(handles[i - 1]);
    }

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
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    openROnly
 * Signature: (JLjava/lang/String;[[B[J)[J
 */
jlongArray Java_org_rocksdb_RocksDB_openROnly__JLjava_lang_String_2_3_3B_3J(
    JNIEnv* env, jclass /*jcls*/, jlong jopt_handle, jstring jdb_path,
    jobjectArray jcolumn_names, jlongArray jcolumn_options) {
  return rocksdb_open_helper(
      env, jopt_handle, jdb_path, jcolumn_names, jcolumn_options,
      [](const rocksdb::DBOptions& options, const std::string& db_path,
         const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
         std::vector<rocksdb::ColumnFamilyHandle*>* handles, rocksdb::DB** db) {
        return rocksdb::DB::OpenForReadOnly(options, db_path, column_families,
                                            handles, db);
      });
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    open
 * Signature: (JLjava/lang/String;[[B[J)[J
 */
jlongArray Java_org_rocksdb_RocksDB_open__JLjava_lang_String_2_3_3B_3J(
    JNIEnv* env, jclass /*jcls*/, jlong jopt_handle, jstring jdb_path,
    jobjectArray jcolumn_names, jlongArray jcolumn_options) {
  return rocksdb_open_helper(
      env, jopt_handle, jdb_path, jcolumn_names, jcolumn_options,
      (rocksdb::Status(*)(const rocksdb::DBOptions&, const std::string&,
                          const std::vector<rocksdb::ColumnFamilyDescriptor>&,
                          std::vector<rocksdb::ColumnFamilyHandle*>*,
                          rocksdb::DB**)) &
          rocksdb::DB::Open);
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::ListColumnFamilies

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    listColumnFamilies
 * Signature: (JLjava/lang/String;)[[B
 */
jobjectArray Java_org_rocksdb_RocksDB_listColumnFamilies(JNIEnv* env,
                                                         jclass /*jclazz*/,
                                                         jlong jopt_handle,
                                                         jstring jdb_path) {
  std::vector<std::string> column_family_names;
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  auto* opt = reinterpret_cast<rocksdb::Options*>(jopt_handle);
  rocksdb::Status s =
      rocksdb::DB::ListColumnFamilies(*opt, db_path, &column_family_names);

  env->ReleaseStringUTFChars(jdb_path, db_path);

  jobjectArray jcolumn_family_names =
      rocksdb::JniUtil::stringsBytes(env, column_family_names);

  return jcolumn_family_names;
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::Put

/**
 * @return true if the put succeeded, false if a Java Exception was thrown
 */
bool rocksdb_put_helper(JNIEnv* env, rocksdb::DB* db,
                        const rocksdb::WriteOptions& write_options,
                        rocksdb::ColumnFamilyHandle* cf_handle, jbyteArray jkey,
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

  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
  rocksdb::Slice value_slice(reinterpret_cast<char*>(value), jval_len);

  rocksdb::Status s;
  if (cf_handle != nullptr) {
    s = db->Put(write_options, cf_handle, key_slice, value_slice);
  } else {
    // backwards compatibility
    s = db->Put(write_options, key_slice, value_slice);
  }

  // cleanup
  delete[] value;
  delete[] key;

  if (s.ok()) {
    return true;
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
    return false;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    put
 * Signature: (J[BII[BII)V
 */
void Java_org_rocksdb_RocksDB_put__J_3BII_3BII(JNIEnv* env, jobject /*jdb*/,
                                               jlong jdb_handle,
                                               jbyteArray jkey, jint jkey_off,
                                               jint jkey_len, jbyteArray jval,
                                               jint jval_off, jint jval_len) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options =
      rocksdb::WriteOptions();

  rocksdb_put_helper(env, db, default_write_options, nullptr, jkey, jkey_off,
                     jkey_len, jval, jval_off, jval_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    put
 * Signature: (J[BII[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_put__J_3BII_3BIIJ(JNIEnv* env, jobject /*jdb*/,
                                                jlong jdb_handle,
                                                jbyteArray jkey, jint jkey_off,
                                                jint jkey_len, jbyteArray jval,
                                                jint jval_off, jint jval_len,
                                                jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options =
      rocksdb::WriteOptions();
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_put_helper(env, db, default_write_options, cf_handle, jkey,
                       jkey_off, jkey_len, jval, jval_off, jval_len);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    put
 * Signature: (JJ[BII[BII)V
 */
void Java_org_rocksdb_RocksDB_put__JJ_3BII_3BII(JNIEnv* env, jobject /*jdb*/,
                                                jlong jdb_handle,
                                                jlong jwrite_options_handle,
                                                jbyteArray jkey, jint jkey_off,
                                                jint jkey_len, jbyteArray jval,
                                                jint jval_off, jint jval_len) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);

  rocksdb_put_helper(env, db, *write_options, nullptr, jkey, jkey_off, jkey_len,
                     jval, jval_off, jval_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    put
 * Signature: (JJ[BII[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_put__JJ_3BII_3BIIJ(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jwrite_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jbyteArray jval,
    jint jval_off, jint jval_len, jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_put_helper(env, db, *write_options, cf_handle, jkey, jkey_off,
                       jkey_len, jval, jval_off, jval_len);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
  }
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::Write
/*
 * Class:     org_rocksdb_RocksDB
 * Method:    write0
 * Signature: (JJJ)V
 */
void Java_org_rocksdb_RocksDB_write0(JNIEnv* env, jobject /*jdb*/,
                                     jlong jdb_handle,
                                     jlong jwrite_options_handle,
                                     jlong jwb_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);

  rocksdb::Status s = db->Write(*write_options, wb);

  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    write1
 * Signature: (JJJ)V
 */
void Java_org_rocksdb_RocksDB_write1(JNIEnv* env, jobject /*jdb*/,
                                     jlong jdb_handle,
                                     jlong jwrite_options_handle,
                                     jlong jwbwi_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  auto* wb = wbwi->GetWriteBatch();

  rocksdb::Status s = db->Write(*write_options, wb);

  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::KeyMayExist
jboolean key_may_exist_helper(JNIEnv* env, rocksdb::DB* db,
                              const rocksdb::ReadOptions& read_opt,
                              rocksdb::ColumnFamilyHandle* cf_handle,
                              jbyteArray jkey, jint jkey_off, jint jkey_len,
                              jobject jstring_builder, bool* has_exception) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    *has_exception = true;
    return false;
  }

  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  std::string value;
  bool value_found = false;
  bool keyMayExist;
  if (cf_handle != nullptr) {
    keyMayExist =
        db->KeyMayExist(read_opt, cf_handle, key_slice, &value, &value_found);
  } else {
    keyMayExist = db->KeyMayExist(read_opt, key_slice, &value, &value_found);
  }

  // cleanup
  delete[] key;

  // extract the value
  if (value_found && !value.empty()) {
    jobject jresult_string_builder =
        rocksdb::StringBuilderJni::append(env, jstring_builder, value.c_str());
    if (jresult_string_builder == nullptr) {
      *has_exception = true;
      return false;
    }
  }

  *has_exception = false;
  return static_cast<jboolean>(keyMayExist);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    keyMayExist
 * Signature: (J[BIILjava/lang/StringBuilder;)Z
 */
jboolean Java_org_rocksdb_RocksDB_keyMayExist__J_3BIILjava_lang_StringBuilder_2(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jbyteArray jkey,
    jint jkey_off, jint jkey_len, jobject jstring_builder) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  bool has_exception = false;
  return key_may_exist_helper(env, db, rocksdb::ReadOptions(), nullptr, jkey,
                              jkey_off, jkey_len, jstring_builder,
                              &has_exception);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    keyMayExist
 * Signature: (J[BIIJLjava/lang/StringBuilder;)Z
 */
jboolean
Java_org_rocksdb_RocksDB_keyMayExist__J_3BIIJLjava_lang_StringBuilder_2(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jbyteArray jkey,
    jint jkey_off, jint jkey_len, jlong jcf_handle, jobject jstring_builder) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    bool has_exception = false;
    return key_may_exist_helper(env, db, rocksdb::ReadOptions(), cf_handle,
                                jkey, jkey_off, jkey_len, jstring_builder,
                                &has_exception);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
    return true;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    keyMayExist
 * Signature: (JJ[BIILjava/lang/StringBuilder;)Z
 */
jboolean
Java_org_rocksdb_RocksDB_keyMayExist__JJ_3BIILjava_lang_StringBuilder_2(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jread_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jobject jstring_builder) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto& read_options =
      *reinterpret_cast<rocksdb::ReadOptions*>(jread_options_handle);
  bool has_exception = false;
  return key_may_exist_helper(env, db, read_options, nullptr, jkey, jkey_off,
                              jkey_len, jstring_builder, &has_exception);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    keyMayExist
 * Signature: (JJ[BIIJLjava/lang/StringBuilder;)Z
 */
jboolean
Java_org_rocksdb_RocksDB_keyMayExist__JJ_3BIIJLjava_lang_StringBuilder_2(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jread_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jlong jcf_handle,
    jobject jstring_builder) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto& read_options =
      *reinterpret_cast<rocksdb::ReadOptions*>(jread_options_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    bool has_exception = false;
    return key_may_exist_helper(env, db, read_options, cf_handle, jkey,
                                jkey_off, jkey_len, jstring_builder,
                                &has_exception);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
    return true;
  }
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::Get

jbyteArray rocksdb_get_helper(JNIEnv* env, rocksdb::DB* db,
                              const rocksdb::ReadOptions& read_opt,
                              rocksdb::ColumnFamilyHandle* column_family_handle,
                              jbyteArray jkey, jint jkey_off, jint jkey_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return nullptr;
  }

  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  std::string value;
  rocksdb::Status s;
  if (column_family_handle != nullptr) {
    s = db->Get(read_opt, column_family_handle, key_slice, &value);
  } else {
    // backwards compatibility
    s = db->Get(read_opt, key_slice, &value);
  }

  // cleanup
  delete[] key;

  if (s.IsNotFound()) {
    return nullptr;
  }

  if (s.ok()) {
    jbyteArray jret_value = rocksdb::JniUtil::copyBytes(env, value);
    if (jret_value == nullptr) {
      // exception occurred
      return nullptr;
    }
    return jret_value;
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  return nullptr;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (J[BII)[B
 */
jbyteArray Java_org_rocksdb_RocksDB_get__J_3BII(JNIEnv* env, jobject /*jdb*/,
                                                jlong jdb_handle,
                                                jbyteArray jkey, jint jkey_off,
                                                jint jkey_len) {
  return rocksdb_get_helper(env, reinterpret_cast<rocksdb::DB*>(jdb_handle),
                            rocksdb::ReadOptions(), nullptr, jkey, jkey_off,
                            jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (J[BIIJ)[B
 */
jbyteArray Java_org_rocksdb_RocksDB_get__J_3BIIJ(JNIEnv* env, jobject /*jdb*/,
                                                 jlong jdb_handle,
                                                 jbyteArray jkey, jint jkey_off,
                                                 jint jkey_len,
                                                 jlong jcf_handle) {
  auto db_handle = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    return rocksdb_get_helper(env, db_handle, rocksdb::ReadOptions(), cf_handle,
                              jkey, jkey_off, jkey_len);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (JJ[BII)[B
 */
jbyteArray Java_org_rocksdb_RocksDB_get__JJ_3BII(JNIEnv* env, jobject /*jdb*/,
                                                 jlong jdb_handle,
                                                 jlong jropt_handle,
                                                 jbyteArray jkey, jint jkey_off,
                                                 jint jkey_len) {
  return rocksdb_get_helper(
      env, reinterpret_cast<rocksdb::DB*>(jdb_handle),
      *reinterpret_cast<rocksdb::ReadOptions*>(jropt_handle), nullptr, jkey,
      jkey_off, jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (JJ[BIIJ)[B
 */
jbyteArray Java_org_rocksdb_RocksDB_get__JJ_3BIIJ(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jropt_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jlong jcf_handle) {
  auto* db_handle = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto& ro_opt = *reinterpret_cast<rocksdb::ReadOptions*>(jropt_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    return rocksdb_get_helper(env, db_handle, ro_opt, cf_handle, jkey, jkey_off,
                              jkey_len);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
    return nullptr;
  }
}

jint rocksdb_get_helper(JNIEnv* env, rocksdb::DB* db,
                        const rocksdb::ReadOptions& read_options,
                        rocksdb::ColumnFamilyHandle* column_family_handle,
                        jbyteArray jkey, jint jkey_off, jint jkey_len,
                        jbyteArray jval, jint jval_off, jint jval_len,
                        bool* has_exception) {
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
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  // TODO(yhchiang): we might save one memory allocation here by adding
  // a DB::Get() function which takes preallocated jbyte* as input.
  std::string cvalue;
  rocksdb::Status s;
  if (column_family_handle != nullptr) {
    s = db->Get(read_options, column_family_handle, key_slice, &cvalue);
  } else {
    // backwards compatibility
    s = db->Get(read_options, key_slice, &cvalue);
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
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);

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

inline void multi_get_helper_release_keys(
    JNIEnv* env, std::vector<std::pair<jbyte*, jobject>>& keys_to_free) {
  auto end = keys_to_free.end();
  for (auto it = keys_to_free.begin(); it != end; ++it) {
    delete[] it->first;
    env->DeleteLocalRef(it->second);
  }
  keys_to_free.clear();
}

/**
 * cf multi get
 *
 * @return byte[][] of values or nullptr if an exception occurs
 */
jobjectArray multi_get_helper(JNIEnv* env, jobject /*jdb*/, rocksdb::DB* db,
                              const rocksdb::ReadOptions& rOpt,
                              jobjectArray jkeys, jintArray jkey_offs,
                              jintArray jkey_lens,
                              jlongArray jcolumn_family_handles) {
  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
  if (jcolumn_family_handles != nullptr) {
    const jsize len_cols = env->GetArrayLength(jcolumn_family_handles);

    jlong* jcfh = env->GetLongArrayElements(jcolumn_family_handles, nullptr);
    if (jcfh == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    for (jsize i = 0; i < len_cols; i++) {
      auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcfh[i]);
      cf_handles.push_back(cf_handle);
    }
    env->ReleaseLongArrayElements(jcolumn_family_handles, jcfh, JNI_ABORT);
  }

  const jsize len_keys = env->GetArrayLength(jkeys);
  if (env->EnsureLocalCapacity(len_keys) != 0) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  jint* jkey_off = env->GetIntArrayElements(jkey_offs, nullptr);
  if (jkey_off == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  jint* jkey_len = env->GetIntArrayElements(jkey_lens, nullptr);
  if (jkey_len == nullptr) {
    // exception thrown: OutOfMemoryError
    env->ReleaseIntArrayElements(jkey_offs, jkey_off, JNI_ABORT);
    return nullptr;
  }

  std::vector<rocksdb::Slice> keys;
  std::vector<std::pair<jbyte*, jobject>> keys_to_free;
  for (jsize i = 0; i < len_keys; i++) {
    jobject jkey = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseIntArrayElements(jkey_lens, jkey_len, JNI_ABORT);
      env->ReleaseIntArrayElements(jkey_offs, jkey_off, JNI_ABORT);
      multi_get_helper_release_keys(env, keys_to_free);
      return nullptr;
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
      multi_get_helper_release_keys(env, keys_to_free);
      return nullptr;
    }

    rocksdb::Slice key_slice(reinterpret_cast<char*>(key), len_key);
    keys.push_back(key_slice);

    keys_to_free.push_back(std::pair<jbyte*, jobject>(key, jkey));
  }

  // cleanup jkey_off and jken_len
  env->ReleaseIntArrayElements(jkey_lens, jkey_len, JNI_ABORT);
  env->ReleaseIntArrayElements(jkey_offs, jkey_off, JNI_ABORT);

  std::vector<std::string> values;
  std::vector<rocksdb::Status> s;
  if (cf_handles.size() == 0) {
    s = db->MultiGet(rOpt, keys, &values);
  } else {
    s = db->MultiGet(rOpt, cf_handles, keys, &values);
  }

  // free up allocated byte arrays
  multi_get_helper_release_keys(env, keys_to_free);

  // prepare the results
  jobjectArray jresults =
      rocksdb::ByteJni::new2dByteArray(env, static_cast<jsize>(s.size()));
  if (jresults == nullptr) {
    // exception occurred
    return nullptr;
  }

  // TODO(AR) it is not clear to me why EnsureLocalCapacity is needed for the
  //     loop as we cleanup references with env->DeleteLocalRef(jentry_value);
  if (env->EnsureLocalCapacity(static_cast<jint>(s.size())) != 0) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  // add to the jresults
  for (std::vector<rocksdb::Status>::size_type i = 0; i != s.size(); i++) {
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
        // exception thrown: ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jentry_value);
        return nullptr;
      }

      env->SetObjectArrayElement(jresults, static_cast<jsize>(i), jentry_value);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jentry_value);
        return nullptr;
      }

      env->DeleteLocalRef(jentry_value);
    }
  }

  return jresults;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    multiGet
 * Signature: (J[[B[I[I)[[B
 */
jobjectArray Java_org_rocksdb_RocksDB_multiGet__J_3_3B_3I_3I(
    JNIEnv* env, jobject jdb, jlong jdb_handle, jobjectArray jkeys,
    jintArray jkey_offs, jintArray jkey_lens) {
  return multi_get_helper(env, jdb, reinterpret_cast<rocksdb::DB*>(jdb_handle),
                          rocksdb::ReadOptions(), jkeys, jkey_offs, jkey_lens,
                          nullptr);
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
  return multi_get_helper(env, jdb, reinterpret_cast<rocksdb::DB*>(jdb_handle),
                          rocksdb::ReadOptions(), jkeys, jkey_offs, jkey_lens,
                          jcolumn_family_handles);
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
      env, jdb, reinterpret_cast<rocksdb::DB*>(jdb_handle),
      *reinterpret_cast<rocksdb::ReadOptions*>(jropt_handle), jkeys, jkey_offs,
      jkey_lens, nullptr);
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
      env, jdb, reinterpret_cast<rocksdb::DB*>(jdb_handle),
      *reinterpret_cast<rocksdb::ReadOptions*>(jropt_handle), jkeys, jkey_offs,
      jkey_lens, jcolumn_family_handles);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (J[BII[BII)I
 */
jint Java_org_rocksdb_RocksDB_get__J_3BII_3BII(JNIEnv* env, jobject /*jdb*/,
                                               jlong jdb_handle,
                                               jbyteArray jkey, jint jkey_off,
                                               jint jkey_len, jbyteArray jval,
                                               jint jval_off, jint jval_len) {
  bool has_exception = false;
  return rocksdb_get_helper(env, reinterpret_cast<rocksdb::DB*>(jdb_handle),
                            rocksdb::ReadOptions(), nullptr, jkey, jkey_off,
                            jkey_len, jval, jval_off, jval_len, &has_exception);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (J[BII[BIIJ)I
 */
jint Java_org_rocksdb_RocksDB_get__J_3BII_3BIIJ(JNIEnv* env, jobject /*jdb*/,
                                                jlong jdb_handle,
                                                jbyteArray jkey, jint jkey_off,
                                                jint jkey_len, jbyteArray jval,
                                                jint jval_off, jint jval_len,
                                                jlong jcf_handle) {
  auto* db_handle = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    bool has_exception = false;
    return rocksdb_get_helper(env, db_handle, rocksdb::ReadOptions(), cf_handle,
                              jkey, jkey_off, jkey_len, jval, jval_off,
                              jval_len, &has_exception);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
    // will never be evaluated
    return 0;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (JJ[BII[BII)I
 */
jint Java_org_rocksdb_RocksDB_get__JJ_3BII_3BII(JNIEnv* env, jobject /*jdb*/,
                                                jlong jdb_handle,
                                                jlong jropt_handle,
                                                jbyteArray jkey, jint jkey_off,
                                                jint jkey_len, jbyteArray jval,
                                                jint jval_off, jint jval_len) {
  bool has_exception = false;
  return rocksdb_get_helper(
      env, reinterpret_cast<rocksdb::DB*>(jdb_handle),
      *reinterpret_cast<rocksdb::ReadOptions*>(jropt_handle), nullptr, jkey,
      jkey_off, jkey_len, jval, jval_off, jval_len, &has_exception);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (JJ[BII[BIIJ)I
 */
jint Java_org_rocksdb_RocksDB_get__JJ_3BII_3BIIJ(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jropt_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jbyteArray jval,
    jint jval_off, jint jval_len, jlong jcf_handle) {
  auto* db_handle = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto& ro_opt = *reinterpret_cast<rocksdb::ReadOptions*>(jropt_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    bool has_exception = false;
    return rocksdb_get_helper(env, db_handle, ro_opt, cf_handle, jkey, jkey_off,
                              jkey_len, jval, jval_off, jval_len,
                              &has_exception);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
    // will never be evaluated
    return 0;
  }
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::Delete()

/**
 * @return true if the delete succeeded, false if a Java Exception was thrown
 */
bool rocksdb_delete_helper(JNIEnv* env, rocksdb::DB* db,
                           const rocksdb::WriteOptions& write_options,
                           rocksdb::ColumnFamilyHandle* cf_handle,
                           jbyteArray jkey, jint jkey_off, jint jkey_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return false;
  }
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  rocksdb::Status s;
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

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    delete
 * Signature: (J[BII)V
 */
void Java_org_rocksdb_RocksDB_delete__J_3BII(JNIEnv* env, jobject /*jdb*/,
                                             jlong jdb_handle, jbyteArray jkey,
                                             jint jkey_off, jint jkey_len) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options =
      rocksdb::WriteOptions();
  rocksdb_delete_helper(env, db, default_write_options, nullptr, jkey, jkey_off,
                        jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    delete
 * Signature: (J[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_delete__J_3BIIJ(JNIEnv* env, jobject /*jdb*/,
                                              jlong jdb_handle, jbyteArray jkey,
                                              jint jkey_off, jint jkey_len,
                                              jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options =
      rocksdb::WriteOptions();
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_delete_helper(env, db, default_write_options, cf_handle, jkey,
                          jkey_off, jkey_len);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    delete
 * Signature: (JJ[BII)V
 */
void Java_org_rocksdb_RocksDB_delete__JJ_3BII(JNIEnv* env, jobject /*jdb*/,
                                              jlong jdb_handle,
                                              jlong jwrite_options,
                                              jbyteArray jkey, jint jkey_off,
                                              jint jkey_len) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options);
  rocksdb_delete_helper(env, db, *write_options, nullptr, jkey, jkey_off,
                        jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    delete
 * Signature: (JJ[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_delete__JJ_3BIIJ(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jwrite_options,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_delete_helper(env, db, *write_options, cf_handle, jkey, jkey_off,
                          jkey_len);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
  }
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::SingleDelete()
/**
 * @return true if the single delete succeeded, false if a Java Exception
 *     was thrown
 */
bool rocksdb_single_delete_helper(JNIEnv* env, rocksdb::DB* db,
                                  const rocksdb::WriteOptions& write_options,
                                  rocksdb::ColumnFamilyHandle* cf_handle,
                                  jbyteArray jkey, jint jkey_len) {
  jbyte* key = env->GetByteArrayElements(jkey, nullptr);
  if (key == nullptr) {
    // exception thrown: OutOfMemoryError
    return false;
  }
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  rocksdb::Status s;
  if (cf_handle != nullptr) {
    s = db->SingleDelete(write_options, cf_handle, key_slice);
  } else {
    // backwards compatibility
    s = db->SingleDelete(write_options, key_slice);
  }

  // trigger java unref on key and value.
  // by passing JNI_ABORT, it will simply release the reference without
  // copying the result back to the java byte array.
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);

  if (s.ok()) {
    return true;
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    singleDelete
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_RocksDB_singleDelete__J_3BI(JNIEnv* env, jobject /*jdb*/,
                                                  jlong jdb_handle,
                                                  jbyteArray jkey,
                                                  jint jkey_len) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options =
      rocksdb::WriteOptions();
  rocksdb_single_delete_helper(env, db, default_write_options, nullptr, jkey,
                               jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    singleDelete
 * Signature: (J[BIJ)V
 */
void Java_org_rocksdb_RocksDB_singleDelete__J_3BIJ(JNIEnv* env, jobject /*jdb*/,
                                                   jlong jdb_handle,
                                                   jbyteArray jkey,
                                                   jint jkey_len,
                                                   jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options =
      rocksdb::WriteOptions();
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_single_delete_helper(env, db, default_write_options, cf_handle,
                                 jkey, jkey_len);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    singleDelete
 * Signature: (JJ[BIJ)V
 */
void Java_org_rocksdb_RocksDB_singleDelete__JJ_3BI(JNIEnv* env, jobject /*jdb*/,
                                                   jlong jdb_handle,
                                                   jlong jwrite_options,
                                                   jbyteArray jkey,
                                                   jint jkey_len) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options);
  rocksdb_single_delete_helper(env, db, *write_options, nullptr, jkey,
                               jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    singleDelete
 * Signature: (JJ[BIJ)V
 */
void Java_org_rocksdb_RocksDB_singleDelete__JJ_3BIJ(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jwrite_options,
    jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_single_delete_helper(env, db, *write_options, cf_handle, jkey,
                                 jkey_len);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
  }
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::DeleteRange()
/**
 * @return true if the delete range succeeded, false if a Java Exception
 *     was thrown
 */
bool rocksdb_delete_range_helper(JNIEnv* env, rocksdb::DB* db,
                                 const rocksdb::WriteOptions& write_options,
                                 rocksdb::ColumnFamilyHandle* cf_handle,
                                 jbyteArray jbegin_key, jint jbegin_key_off,
                                 jint jbegin_key_len, jbyteArray jend_key,
                                 jint jend_key_off, jint jend_key_len) {
  jbyte* begin_key = new jbyte[jbegin_key_len];
  env->GetByteArrayRegion(jbegin_key, jbegin_key_off, jbegin_key_len,
                          begin_key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] begin_key;
    return false;
  }
  rocksdb::Slice begin_key_slice(reinterpret_cast<char*>(begin_key),
                                 jbegin_key_len);

  jbyte* end_key = new jbyte[jend_key_len];
  env->GetByteArrayRegion(jend_key, jend_key_off, jend_key_len, end_key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] begin_key;
    delete[] end_key;
    return false;
  }
  rocksdb::Slice end_key_slice(reinterpret_cast<char*>(end_key), jend_key_len);

  rocksdb::Status s =
      db->DeleteRange(write_options, cf_handle, begin_key_slice, end_key_slice);

  // cleanup
  delete[] begin_key;
  delete[] end_key;

  if (s.ok()) {
    return true;
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteRange
 * Signature: (J[BII[BII)V
 */
void Java_org_rocksdb_RocksDB_deleteRange__J_3BII_3BII(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jbyteArray jbegin_key,
    jint jbegin_key_off, jint jbegin_key_len, jbyteArray jend_key,
    jint jend_key_off, jint jend_key_len) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options =
      rocksdb::WriteOptions();
  rocksdb_delete_range_helper(env, db, default_write_options, nullptr,
                              jbegin_key, jbegin_key_off, jbegin_key_len,
                              jend_key, jend_key_off, jend_key_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteRange
 * Signature: (J[BII[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_deleteRange__J_3BII_3BIIJ(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jbyteArray jbegin_key,
    jint jbegin_key_off, jint jbegin_key_len, jbyteArray jend_key,
    jint jend_key_off, jint jend_key_len, jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options =
      rocksdb::WriteOptions();
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_delete_range_helper(env, db, default_write_options, cf_handle,
                                jbegin_key, jbegin_key_off, jbegin_key_len,
                                jend_key, jend_key_off, jend_key_len);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteRange
 * Signature: (JJ[BII[BII)V
 */
void Java_org_rocksdb_RocksDB_deleteRange__JJ_3BII_3BII(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jwrite_options,
    jbyteArray jbegin_key, jint jbegin_key_off, jint jbegin_key_len,
    jbyteArray jend_key, jint jend_key_off, jint jend_key_len) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options);
  rocksdb_delete_range_helper(env, db, *write_options, nullptr, jbegin_key,
                              jbegin_key_off, jbegin_key_len, jend_key,
                              jend_key_off, jend_key_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    deleteRange
 * Signature: (JJ[BII[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_deleteRange__JJ_3BII_3BIIJ(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jwrite_options,
    jbyteArray jbegin_key, jint jbegin_key_off, jint jbegin_key_len,
    jbyteArray jend_key, jint jend_key_off, jint jend_key_len,
    jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_delete_range_helper(env, db, *write_options, cf_handle, jbegin_key,
                                jbegin_key_off, jbegin_key_len, jend_key,
                                jend_key_off, jend_key_len);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
  }
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::Merge

/**
 * @return true if the merge succeeded, false if a Java Exception was thrown
 */
bool rocksdb_merge_helper(JNIEnv* env, rocksdb::DB* db,
                          const rocksdb::WriteOptions& write_options,
                          rocksdb::ColumnFamilyHandle* cf_handle,
                          jbyteArray jkey, jint jkey_off, jint jkey_len,
                          jbyteArray jval, jint jval_off, jint jval_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return false;
  }
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  jbyte* value = new jbyte[jval_len];
  env->GetByteArrayRegion(jval, jval_off, jval_len, value);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] value;
    delete[] key;
    return false;
  }
  rocksdb::Slice value_slice(reinterpret_cast<char*>(value), jval_len);

  rocksdb::Status s;
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

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    merge
 * Signature: (J[BII[BII)V
 */
void Java_org_rocksdb_RocksDB_merge__J_3BII_3BII(JNIEnv* env, jobject /*jdb*/,
                                                 jlong jdb_handle,
                                                 jbyteArray jkey, jint jkey_off,
                                                 jint jkey_len, jbyteArray jval,
                                                 jint jval_off, jint jval_len) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options =
      rocksdb::WriteOptions();

  rocksdb_merge_helper(env, db, default_write_options, nullptr, jkey, jkey_off,
                       jkey_len, jval, jval_off, jval_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    merge
 * Signature: (J[BII[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_merge__J_3BII_3BIIJ(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jbyteArray jkey,
    jint jkey_off, jint jkey_len, jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options =
      rocksdb::WriteOptions();
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_merge_helper(env, db, default_write_options, cf_handle, jkey,
                         jkey_off, jkey_len, jval, jval_off, jval_len);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    merge
 * Signature: (JJ[BII[BII)V
 */
void Java_org_rocksdb_RocksDB_merge__JJ_3BII_3BII(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jwrite_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jbyteArray jval,
    jint jval_off, jint jval_len) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);

  rocksdb_merge_helper(env, db, *write_options, nullptr, jkey, jkey_off,
                       jkey_len, jval, jval_off, jval_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    merge
 * Signature: (JJ[BII[BIIJ)V
 */
void Java_org_rocksdb_RocksDB_merge__JJ_3BII_3BIIJ(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jwrite_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jbyteArray jval,
    jint jval_off, jint jval_len, jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    rocksdb_merge_helper(env, db, *write_options, cf_handle, jkey, jkey_off,
                         jkey_len, jval, jval_off, jval_len);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid ColumnFamilyHandle."));
  }
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::~DB()

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_disposeInternal(JNIEnv* /*env*/,
                                              jobject /*java_db*/,
                                              jlong jhandle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jhandle);
  assert(db != nullptr);
  delete db;
}

jlong rocksdb_iterator_helper(rocksdb::DB* db,
                              rocksdb::ReadOptions read_options,
                              rocksdb::ColumnFamilyHandle* cf_handle) {
  rocksdb::Iterator* iterator = nullptr;
  if (cf_handle != nullptr) {
    iterator = db->NewIterator(read_options, cf_handle);
  } else {
    iterator = db->NewIterator(read_options);
  }
  return reinterpret_cast<jlong>(iterator);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    iterator
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RocksDB_iterator__J(JNIEnv* /*env*/, jobject /*jdb*/,
                                           jlong db_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(db_handle);
  return rocksdb_iterator_helper(db, rocksdb::ReadOptions(), nullptr);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    iterator
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_RocksDB_iterator__JJ(JNIEnv* /*env*/, jobject /*jdb*/,
                                            jlong db_handle,
                                            jlong jread_options_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(db_handle);
  auto& read_options =
      *reinterpret_cast<rocksdb::ReadOptions*>(jread_options_handle);
  return rocksdb_iterator_helper(db, read_options, nullptr);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    iteratorCF
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_RocksDB_iteratorCF__JJ(JNIEnv* /*env*/, jobject /*jdb*/,
                                              jlong db_handle,
                                              jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(db_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  return rocksdb_iterator_helper(db, rocksdb::ReadOptions(), cf_handle);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    iteratorCF
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_RocksDB_iteratorCF__JJJ(JNIEnv* /*env*/, jobject /*jdb*/,
                                               jlong db_handle,
                                               jlong jcf_handle,
                                               jlong jread_options_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(db_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  auto& read_options =
      *reinterpret_cast<rocksdb::ReadOptions*>(jread_options_handle);
  return rocksdb_iterator_helper(db, read_options, cf_handle);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    iterators
 * Signature: (J[JJ)[J
 */
jlongArray Java_org_rocksdb_RocksDB_iterators(JNIEnv* env, jobject /*jdb*/,
                                              jlong db_handle,
                                              jlongArray jcolumn_family_handles,
                                              jlong jread_options_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(db_handle);
  auto& read_options =
      *reinterpret_cast<rocksdb::ReadOptions*>(jread_options_handle);

  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
  if (jcolumn_family_handles != nullptr) {
    const jsize len_cols = env->GetArrayLength(jcolumn_family_handles);
    jlong* jcfh = env->GetLongArrayElements(jcolumn_family_handles, nullptr);
    if (jcfh == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    for (jsize i = 0; i < len_cols; i++) {
      auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcfh[i]);
      cf_handles.push_back(cf_handle);
    }

    env->ReleaseLongArrayElements(jcolumn_family_handles, jcfh, JNI_ABORT);
  }

  std::vector<rocksdb::Iterator*> iterators;
  rocksdb::Status s = db->NewIterators(read_options, cf_handles, &iterators);
  if (s.ok()) {
    jlongArray jLongArray =
        env->NewLongArray(static_cast<jsize>(iterators.size()));
    if (jLongArray == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    for (std::vector<rocksdb::Iterator*>::size_type i = 0; i < iterators.size();
         i++) {
      env->SetLongArrayRegion(
          jLongArray, static_cast<jsize>(i), 1,
          const_cast<jlong*>(reinterpret_cast<const jlong*>(&iterators[i])));
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jLongArray);
        return nullptr;
      }
    }

    return jLongArray;
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getDefaultColumnFamily
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RocksDB_getDefaultColumnFamily(JNIEnv* /*env*/,
                                                      jobject /*jobj*/,
                                                      jlong jdb_handle) {
  auto* db_handle = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* cf_handle = db_handle->DefaultColumnFamily();
  return reinterpret_cast<jlong>(cf_handle);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    createColumnFamily
 * Signature: (J[BJ)J
 */
jlong Java_org_rocksdb_RocksDB_createColumnFamily(JNIEnv* env, jobject /*jdb*/,
                                                  jlong jdb_handle,
                                                  jbyteArray jcolumn_name,
                                                  jlong jcolumn_options) {
  rocksdb::ColumnFamilyHandle* handle;
  jboolean has_exception = JNI_FALSE;
  std::string column_name = rocksdb::JniUtil::byteString<std::string>(
      env, jcolumn_name,
      [](const char* str, const size_t len) { return std::string(str, len); },
      &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return 0;
  }

  auto* db_handle = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* cfOptions =
      reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jcolumn_options);

  rocksdb::Status s =
      db_handle->CreateColumnFamily(*cfOptions, column_name, &handle);

  if (s.ok()) {
    return reinterpret_cast<jlong>(handle);
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  return 0;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    dropColumnFamily
 * Signature: (JJ)V;
 */
void Java_org_rocksdb_RocksDB_dropColumnFamily(JNIEnv* env, jobject /*jdb*/,
                                               jlong jdb_handle,
                                               jlong jcf_handle) {
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  auto* db_handle = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  rocksdb::Status s = db_handle->DropColumnFamily(cf_handle);
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Method:    getSnapshot
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RocksDB_getSnapshot(JNIEnv* /*env*/, jobject /*jdb*/,
                                           jlong db_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(db_handle);
  const rocksdb::Snapshot* snapshot = db->GetSnapshot();
  return reinterpret_cast<jlong>(snapshot);
}

/*
 * Method:    releaseSnapshot
 * Signature: (JJ)V
 */
void Java_org_rocksdb_RocksDB_releaseSnapshot(JNIEnv* /*env*/, jobject /*jdb*/,
                                              jlong db_handle,
                                              jlong snapshot_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(db_handle);
  auto* snapshot = reinterpret_cast<rocksdb::Snapshot*>(snapshot_handle);
  db->ReleaseSnapshot(snapshot);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getProperty0
 * Signature: (JLjava/lang/String;I)Ljava/lang/String;
 */
jstring Java_org_rocksdb_RocksDB_getProperty0__JLjava_lang_String_2I(
    JNIEnv* env, jobject /*jdb*/, jlong db_handle, jstring jproperty,
    jint jproperty_len) {
  const char* property = env->GetStringUTFChars(jproperty, nullptr);
  if (property == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  rocksdb::Slice property_slice(property, jproperty_len);

  auto* db = reinterpret_cast<rocksdb::DB*>(db_handle);
  std::string property_value;
  bool retCode = db->GetProperty(property_slice, &property_value);
  env->ReleaseStringUTFChars(jproperty, property);

  if (retCode) {
    return env->NewStringUTF(property_value.c_str());
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, rocksdb::Status::NotFound());
  return nullptr;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getProperty0
 * Signature: (JJLjava/lang/String;I)Ljava/lang/String;
 */
jstring Java_org_rocksdb_RocksDB_getProperty0__JJLjava_lang_String_2I(
    JNIEnv* env, jobject /*jdb*/, jlong db_handle, jlong jcf_handle,
    jstring jproperty, jint jproperty_len) {
  const char* property = env->GetStringUTFChars(jproperty, nullptr);
  if (property == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  rocksdb::Slice property_slice(property, jproperty_len);

  auto* db = reinterpret_cast<rocksdb::DB*>(db_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  std::string property_value;
  bool retCode = db->GetProperty(cf_handle, property_slice, &property_value);
  env->ReleaseStringUTFChars(jproperty, property);

  if (retCode) {
    return env->NewStringUTF(property_value.c_str());
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, rocksdb::Status::NotFound());
  return nullptr;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getLongProperty
 * Signature: (JLjava/lang/String;I)L;
 */
jlong Java_org_rocksdb_RocksDB_getLongProperty__JLjava_lang_String_2I(
    JNIEnv* env, jobject /*jdb*/, jlong db_handle, jstring jproperty,
    jint jproperty_len) {
  const char* property = env->GetStringUTFChars(jproperty, nullptr);
  if (property == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  rocksdb::Slice property_slice(property, jproperty_len);

  auto* db = reinterpret_cast<rocksdb::DB*>(db_handle);
  uint64_t property_value = 0;
  bool retCode = db->GetIntProperty(property_slice, &property_value);
  env->ReleaseStringUTFChars(jproperty, property);

  if (retCode) {
    return property_value;
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, rocksdb::Status::NotFound());
  return 0;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getLongProperty
 * Signature: (JJLjava/lang/String;I)L;
 */
jlong Java_org_rocksdb_RocksDB_getLongProperty__JJLjava_lang_String_2I(
    JNIEnv* env, jobject /*jdb*/, jlong db_handle, jlong jcf_handle,
    jstring jproperty, jint jproperty_len) {
  const char* property = env->GetStringUTFChars(jproperty, nullptr);
  if (property == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  rocksdb::Slice property_slice(property, jproperty_len);

  auto* db = reinterpret_cast<rocksdb::DB*>(db_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  uint64_t property_value;
  bool retCode = db->GetIntProperty(cf_handle, property_slice, &property_value);
  env->ReleaseStringUTFChars(jproperty, property);

  if (retCode) {
    return property_value;
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, rocksdb::Status::NotFound());
  return 0;
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::Flush

void rocksdb_flush_helper(JNIEnv* env, rocksdb::DB* db,
                          const rocksdb::FlushOptions& flush_options,
                          rocksdb::ColumnFamilyHandle* column_family_handle) {
  rocksdb::Status s;
  if (column_family_handle != nullptr) {
    s = db->Flush(flush_options, column_family_handle);
  } else {
    s = db->Flush(flush_options);
  }
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    flush
 * Signature: (JJ)V
 */
void Java_org_rocksdb_RocksDB_flush__JJ(JNIEnv* env, jobject /*jdb*/,
                                        jlong jdb_handle,
                                        jlong jflush_options) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* flush_options =
      reinterpret_cast<rocksdb::FlushOptions*>(jflush_options);
  rocksdb_flush_helper(env, db, *flush_options, nullptr);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    flush
 * Signature: (JJJ)V
 */
void Java_org_rocksdb_RocksDB_flush__JJJ(JNIEnv* env, jobject /*jdb*/,
                                         jlong jdb_handle, jlong jflush_options,
                                         jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* flush_options =
      reinterpret_cast<rocksdb::FlushOptions*>(jflush_options);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  rocksdb_flush_helper(env, db, *flush_options, cf_handle);
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::CompactRange - Full

void rocksdb_compactrange_helper(JNIEnv* env, rocksdb::DB* db,
                                 rocksdb::ColumnFamilyHandle* cf_handle,
                                 jboolean jreduce_level, jint jtarget_level,
                                 jint jtarget_path_id) {
  rocksdb::Status s;
  rocksdb::CompactRangeOptions compact_options;
  compact_options.change_level = jreduce_level;
  compact_options.target_level = jtarget_level;
  compact_options.target_path_id = static_cast<uint32_t>(jtarget_path_id);
  if (cf_handle != nullptr) {
    s = db->CompactRange(compact_options, cf_handle, nullptr, nullptr);
  } else {
    // backwards compatibility
    s = db->CompactRange(compact_options, nullptr, nullptr);
  }

  if (s.ok()) {
    return;
  }
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    compactRange0
 * Signature: (JZII)V
 */
void Java_org_rocksdb_RocksDB_compactRange0__JZII(JNIEnv* env, jobject /*jdb*/,
                                                  jlong jdb_handle,
                                                  jboolean jreduce_level,
                                                  jint jtarget_level,
                                                  jint jtarget_path_id) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  rocksdb_compactrange_helper(env, db, nullptr, jreduce_level, jtarget_level,
                              jtarget_path_id);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    compactRange
 * Signature: (JZIIJ)V
 */
void Java_org_rocksdb_RocksDB_compactRange__JZIIJ(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jboolean jreduce_level,
    jint jtarget_level, jint jtarget_path_id, jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  rocksdb_compactrange_helper(env, db, cf_handle, jreduce_level, jtarget_level,
                              jtarget_path_id);
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::CompactRange - Range

/**
 * @return true if the compact range succeeded, false if a Java Exception
 *     was thrown
 */
bool rocksdb_compactrange_helper(JNIEnv* env, rocksdb::DB* db,
                                 rocksdb::ColumnFamilyHandle* cf_handle,
                                 jbyteArray jbegin, jint jbegin_len,
                                 jbyteArray jend, jint jend_len,
                                 jboolean jreduce_level, jint jtarget_level,
                                 jint jtarget_path_id) {
  jbyte* begin = env->GetByteArrayElements(jbegin, nullptr);
  if (begin == nullptr) {
    // exception thrown: OutOfMemoryError
    return false;
  }

  jbyte* end = env->GetByteArrayElements(jend, nullptr);
  if (end == nullptr) {
    // exception thrown: OutOfMemoryError
    env->ReleaseByteArrayElements(jbegin, begin, JNI_ABORT);
    return false;
  }

  const rocksdb::Slice begin_slice(reinterpret_cast<char*>(begin), jbegin_len);
  const rocksdb::Slice end_slice(reinterpret_cast<char*>(end), jend_len);

  rocksdb::Status s;
  rocksdb::CompactRangeOptions compact_options;
  compact_options.change_level = jreduce_level;
  compact_options.target_level = jtarget_level;
  compact_options.target_path_id = static_cast<uint32_t>(jtarget_path_id);
  if (cf_handle != nullptr) {
    s = db->CompactRange(compact_options, cf_handle, &begin_slice, &end_slice);
  } else {
    // backwards compatibility
    s = db->CompactRange(compact_options, &begin_slice, &end_slice);
  }

  env->ReleaseByteArrayElements(jend, end, JNI_ABORT);
  env->ReleaseByteArrayElements(jbegin, begin, JNI_ABORT);

  if (s.ok()) {
    return true;
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    compactRange0
 * Signature: (J[BI[BIZII)V
 */
void Java_org_rocksdb_RocksDB_compactRange0__J_3BI_3BIZII(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jbyteArray jbegin,
    jint jbegin_len, jbyteArray jend, jint jend_len, jboolean jreduce_level,
    jint jtarget_level, jint jtarget_path_id) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  rocksdb_compactrange_helper(env, db, nullptr, jbegin, jbegin_len, jend,
                              jend_len, jreduce_level, jtarget_level,
                              jtarget_path_id);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    compactRange
 * Signature: (JJ[BI[BIZII)V
 */
void Java_org_rocksdb_RocksDB_compactRange__J_3BI_3BIZIIJ(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jbyteArray jbegin,
    jint jbegin_len, jbyteArray jend, jint jend_len, jboolean jreduce_level,
    jint jtarget_level, jint jtarget_path_id, jlong jcf_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  rocksdb_compactrange_helper(env, db, cf_handle, jbegin, jbegin_len, jend,
                              jend_len, jreduce_level, jtarget_level,
                              jtarget_path_id);
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::PauseBackgroundWork

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    pauseBackgroundWork
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_pauseBackgroundWork(JNIEnv* env, jobject /*jobj*/,
                                                  jlong jdb_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto s = db->PauseBackgroundWork();
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::ContinueBackgroundWork

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    continueBackgroundWork
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_continueBackgroundWork(JNIEnv* env,
                                                     jobject /*jobj*/,
                                                     jlong jdb_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto s = db->ContinueBackgroundWork();
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::GetLatestSequenceNumber

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getLatestSequenceNumber
 * Signature: (J)V
 */
jlong Java_org_rocksdb_RocksDB_getLatestSequenceNumber(JNIEnv* /*env*/,
                                                       jobject /*jdb*/,
                                                       jlong jdb_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  return db->GetLatestSequenceNumber();
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB enable/disable file deletions

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    enableFileDeletions
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_disableFileDeletions(JNIEnv* env, jobject /*jdb*/,
                                                   jlong jdb_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  rocksdb::Status s = db->DisableFileDeletions();
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    enableFileDeletions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_RocksDB_enableFileDeletions(JNIEnv* env, jobject /*jdb*/,
                                                  jlong jdb_handle,
                                                  jboolean jforce) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  rocksdb::Status s = db->EnableFileDeletions(jforce);
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::GetUpdatesSince

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    getUpdatesSince
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_RocksDB_getUpdatesSince(JNIEnv* env, jobject /*jdb*/,
                                               jlong jdb_handle,
                                               jlong jsequence_number) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  rocksdb::SequenceNumber sequence_number =
      static_cast<rocksdb::SequenceNumber>(jsequence_number);
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  rocksdb::Status s = db->GetUpdatesSince(sequence_number, &iter);
  if (s.ok()) {
    return reinterpret_cast<jlong>(iter.release());
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  return 0;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    setOptions
 * Signature: (JJ[Ljava/lang/String;[Ljava/lang/String;)V
 */
void Java_org_rocksdb_RocksDB_setOptions(JNIEnv* env, jobject /*jdb*/,
                                         jlong jdb_handle, jlong jcf_handle,
                                         jobjectArray jkeys,
                                         jobjectArray jvalues) {
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

    jstring jkey = reinterpret_cast<jstring>(jobj_key);
    jstring jval = reinterpret_cast<jstring>(jobj_value);

    const char* key = env->GetStringUTFChars(jkey, nullptr);
    if (key == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jobj_value);
      env->DeleteLocalRef(jobj_key);
      return;
    }

    const char* value = env->GetStringUTFChars(jval, nullptr);
    if (value == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ReleaseStringUTFChars(jkey, key);
      env->DeleteLocalRef(jobj_value);
      env->DeleteLocalRef(jobj_key);
      return;
    }

    std::string s_key(key);
    std::string s_value(value);
    options_map[s_key] = s_value;

    env->ReleaseStringUTFChars(jkey, key);
    env->ReleaseStringUTFChars(jval, value);
    env->DeleteLocalRef(jobj_key);
    env->DeleteLocalRef(jobj_value);
  }

  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  db->SetOptions(cf_handle, options_map);
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::IngestExternalFile

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    ingestExternalFile
 * Signature: (JJ[Ljava/lang/String;IJ)V
 */
void Java_org_rocksdb_RocksDB_ingestExternalFile(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jcf_handle,
    jobjectArray jfile_path_list, jint jfile_path_list_len,
    jlong jingest_external_file_options_handle) {
  jboolean has_exception = JNI_FALSE;
  std::vector<std::string> file_path_list = rocksdb::JniUtil::copyStrings(
      env, jfile_path_list, jfile_path_list_len, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }

  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* column_family =
      reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  auto* ifo = reinterpret_cast<rocksdb::IngestExternalFileOptions*>(
      jingest_external_file_options_handle);
  rocksdb::Status s =
      db->IngestExternalFile(column_family, file_path_list, *ifo);
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    destroyDB
 * Signature: (Ljava/lang/String;J)V
 */
void Java_org_rocksdb_RocksDB_destroyDB(JNIEnv* env, jclass /*jcls*/,
                                        jstring jdb_path,
                                        jlong joptions_handle) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* options = reinterpret_cast<rocksdb::Options*>(joptions_handle);
  if (options == nullptr) {
    rocksdb::RocksDBExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Invalid Options."));
  }

  rocksdb::Status s = rocksdb::DestroyDB(db_path, *options);
  env->ReleaseStringUTFChars(jdb_path, db_path);

  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}
