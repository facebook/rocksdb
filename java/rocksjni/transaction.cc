// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++
// for ROCKSDB_NAMESPACE::Transaction.

#include "rocksdb/utilities/transaction.h"

#include <jni.h>

#include <functional>

#include "include/org_rocksdb_Transaction.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/jni_multiget_helpers.h"
#include "rocksjni/kv_helper.h"
#include "rocksjni/portal.h"

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4503)  // identifier' : decorated name length
                                 // exceeded, name was truncated
#endif

/*
 * Class:     org_rocksdb_Transaction
 * Method:    setSnapshot
 * Signature: (J)V
 */
void Java_org_rocksdb_Transaction_setSnapshot(JNIEnv* /*env*/, jclass /*jcls*/,
                                              jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  txn->SetSnapshot();
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    setSnapshotOnNextOperation
 * Signature: (J)V
 */
void Java_org_rocksdb_Transaction_setSnapshotOnNextOperation__J(JNIEnv* /*env*/,
                                                                jclass /*jcls*/,
                                                                jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  txn->SetSnapshotOnNextOperation(nullptr);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    setSnapshotOnNextOperation
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Transaction_setSnapshotOnNextOperation__JJ(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle,
    jlong jtxn_notifier_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* txn_notifier = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::TransactionNotifierJniCallback>*>(
      jtxn_notifier_handle);
  txn->SetSnapshotOnNextOperation(*txn_notifier);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getSnapshot
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Transaction_getSnapshot(JNIEnv* /*env*/, jclass /*jcls*/,
                                               jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  const ROCKSDB_NAMESPACE::Snapshot* snapshot = txn->GetSnapshot();
  return GET_CPLUSPLUS_POINTER(snapshot);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    clearSnapshot
 * Signature: (J)V
 */
void Java_org_rocksdb_Transaction_clearSnapshot(JNIEnv* /*env*/,
                                                jclass /*jcls*/,
                                                jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  txn->ClearSnapshot();
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    prepare
 * Signature: (J)V
 */
void Java_org_rocksdb_Transaction_prepare(JNIEnv* env, jclass /*jobj*/,
                                          jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  ROCKSDB_NAMESPACE::Status s = txn->Prepare();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    commit
 * Signature: (J)V
 */
void Java_org_rocksdb_Transaction_commit(JNIEnv* env, jclass /*jobj*/,
                                         jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  ROCKSDB_NAMESPACE::Status s = txn->Commit();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    rollback
 * Signature: (J)V
 */
void Java_org_rocksdb_Transaction_rollback(JNIEnv* env, jclass /*jobj*/,
                                           jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  ROCKSDB_NAMESPACE::Status s = txn->Rollback();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    setSavePoint
 * Signature: (J)V
 */
void Java_org_rocksdb_Transaction_setSavePoint(JNIEnv* /*env*/, jclass /*jcls*/,
                                               jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  txn->SetSavePoint();
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    rollbackToSavePoint
 * Signature: (J)V
 */
void Java_org_rocksdb_Transaction_rollbackToSavePoint(JNIEnv* env,
                                                      jclass /*jcls*/,
                                                      jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  ROCKSDB_NAMESPACE::Status s = txn->RollbackToSavePoint();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

typedef std::function<ROCKSDB_NAMESPACE::Status(
    const ROCKSDB_NAMESPACE::ReadOptions&, const ROCKSDB_NAMESPACE::Slice&,
    std::string*)>
    FnGet;

/*
 * Class:     org_rocksdb_Transaction
 * Method:    get
 * Signature: (JJ[BIIJ)[B
 */
jbyteArray Java_org_rocksdb_Transaction_get__JJ_3BIIJ(
    JNIEnv* env, jclass, jlong jhandle, jlong jread_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_part_len,
    jlong jcolumn_family_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* read_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  try {
    ROCKSDB_NAMESPACE::JByteArraySlice key(env, jkey, jkey_off, jkey_part_len);
    ROCKSDB_NAMESPACE::JByteArrayPinnableSlice value(env);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->Get(*read_options, column_family_handle, key.slice(),
                      &value.pinnable_slice()));
    return value.NewByteArray();
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    get
 * Signature: (JJ[BII)[B
 */
jbyteArray Java_org_rocksdb_Transaction_get__JJ_3BII(
    JNIEnv* env, jclass, jlong jhandle, jlong jread_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_part_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* read_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  try {
    ROCKSDB_NAMESPACE::JByteArraySlice key(env, jkey, jkey_off, jkey_part_len);
    ROCKSDB_NAMESPACE::JByteArrayPinnableSlice value(env);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->Get(*read_options, key.slice(), &value.pinnable_slice()));
    return value.NewByteArray();
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    get
 * Signature: (JJ[BII[BIIJ)I
 */
jint Java_org_rocksdb_Transaction_get__JJ_3BII_3BIIJ(
    JNIEnv* env, jclass, jlong jhandle, jlong jread_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_part_len, jbyteArray jval,
    jint jval_off, jint jval_part_len, jlong jcolumn_family_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* read_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  try {
    ROCKSDB_NAMESPACE::JByteArraySlice key(env, jkey, jkey_off, jkey_part_len);
    ROCKSDB_NAMESPACE::JByteArrayPinnableSlice value(env, jval, jval_off,
                                                     jval_part_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->Get(*read_options, column_family_handle, key.slice(),
                      &value.pinnable_slice()));
    return value.Fetch();
  } catch (ROCKSDB_NAMESPACE::KVException& e) {
    return e.Code();
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getDirect
 * Signature: (JJLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJ)I
 */
jint Java_org_rocksdb_Transaction_getDirect(JNIEnv* env, jclass, jlong jhandle,
                                            jlong jread_options_handle,
                                            jobject jkey_bb, jint jkey_off,
                                            jint jkey_part_len, jobject jval_bb,
                                            jint jval_off, jint jval_part_len,
                                            jlong jcolumn_family_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* read_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);

  try {
    ROCKSDB_NAMESPACE::JDirectBufferSlice key(env, jkey_bb, jkey_off,
                                              jkey_part_len);
    ROCKSDB_NAMESPACE::JDirectBufferPinnableSlice value(env, jval_bb, jval_off,
                                                        jval_part_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->Get(*read_options, column_family_handle, key.slice(),
                      &value.pinnable_slice()));
    return value.Fetch();
  } catch (const ROCKSDB_NAMESPACE::KVException& e) {
    return e.Code();
  }
}

typedef std::function<std::vector<ROCKSDB_NAMESPACE::Status>(
    const ROCKSDB_NAMESPACE::ReadOptions&,
    const std::vector<ROCKSDB_NAMESPACE::Slice>&, std::vector<std::string>*)>
    FnMultiGet;

void free_parts(
    JNIEnv* env,
    std::vector<std::tuple<jbyteArray, jbyte*, jobject>>& parts_to_free) {
  for (auto& value : parts_to_free) {
    jobject jk;
    jbyteArray jk_ba;
    jbyte* jk_val;
    std::tie(jk_ba, jk_val, jk) = value;
    env->ReleaseByteArrayElements(jk_ba, jk_val, JNI_ABORT);
    env->DeleteLocalRef(jk);
  }
}

void free_key_values(std::vector<jbyte*>& keys_to_free) {
  for (auto& key : keys_to_free) {
    delete[] key;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    multiGet
 * Signature: (JJ[[B[J)[[B
 */
jobjectArray Java_org_rocksdb_Transaction_multiGet__JJ_3_3B_3J(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jlong jread_options_handle,
    jobjectArray jkey_parts, jlongArray jcolumn_family_handles) {
  ROCKSDB_NAMESPACE::MultiGetJNIKeys keys;
  if (!keys.fromByteArrays(env, jkey_parts)) {
    return nullptr;
  }
  auto cf_handles =
      ROCKSDB_NAMESPACE::ColumnFamilyJNIHelpers::handlesFromJLongArray(
          env, jcolumn_family_handles);
  if (!cf_handles) return nullptr;
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  std::vector<std::string> values(keys.size());
  std::vector<ROCKSDB_NAMESPACE::Status> statuses = txn->MultiGet(
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle),
      *cf_handles, keys.slices(), &values);

  return ROCKSDB_NAMESPACE::MultiGetJNIValues::byteArrays(env, values,
                                                          statuses);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    multiGet
 * Signature: (JJ[[B)[[B
 */
jobjectArray Java_org_rocksdb_Transaction_multiGet__JJ_3_3B(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jlong jread_options_handle,
    jobjectArray jkey_parts) {
  ROCKSDB_NAMESPACE::MultiGetJNIKeys keys;
  if (!keys.fromByteArrays(env, jkey_parts)) {
    return nullptr;
  }

  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  std::vector<std::string> values(keys.size());
  std::vector<ROCKSDB_NAMESPACE::Status> statuses = txn->MultiGet(
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle),
      keys.slices(), &values);

  return ROCKSDB_NAMESPACE::MultiGetJNIValues::byteArrays(env, values,
                                                          statuses);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getForUpdate
 * Signature: (JJ[BIIJZZ)[B
 */
jbyteArray Java_org_rocksdb_Transaction_getForUpdate__JJ_3BIIJZZ(
    JNIEnv* env, jclass, jlong jhandle, jlong jread_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_part_len,
    jlong jcolumn_family_handle, jboolean jexclusive, jboolean jdo_validate) {
  auto* read_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  try {
    ROCKSDB_NAMESPACE::JByteArraySlice key(env, jkey, jkey_off, jkey_part_len);
    ROCKSDB_NAMESPACE::JByteArrayPinnableSlice value(env);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env,
        txn->GetForUpdate(*read_options, column_family_handle, key.slice(),
                          &value.pinnable_slice(), jexclusive, jdo_validate));
    return value.NewByteArray();
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getForUpdate
 * Signature: (JJ[BII[BIIJZZ)I
 */
jint Java_org_rocksdb_Transaction_getForUpdate__JJ_3BII_3BIIJZZ(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jlong jread_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_part_len, jbyteArray jval,
    jint jval_off, jint jval_len, jlong jcolumn_family_handle,
    jboolean jexclusive, jboolean jdo_validate) {
  auto* read_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  try {
    ROCKSDB_NAMESPACE::JByteArraySlice key(env, jkey, jkey_off, jkey_part_len);
    ROCKSDB_NAMESPACE::JByteArrayPinnableSlice value(env, jval, jval_off,
                                                     jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env,
        txn->GetForUpdate(*read_options, column_family_handle, key.slice(),
                          &value.pinnable_slice(), jexclusive, jdo_validate));
    return value.Fetch();
  } catch (ROCKSDB_NAMESPACE::KVException& e) {
    return e.Code();
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getDirect
 * Signature: (JJLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJZZ)I
 */
jint Java_org_rocksdb_Transaction_getDirect(
    JNIEnv* env, jobject /*jobj*/, jlong jhandle, jlong jread_options_handle,
    jobject jkey_bb, jint jkey_off, jint jkey_part_len, jobject jval_bb,
    jint jval_off, jint jval_len, jlong jcolumn_family_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* read_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);

  try {
    ROCKSDB_NAMESPACE::JDirectBufferSlice key(env, jkey_bb, jkey_off,
                                              jkey_part_len);
    ROCKSDB_NAMESPACE::JDirectBufferPinnableSlice value(env, jval_bb, jval_off,
                                                        jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->Get(*read_options, column_family_handle, key.slice(),
                      &value.pinnable_slice()));
    return value.Fetch();
  } catch (const ROCKSDB_NAMESPACE::KVException& e) {
    return e.Code();
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getDirectForUpdate
 * Signature: (JJLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJZZ)I
 */
jint Java_org_rocksdb_Transaction_getDirectForUpdate(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jlong jread_options_handle,
    jobject jkey_bb, jint jkey_off, jint jkey_part_len, jobject jval_bb,
    jint jval_off, jint jval_len, jlong jcolumn_family_handle,
    jboolean jexclusive, jboolean jdo_validate) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* read_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);

  try {
    ROCKSDB_NAMESPACE::JDirectBufferSlice key(env, jkey_bb, jkey_off,
                                              jkey_part_len);
    ROCKSDB_NAMESPACE::JDirectBufferPinnableSlice value(env, jval_bb, jval_off,
                                                        jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env,
        txn->GetForUpdate(*read_options, column_family_handle, key.slice(),
                          &value.pinnable_slice(), jexclusive, jdo_validate));
    return value.Fetch();
  } catch (const ROCKSDB_NAMESPACE::KVException& e) {
    return e.Code();
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    multiGetForUpdate
 * Signature: (JJ[[B[J)[[B
 */
jobjectArray Java_org_rocksdb_Transaction_multiGetForUpdate__JJ_3_3B_3J(
    JNIEnv* env, jclass, jlong jhandle, jlong jread_options_handle,
    jobjectArray jkey_parts, jlongArray jcolumn_family_handles) {
  ROCKSDB_NAMESPACE::MultiGetJNIKeys keys;
  if (!keys.fromByteArrays(env, jkey_parts)) {
    return nullptr;
  }
  auto cf_handles =
      ROCKSDB_NAMESPACE::ColumnFamilyJNIHelpers::handlesFromJLongArray(
          env, jcolumn_family_handles);
  if (!cf_handles) return nullptr;
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  std::vector<std::string> values(keys.size());
  std::vector<ROCKSDB_NAMESPACE::Status> statuses = txn->MultiGetForUpdate(
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle),
      *cf_handles, keys.slices(), &values);

  return ROCKSDB_NAMESPACE::MultiGetJNIValues::byteArrays(env, values,
                                                          statuses);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    multiGetForUpdate
 * Signature: (JJ[[B)[[B
 */
jobjectArray Java_org_rocksdb_Transaction_multiGetForUpdate__JJ_3_3B(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jlong jread_options_handle,
    jobjectArray jkey_parts) {
  ROCKSDB_NAMESPACE::MultiGetJNIKeys keys;
  if (!keys.fromByteArrays(env, jkey_parts)) {
    return nullptr;
  }

  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  std::vector<std::string> values(keys.size());
  std::vector<ROCKSDB_NAMESPACE::Status> statuses = txn->MultiGetForUpdate(
      *reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle),
      keys.slices(), &values);

  return ROCKSDB_NAMESPACE::MultiGetJNIValues::byteArrays(env, values,
                                                          statuses);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getIterator
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_Transaction_getIterator(JNIEnv* /*env*/, jclass,
                                               jlong jhandle,
                                               jlong jread_options_handle,
                                               jlong jcolumn_family_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* read_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  return GET_CPLUSPLUS_POINTER(
      txn->GetIterator(*read_options, column_family_handle));
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    put
 * Signature: (J[BII[BIIJZ)V
 */
void Java_org_rocksdb_Transaction_put__J_3BII_3BIIJZ(
    JNIEnv* env, jclass, jlong jhandle, jbyteArray jkey, jint jkey_off,
    jint jkey_part_len, jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcolumn_family_handle, jboolean jassume_tracked) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  try {
    ROCKSDB_NAMESPACE::JByteArraySlice key(env, jkey, jkey_off, jkey_part_len);
    ROCKSDB_NAMESPACE::JByteArraySlice value(env, jval, jval_off, jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->Put(column_family_handle, key.slice(), value.slice(),
                      jassume_tracked));
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    put
 * Signature: (J[BII[BII)V
 */
void Java_org_rocksdb_Transaction_put__J_3BII_3BII(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jbyteArray jkey, jint jkey_off,
    jint jkey_part_len, jbyteArray jval, jint jval_off, jint jval_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  try {
    ROCKSDB_NAMESPACE::JByteArraySlice key(env, jkey, jkey_off, jkey_part_len);
    ROCKSDB_NAMESPACE::JByteArraySlice value(env, jval, jval_off, jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->Put(key.slice(), value.slice()));
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    putDirect
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJZ)V
 */
void Java_org_rocksdb_Transaction_putDirect__JLjava_nio_ByteBuffer_2IILjava_nio_ByteBuffer_2IIJZ(
    JNIEnv* env, jclass, jlong jhandle, jobject jkey_bb, jint jkey_off,
    jint jkey_len, jobject jval_bb, jint jval_off, jint jval_len,
    jlong jcolumn_family_handle, jboolean jassume_tracked) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  try {
    ROCKSDB_NAMESPACE::JDirectBufferSlice key(env, jkey_bb, jkey_off, jkey_len);
    ROCKSDB_NAMESPACE::JDirectBufferSlice value(env, jval_bb, jval_off,
                                                jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->Put(column_family_handle, key.slice(), value.slice(),
                      jassume_tracked));
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    putDirect
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;II)V
 */
void Java_org_rocksdb_Transaction_putDirect__JLjava_nio_ByteBuffer_2IILjava_nio_ByteBuffer_2II(
    JNIEnv* env, jclass, jlong jhandle, jobject jkey_bb, jint jkey_off,
    jint jkey_len, jobject jval_bb, jint jval_off, jint jval_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  try {
    ROCKSDB_NAMESPACE::JDirectBufferSlice key(env, jkey_bb, jkey_off, jkey_len);
    ROCKSDB_NAMESPACE::JDirectBufferSlice value(env, jval_bb, jval_off,
                                                jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->Put(key.slice(), value.slice()));
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return;
  }
}

typedef std::function<ROCKSDB_NAMESPACE::Status(
    const ROCKSDB_NAMESPACE::SliceParts&, const ROCKSDB_NAMESPACE::SliceParts&)>
    FnWriteKVParts;

// TODO(AR) consider refactoring to share this between here and rocksjni.cc
void txn_write_kv_parts_helper(JNIEnv* env,
                               const FnWriteKVParts& fn_write_kv_parts,
                               const jobjectArray& jkey_parts,
                               const jint& jkey_parts_len,
                               const jobjectArray& jvalue_parts,
                               const jint& jvalue_parts_len) {
#ifndef DEBUG
  (void)jvalue_parts_len;
#else
  assert(jkey_parts_len == jvalue_parts_len);
#endif

  auto key_parts = std::vector<ROCKSDB_NAMESPACE::Slice>();
  auto value_parts = std::vector<ROCKSDB_NAMESPACE::Slice>();
  auto jparts_to_free = std::vector<std::tuple<jbyteArray, jbyte*, jobject>>();

  // Since this is fundamentally a gather write at the RocksDB level,
  // it seems wrong to refactor it by copying (gathering) keys and data here,
  // in order to avoid the local reference limit.
  // The user needs to be a aware that there is a limit to the number of parts
  // which can be gathered.
  if (env->EnsureLocalCapacity(jkey_parts_len + jvalue_parts_len) != 0) {
    // no space for all the jobjects we store up
    env->ExceptionClear();
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, "Insufficient JNI local references for " +
                 std::to_string(jkey_parts_len) + " key/value parts");
    return;
  }

  // convert java key_parts/value_parts byte[][] to Slice(s)
  for (jsize i = 0; i < jkey_parts_len; ++i) {
    const jobject jobj_key_part = env->GetObjectArrayElement(jkey_parts, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      free_parts(env, jparts_to_free);
      return;
    }
    const jobject jobj_value_part = env->GetObjectArrayElement(jvalue_parts, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jobj_key_part);
      free_parts(env, jparts_to_free);
      return;
    }

    const jbyteArray jba_key_part = reinterpret_cast<jbyteArray>(jobj_key_part);
    const jsize jkey_part_len = env->GetArrayLength(jba_key_part);
    jbyte* jkey_part = env->GetByteArrayElements(jba_key_part, nullptr);
    if (jkey_part == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jobj_value_part);
      env->DeleteLocalRef(jobj_key_part);
      free_parts(env, jparts_to_free);
      return;
    }

    const jbyteArray jba_value_part =
        reinterpret_cast<jbyteArray>(jobj_value_part);
    const jsize jvalue_part_len = env->GetArrayLength(jba_value_part);
    jbyte* jvalue_part = env->GetByteArrayElements(jba_value_part, nullptr);
    if (jvalue_part == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jobj_value_part);
      env->DeleteLocalRef(jobj_key_part);
      env->ReleaseByteArrayElements(jba_key_part, jkey_part, JNI_ABORT);
      free_parts(env, jparts_to_free);
      return;
    }

    jparts_to_free.push_back(
        std::make_tuple(jba_key_part, jkey_part, jobj_key_part));
    jparts_to_free.push_back(
        std::make_tuple(jba_value_part, jvalue_part, jobj_value_part));

    key_parts.push_back(ROCKSDB_NAMESPACE::Slice(
        reinterpret_cast<char*>(jkey_part), jkey_part_len));
    value_parts.push_back(ROCKSDB_NAMESPACE::Slice(
        reinterpret_cast<char*>(jvalue_part), jvalue_part_len));
  }

  // call the write_multi function
  ROCKSDB_NAMESPACE::Status s = fn_write_kv_parts(
      ROCKSDB_NAMESPACE::SliceParts(key_parts.data(), (int)key_parts.size()),
      ROCKSDB_NAMESPACE::SliceParts(value_parts.data(),
                                    (int)value_parts.size()));

  // cleanup temporary memory
  free_parts(env, jparts_to_free);

  // return
  if (s.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    put
 * Signature: (J[[BI[[BIJZ)V
 */
void Java_org_rocksdb_Transaction_put__J_3_3BI_3_3BIJZ(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jobjectArray jkey_parts,
    jint jkey_parts_len, jobjectArray jvalue_parts, jint jvalue_parts_len,
    jlong jcolumn_family_handle, jboolean jassume_tracked) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  FnWriteKVParts fn_put_parts =
      std::bind<ROCKSDB_NAMESPACE::Status (ROCKSDB_NAMESPACE::Transaction::*)(
          ROCKSDB_NAMESPACE::ColumnFamilyHandle*,
          const ROCKSDB_NAMESPACE::SliceParts&,
          const ROCKSDB_NAMESPACE::SliceParts&, bool)>(
          &ROCKSDB_NAMESPACE::Transaction::Put, txn, column_family_handle,
          std::placeholders::_1, std::placeholders::_2, jassume_tracked);
  txn_write_kv_parts_helper(env, fn_put_parts, jkey_parts, jkey_parts_len,
                            jvalue_parts, jvalue_parts_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    put
 * Signature: (J[[BI[[BI)V
 */
void Java_org_rocksdb_Transaction_put__J_3_3BI_3_3BI(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jobjectArray jkey_parts,
    jint jkey_parts_len, jobjectArray jvalue_parts, jint jvalue_parts_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  FnWriteKVParts fn_put_parts = std::bind<ROCKSDB_NAMESPACE::Status (
      ROCKSDB_NAMESPACE::Transaction::*)(const ROCKSDB_NAMESPACE::SliceParts&,
                                         const ROCKSDB_NAMESPACE::SliceParts&)>(
      &ROCKSDB_NAMESPACE::Transaction::Put, txn, std::placeholders::_1,
      std::placeholders::_2);
  txn_write_kv_parts_helper(env, fn_put_parts, jkey_parts, jkey_parts_len,
                            jvalue_parts, jvalue_parts_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    merge
 * Signature: (J[BII[BIIJZ)V
 */
void Java_org_rocksdb_Transaction_merge__J_3BII_3BIIJZ(
    JNIEnv* env, jclass, jlong jhandle, jbyteArray jkey, jint jkey_off,
    jint jkey_part_len, jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcolumn_family_handle, jboolean jassume_tracked) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  try {
    ROCKSDB_NAMESPACE::JByteArraySlice key(env, jkey, jkey_off, jkey_part_len);
    ROCKSDB_NAMESPACE::JByteArraySlice value(env, jval, jval_off, jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->Merge(column_family_handle, key.slice(), value.slice(),
                        jassume_tracked));
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    merge
 * Signature: (J[BII[BII)V
 */
void Java_org_rocksdb_Transaction_merge__J_3BII_3BII(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jbyteArray jkey, jint jkey_off,
    jint jkey_part_len, jbyteArray jval, jint jval_off, jint jval_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  try {
    ROCKSDB_NAMESPACE::JByteArraySlice key(env, jkey, jkey_off, jkey_part_len);
    ROCKSDB_NAMESPACE::JByteArraySlice value(env, jval, jval_off, jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->Merge(key.slice(), value.slice()));
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    mergeDirect
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJZ)V
 */
JNIEXPORT void JNICALL
Java_org_rocksdb_Transaction_mergeDirect__JLjava_nio_ByteBuffer_2IILjava_nio_ByteBuffer_2IIJZ(
    JNIEnv* env, jclass, jlong jhandle, jobject jkey_bb, jint jkey_off,
    jint jkey_len, jobject jval_bb, jint jval_off, jint jval_len,
    jlong jcolumn_family_handle, jboolean jassume_tracked) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  try {
    ROCKSDB_NAMESPACE::JDirectBufferSlice key(env, jkey_bb, jkey_off, jkey_len);
    ROCKSDB_NAMESPACE::JDirectBufferSlice value(env, jval_bb, jval_off,
                                                jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->Merge(column_family_handle, key.slice(), value.slice(),
                        jassume_tracked));
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    mergeDirect
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;II)V
 */
JNIEXPORT void JNICALL
Java_org_rocksdb_Transaction_mergeDirect__JLjava_nio_ByteBuffer_2IILjava_nio_ByteBuffer_2II(
    JNIEnv* env, jclass, jlong jhandle, jobject jkey_bb, jint jkey_off,
    jint jkey_len, jobject jval_bb, jint jval_off, jint jval_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  try {
    ROCKSDB_NAMESPACE::JDirectBufferSlice key(env, jkey_bb, jkey_off, jkey_len);
    ROCKSDB_NAMESPACE::JDirectBufferSlice value(env, jval_bb, jval_off,
                                                jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->Merge(key.slice(), value.slice()));
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return;
  }
}

typedef std::function<ROCKSDB_NAMESPACE::Status(
    const ROCKSDB_NAMESPACE::Slice&)>
    FnWriteK;

// TODO(AR) consider refactoring to share this between here and rocksjni.cc
void txn_write_k_helper(JNIEnv* env, const FnWriteK& fn_write_k,
                        const jbyteArray& jkey, const jint& jkey_part_len) {
  jbyte* key = env->GetByteArrayElements(jkey, nullptr);
  if (key == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key),
                                     jkey_part_len);

  ROCKSDB_NAMESPACE::Status s = fn_write_k(key_slice);

  // trigger java unref on key.
  // by passing JNI_ABORT, it will simply release the reference without
  // copying the result back to the java byte array.
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);

  if (s.ok()) {
    return;
  }
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    delete
 * Signature: (J[BIJZ)V
 */
void Java_org_rocksdb_Transaction_delete__J_3BIJZ(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jbyteArray jkey,
    jint jkey_part_len, jlong jcolumn_family_handle, jboolean jassume_tracked) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  FnWriteK fn_delete =
      std::bind<ROCKSDB_NAMESPACE::Status (ROCKSDB_NAMESPACE::Transaction::*)(
          ROCKSDB_NAMESPACE::ColumnFamilyHandle*,
          const ROCKSDB_NAMESPACE::Slice&, bool)>(
          &ROCKSDB_NAMESPACE::Transaction::Delete, txn, column_family_handle,
          std::placeholders::_1, jassume_tracked);
  txn_write_k_helper(env, fn_delete, jkey, jkey_part_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    delete
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_Transaction_delete__J_3BI(JNIEnv* env, jclass /*jobj*/,
                                                jlong jhandle, jbyteArray jkey,
                                                jint jkey_part_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  FnWriteK fn_delete = std::bind<ROCKSDB_NAMESPACE::Status (
      ROCKSDB_NAMESPACE::Transaction::*)(const ROCKSDB_NAMESPACE::Slice&)>(
      &ROCKSDB_NAMESPACE::Transaction::Delete, txn, std::placeholders::_1);
  txn_write_k_helper(env, fn_delete, jkey, jkey_part_len);
}

typedef std::function<ROCKSDB_NAMESPACE::Status(
    const ROCKSDB_NAMESPACE::SliceParts&)>
    FnWriteKParts;

// TODO(AR) consider refactoring to share this between here and rocksjni.cc
void txn_write_k_parts_helper(JNIEnv* env,
                              const FnWriteKParts& fn_write_k_parts,
                              const jobjectArray& jkey_parts,
                              const jint& jkey_parts_len) {
  std::vector<ROCKSDB_NAMESPACE::Slice> key_parts;
  std::vector<std::tuple<jbyteArray, jbyte*, jobject>> jkey_parts_to_free;

  // convert java key_parts byte[][] to Slice(s)
  for (jint i = 0; i < jkey_parts_len; ++i) {
    const jobject jobj_key_part = env->GetObjectArrayElement(jkey_parts, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      free_parts(env, jkey_parts_to_free);
      return;
    }

    const jbyteArray jba_key_part = reinterpret_cast<jbyteArray>(jobj_key_part);
    const jsize jkey_part_len = env->GetArrayLength(jba_key_part);
    jbyte* jkey_part = env->GetByteArrayElements(jba_key_part, nullptr);
    if (jkey_part == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jobj_key_part);
      free_parts(env, jkey_parts_to_free);
      return;
    }

    jkey_parts_to_free.push_back(std::tuple<jbyteArray, jbyte*, jobject>(
        jba_key_part, jkey_part, jobj_key_part));

    key_parts.push_back(ROCKSDB_NAMESPACE::Slice(
        reinterpret_cast<char*>(jkey_part), jkey_part_len));
  }

  // call the write_multi function
  ROCKSDB_NAMESPACE::Status s = fn_write_k_parts(
      ROCKSDB_NAMESPACE::SliceParts(key_parts.data(), (int)key_parts.size()));

  // cleanup temporary memory
  free_parts(env, jkey_parts_to_free);

  // return
  if (s.ok()) {
    return;
  }
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    delete
 * Signature: (J[[BIJZ)V
 */
void Java_org_rocksdb_Transaction_delete__J_3_3BIJZ(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jobjectArray jkey_parts,
    jint jkey_parts_len, jlong jcolumn_family_handle,
    jboolean jassume_tracked) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  FnWriteKParts fn_delete_parts =
      std::bind<ROCKSDB_NAMESPACE::Status (ROCKSDB_NAMESPACE::Transaction::*)(
          ROCKSDB_NAMESPACE::ColumnFamilyHandle*,
          const ROCKSDB_NAMESPACE::SliceParts&, bool)>(
          &ROCKSDB_NAMESPACE::Transaction::Delete, txn, column_family_handle,
          std::placeholders::_1, jassume_tracked);
  txn_write_k_parts_helper(env, fn_delete_parts, jkey_parts, jkey_parts_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    delete
 * Signature: (J[[BI)V
 */
void Java_org_rocksdb_Transaction_delete__J_3_3BI(JNIEnv* env, jclass /*jobj*/,
                                                  jlong jhandle,
                                                  jobjectArray jkey_parts,
                                                  jint jkey_parts_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  FnWriteKParts fn_delete_parts = std::bind<ROCKSDB_NAMESPACE::Status (
      ROCKSDB_NAMESPACE::Transaction::*)(const ROCKSDB_NAMESPACE::SliceParts&)>(
      &ROCKSDB_NAMESPACE::Transaction::Delete, txn, std::placeholders::_1);
  txn_write_k_parts_helper(env, fn_delete_parts, jkey_parts, jkey_parts_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    singleDelete
 * Signature: (J[BIJZ)V
 */
void Java_org_rocksdb_Transaction_singleDelete__J_3BIJZ(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jbyteArray jkey,
    jint jkey_part_len, jlong jcolumn_family_handle, jboolean jassume_tracked) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  FnWriteK fn_single_delete =
      std::bind<ROCKSDB_NAMESPACE::Status (ROCKSDB_NAMESPACE::Transaction::*)(
          ROCKSDB_NAMESPACE::ColumnFamilyHandle*,
          const ROCKSDB_NAMESPACE::Slice&, bool)>(
          &ROCKSDB_NAMESPACE::Transaction::SingleDelete, txn,
          column_family_handle, std::placeholders::_1, jassume_tracked);
  txn_write_k_helper(env, fn_single_delete, jkey, jkey_part_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    singleDelete
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_Transaction_singleDelete__J_3BI(JNIEnv* env,
                                                      jclass /*jcls*/,
                                                      jlong jhandle,
                                                      jbyteArray jkey,
                                                      jint jkey_part_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  FnWriteK fn_single_delete = std::bind<ROCKSDB_NAMESPACE::Status (
      ROCKSDB_NAMESPACE::Transaction::*)(const ROCKSDB_NAMESPACE::Slice&)>(
      &ROCKSDB_NAMESPACE::Transaction::SingleDelete, txn,
      std::placeholders::_1);
  txn_write_k_helper(env, fn_single_delete, jkey, jkey_part_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    singleDelete
 * Signature: (J[[BIJZ)V
 */
void Java_org_rocksdb_Transaction_singleDelete__J_3_3BIJZ(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jobjectArray jkey_parts,
    jint jkey_parts_len, jlong jcolumn_family_handle,
    jboolean jassume_tracked) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  FnWriteKParts fn_single_delete_parts =
      std::bind<ROCKSDB_NAMESPACE::Status (ROCKSDB_NAMESPACE::Transaction::*)(
          ROCKSDB_NAMESPACE::ColumnFamilyHandle*,
          const ROCKSDB_NAMESPACE::SliceParts&, bool)>(
          &ROCKSDB_NAMESPACE::Transaction::SingleDelete, txn,
          column_family_handle, std::placeholders::_1, jassume_tracked);
  txn_write_k_parts_helper(env, fn_single_delete_parts, jkey_parts,
                           jkey_parts_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    singleDelete
 * Signature: (J[[BI)V
 */
void Java_org_rocksdb_Transaction_singleDelete__J_3_3BI(JNIEnv* env,
                                                        jclass /*jcls*/,
                                                        jlong jhandle,
                                                        jobjectArray jkey_parts,
                                                        jint jkey_parts_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  FnWriteKParts fn_single_delete_parts = std::bind<ROCKSDB_NAMESPACE::Status (
      ROCKSDB_NAMESPACE::Transaction::*)(const ROCKSDB_NAMESPACE::SliceParts&)>(
      &ROCKSDB_NAMESPACE::Transaction::SingleDelete, txn,
      std::placeholders::_1);
  txn_write_k_parts_helper(env, fn_single_delete_parts, jkey_parts,
                           jkey_parts_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    putUntracked
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_Transaction_putUntracked__J_3BI_3BIJ(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jbyteArray jkey,
    jint jkey_part_len, jbyteArray jval, jint jval_len,
    jlong jcolumn_family_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  try {
    ROCKSDB_NAMESPACE::JByteArraySlice key(env, jkey, 0, jkey_part_len);
    ROCKSDB_NAMESPACE::JByteArraySlice value(env, jval, 0, jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env,
        txn->PutUntracked(column_family_handle, key.slice(), value.slice()));
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    putUntracked
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_Transaction_putUntracked__J_3BI_3BI(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jbyteArray jkey,
    jint jkey_part_len, jbyteArray jval, jint jval_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  try {
    ROCKSDB_NAMESPACE::JByteArraySlice key(env, jkey, 0, jkey_part_len);
    ROCKSDB_NAMESPACE::JByteArraySlice value(env, jval, 0, jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env, txn->PutUntracked(key.slice(), value.slice()));
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    putUntracked
 * Signature: (J[[BI[[BIJ)V
 */
void Java_org_rocksdb_Transaction_putUntracked__J_3_3BI_3_3BIJ(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jobjectArray jkey_parts,
    jint jkey_parts_len, jobjectArray jvalue_parts, jint jvalue_parts_len,
    jlong jcolumn_family_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  FnWriteKVParts fn_put_parts_untracked = std::bind<ROCKSDB_NAMESPACE::Status (
      ROCKSDB_NAMESPACE::Transaction::*)(ROCKSDB_NAMESPACE::ColumnFamilyHandle*,
                                         const ROCKSDB_NAMESPACE::SliceParts&,
                                         const ROCKSDB_NAMESPACE::SliceParts&)>(
      &ROCKSDB_NAMESPACE::Transaction::PutUntracked, txn, column_family_handle,
      std::placeholders::_1, std::placeholders::_2);
  txn_write_kv_parts_helper(env, fn_put_parts_untracked, jkey_parts,
                            jkey_parts_len, jvalue_parts, jvalue_parts_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    putUntracked
 * Signature: (J[[BI[[BI)V
 */
void Java_org_rocksdb_Transaction_putUntracked__J_3_3BI_3_3BI(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jobjectArray jkey_parts,
    jint jkey_parts_len, jobjectArray jvalue_parts, jint jvalue_parts_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  FnWriteKVParts fn_put_parts_untracked = std::bind<ROCKSDB_NAMESPACE::Status (
      ROCKSDB_NAMESPACE::Transaction::*)(const ROCKSDB_NAMESPACE::SliceParts&,
                                         const ROCKSDB_NAMESPACE::SliceParts&)>(
      &ROCKSDB_NAMESPACE::Transaction::PutUntracked, txn, std::placeholders::_1,
      std::placeholders::_2);
  txn_write_kv_parts_helper(env, fn_put_parts_untracked, jkey_parts,
                            jkey_parts_len, jvalue_parts, jvalue_parts_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    mergeUntracked
 * Signature: (J[BII[BIIJ)V
 */
void Java_org_rocksdb_Transaction_mergeUntracked(
    JNIEnv* env, jclass, jlong jhandle, jbyteArray jkey, jint jkey_off,
    jint jkey_part_len, jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcolumn_family_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  try {
    ROCKSDB_NAMESPACE::JByteArraySlice key(env, jkey, jkey_off, jkey_part_len);
    ROCKSDB_NAMESPACE::JByteArraySlice value(env, jval, jval_off, jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env,
        txn->MergeUntracked(column_family_handle, key.slice(), value.slice()));
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    mergeUntrackedDirect
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_rocksdb_Transaction_mergeUntrackedDirect(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jobject jkey, jint jkey_off,
    jint jkey_part_len, jobject jval, jint jval_off, jint jval_len,
    jlong jcolumn_family_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  try {
    ROCKSDB_NAMESPACE::JDirectBufferSlice key(env, jkey, jkey_off,
                                              jkey_part_len);
    ROCKSDB_NAMESPACE::JDirectBufferSlice value(env, jval, jval_off, jval_len);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(
        env,
        txn->MergeUntracked(column_family_handle, key.slice(), value.slice()));
  } catch (ROCKSDB_NAMESPACE::KVException&) {
    return;
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    deleteUntracked
 * Signature: (J[BIJ)V
 */
void Java_org_rocksdb_Transaction_deleteUntracked__J_3BIJ(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jbyteArray jkey,
    jint jkey_part_len, jlong jcolumn_family_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  FnWriteK fn_delete_untracked = std::bind<ROCKSDB_NAMESPACE::Status (
      ROCKSDB_NAMESPACE::Transaction::*)(ROCKSDB_NAMESPACE::ColumnFamilyHandle*,
                                         const ROCKSDB_NAMESPACE::Slice&)>(
      &ROCKSDB_NAMESPACE::Transaction::DeleteUntracked, txn,
      column_family_handle, std::placeholders::_1);
  txn_write_k_helper(env, fn_delete_untracked, jkey, jkey_part_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    deleteUntracked
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_Transaction_deleteUntracked__J_3BI(JNIEnv* env,
                                                         jclass /*jcls*/,
                                                         jlong jhandle,
                                                         jbyteArray jkey,
                                                         jint jkey_part_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  FnWriteK fn_delete_untracked = std::bind<ROCKSDB_NAMESPACE::Status (
      ROCKSDB_NAMESPACE::Transaction::*)(const ROCKSDB_NAMESPACE::Slice&)>(
      &ROCKSDB_NAMESPACE::Transaction::DeleteUntracked, txn,
      std::placeholders::_1);
  txn_write_k_helper(env, fn_delete_untracked, jkey, jkey_part_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    deleteUntracked
 * Signature: (J[[BIJ)V
 */
void Java_org_rocksdb_Transaction_deleteUntracked__J_3_3BIJ(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jobjectArray jkey_parts,
    jint jkey_parts_len, jlong jcolumn_family_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  FnWriteKParts fn_delete_untracked_parts =
      std::bind<ROCKSDB_NAMESPACE::Status (ROCKSDB_NAMESPACE::Transaction::*)(
          ROCKSDB_NAMESPACE::ColumnFamilyHandle*,
          const ROCKSDB_NAMESPACE::SliceParts&)>(
          &ROCKSDB_NAMESPACE::Transaction::DeleteUntracked, txn,
          column_family_handle, std::placeholders::_1);
  txn_write_k_parts_helper(env, fn_delete_untracked_parts, jkey_parts,
                           jkey_parts_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    deleteUntracked
 * Signature: (J[[BI)V
 */
void Java_org_rocksdb_Transaction_deleteUntracked__J_3_3BI(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jobjectArray jkey_parts,
    jint jkey_parts_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  FnWriteKParts fn_delete_untracked_parts =
      std::bind<ROCKSDB_NAMESPACE::Status (ROCKSDB_NAMESPACE::Transaction::*)(
          const ROCKSDB_NAMESPACE::SliceParts&)>(
          &ROCKSDB_NAMESPACE::Transaction::DeleteUntracked, txn,
          std::placeholders::_1);
  txn_write_k_parts_helper(env, fn_delete_untracked_parts, jkey_parts,
                           jkey_parts_len);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    putLogData
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_Transaction_putLogData(JNIEnv* env, jclass /*jobj*/,
                                             jlong jhandle, jbyteArray jkey,
                                             jint jkey_part_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);

  jbyte* key = env->GetByteArrayElements(jkey, nullptr);
  if (key == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key),
                                     jkey_part_len);
  txn->PutLogData(key_slice);

  // trigger java unref on key.
  // by passing JNI_ABORT, it will simply release the reference without
  // copying the result back to the java byte array.
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    disableIndexing
 * Signature: (J)V
 */
void Java_org_rocksdb_Transaction_disableIndexing(JNIEnv* /*env*/,
                                                  jclass /*jcls*/,
                                                  jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  txn->DisableIndexing();
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    enableIndexing
 * Signature: (J)V
 */
void Java_org_rocksdb_Transaction_enableIndexing(JNIEnv* /*env*/,
                                                 jclass /*jcls*/,
                                                 jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  txn->EnableIndexing();
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getNumKeys
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Transaction_getNumKeys(JNIEnv* /*env*/, jclass /*jcls*/,
                                              jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  return txn->GetNumKeys();
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getNumPuts
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Transaction_getNumPuts(JNIEnv* /*env*/, jclass /*jcls*/,
                                              jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  return txn->GetNumPuts();
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getNumDeletes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Transaction_getNumDeletes(JNIEnv* /*env*/,
                                                 jclass /*jcls*/,
                                                 jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  return txn->GetNumDeletes();
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getNumMerges
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Transaction_getNumMerges(JNIEnv* /*env*/,
                                                jclass /*jcls*/,
                                                jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  return txn->GetNumMerges();
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getElapsedTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Transaction_getElapsedTime(JNIEnv* /*env*/,
                                                  jclass /*jcls*/,
                                                  jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  return txn->GetElapsedTime();
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getWriteBatch
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Transaction_getWriteBatch(JNIEnv* /*env*/,
                                                 jclass /*jcls*/,
                                                 jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  return GET_CPLUSPLUS_POINTER(txn->GetWriteBatch());
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    setLockTimeout
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Transaction_setLockTimeout(JNIEnv* /*env*/,
                                                 jclass /*jcls*/, jlong jhandle,
                                                 jlong jlock_timeout) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  txn->SetLockTimeout(jlock_timeout);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getWriteOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Transaction_getWriteOptions(JNIEnv* /*env*/,
                                                   jclass /*jcls*/,
                                                   jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  return GET_CPLUSPLUS_POINTER(txn->GetWriteOptions());
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    setWriteOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Transaction_setWriteOptions(JNIEnv* /*env*/,
                                                  jclass /*jcls*/,
                                                  jlong jhandle,
                                                  jlong jwrite_options_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  txn->SetWriteOptions(*write_options);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    undo
 * Signature: (J[BIJ)V
 */
void Java_org_rocksdb_Transaction_undoGetForUpdate__J_3BIJ(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jbyteArray jkey,
    jint jkey_part_len, jlong jcolumn_family_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* column_family_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
          jcolumn_family_handle);
  jbyte* key = env->GetByteArrayElements(jkey, nullptr);
  if (key == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key),
                                     jkey_part_len);
  txn->UndoGetForUpdate(column_family_handle, key_slice);

  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    undoGetForUpdate
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_Transaction_undoGetForUpdate__J_3BI(JNIEnv* env,
                                                          jclass /*jcls*/,
                                                          jlong jhandle,
                                                          jbyteArray jkey,
                                                          jint jkey_part_len) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  jbyte* key = env->GetByteArrayElements(jkey, nullptr);
  if (key == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key),
                                     jkey_part_len);
  txn->UndoGetForUpdate(key_slice);

  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    rebuildFromWriteBatch
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Transaction_rebuildFromWriteBatch(
    JNIEnv* env, jclass /*jobj*/, jlong jhandle, jlong jwrite_batch_handle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  auto* write_batch =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwrite_batch_handle);
  ROCKSDB_NAMESPACE::Status s = txn->RebuildFromWriteBatch(write_batch);
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getCommitTimeWriteBatch
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Transaction_getCommitTimeWriteBatch(JNIEnv* /*env*/,
                                                           jclass /*jcls*/,
                                                           jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  return GET_CPLUSPLUS_POINTER(txn->GetCommitTimeWriteBatch());
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    setLogNumber
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Transaction_setLogNumber(JNIEnv* /*env*/, jclass /*jcls*/,
                                               jlong jhandle,
                                               jlong jlog_number) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  txn->SetLogNumber(jlog_number);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getLogNumber
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Transaction_getLogNumber(JNIEnv* /*env*/,
                                                jclass /*jcls*/,
                                                jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  return txn->GetLogNumber();
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    setName
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_Transaction_setName(JNIEnv* env, jclass /*jobj*/,
                                          jlong jhandle, jstring jname) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  const char* name = env->GetStringUTFChars(jname, nullptr);
  if (name == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  ROCKSDB_NAMESPACE::Status s = txn->SetName(name);

  env->ReleaseStringUTFChars(jname, name);

  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getName
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_Transaction_getName(JNIEnv* env, jclass /*jobj*/,
                                             jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  ROCKSDB_NAMESPACE::TransactionName name = txn->GetName();
  return env->NewStringUTF(name.data());
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getID
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Transaction_getID(JNIEnv* /*env*/, jclass /*jcls*/,
                                         jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  ROCKSDB_NAMESPACE::TransactionID id = txn->GetID();
  return static_cast<jlong>(id);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    isDeadlockDetect
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Transaction_isDeadlockDetect(JNIEnv* /*env*/,
                                                       jclass /*jcls*/,
                                                       jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  return static_cast<jboolean>(txn->IsDeadlockDetect());
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getWaitingTxns
 * Signature: (J)Lorg/rocksdb/Transaction/WaitingTransactions;
 */
jobject Java_org_rocksdb_Transaction_getWaitingTxns(JNIEnv* env,
                                                    jobject jtransaction_obj,
                                                    jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  uint32_t column_family_id;
  std::string key;
  std::vector<ROCKSDB_NAMESPACE::TransactionID> waiting_txns =
      txn->GetWaitingTxns(&column_family_id, &key);
  jobject jwaiting_txns =
      ROCKSDB_NAMESPACE::TransactionJni::newWaitingTransactions(
          env, jtransaction_obj, column_family_id, key, waiting_txns);
  return jwaiting_txns;
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getState
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Transaction_getState(JNIEnv* /*env*/, jclass /*jcls*/,
                                            jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  ROCKSDB_NAMESPACE::Transaction::TransactionState txn_status = txn->GetState();
  switch (txn_status) {
    case ROCKSDB_NAMESPACE::Transaction::TransactionState::STARTED:
      return 0x0;

    case ROCKSDB_NAMESPACE::Transaction::TransactionState::AWAITING_PREPARE:
      return 0x1;

    case ROCKSDB_NAMESPACE::Transaction::TransactionState::PREPARED:
      return 0x2;

    case ROCKSDB_NAMESPACE::Transaction::TransactionState::AWAITING_COMMIT:
      return 0x3;

    case ROCKSDB_NAMESPACE::Transaction::TransactionState::COMMITTED:
      return 0x4;

    case ROCKSDB_NAMESPACE::Transaction::TransactionState::AWAITING_ROLLBACK:
      return 0x5;

    case ROCKSDB_NAMESPACE::Transaction::TransactionState::ROLLEDBACK:
      return 0x6;

    case ROCKSDB_NAMESPACE::Transaction::TransactionState::LOCKS_STOLEN:
      return 0x7;
  }

  assert(false);
  return static_cast<jbyte>(-1);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    getId
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Transaction_getId(JNIEnv* /*env*/, jclass /*jcls*/,
                                         jlong jhandle) {
  auto* txn = reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
  uint64_t id = txn->GetId();
  return static_cast<jlong>(id);
}

/*
 * Class:     org_rocksdb_Transaction
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Transaction_disposeInternal(JNIEnv* /*env*/,
                                                  jobject /*jobj*/,
                                                  jlong jhandle) {
  delete reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jhandle);
}
