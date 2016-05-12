// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::WriteBatch methods from Java side.
#include <memory>

#include "include/org_rocksdb_WriteBatch.h"
#include "include/org_rocksdb_WriteBatch_Handler.h"
#include "rocksjni/portal.h"
#include "rocksjni/writebatchhandlerjnicallback.h"
#include "rocksdb/db.h"
#include "rocksdb/immutable_options.h"
#include "db/memtable.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/status.h"
#include "db/write_batch_internal.h"
#include "db/writebuffer.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "table/scoped_arena_iterator.h"
#include "util/logging.h"
#include "util/testharness.h"

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    newWriteBatch
 * Signature: (I)J
 */
jlong Java_org_rocksdb_WriteBatch_newWriteBatch(
    JNIEnv* env, jclass jcls, jint jreserved_bytes) {
  rocksdb::WriteBatch* wb = new rocksdb::WriteBatch(
      static_cast<size_t>(jreserved_bytes));
  return reinterpret_cast<jlong>(wb);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    count0
 * Signature: (J)I
 */
jint Java_org_rocksdb_WriteBatch_count0(JNIEnv* env, jobject jobj,
    jlong jwb_handle) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return static_cast<jint>(wb->Count());
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    clear0
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatch_clear0(JNIEnv* env, jobject jobj,
    jlong jwb_handle) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  wb->Clear();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    put
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatch_put__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwb_handle,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto put = [&wb] (rocksdb::Slice key, rocksdb::Slice value) {
    wb->Put(key, value);
  };
  rocksdb::JniUtil::kv_op(put, env, jobj, jkey, jkey_len, jentry_value,
      jentry_value_len);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    put
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatch_put__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwb_handle,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len, jlong jcf_handle) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto put = [&wb, &cf_handle] (rocksdb::Slice key, rocksdb::Slice value) {
    wb->Put(cf_handle, key, value);
  };
  rocksdb::JniUtil::kv_op(put, env, jobj, jkey, jkey_len, jentry_value,
      jentry_value_len);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    merge
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatch_merge__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwb_handle,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto merge = [&wb] (rocksdb::Slice key, rocksdb::Slice value) {
    wb->Merge(key, value);
  };
  rocksdb::JniUtil::kv_op(merge, env, jobj, jkey, jkey_len, jentry_value,
      jentry_value_len);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    merge
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatch_merge__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwb_handle,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len, jlong jcf_handle) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto merge = [&wb, &cf_handle] (rocksdb::Slice key, rocksdb::Slice value) {
    wb->Merge(cf_handle, key, value);
  };
  rocksdb::JniUtil::kv_op(merge, env, jobj, jkey, jkey_len, jentry_value,
      jentry_value_len);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    remove
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatch_remove__J_3BI(
    JNIEnv* env, jobject jobj, jlong jwb_handle,
    jbyteArray jkey, jint jkey_len) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto remove = [&wb] (rocksdb::Slice key) {
    wb->Delete(key);
  };
  rocksdb::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    remove
 * Signature: (J[BIJ)V
 */
void Java_org_rocksdb_WriteBatch_remove__J_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwb_handle,
    jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto remove = [&wb, &cf_handle] (rocksdb::Slice key) {
    wb->Delete(cf_handle, key);
  };
  rocksdb::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    putLogData
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatch_putLogData(
    JNIEnv* env, jobject jobj, jlong jwb_handle, jbyteArray jblob,
    jint jblob_len) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto putLogData = [&wb] (rocksdb::Slice blob) {
    wb->PutLogData(blob);
  };
  rocksdb::JniUtil::k_op(putLogData, env, jobj, jblob, jblob_len);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    iterate
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WriteBatch_iterate(
    JNIEnv* env , jobject jobj, jlong jwb_handle, jlong handlerHandle) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  rocksdb::Status s = wb->Iterate(
    reinterpret_cast<rocksdb::WriteBatchHandlerJniCallback*>(handlerHandle));

  if (s.ok()) {
    return;
  }
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatch_disposeInternal(
    JNIEnv* env, jobject jobj, jlong handle) {
  delete reinterpret_cast<rocksdb::WriteBatch*>(handle);
}

/*
 * Class:     org_rocksdb_WriteBatch_Handler
 * Method:    createNewHandler0
 * Signature: ()J
 */
jlong Java_org_rocksdb_WriteBatch_00024Handler_createNewHandler0(
    JNIEnv* env, jobject jobj) {
  const rocksdb::WriteBatchHandlerJniCallback* h =
    new rocksdb::WriteBatchHandlerJniCallback(env, jobj);
  return reinterpret_cast<jlong>(h);
}

/*
 * Class:     org_rocksdb_WriteBatch_Handler
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatch_00024Handler_disposeInternal(
    JNIEnv* env, jobject jobj, jlong handle) {
  delete reinterpret_cast<rocksdb::WriteBatchHandlerJniCallback*>(handle);
}
