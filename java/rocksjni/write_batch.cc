// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::WriteBatch methods from Java side.
#include <memory>

#include "include/org_rocksdb_WriteBatch.h"
#include "include/org_rocksdb_WriteBatchInternal.h"
#include "include/org_rocksdb_WriteBatchTest.h"
#include "rocksjni/portal.h"
#include "rocksdb/db.h"
#include "db/memtable.h"
#include "rocksdb/write_batch.h"
#include "db/write_batch_internal.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "util/logging.h"
#include "util/testharness.h"

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    newWriteBatch
 * Signature: (I)V
 */
void Java_org_rocksdb_WriteBatch_newWriteBatch(
    JNIEnv* env, jobject jobj, jint jreserved_bytes) {
  rocksdb::WriteBatch* wb = new rocksdb::WriteBatch(
      static_cast<size_t>(jreserved_bytes));

  rocksdb::WriteBatchJni::setHandle(env, jobj, wb);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    count
 * Signature: ()I
 */
jint Java_org_rocksdb_WriteBatch_count(JNIEnv* env, jobject jobj) {
  rocksdb::WriteBatch* wb = rocksdb::WriteBatchJni::getHandle(env, jobj);
  assert(wb != nullptr);

  return static_cast<jint>(wb->Count());
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    clear
 * Signature: ()V
 */
void Java_org_rocksdb_WriteBatch_clear(JNIEnv* env, jobject jobj) {
  rocksdb::WriteBatch* wb = rocksdb::WriteBatchJni::getHandle(env, jobj);
  assert(wb != nullptr);

  wb->Clear();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    put
 * Signature: ([BI[BI)V
 */
void Java_org_rocksdb_WriteBatch_put(
    JNIEnv* env, jobject jobj,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jvalue, jint jvalue_len) {
  rocksdb::WriteBatch* wb = rocksdb::WriteBatchJni::getHandle(env, jobj);
  assert(wb != nullptr);

  jbyte* key = env->GetByteArrayElements(jkey, nullptr);
  jbyte* value = env->GetByteArrayElements(jvalue, nullptr);
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
  rocksdb::Slice value_slice(reinterpret_cast<char*>(value), jvalue_len);
  wb->Put(key_slice, value_slice);
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
  env->ReleaseByteArrayElements(jvalue, value, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    merge
 * Signature: ([BI[BI)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_WriteBatch_merge(
    JNIEnv* env, jobject jobj,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jvalue, jint jvalue_len) {
  rocksdb::WriteBatch* wb = rocksdb::WriteBatchJni::getHandle(env, jobj);
  assert(wb != nullptr);

  jbyte* key = env->GetByteArrayElements(jkey, nullptr);
  jbyte* value = env->GetByteArrayElements(jvalue, nullptr);
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
  rocksdb::Slice value_slice(reinterpret_cast<char*>(value), jvalue_len);
  wb->Merge(key_slice, value_slice);
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
  env->ReleaseByteArrayElements(jvalue, value, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    remove
 * Signature: ([BI)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_WriteBatch_remove(
    JNIEnv* env, jobject jobj,
    jbyteArray jkey, jint jkey_len) {
  rocksdb::WriteBatch* wb = rocksdb::WriteBatchJni::getHandle(env, jobj);
  assert(wb != nullptr);

  jbyte* key = env->GetByteArrayElements(jkey, nullptr);
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
  wb->Delete(key_slice);
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    putLogData
 * Signature: ([BI)V
 */
void Java_org_rocksdb_WriteBatch_putLogData(
    JNIEnv* env, jobject jobj, jbyteArray jblob, jint jblob_len) {
  rocksdb::WriteBatch* wb = rocksdb::WriteBatchJni::getHandle(env, jobj);
  assert(wb != nullptr);

  jbyte* blob = env->GetByteArrayElements(jblob, nullptr);
  rocksdb::Slice blob_slice(reinterpret_cast<char*>(blob), jblob_len);
  wb->PutLogData(blob_slice);
  env->ReleaseByteArrayElements(jblob, blob, JNI_ABORT);
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
 * Class:     org_rocksdb_WriteBatchInternal
 * Method:    setSequence
 * Signature: (Lorg/rocksdb/WriteBatch;J)V
 */
void Java_org_rocksdb_WriteBatchInternal_setSequence(
    JNIEnv* env, jclass jclazz, jobject jobj, jlong jsn) {
  rocksdb::WriteBatch* wb = rocksdb::WriteBatchJni::getHandle(env, jobj);
  assert(wb != nullptr);

  rocksdb::WriteBatchInternal::SetSequence(
      wb, static_cast<rocksdb::SequenceNumber>(jsn));
}

/*
 * Class:     org_rocksdb_WriteBatchInternal
 * Method:    sequence
 * Signature: (Lorg/rocksdb/WriteBatch;)J
 */
jlong Java_org_rocksdb_WriteBatchInternal_sequence(
    JNIEnv* env, jclass jclazz, jobject jobj) {
  rocksdb::WriteBatch* wb = rocksdb::WriteBatchJni::getHandle(env, jobj);
  assert(wb != nullptr);

  return static_cast<jlong>(rocksdb::WriteBatchInternal::Sequence(wb));
}

/*
 * Class:     org_rocksdb_WriteBatchInternal
 * Method:    append
 * Signature: (Lorg/rocksdb/WriteBatch;Lorg/rocksdb/WriteBatch;)V
 */
void Java_org_rocksdb_WriteBatchInternal_append(
    JNIEnv* env, jclass jclazz, jobject jwb1, jobject jwb2) {
  rocksdb::WriteBatch* wb1 = rocksdb::WriteBatchJni::getHandle(env, jwb1);
  assert(wb1 != nullptr);
  rocksdb::WriteBatch* wb2 = rocksdb::WriteBatchJni::getHandle(env, jwb2);
  assert(wb2 != nullptr);

  rocksdb::WriteBatchInternal::Append(wb1, wb2);
}

/*
 * Class:     org_rocksdb_WriteBatchTest
 * Method:    getContents
 * Signature: (Lorg/rocksdb/WriteBatch;)[B
 */
jbyteArray Java_org_rocksdb_WriteBatchTest_getContents(
    JNIEnv* env, jclass jclazz, jobject jobj) {
  rocksdb::WriteBatch* b = rocksdb::WriteBatchJni::getHandle(env, jobj);
  assert(b != nullptr);

  // todo: Currently the following code is directly copied from
  // db/write_bench_test.cc.  It could be implemented in java once
  // all the necessary components can be accessed via jni api.

  rocksdb::InternalKeyComparator cmp(rocksdb::BytewiseComparator());
  auto factory = std::make_shared<rocksdb::SkipListFactory>();
  rocksdb::Options options;
  options.memtable_factory = factory;
  rocksdb::MemTable* mem = new rocksdb::MemTable(cmp, options);
  mem->Ref();
  std::string state;
  rocksdb::ColumnFamilyMemTablesDefault cf_mems_default(mem, &options);
  rocksdb::Status s =
      rocksdb::WriteBatchInternal::InsertInto(b, &cf_mems_default);
  int count = 0;
  rocksdb::Iterator* iter = mem->NewIterator(rocksdb::ReadOptions());
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    rocksdb::ParsedInternalKey ikey;
    memset(reinterpret_cast<void*>(&ikey), 0, sizeof(ikey));
    ASSERT_TRUE(rocksdb::ParseInternalKey(iter->key(), &ikey));
    switch (ikey.type) {
      case rocksdb::kTypeValue:
        state.append("Put(");
        state.append(ikey.user_key.ToString());
        state.append(", ");
        state.append(iter->value().ToString());
        state.append(")");
        count++;
        break;
      case rocksdb::kTypeMerge:
        state.append("Merge(");
        state.append(ikey.user_key.ToString());
        state.append(", ");
        state.append(iter->value().ToString());
        state.append(")");
        count++;
        break;
      case rocksdb::kTypeDeletion:
        state.append("Delete(");
        state.append(ikey.user_key.ToString());
        state.append(")");
        count++;
        break;
      default:
        assert(false);
        break;
    }
    state.append("@");
    state.append(rocksdb::NumberToString(ikey.sequence));
  }
  delete iter;
  if (!s.ok()) {
    state.append(s.ToString());
  } else if (count != rocksdb::WriteBatchInternal::Count(b)) {
    state.append("CountMismatch()");
  }
  delete mem->Unref();

  jbyteArray jstate = env->NewByteArray(state.size());
  env->SetByteArrayRegion(
      jstate, 0, state.size(),
      reinterpret_cast<const jbyte*>(state.c_str()));

  return jstate;
}
