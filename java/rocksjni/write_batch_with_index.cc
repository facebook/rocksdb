// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::WriteBatchWithIndex methods from Java side.

#include "include/org_rocksdb_WBWIRocksIterator.h"
#include "include/org_rocksdb_WriteBatchWithIndex.h"
#include "rocksdb/comparator.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: ()V
 */
void Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__(
    JNIEnv* env, jobject jobj) {
  rocksdb::WriteBatchWithIndex* wbwi = new rocksdb::WriteBatchWithIndex();
  rocksdb::WriteBatchWithIndexJni::setHandle(env, jobj, wbwi);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: (Z)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__Z(
    JNIEnv* env, jobject jobj, jboolean joverwrite_key) {
  rocksdb::WriteBatchWithIndex* wbwi =
      new rocksdb::WriteBatchWithIndex(rocksdb::BytewiseComparator(), 0,
      static_cast<bool>(joverwrite_key));
  rocksdb::WriteBatchWithIndexJni::setHandle(env, jobj, wbwi);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: (JIZ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__JIZ(
    JNIEnv* env, jobject jobj, jlong jfallback_index_comparator_handle,
    jint jreserved_bytes, jboolean joverwrite_key) {
  rocksdb::WriteBatchWithIndex* wbwi =
      new rocksdb::WriteBatchWithIndex(
      reinterpret_cast<rocksdb::Comparator*>(jfallback_index_comparator_handle),
      static_cast<size_t>(jreserved_bytes), static_cast<bool>(joverwrite_key));
  rocksdb::WriteBatchWithIndexJni::setHandle(env, jobj, wbwi);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    count
 * Signature: ()I
 */
jint Java_org_rocksdb_WriteBatchWithIndex_count0(
    JNIEnv* env, jobject jobj) {
  rocksdb::WriteBatchWithIndex* wbwi =
      rocksdb::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);

  return static_cast<jint>(wbwi->GetWriteBatch()->Count());
}

//TODO(AR) make generic with WriteBatch equivalent
/*
 * Helper for WriteBatchWithIndex put operations
 */
void write_batch_with_index_put_helper(
    JNIEnv* env, jobject jobj,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len,
    rocksdb::ColumnFamilyHandle* cf_handle) {
  rocksdb::WriteBatchWithIndex* wbwi =
      rocksdb::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);

  jbyte* key = env->GetByteArrayElements(jkey, nullptr);
  jbyte* value = env->GetByteArrayElements(jentry_value, nullptr);
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
  rocksdb::Slice value_slice(reinterpret_cast<char*>(value),
      jentry_value_len);
  if (cf_handle != nullptr) {
    wbwi->Put(cf_handle, key_slice, value_slice);
  } else {
    // backwards compatibility
    wbwi->Put(key_slice, value_slice);
  }
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
  env->ReleaseByteArrayElements(jentry_value, value, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    put
 * Signature: ([BI[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_put___3BI_3BI(
    JNIEnv* env, jobject jobj, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len) {
  write_batch_with_index_put_helper(env, jobj, jkey, jkey_len, jentry_value,
      jentry_value_len, nullptr);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    put
 * Signature: ([BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_put___3BI_3BIJ(
    JNIEnv* env, jobject jobj, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len, jlong jcf_handle) {
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  write_batch_with_index_put_helper(env, jobj, jkey, jkey_len, jentry_value,
      jentry_value_len, cf_handle);
}

//TODO(AR) make generic with WriteBatch equivalent
/*
 * Helper for WriteBatchWithIndex merge operations
 */
void write_batch_with_index_merge_helper(
    JNIEnv* env, jobject jobj,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len,
    rocksdb::ColumnFamilyHandle* cf_handle) {
  rocksdb::WriteBatchWithIndex* wbwi =
      rocksdb::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);

  jbyte* key = env->GetByteArrayElements(jkey, nullptr);
  jbyte* value = env->GetByteArrayElements(jentry_value, nullptr);
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
  rocksdb::Slice value_slice(reinterpret_cast<char*>(value),
      jentry_value_len);
  if (cf_handle != nullptr) {
    wbwi->Merge(cf_handle, key_slice, value_slice);
  } else {
    // backwards compatibility
    wbwi->Merge(key_slice, value_slice);
  }
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
  env->ReleaseByteArrayElements(jentry_value, value, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    merge
 * Signature: ([BI[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_merge___3BI_3BI(
    JNIEnv* env, jobject jobj, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len) {
  write_batch_with_index_merge_helper(env, jobj, jkey, jkey_len,
      jentry_value, jentry_value_len, nullptr);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    merge
 * Signature: ([BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_merge___3BI_3BIJ(
    JNIEnv* env, jobject jobj, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len, jlong jcf_handle) {
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  write_batch_with_index_merge_helper(env, jobj, jkey, jkey_len,
      jentry_value, jentry_value_len, cf_handle);
}

//TODO(AR) make generic with WriteBatch equivalent
/*
 * Helper for write batch remove operations
 */
void write_batch_with_index_remove_helper(
    JNIEnv* env, jobject jobj,
    jbyteArray jkey, jint jkey_len,
    rocksdb::ColumnFamilyHandle* cf_handle) {
  rocksdb::WriteBatchWithIndex* wbwi =
      rocksdb::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);

  jbyte* key = env->GetByteArrayElements(jkey, nullptr);
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
  if (cf_handle != nullptr) {
    wbwi->Delete(cf_handle, key_slice);
  } else {
    wbwi->Delete(key_slice);
  }
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    remove
 * Signature: ([BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_remove___3BI(
    JNIEnv* env, jobject jobj, jbyteArray jkey, jint jkey_len) {
  write_batch_with_index_remove_helper(env, jobj, jkey, jkey_len, nullptr);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    remove
 * Signature: ([BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_remove___3BIJ(
    JNIEnv* env, jobject jobj,
    jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  write_batch_with_index_remove_helper(env, jobj, jkey, jkey_len, cf_handle);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    putLogData
 * Signature: ([BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_putLogData(
    JNIEnv* env, jobject jobj, jbyteArray jblob, jint jblob_len) {
  rocksdb::WriteBatchWithIndex* wbwi =
      rocksdb::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);

  jbyte* blob = env->GetByteArrayElements(jblob, nullptr);
  rocksdb::Slice blob_slice(reinterpret_cast<char*>(blob), jblob_len);
  wbwi->PutLogData(blob_slice);
  env->ReleaseByteArrayElements(jblob, blob, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    clear
 * Signature: ()V
 */
void Java_org_rocksdb_WriteBatchWithIndex_clear0(
    JNIEnv* env, jobject jobj) {
  rocksdb::WriteBatchWithIndex* wbwi =
      rocksdb::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);

  wbwi->GetWriteBatch()->Clear();
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iterator0
 * Signature: ()J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iterator0(
    JNIEnv* env, jobject jobj) {
  rocksdb::WriteBatchWithIndex* wbwi =
      rocksdb::WriteBatchWithIndexJni::getHandle(env, jobj);
  rocksdb::WBWIIterator* wbwi_iterator = wbwi->NewIterator();
  return reinterpret_cast<jlong>(wbwi_iterator);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iterator1
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iterator1(
    JNIEnv* env, jobject jobj, jlong jcf_handle) {
  rocksdb::WriteBatchWithIndex* wbwi =
      rocksdb::WriteBatchWithIndexJni::getHandle(env, jobj);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  rocksdb::WBWIIterator* wbwi_iterator = wbwi->NewIterator(cf_handle);
  return reinterpret_cast<jlong>(wbwi_iterator);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iteratorWithBase
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iteratorWithBase(
    JNIEnv* env, jobject jobj, jlong jcf_handle, jlong jbi_handle) {
  rocksdb::WriteBatchWithIndex* wbwi =
      rocksdb::WriteBatchWithIndexJni::getHandle(env, jobj);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  auto* base_iterator = reinterpret_cast<rocksdb::Iterator*>(jbi_handle);
  auto* iterator = wbwi->NewIteratorWithBase(cf_handle, base_iterator);
  return reinterpret_cast<jlong>(iterator);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_disposeInternal(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(handle);
  delete wbwi;
}

/* WBWIRocksIterator below */

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_disposeInternal(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto* it = reinterpret_cast<rocksdb::WBWIIterator*>(handle);
  delete it;
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    isValid0
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WBWIRocksIterator_isValid0(
    JNIEnv* env, jobject jobj, jlong handle) {
  return reinterpret_cast<rocksdb::WBWIIterator*>(handle)->Valid();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekToFirst0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekToFirst0(
    JNIEnv* env, jobject jobj, jlong handle) {
  reinterpret_cast<rocksdb::WBWIIterator*>(handle)->SeekToFirst();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekToLast0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekToLast0(
    JNIEnv* env, jobject jobj, jlong handle) {
  reinterpret_cast<rocksdb::WBWIIterator*>(handle)->SeekToLast();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    next0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_next0(
    JNIEnv* env, jobject jobj, jlong handle) {
  reinterpret_cast<rocksdb::WBWIIterator*>(handle)->Next();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    prev0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_prev0(
    JNIEnv* env, jobject jobj, jlong handle) {
  reinterpret_cast<rocksdb::WBWIIterator*>(handle)->Prev();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seek0
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seek0(
    JNIEnv* env, jobject jobj, jlong handle, jbyteArray jtarget,
    jint jtarget_len) {
  auto* it = reinterpret_cast<rocksdb::WBWIIterator*>(handle);
  jbyte* target = env->GetByteArrayElements(jtarget, 0);
  rocksdb::Slice target_slice(
      reinterpret_cast<char*>(target), jtarget_len);

  it->Seek(target_slice);

  env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    status0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_status0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto* it = reinterpret_cast<rocksdb::WBWIIterator*>(handle);
  rocksdb::Status s = it->status();

  if (s.ok()) {
    return;
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    entry1
 * Signature: (JLorg/rocksdb/WBWIRocksIterator/WriteEntry;)V
 */
void Java_org_rocksdb_WBWIRocksIterator_entry1(
    JNIEnv* env, jobject jobj, jlong handle, jobject jwrite_entry) {
  auto* it = reinterpret_cast<rocksdb::WBWIIterator*>(handle);
  const rocksdb::WriteEntry& we = it->Entry();
  jobject jwe = rocksdb::WBWIRocksIteratorJni::getWriteEntry(env, jobj);
  rocksdb::WriteEntryJni::setWriteType(env, jwe, we.type);
  rocksdb::WriteEntryJni::setKey(env, jwe, &we.key);
  if (we.type == rocksdb::kDeleteRecord || we.type == rocksdb::kLogDataRecord) {
    // set native handle of value slice to null if no value available
    rocksdb::WriteEntryJni::setValue(env, jwe, NULL);
  } else {
    rocksdb::WriteEntryJni::setValue(env, jwe, &we.value);
  }
}
