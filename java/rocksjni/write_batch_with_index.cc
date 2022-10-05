// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::WriteBatchWithIndex methods from Java side.

#include "rocksdb/utilities/write_batch_with_index.h"

#include "api_columnfamilyhandle.h"
#include "api_iterator.h"
#include "api_wrapper.h"
#include "include/org_rocksdb_WBWIRocksIterator.h"
#include "include/org_rocksdb_WriteBatchWithIndex.h"
#include "rocksdb/comparator.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

using APIWriteBatchWithIndex =
    APIWrapper<ROCKSDB_NAMESPACE::WriteBatchWithIndex>;
using WBWIAPIIterator = APIIterator<ROCKSDB_NAMESPACE::WriteBatchWithIndex,
                                    ROCKSDB_NAMESPACE::WBWIIterator>;

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: ()J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__(
    JNIEnv* /*env*/, jclass /*jcls*/) {
  auto wbwi = std::shared_ptr<ROCKSDB_NAMESPACE::WriteBatchWithIndex>(
      new ROCKSDB_NAMESPACE::WriteBatchWithIndex());
  std::unique_ptr<APIWriteBatchWithIndex> wbwiAPI(
      new APIWriteBatchWithIndex(wbwi));
  return GET_CPLUSPLUS_POINTER(wbwiAPI.release());
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: (Z)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__Z(
    JNIEnv* /*env*/, jclass /*jcls*/, jboolean joverwrite_key) {
  auto wbwi = std::shared_ptr<ROCKSDB_NAMESPACE::WriteBatchWithIndex>(
      new ROCKSDB_NAMESPACE::WriteBatchWithIndex(
          ROCKSDB_NAMESPACE::BytewiseComparator(), 0,
          static_cast<bool>(joverwrite_key)));

  std::unique_ptr<APIWriteBatchWithIndex> wbwiAPI(
      new APIWriteBatchWithIndex(wbwi));
  return GET_CPLUSPLUS_POINTER(wbwiAPI.release());
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: (JBIZ)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__JBIZ(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jfallback_index_comparator_handle,
    jbyte jcomparator_type, jint jreserved_bytes, jboolean joverwrite_key) {
  ROCKSDB_NAMESPACE::Comparator* fallback_comparator = nullptr;
  switch (jcomparator_type) {
    // JAVA_COMPARATOR
    case 0x0:
      fallback_comparator =
          reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallback*>(
              jfallback_index_comparator_handle);
      break;

    // JAVA_NATIVE_COMPARATOR_WRAPPER
    case 0x1:
      fallback_comparator = reinterpret_cast<ROCKSDB_NAMESPACE::Comparator*>(
          jfallback_index_comparator_handle);
      break;
  }
  auto wbwi = std::shared_ptr<ROCKSDB_NAMESPACE::WriteBatchWithIndex>(
      new ROCKSDB_NAMESPACE::WriteBatchWithIndex(
          fallback_comparator, static_cast<size_t>(jreserved_bytes),
          static_cast<bool>(joverwrite_key)));

  std::unique_ptr<APIWriteBatchWithIndex> wbwiAPI(
      new APIWriteBatchWithIndex(wbwi));
  return GET_CPLUSPLUS_POINTER(wbwiAPI.release());
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    count0
 * Signature: (J)I
 */
jint Java_org_rocksdb_WriteBatchWithIndex_count0(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  return static_cast<jint>(wbwi->GetWriteBatch()->Count());
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    put
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_put__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jbyteArray jentry_value, jint jentry_value_len) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto put = [&wbwiAPI](ROCKSDB_NAMESPACE::Slice key,
                        ROCKSDB_NAMESPACE::Slice value) {
    return wbwiAPI->Put(key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(put, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    put
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_put__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jbyteArray jentry_value, jint jentry_value_len,
    jlong jcf_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto* cfhAPI =
      reinterpret_cast<APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>*>(
          jcf_handle);
  auto cfh = cfhAPI->cfhLock(env);
  if (!cfh) {
    // exception was raised
    return;
  }
  auto put = [&wbwiAPI, &cfh](ROCKSDB_NAMESPACE::Slice key,
                              ROCKSDB_NAMESPACE::Slice value) {
    return wbwiAPI->Put(cfh.get(), key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(put, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    putDirect
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_putDirect(
    JNIEnv* env, jobject /*jobj*/, jlong jwb_handle, jobject jkey,
    jint jkey_offset, jint jkey_len, jobject jval, jint jval_offset,
    jint jval_len, jlong jcf_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwb_handle);
  auto* cfhAPI =
      reinterpret_cast<APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>*>(
          jcf_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cfh = nullptr;
  if (cfhAPI != nullptr) {
    auto cfhPtr = cfhAPI->cfhLock(env);
    if (!cfhPtr) {
      // exception was raised
      return;
    }
    cfh = cfhPtr.get();
  }
  auto put = [&wbwiAPI, &cfh](ROCKSDB_NAMESPACE::Slice& key,
                              ROCKSDB_NAMESPACE::Slice& value) {
    if (cfh == nullptr) {
      wbwiAPI->Put(key, value);
    } else {
      wbwiAPI->Put(cfh, key, value);
    }
  };
  ROCKSDB_NAMESPACE::JniUtil::kv_op_direct(
      put, env, jkey, jkey_offset, jkey_len, jval, jval_offset, jval_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    merge
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_merge__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jbyteArray jentry_value, jint jentry_value_len) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto merge = [&wbwiAPI](ROCKSDB_NAMESPACE::Slice key,
                          ROCKSDB_NAMESPACE::Slice value) {
    return wbwiAPI->Merge(key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(merge, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    merge
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_merge__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jbyteArray jentry_value, jint jentry_value_len,
    jlong jcf_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto* cfhAPI =
      reinterpret_cast<APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>*>(
          jcf_handle);
  auto cfh = cfhAPI->cfhLock(env);
  if (!cfh) {
    // exception was raised
    return;
  }
  auto merge = [&wbwiAPI, &cfh](ROCKSDB_NAMESPACE::Slice key,
                                ROCKSDB_NAMESPACE::Slice value) {
    return wbwiAPI->Merge(cfh.get(), key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(merge, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    delete
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_delete__J_3BI(JNIEnv* env,
                                                        jobject jobj,
                                                        jlong jwbwi_handle,
                                                        jbyteArray jkey,
                                                        jint jkey_len) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto remove = [&wbwiAPI](ROCKSDB_NAMESPACE::Slice key) {
    return wbwiAPI->Delete(key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    delete
 * Signature: (J[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_delete__J_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jlong jcf_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto* cfhAPI =
      reinterpret_cast<APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>*>(
          jcf_handle);
  auto cfh = cfhAPI->cfhLock(env);
  if (!cfh) {
    // exception was raised
    return;
  }
  auto remove = [&wbwiAPI, &cfh](ROCKSDB_NAMESPACE::Slice key) {
    return wbwiAPI->Delete(cfh.get(), key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    singleDelete
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_singleDelete__J_3BI(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto single_delete = [&wbwiAPI](ROCKSDB_NAMESPACE::Slice key) {
    return wbwiAPI->SingleDelete(key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(single_delete, env, jobj, jkey,
                                       jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    singleDelete
 * Signature: (J[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_singleDelete__J_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jlong jcf_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto* cfhAPI =
      reinterpret_cast<APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>*>(
          jcf_handle);
  auto cfh = cfhAPI->cfhLock(env);
  if (!cfh) {
    // exception was raised
    return;
  }
  auto single_delete = [&wbwiAPI, &cfh](ROCKSDB_NAMESPACE::Slice key) {
    return wbwiAPI->SingleDelete(cfh.get(), key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(single_delete, env, jobj, jkey,
                                       jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    deleteDirect
 * Signature: (JLjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_deleteDirect(
    JNIEnv* env, jobject /*jobj*/, jlong jwb_handle, jobject jkey,
    jint jkey_offset, jint jkey_len, jlong jcf_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwb_handle);
  auto* cfhAPI =
      reinterpret_cast<APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>*>(
          jcf_handle);
  if (cfhAPI == nullptr) {
    auto remove = [&wbwiAPI](ROCKSDB_NAMESPACE::Slice& key) {
      wbwiAPI->Delete(key);
    };
    ROCKSDB_NAMESPACE::JniUtil::k_op_direct(remove, env, jkey, jkey_offset,
                                            jkey_len);
  } else {
    auto cfh = cfhAPI->cfhLock(env);
    if (!cfh) {
      // exception was raised
      return;
    }
    auto remove = [&wbwiAPI, &cfh](ROCKSDB_NAMESPACE::Slice& key) {
      wbwiAPI->Delete(cfh.get(), key);
    };
    ROCKSDB_NAMESPACE::JniUtil::k_op_direct(remove, env, jkey, jkey_offset,
                                            jkey_len);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    deleteRange
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_deleteRange__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jbegin_key,
    jint jbegin_key_len, jbyteArray jend_key, jint jend_key_len) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto deleteRange = [&wbwiAPI](ROCKSDB_NAMESPACE::Slice beginKey,
                                ROCKSDB_NAMESPACE::Slice endKey) {
    return wbwiAPI->DeleteRange(beginKey, endKey);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(deleteRange, env, jobj, jbegin_key,
                                        jbegin_key_len, jend_key, jend_key_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    deleteRange
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_deleteRange__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jbegin_key,
    jint jbegin_key_len, jbyteArray jend_key, jint jend_key_len,
    jlong jcf_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto* cfhAPI =
      reinterpret_cast<APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>*>(
          jcf_handle);
  auto cfh = cfhAPI->cfhLock(env);
  if (!cfh) {
    // exception was raised
    return;
  }
  auto deleteRange = [&wbwiAPI, &cfh](ROCKSDB_NAMESPACE::Slice beginKey,
                                      ROCKSDB_NAMESPACE::Slice endKey) {
    return wbwiAPI->DeleteRange(cfh.get(), beginKey, endKey);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(deleteRange, env, jobj, jbegin_key,
                                        jbegin_key_len, jend_key, jend_key_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    putLogData
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_putLogData(JNIEnv* env, jobject jobj,
                                                     jlong jwbwi_handle,
                                                     jbyteArray jblob,
                                                     jint jblob_len) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto putLogData = [&wbwiAPI](ROCKSDB_NAMESPACE::Slice blob) {
    return wbwiAPI->PutLogData(blob);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(putLogData, env, jobj, jblob, jblob_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    clear
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_clear0(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong jwbwi_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  wbwiAPI->Clear();
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    setSavePoint0
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_setSavePoint0(JNIEnv* /*env*/,
                                                        jobject /*jobj*/,
                                                        jlong jwbwi_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  wbwiAPI->SetSavePoint();
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    rollbackToSavePoint0
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_rollbackToSavePoint0(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto s = wbwiAPI->RollbackToSavePoint();

  if (s.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    popSavePoint
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_popSavePoint(JNIEnv* env,
                                                       jobject /*jobj*/,
                                                       jlong jwbwi_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto s = wbwiAPI->PopSavePoint();

  if (s.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    setMaxBytes
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_setMaxBytes(JNIEnv* /*env*/,
                                                      jobject /*jobj*/,
                                                      jlong jwbwi_handle,
                                                      jlong jmax_bytes) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  wbwiAPI->SetMaxBytes(static_cast<size_t>(jmax_bytes));
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getWriteBatch
 * Signature: (J)Lorg/rocksdb/WriteBatch;
 */
jobject Java_org_rocksdb_WriteBatchWithIndex_getWriteBatch(JNIEnv* env,
                                                           jobject /*jobj*/,
                                                           jlong jwbwi_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto* wb = wbwiAPI->GetWriteBatch();

  // TODO(AR) is the `wb` object owned by us?
  return ROCKSDB_NAMESPACE::WriteBatchJni::construct(env, wb);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iterator0
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iterator0(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong jwbwi_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto* wbwi_iterator = wbwiAPI->NewIterator();
  std::unique_ptr<ROCKSDB_NAMESPACE::WBWIIterator> iter(wbwi_iterator);
  auto wbwiIterAPI =
      std::make_unique<WBWIAPIIterator>(wbwiAPI.wrapped, std::move(iter));
  return GET_CPLUSPLUS_POINTER(wbwiIterAPI.release());
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iterator1
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iterator1(JNIEnv* env,
                                                     jobject /*jobj*/,
                                                     jlong jwbwi_handle,
                                                     jlong jcf_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto* cfhAPI =
      reinterpret_cast<APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>*>(
          jcf_handle);
  auto cfhLocked = cfhAPI->cfhLock(env);
  if (!cfhLocked) {
    return 0L;
  }
  auto* wbwi_iterator = wbwiAPI->NewIterator(cfhLocked.get());
  std::unique_ptr<ROCKSDB_NAMESPACE::WBWIIterator> iter(wbwi_iterator);
  auto wbwiIterAPI = std::make_unique<WBWIAPIIterator>(
      wbwiAPI.wrapped, std::move(iter), cfhLocked);
  return GET_CPLUSPLUS_POINTER(wbwiIterAPI.release());
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iteratorWithBase
 * Signature: (JJJJ)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iteratorWithBase(
    JNIEnv* env, jobject, jlong jwbwi_handle, jlong jcf_handle,
    jlong jbase_iterator_handle, jlong jread_opts_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto* cfhAPI =
      reinterpret_cast<APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>*>(
          jcf_handle);
  auto* baseIteratorAPI = reinterpret_cast<
      APIIterator<ROCKSDB_NAMESPACE::DB, ROCKSDB_NAMESPACE::Iterator>*>(
      jbase_iterator_handle);
  auto cfhLocked = cfhAPI->cfhLock(env);
  if (!cfhLocked) {
    return 0L;
  }

  ROCKSDB_NAMESPACE::ReadOptions* read_opts =
      jread_opts_handle == 0
          ? nullptr
          : reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(
                jread_opts_handle);

  auto baseIterator = baseIteratorAPI->get();
  auto* iterator =
      wbwiAPI->NewIteratorWithBase(cfhLocked.get(), baseIterator, read_opts);
  auto wbwiIterAPI =
      baseIteratorAPI->childIteratorWithBase(iterator, cfhLocked);
  return GET_CPLUSPLUS_POINTER(wbwiIterAPI.release());
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    nativeClose
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_nativeClose(JNIEnv*, jobject,
                                                      jlong jhandle) {
  std::unique_ptr<APIWriteBatchWithIndex> wbwiAPI(
      reinterpret_cast<APIWriteBatchWithIndex*>(jhandle));
  wbwiAPI->check("nativeClose()");
  // Now the unique_ptr destructor will delete() referenced shared_ptr contents
  // in the API object.
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getFromBatch
 * Signature: (JJ[BI)[B
 */
jbyteArray JNICALL Java_org_rocksdb_WriteBatchWithIndex_getFromBatch__JJ_3BI(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle, jlong jdbopt_handle,
    jbyteArray jkey, jint jkey_len) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto* dbopt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jdbopt_handle);

  auto getter = [&wbwiAPI, &dbopt](const ROCKSDB_NAMESPACE::Slice& key,
                                   std::string* value) {
    return wbwiAPI->GetFromBatch(*dbopt, key, value);
  };

  return ROCKSDB_NAMESPACE::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getFromBatch
 * Signature: (JJ[BIJ)[B
 */
jbyteArray Java_org_rocksdb_WriteBatchWithIndex_getFromBatch__JJ_3BIJ(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle, jlong jdbopt_handle,
    jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto* dbopt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jdbopt_handle);

  auto* cfhAPI =
      reinterpret_cast<APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>*>(
          jcf_handle);
  auto cfhLocked = cfhAPI->cfhLock(env);
  if (!cfhLocked) {
    return 0L;
  }

  auto getter = [&wbwiAPI, &cfhLocked, &dbopt](
                    const ROCKSDB_NAMESPACE::Slice& key, std::string* value) {
    return wbwiAPI->GetFromBatch(cfhLocked.get(), *dbopt, key, value);
  };

  return ROCKSDB_NAMESPACE::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getFromBatchAndDB
 * Signature: (JJJ[BI)[B
 */
jbyteArray Java_org_rocksdb_WriteBatchWithIndex_getFromBatchAndDB__JJJ_3BI(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle, jlong jdb_handle,
    jlong jreadopt_handle, jbyteArray jkey, jint jkey_len) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto& dbAPI =
      *reinterpret_cast<APIRocksDB<ROCKSDB_NAMESPACE::DB>*>(jdb_handle);
  auto* readopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jreadopt_handle);

  auto getter = [&wbwiAPI, &dbAPI, &readopt](
                    const ROCKSDB_NAMESPACE::Slice& key, std::string* value) {
    return wbwiAPI->GetFromBatchAndDB(dbAPI.get(), *readopt, key, value);
  };

  return ROCKSDB_NAMESPACE::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getFromBatchAndDB
 * Signature: (JJJ[BIJ)[B
 */
jbyteArray Java_org_rocksdb_WriteBatchWithIndex_getFromBatchAndDB__JJJ_3BIJ(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle, jlong jdb_handle,
    jlong jreadopt_handle, jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto& wbwiAPI = *reinterpret_cast<APIWriteBatchWithIndex*>(jwbwi_handle);
  auto& dbAPI =
      *reinterpret_cast<APIRocksDB<ROCKSDB_NAMESPACE::DB>*>(jdb_handle);
  auto* readopt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(jreadopt_handle);
  auto* cfhAPI =
      reinterpret_cast<APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>*>(
          jcf_handle);
  auto cfhLocked = cfhAPI->cfhLock(env);
  if (!cfhLocked) {
    return 0L;
  }

  auto getter = [&wbwiAPI, &dbAPI, &cfhLocked, &readopt](
                    const ROCKSDB_NAMESPACE::Slice& key, std::string* value) {
    return wbwiAPI->GetFromBatchAndDB(dbAPI.get(), *readopt, cfhLocked.get(),
                                      key, value);
  };

  return ROCKSDB_NAMESPACE::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/* WBWIRocksIterator below */

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    isValid0
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WBWIRocksIterator_isValid0(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong handle) {
  // TODO (AP) from here
  return (*reinterpret_cast<WBWIAPIIterator*>(handle))->Valid();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekToFirst0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekToFirst0(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong handle) {
  (*reinterpret_cast<WBWIAPIIterator*>(handle))->SeekToFirst();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekToLast0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekToLast0(JNIEnv* /*env*/,
                                                    jobject /*jobj*/,
                                                    jlong handle) {
  (*reinterpret_cast<WBWIAPIIterator*>(handle))->SeekToLast();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    next0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_next0(JNIEnv* /*env*/, jobject /*jobj*/,
                                              jlong handle) {
  (*reinterpret_cast<WBWIAPIIterator*>(handle))->Next();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    prev0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_prev0(JNIEnv* /*env*/, jobject /*jobj*/,
                                              jlong handle) {
  (*reinterpret_cast<WBWIAPIIterator*>(handle))->Prev();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seek0
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seek0(JNIEnv* env, jobject /*jobj*/,
                                              jlong handle, jbyteArray jtarget,
                                              jint jtarget_len) {
  auto& wbwiAPIIterator = *reinterpret_cast<WBWIAPIIterator*>(handle);
  jbyte* target = new jbyte[jtarget_len];
  env->GetByteArrayRegion(jtarget, 0, jtarget_len, target);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] target;
    return;
  }

  ROCKSDB_NAMESPACE::Slice target_slice(reinterpret_cast<char*>(target),
                                        jtarget_len);

  wbwiAPIIterator->Seek(target_slice);

  delete[] target;
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekDirect0
 * Signature: (JLjava/nio/ByteBuffer;II)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekDirect0(
    JNIEnv* env, jobject /*jobj*/, jlong handle, jobject jtarget,
    jint jtarget_off, jint jtarget_len) {
  auto& wbwiAPIIterator = *reinterpret_cast<WBWIAPIIterator*>(handle);
  auto seek = [&wbwiAPIIterator](ROCKSDB_NAMESPACE::Slice& target_slice) {
    wbwiAPIIterator->Seek(target_slice);
  };
  ROCKSDB_NAMESPACE::JniUtil::k_op_direct(seek, env, jtarget, jtarget_off,
                                          jtarget_len);
}

/*
 * This method supports fetching into indirect byte buffers;
 * the Java wrapper extracts the byte[] and passes it here.
 *
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekByteArray0
 * Signature: (J[BII)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekByteArray0(
    JNIEnv* env, jobject /*jobj*/, jlong handle, jbyteArray jtarget,
    jint jtarget_off, jint jtarget_len) {
  const std::unique_ptr<char[]> target(new char[jtarget_len]);
  if (target == nullptr) {
    jclass oom_class = env->FindClass("/lang/java/OutOfMemoryError");
    env->ThrowNew(oom_class,
                  "Memory allocation failed in RocksDB JNI function");
    return;
  }
  env->GetByteArrayRegion(jtarget, jtarget_off, jtarget_len,
                          reinterpret_cast<jbyte*>(target.get()));

  ROCKSDB_NAMESPACE::Slice target_slice(target.get(), jtarget_len);

  auto& wbwiAPIIterator = *reinterpret_cast<WBWIAPIIterator*>(handle);
  wbwiAPIIterator->Seek(target_slice);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekForPrev0
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekForPrev0(JNIEnv* env,
                                                     jobject /*jobj*/,
                                                     jlong handle,
                                                     jbyteArray jtarget,
                                                     jint jtarget_len) {
  auto& wbwiAPIIterator = *reinterpret_cast<WBWIAPIIterator*>(handle);
  jbyte* target = new jbyte[jtarget_len];
  env->GetByteArrayRegion(jtarget, 0, jtarget_len, target);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] target;
    return;
  }

  ROCKSDB_NAMESPACE::Slice target_slice(reinterpret_cast<char*>(target),
                                        jtarget_len);

  wbwiAPIIterator->SeekForPrev(target_slice);

  delete[] target;
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekForPrevDirect0
 * Signature: (JLjava/nio/ByteBuffer;II)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekForPrevDirect0(
    JNIEnv* env, jobject /*jobj*/, jlong handle, jobject jtarget,
    jint jtarget_off, jint jtarget_len) {
  auto& wbwiAPIIterator = *reinterpret_cast<WBWIAPIIterator*>(handle);
  auto seek_for_prev =
      [&wbwiAPIIterator](ROCKSDB_NAMESPACE::Slice& target_slice) {
        wbwiAPIIterator->SeekForPrev(target_slice);
      };
  ROCKSDB_NAMESPACE::JniUtil::k_op_direct(seek_for_prev, env, jtarget,
                                          jtarget_off, jtarget_len);
}

/*
 * This method supports fetching into indirect byte buffers;
 * the Java wrapper extracts the byte[] and passes it here.
 *
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekForPrevByteArray0
 * Signature: (J[BII)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekForPrevByteArray0(
    JNIEnv* env, jobject /*jobj*/, jlong handle, jbyteArray jtarget,
    jint jtarget_off, jint jtarget_len) {
  const std::unique_ptr<char[]> target(new char[jtarget_len]);
  if (target == nullptr) {
    jclass oom_class = env->FindClass("/lang/java/OutOfMemoryError");
    env->ThrowNew(oom_class,
                  "Memory allocation failed in RocksDB JNI function");
    return;
  }
  env->GetByteArrayRegion(jtarget, jtarget_off, jtarget_len,
                          reinterpret_cast<jbyte*>(target.get()));

  ROCKSDB_NAMESPACE::Slice target_slice(target.get(), jtarget_len);

  auto& wbwiAPIIterator = *reinterpret_cast<WBWIAPIIterator*>(handle);
  wbwiAPIIterator->SeekForPrev(target_slice);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    status0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_status0(JNIEnv* env, jobject /*jobj*/,
                                                jlong handle) {
  auto& wbwiAPIIterator = *reinterpret_cast<WBWIAPIIterator*>(handle);

  ROCKSDB_NAMESPACE::Status s = wbwiAPIIterator->status();

  if (s.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    entry1
 * Signature: (J)[J
 */
jlongArray Java_org_rocksdb_WBWIRocksIterator_entry1(JNIEnv* env,
                                                     jobject /*jobj*/,
                                                     jlong handle) {
  auto& wbwiAPIIterator = *reinterpret_cast<WBWIAPIIterator*>(handle);
  const ROCKSDB_NAMESPACE::WriteEntry& we = wbwiAPIIterator->Entry();

  jlong results[3];

  // set the type of the write entry
  results[0] = ROCKSDB_NAMESPACE::WriteTypeJni::toJavaWriteType(we.type);

  // NOTE: key_slice and value_slice will be freed by
  // org.rocksdb.DirectSlice#close

  auto* key_slice = new ROCKSDB_NAMESPACE::Slice(we.key.data(), we.key.size());
  results[1] = GET_CPLUSPLUS_POINTER(key_slice);
  if (we.type == ROCKSDB_NAMESPACE::kDeleteRecord ||
      we.type == ROCKSDB_NAMESPACE::kSingleDeleteRecord ||
      we.type == ROCKSDB_NAMESPACE::kLogDataRecord) {
    // set native handle of value slice to null if no value available
    results[2] = 0;
  } else {
    auto* value_slice =
        new ROCKSDB_NAMESPACE::Slice(we.value.data(), we.value.size());
    results[2] = GET_CPLUSPLUS_POINTER(value_slice);
  }

  jlongArray jresults = env->NewLongArray(3);
  if (jresults == nullptr) {
    // exception thrown: OutOfMemoryError
    if (results[2] != 0) {
      auto* value_slice =
          reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(results[2]);
      delete value_slice;
    }
    delete key_slice;
    return nullptr;
  }

  env->SetLongArrayRegion(jresults, 0, 3, results);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresults);
    if (results[2] != 0) {
      auto* value_slice =
          reinterpret_cast<ROCKSDB_NAMESPACE::Slice*>(results[2]);
      delete value_slice;
    }
    delete key_slice;
    return nullptr;
  }

  return jresults;
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    refresh0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_refresh0(JNIEnv* env) {
  ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::Status::NotSupported("Refresh() is not supported");
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    nativeClose
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_nativeClose(JNIEnv*, jobject,
                                                    jlong jhandle) {
  std::unique_ptr<WBWIAPIIterator> wbwiAPIIterator(
      reinterpret_cast<WBWIAPIIterator*>(jhandle));
  wbwiAPIIterator->check("nativeClose()");
  // Now the unique_ptr destructor will delete() referenced shared_ptr contents
  // in the API object.
}
