// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::WriteBatchJavaNative methods from Java side.

#include "rocksjni/write_batch_java_native.h"

#include "db/column_family.h"
#include "db/dbformat.h"
#include "db/write_batch_internal.h"
#include "include/org_rocksdb_WBWIRocksIterator.h"
#include "include/org_rocksdb_WriteBatchJavaNative.h"
#include "rocksdb/comparator.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/kv_helper.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_WriteBatchJavaNative
 * Method:    disposeInternalWriteBatchJavaNative
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchJavaNative_disposeInternalWriteBatchJavaNative(
    JNIEnv* /* env */, jclass /* jcls */, jlong handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchJavaNative*>(handle);
  assert(wb != nullptr);
  delete wb;
}

/**
 * @brief
 *
 * @param slice
 * @return ROCKSDB_NAMESPACE::WriteBatchJavaNative& this
 */
ROCKSDB_NAMESPACE::WriteBatchJavaNative&
ROCKSDB_NAMESPACE::WriteBatchJavaNative::Append(const Slice& slice) {
  if (Count() == 0) {
    WriteBatchInternal::SetContents(this, slice);
  } else {
    WriteBatchInternal::AppendContents(this, slice);
  }

  return *this;
}

/*
 * Class:     org_rocksdb_WriteBatchJavaNative
 * Method:    flushWriteBatchJavaNativeArray
 * Signature: (JJ[B)V
 */
jlong Java_org_rocksdb_WriteBatchJavaNative_flushWriteBatchJavaNativeArray(
    JNIEnv* env, jclass /*jcls*/, jlong jwb_handle, jlong wb_capacity,
    jbyteArray jbuf, jint jbuf_len) {
  ROCKSDB_NAMESPACE::WriteBatchJavaNative* wb;
  if (jwb_handle == 0) {
    wb = new ROCKSDB_NAMESPACE::WriteBatchJavaNative(
        static_cast<size_t>(wb_capacity));
  } else {
    wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchJavaNative*>(jwb_handle);
  }

  jbyte* buf = env->GetByteArrayElements(jbuf, nullptr);
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    return -1L;
  }

  try {
    wb->Append(
        ROCKSDB_NAMESPACE::Slice(reinterpret_cast<char*>(buf), jbuf_len));
  } catch (ROCKSDB_NAMESPACE::WriteBatchJavaNativeException& e) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, e.Message());
    return e.Code();
  }

  env->ReleaseByteArrayElements(jbuf, buf, JNI_ABORT);

  return GET_CPLUSPLUS_POINTER(wb);
}

/*
 * Class:     org_rocksdb_WriteBatchJavaNative
 * Method:    flushWriteBatchJavaNativeDirect
 * Signature: (JJLjava/nio/ByteBuffer;II)J
 */
jlong Java_org_rocksdb_WriteBatchJavaNative_flushWriteBatchJavaNativeDirect(
    JNIEnv* env, jclass /*jcls*/, jlong jwb_handle, jlong wb_capacity,
    jobject jbuf, jint jbuf_pos, jint jbuf_limit) {
  ROCKSDB_NAMESPACE::WriteBatchJavaNative* wb;
  if (jwb_handle == 0) {
    wb = new ROCKSDB_NAMESPACE::WriteBatchJavaNative(
        static_cast<size_t>(wb_capacity));
  } else {
    wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchJavaNative*>(jwb_handle);
  }

  jbyte* buf = reinterpret_cast<jbyte*>(env->GetDirectBufferAddress(jbuf));

  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    return -1L;
  }

  try {
    wb->Append(ROCKSDB_NAMESPACE::Slice(reinterpret_cast<char*>(buf + jbuf_pos),
                                        jbuf_limit - jbuf_pos));
  } catch (ROCKSDB_NAMESPACE::WriteBatchJavaNativeException& e) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, e.Message());
    return e.Code();
  }

  return GET_CPLUSPLUS_POINTER(wb);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    writeJavaNative0
 * Signature: (JJJ)V
 *
 * Write a "java native" write batch to the DB.
 * The C++ side write batch object may not have been created; if it hasn't,
 * create it now.
 */
jlong Java_org_rocksdb_WriteBatchJavaNative_writeWriteBatchJavaNativeArray(
    JNIEnv* env, jclass, jlong jdb_handle, jlong jwrite_options_handle,
    jlong jwb_handle, jlong wb_capacity, jbyteArray jbuf, jint jbuf_len) {
  auto* db = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);

  ROCKSDB_NAMESPACE::WriteBatchJavaNative* wb;
  if (jwb_handle == 0) {
    wb = new ROCKSDB_NAMESPACE::WriteBatchJavaNative(
        static_cast<size_t>(wb_capacity));
  } else {
    wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchJavaNative*>(jwb_handle);
  }

  jbyte* buf = env->GetByteArrayElements(jbuf, nullptr);
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    return -1L;
  }

  try {
    wb->Append(
        ROCKSDB_NAMESPACE::Slice(reinterpret_cast<char*>(buf), jbuf_len));
  } catch (ROCKSDB_NAMESPACE::WriteBatchJavaNativeException& e) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, e.Message());
    return e.Code();
  }

  env->ReleaseByteArrayElements(jbuf, buf, JNI_ABORT);

  ROCKSDB_NAMESPACE::Status s = db->Write(*write_options, wb);

  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }

  return GET_CPLUSPLUS_POINTER(wb);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    writeJavaNative0
 * Signature: (JJJ)V
 *
 * Write a "java native" write batch to the DB.
 * The C++ side write batch object may not have been created; if it hasn't,
 * create it now.
 */
jlong Java_org_rocksdb_WriteBatchJavaNative_writeWriteBatchJavaNativeDirect(
    JNIEnv* env, jclass, jlong jdb_handle, jlong jwrite_options_handle,
    jlong jwb_handle, jlong jbuf_capacity, jobject jbuf, jint jbuf_pos,
    jint jbuf_limit) {
  auto* db = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);

  ROCKSDB_NAMESPACE::WriteBatchJavaNative* wb;
  if (jwb_handle == 0) {
    wb = new ROCKSDB_NAMESPACE::WriteBatchJavaNative(
        static_cast<size_t>(jbuf_capacity));
  } else {
    wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchJavaNative*>(jwb_handle);
  }

  jbyte* buf = reinterpret_cast<jbyte*>(env->GetDirectBufferAddress(jbuf));
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    return -1L;
  }
  try {
    wb->Append(ROCKSDB_NAMESPACE::Slice(
        reinterpret_cast<const char*>(buf) + jbuf_pos, jbuf_limit - jbuf_pos));
  } catch (ROCKSDB_NAMESPACE::WriteBatchJavaNativeException& e) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, e.Message());
    return e.Code();
  }
  ROCKSDB_NAMESPACE::Status s = db->Write(*write_options, wb);

  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }

  return GET_CPLUSPLUS_POINTER(wb);
}

/*
 * Class:     org_rocksdb_WriteBatchJavaNative
 * Method:    putWriteBatchJavaNativeArray
 * Signature: (JJ[BI[BIJ)J
 */
jlong Java_org_rocksdb_WriteBatchJavaNative_putWriteBatchJavaNativeArray(
    JNIEnv* env, jclass, jlong jwb_handle, jlong jwb_capacity, jbyteArray jkey,
    jint jkey_len, jbyteArray jvalue, jint jvalue_len, jlong jcf_handle) {
  ROCKSDB_NAMESPACE::WriteBatchJavaNative* wb;
  if (jwb_handle == 0) {
    wb = new ROCKSDB_NAMESPACE::WriteBatchJavaNative(
        static_cast<size_t>(jwb_capacity));
  } else {
    wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchJavaNative*>(jwb_handle);
  }

  try {
    ROCKSDB_NAMESPACE::JByteArraySlice key(env, jkey, 0, jkey_len);
    ROCKSDB_NAMESPACE::JByteArraySlice value(env, jvalue, 0, jvalue_len);
    if (jcf_handle == 0) {
      ROCKSDB_NAMESPACE::KVException::ThrowOnError(
          env, wb->Put(key.slice(), value.slice()));
    } else {
      ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle =
          reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
      ROCKSDB_NAMESPACE::KVException::ThrowOnError(
          env, wb->Put(cf_handle, key.slice(), value.slice()));
    }
  } catch (const ROCKSDB_NAMESPACE::KVException& e) {
    return e.Code();
  }

  return GET_CPLUSPLUS_POINTER(wb);
}

/*
 * Class:     org_rocksdb_WriteBatchJavaNative
 * Method:    putWriteBatchJavaNativeDirect
 * Signature: (JJLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJ)J
 */
jlong Java_org_rocksdb_WriteBatchJavaNative_putWriteBatchJavaNativeDirect(
    JNIEnv* env, jclass, jlong jwb_handle, jlong jwb_capacity, jobject jkey,
    jint jkey_pos, jint jkey_remaining, jobject jvalue, jint jvalue_pos,
    jint jvalue_remaining, jlong jcf_handle) {
  ROCKSDB_NAMESPACE::WriteBatchJavaNative* wb;
  if (jwb_handle == 0) {
    wb = new ROCKSDB_NAMESPACE::WriteBatchJavaNative(
        static_cast<size_t>(jwb_capacity));
  } else {
    wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchJavaNative*>(jwb_handle);
  }

  try {
    ROCKSDB_NAMESPACE::JDirectBufferSlice key(env, jkey, jkey_pos,
                                              jkey_remaining);
    ROCKSDB_NAMESPACE::JDirectBufferSlice value(env, jvalue, jvalue_pos,
                                                jvalue_remaining);
    if (jcf_handle == 0) {
      ROCKSDB_NAMESPACE::KVException::ThrowOnError(
          env, wb->Put(key.slice(), value.slice()));
    } else {
      ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle =
          reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
      ROCKSDB_NAMESPACE::KVException::ThrowOnError(
          env, wb->Put(cf_handle, key.slice(), value.slice()));
    }
  } catch (const ROCKSDB_NAMESPACE::KVException& e) {
    return e.Code();
  }

  return GET_CPLUSPLUS_POINTER(wb);
}
