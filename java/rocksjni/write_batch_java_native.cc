// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::WriteBatchJavaNative methods from Java side.

#include "rocksjni/write_batch_java_native.h"

#include "db/dbformat.h"
#include "db/write_batch_internal.h"
#include "include/org_rocksdb_WBWIRocksIterator.h"
#include "include/org_rocksdb_WriteBatchJavaNative.h"
#include "rocksdb/comparator.h"
#include "rocksjni/cplusplus_to_java_convert.h"
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
 * @brief copy operations from Java-side write batch cache to C++ side write
 * batch
 *
 * @param wb write batch
 */
void ROCKSDB_NAMESPACE::WriteBatchJavaNativeBuffer::copy_write_batch_from_java(
    ROCKSDB_NAMESPACE::WriteBatchJavaNative* wb) {
  while (has_next()) {
    jint op = next_int();
    switch (op) {
      case ROCKSDB_NAMESPACE::ValueType::kTypeValue: {
        jint key_len = next_int();
        jint value_len = next_int();

        // *** TODO (AP) how to handle exceptions here ?
        // *** pass in the message to bp->slice ?
        // *** throw Java exception like KVException

        ROCKSDB_NAMESPACE::Slice key_slice = slice(key_len);
        ROCKSDB_NAMESPACE::Slice value_slice = slice(value_len);

        ROCKSDB_NAMESPACE::Status status = wb->Put(key_slice, value_slice);
        if (!status.ok()) {
          ROCKSDB_NAMESPACE::WriteBatchJavaNativeException::ThrowNew(env,
                                                                     status);
          return;
        }
      } break;

      case ROCKSDB_NAMESPACE::ValueType::kTypeColumnFamilyValue: {
        jlong jcf_handle = next_long();
        auto* cfh = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);

        jint key_len = next_int();
        jint value_len = next_int();
        auto key_slice = slice(key_len);
        auto value_slice = slice(value_len);

        auto status = wb->Put(cfh, key_slice, value_slice);
        if(!status.ok()) {
          ROCKSDB_NAMESPACE::WriteBatchJavaNativeException::ThrowNew(env,
                                                                     status);
          return;
        }
      } break;

      default: {
        ROCKSDB_NAMESPACE::WriteBatchJavaNativeException::ThrowNew(
            env, std::string("Unexpected writebatch command ")
                     .append(std::to_string(op)));
        return;
      } break;
    }
  }
}

/*
 * Class:     org_rocksdb_WriteBatchJavaNative
 * Method:    flushWriteBatchJavaNativeArray
 * Signature: (JJ[B)V
 */
jlong Java_org_rocksdb_WriteBatchJavaNative_flushWriteBatchJavaNativeArray(
    JNIEnv* env, jclass /*jcls*/, jlong jwb_handle, jlong jbuf_capacity,
    jlong jbuf_len, jbyteArray jbuf) {
  ROCKSDB_NAMESPACE::WriteBatchJavaNative* wb;
  if (jwb_handle == 0) {
    wb = new ROCKSDB_NAMESPACE::WriteBatchJavaNative(
        static_cast<size_t>(jbuf_capacity));
  } else {
    wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchJavaNative*>(jwb_handle);
  }

  jbyte* buf = env->GetByteArrayElements(jbuf, nullptr);
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    return -1L;
  }

  auto bp = std::make_unique<ROCKSDB_NAMESPACE::WriteBatchJavaNativeBuffer>(
      env, buf, jbuf_len);

  if (bp->sequence() > 0) {
    ROCKSDB_NAMESPACE::WriteBatchInternal::SetSequence(
        wb, static_cast<ROCKSDB_NAMESPACE::SequenceNumber>(bp->sequence()));
  }

  try {
    bp->copy_write_batch_from_java(wb);
  } catch (ROCKSDB_NAMESPACE::WriteBatchJavaNativeException&) {
    // Java exception is set
    return -1L;
  }

  env->ReleaseByteArrayElements(jbuf, buf, JNI_ABORT);

  return GET_CPLUSPLUS_POINTER(wb);
}

/**
 * @brief 
 * 
 */
jlong Java_org_rocksdb_WriteBatchJavaNative_flushWriteBatchJavaNativeDirect(
    JNIEnv* env, jclass /*jcls*/, jlong jwb_handle, jlong jbuf_capacity,
    jlong jbuf_len, jobject jbuf) {
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
  auto bp = std::make_unique<ROCKSDB_NAMESPACE::WriteBatchJavaNativeBuffer>(
      env, buf, jbuf_len);

  if (bp->sequence() > 0) {
    ROCKSDB_NAMESPACE::WriteBatchInternal::SetSequence(
        wb, static_cast<ROCKSDB_NAMESPACE::SequenceNumber>(bp->sequence()));
  }

  try {
    bp->copy_write_batch_from_java(wb);
  } catch (ROCKSDB_NAMESPACE::WriteBatchJavaNativeException&) {
    // Java exception is set
    return -1L;
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
    jlong jwb_handle, jlong jbuf_capacity, jlong jbuf_len, jbyteArray jbuf) {
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

  jbyte* buf = env->GetByteArrayElements(jbuf, nullptr);
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    return -1L;
  }
  auto bp = std::make_unique<ROCKSDB_NAMESPACE::WriteBatchJavaNativeBuffer>(
      env, buf, jbuf_len);

  if (bp->sequence() > 0) {
    ROCKSDB_NAMESPACE::WriteBatchInternal::SetSequence(
        wb, static_cast<ROCKSDB_NAMESPACE::SequenceNumber>(bp->sequence()));
  }
  try {
    bp->copy_write_batch_from_java(wb);
  } catch (ROCKSDB_NAMESPACE::WriteBatchJavaNativeException&) {
    // Java exception is set
    return -1L;
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
    jlong jwb_handle, jlong jbuf_capacity, jlong jbuf_len, jobject jbuf) {
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
  auto bp = std::make_unique<ROCKSDB_NAMESPACE::WriteBatchJavaNativeBuffer>(
      env, buf, jbuf_len);

  if (bp->sequence() > 0) {
    ROCKSDB_NAMESPACE::WriteBatchInternal::SetSequence(
        wb, static_cast<ROCKSDB_NAMESPACE::SequenceNumber>(bp->sequence()));
  }
  try {
    bp->copy_write_batch_from_java(wb);
  } catch (ROCKSDB_NAMESPACE::WriteBatchJavaNativeException&) {
    // Java exception is set
    return -1L;
  }
  ROCKSDB_NAMESPACE::Status s = db->Write(*write_options, wb);

  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }

  return GET_CPLUSPLUS_POINTER(wb);
}
