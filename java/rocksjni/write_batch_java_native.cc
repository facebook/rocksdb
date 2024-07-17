// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::WriteBatchJavaNative methods from Java side.


#include "include/org_rocksdb_WBWIRocksIterator.h"
#include "include/org_rocksdb_WriteBatchJavaNative.h"
#include "rocksdb/comparator.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"
#include "db/dbformat.h"


/*
 * Class:     org_rocksdb_WriteBatchJavaNative
 * Method:    newWriteBatchJavaNative
 * Signature: (I)J
 */
jlong Java_org_rocksdb_WriteBatchJavaNative_newWriteBatchJavaNative(
    JNIEnv* /*env*/, jclass /*jcls*/, jint jreserved_bytes) {
  auto* wb =
      new ROCKSDB_NAMESPACE::WriteBatch(static_cast<size_t>(jreserved_bytes));
  return GET_CPLUSPLUS_POINTER(wb);
}

static int ALIGN = sizeof(int) - 1;

jint next_int(jbyte* buf, jint& pos) {
  jint result = *reinterpret_cast<jint*>(buf + pos);
  pos += sizeof(jint);
  return result;
}

/*
 * Class:     org_rocksdb_WriteBatchJavaNative
 * Method:    disposeInternalWriteBatchJavaNative
 * Signature: (J)V
 * 
 * This variant on WriteBatch (Java) class is represented on the C++ by a plain old WriteBatch.
 * We may want our own class (subclass or wrapper) in the long run.
 */
void Java_org_rocksdb_WriteBatchJavaNative_disposeInternalWriteBatchJavaNative(
    JNIEnv* /* env */, jclass /* jcls */, jlong handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(handle);
  assert(wb != nullptr);
  delete wb;
}

/*
 * Class:     org_rocksdb_WriteBatchJavaNative
 * Method:    flushWriteBatchJavaNative
 * Signature: (JJ[B)V
 */
void Java_org_rocksdb_WriteBatchJavaNative_flushWriteBatchJavaNative(
    JNIEnv* env, jclass /*jcls*/, jlong jwb_handle, jlong jbuf_len,
    jbyteArray jbuf) {
  auto* wb =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchWithIndex*>(jwb_handle);
  assert(wb != nullptr);

  jbyte* buf = env->GetByteArrayElements(jbuf, nullptr);
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jint pos = 0;
  while (pos < jbuf_len) {
    jint op = next_int(buf, pos);
    switch (op) {
      case ROCKSDB_NAMESPACE::ValueType::kTypeValue: {
        jint key_len = next_int(buf, pos);
        jint value_len = next_int(buf, pos);
        char* key_ptr = reinterpret_cast<char*>(buf + pos);
        if ((pos += key_len + ALIGN & ~ALIGN) > jbuf_len) {
          ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
              env,
              "Corrupt java native write batch ? no space for expected key");
          return;
        }
        char* value_ptr = reinterpret_cast<char*>(buf + pos);
        if ((pos += value_len + ALIGN & ~ALIGN) > jbuf_len) {
          ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
              env,
              "Corrupt java native write batch ? no space for expected value");
          return;
        }

        ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key_ptr),
                                           key_len);
        ROCKSDB_NAMESPACE::Slice value_slice(reinterpret_cast<char*>(value_ptr),
                                             value_len);
        ROCKSDB_NAMESPACE::Status status = wb->Put(key_slice, value_slice);
        if (!status.ok()) {
          ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
          return;
        }
      } break;

      default: {
        ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
            env, std::string("Unexpected writebatch command ")
                     .append(std::to_string(op)));
        return;
      } break;
    }
  }

  env->ReleaseByteArrayElements(jbuf, buf, JNI_ABORT);
}
