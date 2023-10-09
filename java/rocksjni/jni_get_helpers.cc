//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksjni/jni_get_helpers.h"

#include "jni_get_helpers.h"
#include "rocksjni/portal.h"

namespace ROCKSDB_NAMESPACE {

bool GetJNIKey::fromByteArray(JNIEnv* env, jbyteArray jkey, jint jkey_off,
                              jint jkey_len) {
  key_buf = std::make_unique<jbyte[]>(jkey_len);
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key_buf.get());
  if (env->ExceptionCheck()) {
    return false;
  }
  slice_ = Slice(reinterpret_cast<char*>(key_buf.get()), jkey_len);

  return true;
}

bool GetJNIKey::fromByteBuffer(JNIEnv* env, jobject jkey, jint jkey_off,
                               jint jkey_len) {
  char* key = reinterpret_cast<char*>(env->GetDirectBufferAddress(jkey));
  if (key == nullptr) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env,
        "Invalid key argument (argument is not a valid direct ByteBuffer)");
    return false;
  }
  if (env->GetDirectBufferCapacity(jkey) < (jkey_off + jkey_len)) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env,
        "Invalid key argument. Capacity is less than requested region (offset "
        "+ length).");
    return false;
  }

  slice_ = Slice(key + jkey_off, jkey_len);

  return true;
}

jint GetJNIValue::fillValue(JNIEnv* env, ROCKSDB_NAMESPACE::Status& s,
                            ROCKSDB_NAMESPACE::PinnableSlice& pinnable_value,
                            jbyteArray jval, jint jval_off, jint jval_len) {
  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
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

  const jint pinnable_value_len = static_cast<jint>(pinnable_value.size());
  const jint length = std::min(jval_len, pinnable_value_len);

  env->SetByteArrayRegion(jval, jval_off, length,
                          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(
                              pinnable_value.data())));
  pinnable_value.Reset();
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    return kStatusError;
  }

  return pinnable_value_len;
}

jint GetJNIValue::fillByteBuffer(
    JNIEnv* env, ROCKSDB_NAMESPACE::Status& s,
    ROCKSDB_NAMESPACE::PinnableSlice& pinnable_value, jobject jval,
    jint jval_off, jint jval_len) {
  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
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

  char* value = reinterpret_cast<char*>(env->GetDirectBufferAddress(jval));
  if (value == nullptr) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env,
        "Invalid value argument (argument is not a valid direct ByteBuffer)");
    return kArgumentError;
  }

  if (env->GetDirectBufferCapacity(jval) < (jval_off + jval_len)) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env,
        "Invalid value argument. Capacity is less than requested region "
        "(offset + length).");
    return kArgumentError;
  }

  const jint pinnable_value_len = static_cast<jint>(pinnable_value.size());
  const jint length = std::min(jval_len, pinnable_value_len);

  memcpy(value + jval_off, pinnable_value.data(), length);
  pinnable_value.Reset();

  return pinnable_value_len;
}

jbyteArray GetJNIValue::byteArray(JNIEnv* env, ROCKSDB_NAMESPACE::Status& s,
                                  ROCKSDB_NAMESPACE::PinnableSlice& value) {
  if (s.IsNotFound()) {
    return nullptr;
  }

  if (s.ok()) {
    jbyteArray jret_value = ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, value);
    value.Reset();
    if (jret_value == nullptr) {
      // exception occurred
      return nullptr;
    }
    return jret_value;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  return nullptr;
}

bool MultiGetJNIKeys::fromByteArrays(JNIEnv* env, jobjectArray jkeys) {
  const jsize num_keys = env->GetArrayLength(jkeys);

  for (jsize i = 0; i < num_keys; i++) {
    jobject jkey = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      return false;
    }

    jbyteArray jkey_ba = reinterpret_cast<jbyteArray>(jkey);
    const jsize len_key = env->GetArrayLength(jkey_ba);
    std::unique_ptr<jbyte[]> key = std::make_unique<jbyte[]>(len_key);
    jbyte* raw_key = reinterpret_cast<jbyte*>(key.get());
    key_bufs.push_back(std::move(key));
    env->GetByteArrayRegion(jkey_ba, 0, len_key, raw_key);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jkey);
      return false;
    }

    slices_.push_back(
        ROCKSDB_NAMESPACE::Slice(reinterpret_cast<char*>(raw_key), len_key));
    env->DeleteLocalRef(jkey);
  }

  return true;
}

bool MultiGetJNIKeys::fromByteArrays(JNIEnv* env, jobjectArray jkeys,
                                     jintArray jkey_offs, jintArray jkey_lens) {
  const jsize num_keys = env->GetArrayLength(jkeys);

  std::unique_ptr<jint[]> key_offs = std::make_unique<jint[]>(num_keys);
  env->GetIntArrayRegion(jkey_offs, 0, num_keys, key_offs.get());
  if (env->ExceptionCheck()) {
    return false;  // exception thrown: ArrayIndexOutOfBoundsException
  }

  std::unique_ptr<jint[]> key_lens = std::make_unique<jint[]>(num_keys);
  env->GetIntArrayRegion(jkey_lens, 0, num_keys, key_lens.get());
  if (env->ExceptionCheck()) {
    return false;  // exception thrown: ArrayIndexOutOfBoundsException
  }

  for (jsize i = 0; i < num_keys; i++) {
    jobject jkey = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      return false;
    }

    jbyteArray jkey_ba = reinterpret_cast<jbyteArray>(jkey);
    const jint len_key = key_lens[i];
    std::unique_ptr<jbyte[]> key = std::make_unique<jbyte[]>(len_key);
    jbyte* raw_key = reinterpret_cast<jbyte*>(key.get());
    key_bufs.push_back(std::move(key));
    env->GetByteArrayRegion(jkey_ba, key_offs[i], len_key, raw_key);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jkey);
      return false;
    }

    slices_.push_back(
        ROCKSDB_NAMESPACE::Slice(reinterpret_cast<char*>(raw_key), len_key));
    env->DeleteLocalRef(jkey);
  }
  return true;
}

bool MultiGetJNIKeys::fromByteBuffers(JNIEnv* env, jobjectArray jkeys,
                                      jintArray jkey_offs,
                                      jintArray jkey_lens) {
  const jsize num_keys = env->GetArrayLength(jkeys);

  std::unique_ptr<jint[]> key_offs = std::make_unique<jint[]>(num_keys);
  env->GetIntArrayRegion(jkey_offs, 0, num_keys, key_offs.get());
  if (env->ExceptionCheck()) {
    return false;  // exception thrown: ArrayIndexOutOfBoundsException
  }

  std::unique_ptr<jint[]> key_lens = std::make_unique<jint[]>(num_keys);
  env->GetIntArrayRegion(jkey_lens, 0, num_keys, key_lens.get());
  if (env->ExceptionCheck()) {
    return false;  // exception thrown: ArrayIndexOutOfBoundsException
  }

  for (jsize i = 0; i < num_keys; i++) {
    jobject jkey = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      return false;
    }

    char* key = reinterpret_cast<char*>(env->GetDirectBufferAddress(jkey));
    ROCKSDB_NAMESPACE::Slice key_slice(key + key_offs[i], key_lens[i]);
    slices_.push_back(key_slice);

    env->DeleteLocalRef(jkey);
  }
  return true;
}

ROCKSDB_NAMESPACE::Slice* MultiGetJNIKeys::data() { return slices_.data(); }

std::vector<ROCKSDB_NAMESPACE::Slice>::size_type MultiGetJNIKeys::size() {
  return slices_.size();
}

template <class TValue>
jobjectArray MultiGetJNIValues::byteArrays(
    JNIEnv* env, std::vector<TValue>& values,
    std::vector<ROCKSDB_NAMESPACE::Status>& s) {
  jobjectArray jresults = ROCKSDB_NAMESPACE::ByteJni::new2dByteArray(
      env, static_cast<jsize>(s.size()));
  if (jresults == nullptr) {
    // exception occurred
    OutOfMemoryErrorJni::ThrowNew(env, "Insufficient Memory for results.");
    return nullptr;
  }

  // add to the jresults
  for (std::vector<ROCKSDB_NAMESPACE::Status>::size_type i = 0; i != s.size();
       i++) {
    if (s[i].ok()) {
      TValue* value = &values[i];
      const jsize jvalue_len = static_cast<jsize>(value->size());
      jbyteArray jentry_value = env->NewByteArray(jvalue_len);
      if (jentry_value == nullptr) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      env->SetByteArrayRegion(
          jentry_value, 0, static_cast<jsize>(jvalue_len),
          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value->data())));
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

template jobjectArray MultiGetJNIValues::byteArrays<std::string>(
    JNIEnv* env, std::vector<std::string>& values,
    std::vector<ROCKSDB_NAMESPACE::Status>& s);

template jobjectArray
MultiGetJNIValues::byteArrays<ROCKSDB_NAMESPACE::PinnableSlice>(
    JNIEnv* env, std::vector<ROCKSDB_NAMESPACE::PinnableSlice>& values,
    std::vector<ROCKSDB_NAMESPACE::Status>& s);

template <class TValue>
void MultiGetJNIValues::fillValuesStatusObjects(
    JNIEnv* env, std::vector<TValue>& values,
    std::vector<ROCKSDB_NAMESPACE::Status>& s, jobjectArray jvalues,
    jintArray jvalue_sizes, jobjectArray jstatuses) {
  std::vector<jint> value_size;
  for (int i = 0; i < static_cast<jint>(values.size()); i++) {
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

  env->SetIntArrayRegion(jvalue_sizes, 0, static_cast<jint>(values.size()),
                         value_size.data());
}

template void
MultiGetJNIValues::fillValuesStatusObjects<ROCKSDB_NAMESPACE::PinnableSlice>(
    JNIEnv* env, std::vector<ROCKSDB_NAMESPACE::PinnableSlice>& values,
    std::vector<ROCKSDB_NAMESPACE::Status>& s, jobjectArray jvalues,
    jintArray jvalue_sizes, jobjectArray jstatuses);

std::unique_ptr<std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>>
ColumnFamilyJNIHelpers::handlesFromJLongArray(
    JNIEnv* env, jlongArray jcolumn_family_handles) {
  if (jcolumn_family_handles == nullptr) return nullptr;

  const jsize num_cols = env->GetArrayLength(jcolumn_family_handles);
  std::unique_ptr<jlong[]> jcf_handles = std::make_unique<jlong[]>(num_cols);
  env->GetLongArrayRegion(jcolumn_family_handles, 0, num_cols,
                          jcf_handles.get());
  if (env->ExceptionCheck())
    // ArrayIndexOutOfBoundsException
    return nullptr;
  auto cf_handles =
      std::make_unique<std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>>();

  for (jsize i = 0; i < num_cols; i++) {
    auto* cf_handle = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
        jcf_handles.get()[i]);
    cf_handles->push_back(cf_handle);
  }

  return cf_handles;
}

ROCKSDB_NAMESPACE::ColumnFamilyHandle* ColumnFamilyJNIHelpers::handleFromJLong(
    JNIEnv* env, jlong jcolumn_family_handle) {
  auto cf_handle = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(
      jcolumn_family_handle);
  if (cf_handle == nullptr) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, ROCKSDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
    return nullptr;
  }
  return cf_handle;
};

};  // namespace ROCKSDB_NAMESPACE
