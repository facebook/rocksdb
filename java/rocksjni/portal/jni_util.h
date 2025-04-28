// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#pragma once

#include <jni.h>

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksjni/portal/common.h"

namespace ROCKSDB_NAMESPACE {
// various utility functions for working with RocksDB and JNI
class JniUtil {
 public:
  /**
   * Detect if jlong overflows size_t
   *
   * @param jvalue the jlong value
   *
   * @return
   */
  inline static Status check_if_jlong_fits_size_t(const jlong& jvalue) {
    Status s = Status::OK();
    if (static_cast<uint64_t>(jvalue) > std::numeric_limits<size_t>::max()) {
      s = Status::InvalidArgument(Slice("jlong overflows 32 bit value."));
    }
    return s;
  }

  /**
   * Obtains a reference to the JNIEnv from
   * the JVM
   *
   * If the current thread is not attached to the JavaVM
   * then it will be attached so as to retrieve the JNIEnv
   *
   * If a thread is attached, it must later be manually
   * released by calling JavaVM::DetachCurrentThread.
   * This can be handled by always matching calls to this
   * function with calls to {@link JniUtil::releaseJniEnv(JavaVM*, jboolean)}
   *
   * @param jvm (IN) A pointer to the JavaVM instance
   * @param attached (OUT) A pointer to a boolean which
   *     will be set to JNI_TRUE if we had to attach the thread
   *
   * @return A pointer to the JNIEnv or nullptr if a fatal error
   *     occurs and the JNIEnv cannot be retrieved
   */
  static JNIEnv* getJniEnv(JavaVM* jvm, jboolean* attached) {
    assert(jvm != nullptr);

    JNIEnv* env;
    const jint env_rs =
        jvm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_6);

    if (env_rs == JNI_OK) {
      // current thread is already attached, return the JNIEnv
      *attached = JNI_FALSE;
      return env;
    } else if (env_rs == JNI_EDETACHED) {
      // current thread is not attached, attempt to attach
      const jint rs_attach =
          jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
      if (rs_attach == JNI_OK) {
        *attached = JNI_TRUE;
        return env;
      } else {
        // error, could not attach the thread
        std::cerr << "JniUtil::getJniEnv - Fatal: could not attach current "
                     "thread to JVM!"
                  << std::endl;
        return nullptr;
      }
    } else if (env_rs == JNI_EVERSION) {
      // error, JDK does not support JNI_VERSION_1_6+
      std::cerr
          << "JniUtil::getJniEnv - Fatal: JDK does not support JNI_VERSION_1_6"
          << std::endl;
      return nullptr;
    } else {
      std::cerr << "JniUtil::getJniEnv - Fatal: Unknown error: env_rs="
                << env_rs << std::endl;
      return nullptr;
    }
  }

  /**
   * Counterpart to {@link JniUtil::getJniEnv(JavaVM*, jboolean*)}
   *
   * Detachess the current thread from the JVM if it was previously
   * attached
   *
   * @param jvm (IN) A pointer to the JavaVM instance
   * @param attached (IN) JNI_TRUE if we previously had to attach the thread
   *     to the JavaVM to get the JNIEnv
   */
  static void releaseJniEnv(JavaVM* jvm, jboolean& attached) {
    assert(jvm != nullptr);
    if (attached == JNI_TRUE) {
      const jint rs_detach = jvm->DetachCurrentThread();
      assert(rs_detach == JNI_OK);
      if (rs_detach != JNI_OK) {
        std::cerr << "JniUtil::getJniEnv - Warn: Unable to detach current "
                     "thread from JVM!"
                  << std::endl;
      }
    }
  }

  /**
   * Copies a Java String[] to a C++ std::vector<std::string>
   *
   * @param env (IN) A pointer to the java environment
   * @param jss (IN) The Java String array to copy
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError or ArrayIndexOutOfBoundsException
   *     exception occurs
   *
   * @return A std::vector<std:string> containing copies of the Java strings
   */
  static std::vector<std::string> copyStrings(JNIEnv* env, jobjectArray jss,
                                              jboolean* has_exception) {
    return ROCKSDB_NAMESPACE::JniUtil::copyStrings(
        env, jss, env->GetArrayLength(jss), has_exception);
  }

  /**
   * Copies a Java String[] to a C++ std::vector<std::string>
   *
   * @param env (IN) A pointer to the java environment
   * @param jss (IN) The Java String array to copy
   * @param jss_len (IN) The length of the Java String array to copy
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError or ArrayIndexOutOfBoundsException
   *     exception occurs
   *
   * @return A std::vector<std:string> containing copies of the Java strings
   */
  static std::vector<std::string> copyStrings(JNIEnv* env, jobjectArray jss,
                                              const jsize jss_len,
                                              jboolean* has_exception) {
    std::vector<std::string> strs;
    strs.reserve(jss_len);
    for (jsize i = 0; i < jss_len; i++) {
      jobject js = env->GetObjectArrayElement(jss, i);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        *has_exception = JNI_TRUE;
        return strs;
      }

      jstring jstr = static_cast<jstring>(js);
      const char* str = env->GetStringUTFChars(jstr, nullptr);
      if (str == nullptr) {
        // exception thrown: OutOfMemoryError
        env->DeleteLocalRef(js);
        *has_exception = JNI_TRUE;
        return strs;
      }

      strs.push_back(std::string(str));

      env->ReleaseStringUTFChars(jstr, str);
      env->DeleteLocalRef(js);
    }

    *has_exception = JNI_FALSE;
    return strs;
  }

  /**
   * Copies a jstring to a C-style null-terminated byte string
   * and releases the original jstring
   *
   * The jstring is copied as UTF-8
   *
   * If an exception occurs, then JNIEnv::ExceptionCheck()
   * will have been called
   *
   * @param env (IN) A pointer to the java environment
   * @param js (IN) The java string to copy
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError exception occurs
   *
   * @return A pointer to the copied string, or a
   *     nullptr if has_exception == JNI_TRUE
   */
  static std::unique_ptr<char[]> copyString(JNIEnv* env, jstring js,
                                            jboolean* has_exception) {
    const char* utf = env->GetStringUTFChars(js, nullptr);
    if (utf == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ExceptionCheck();
      *has_exception = JNI_TRUE;
      return nullptr;
    } else if (env->ExceptionCheck()) {
      // exception thrown
      env->ReleaseStringUTFChars(js, utf);
      *has_exception = JNI_TRUE;
      return nullptr;
    }

    const jsize utf_len = env->GetStringUTFLength(js);
    std::unique_ptr<char[]> str(
        new char[utf_len +
                 1]);  // Note: + 1 is needed for the c_str null terminator
    std::strcpy(str.get(), utf);
    env->ReleaseStringUTFChars(js, utf);
    *has_exception = JNI_FALSE;
    return str;
  }

  /**
   * Copies a jstring to a std::string
   * and releases the original jstring
   *
   * If an exception occurs, then JNIEnv::ExceptionCheck()
   * will have been called
   *
   * @param env (IN) A pointer to the java environment
   * @param js (IN) The java string to copy
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError exception occurs
   *
   * @return A std:string copy of the jstring, or an
   *     empty std::string if has_exception == JNI_TRUE
   */
  static std::string copyStdString(JNIEnv* env, jstring js,
                                   jboolean* has_exception) {
    const char* utf = env->GetStringUTFChars(js, nullptr);
    if (utf == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ExceptionCheck();
      *has_exception = JNI_TRUE;
      return std::string();
    } else if (env->ExceptionCheck()) {
      // exception thrown
      env->ReleaseStringUTFChars(js, utf);
      *has_exception = JNI_TRUE;
      return std::string();
    }

    std::string name(utf);
    env->ReleaseStringUTFChars(js, utf);
    *has_exception = JNI_FALSE;
    return name;
  }

  /**
   * Copies bytes from a std::string to a jByteArray
   *
   * @param env A pointer to the java environment
   * @param bytes The bytes to copy
   *
   * @return the Java byte[], or nullptr if an exception occurs
   *
   * @throws RocksDBException thrown
   *   if memory size to copy exceeds general java specific array size
   * limitation.
   */
  static jbyteArray copyBytes(JNIEnv* env, std::string bytes) {
    return createJavaByteArrayWithSizeCheck(env, bytes.c_str(), bytes.size());
  }

  /**
   * Given a Java byte[][] which is an array of java.lang.Strings
   * where each String is a byte[], the passed function `string_fn`
   * will be called on each String, the result is the collected by
   * calling the passed function `collector_fn`
   *
   * @param env (IN) A pointer to the java environment
   * @param jbyte_strings (IN) A Java array of Strings expressed as bytes
   * @param string_fn (IN) A transform function to call for each String
   * @param collector_fn (IN) A collector which is called for the result
   *     of each `string_fn`
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an ArrayIndexOutOfBoundsException or OutOfMemoryError
   *     exception occurs
   */
  template <typename T>
  static void byteStrings(JNIEnv* env, jobjectArray jbyte_strings,
                          std::function<T(const char*, const size_t)> string_fn,
                          std::function<void(size_t, T)> collector_fn,
                          jboolean* has_exception) {
    const jsize jlen = env->GetArrayLength(jbyte_strings);

    for (jsize i = 0; i < jlen; i++) {
      jobject jbyte_string_obj = env->GetObjectArrayElement(jbyte_strings, i);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        *has_exception = JNI_TRUE;  // signal error
        return;
      }

      jbyteArray jbyte_string_ary =
          reinterpret_cast<jbyteArray>(jbyte_string_obj);
      T result = byteString(env, jbyte_string_ary, string_fn, has_exception);

      env->DeleteLocalRef(jbyte_string_obj);

      if (*has_exception == JNI_TRUE) {
        // exception thrown: OutOfMemoryError
        return;
      }

      collector_fn(i, result);
    }

    *has_exception = JNI_FALSE;
  }

  /**
   * Given a Java String which is expressed as a Java Byte Array byte[],
   * the passed function `string_fn` will be called on the String
   * and the result returned
   *
   * @param env (IN) A pointer to the java environment
   * @param jbyte_string_ary (IN) A Java String expressed in bytes
   * @param string_fn (IN) A transform function to call on the String
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError exception occurs
   */
  template <typename T>
  static T byteString(JNIEnv* env, jbyteArray jbyte_string_ary,
                      std::function<T(const char*, const size_t)> string_fn,
                      jboolean* has_exception) {
    const jsize jbyte_string_len = env->GetArrayLength(jbyte_string_ary);
    return byteString<T>(env, jbyte_string_ary, jbyte_string_len, string_fn,
                         has_exception);
  }

  /**
   * Given a Java String which is expressed as a Java Byte Array byte[],
   * the passed function `string_fn` will be called on the String
   * and the result returned
   *
   * @param env (IN) A pointer to the java environment
   * @param jbyte_string_ary (IN) A Java String expressed in bytes
   * @param jbyte_string_len (IN) The length of the Java String
   *     expressed in bytes
   * @param string_fn (IN) A transform function to call on the String
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError exception occurs
   */
  template <typename T>
  static T byteString(JNIEnv* env, jbyteArray jbyte_string_ary,
                      const jsize jbyte_string_len,
                      std::function<T(const char*, const size_t)> string_fn,
                      jboolean* has_exception) {
    jbyte* jbyte_string = env->GetByteArrayElements(jbyte_string_ary, nullptr);
    if (jbyte_string == nullptr) {
      // exception thrown: OutOfMemoryError
      *has_exception = JNI_TRUE;
      return nullptr;  // signal error
    }

    T result =
        string_fn(reinterpret_cast<char*>(jbyte_string), jbyte_string_len);

    env->ReleaseByteArrayElements(jbyte_string_ary, jbyte_string, JNI_ABORT);

    *has_exception = JNI_FALSE;
    return result;
  }

  /**
   * Converts a std::vector<string> to a Java byte[][] where each Java String
   * is expressed as a Java Byte Array byte[].
   *
   * @param env A pointer to the java environment
   * @param strings A vector of Strings
   *
   * @return A Java array of Strings expressed as bytes,
   *     or nullptr if an exception is thrown
   */
  static jobjectArray stringsBytes(JNIEnv* env,
                                   std::vector<std::string> strings) {
    jclass jcls_ba = ByteJni::getArrayJClass(env);
    if (jcls_ba == nullptr) {
      // exception occurred
      return nullptr;
    }

    const jsize len = static_cast<jsize>(strings.size());

    jobjectArray jbyte_strings = env->NewObjectArray(len, jcls_ba, nullptr);
    if (jbyte_strings == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    for (jsize i = 0; i < len; i++) {
      std::string* str = &strings[i];
      const jsize str_len = static_cast<jsize>(str->size());

      jbyteArray jbyte_string_ary = env->NewByteArray(str_len);
      if (jbyte_string_ary == nullptr) {
        // exception thrown: OutOfMemoryError
        env->DeleteLocalRef(jbyte_strings);
        return nullptr;
      }

      env->SetByteArrayRegion(
          jbyte_string_ary, 0, str_len,
          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(str->c_str())));
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jbyte_string_ary);
        env->DeleteLocalRef(jbyte_strings);
        return nullptr;
      }

      env->SetObjectArrayElement(jbyte_strings, i, jbyte_string_ary);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        // or ArrayStoreException
        env->DeleteLocalRef(jbyte_string_ary);
        env->DeleteLocalRef(jbyte_strings);
        return nullptr;
      }

      env->DeleteLocalRef(jbyte_string_ary);
    }

    return jbyte_strings;
  }

  /**
   * Converts a std::vector<std::string> to a Java String[].
   *
   * @param env A pointer to the java environment
   * @param strings A vector of Strings
   *
   * @return A Java array of Strings,
   *     or nullptr if an exception is thrown
   */
  static jobjectArray toJavaStrings(JNIEnv* env,
                                    const std::vector<std::string>* strings) {
    jclass jcls_str = env->FindClass("java/lang/String");
    if (jcls_str == nullptr) {
      // exception occurred
      return nullptr;
    }

    const jsize len = static_cast<jsize>(strings->size());

    jobjectArray jstrings = env->NewObjectArray(len, jcls_str, nullptr);
    if (jstrings == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    for (jsize i = 0; i < len; i++) {
      const std::string* str = &((*strings)[i]);
      jstring js = ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, str);
      if (js == nullptr) {
        env->DeleteLocalRef(jstrings);
        return nullptr;
      }

      env->SetObjectArrayElement(jstrings, i, js);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        // or ArrayStoreException
        env->DeleteLocalRef(js);
        env->DeleteLocalRef(jstrings);
        return nullptr;
      }
    }

    return jstrings;
  }

  /**
   * Creates a Java UTF String from a C++ std::string
   *
   * @param env A pointer to the java environment
   * @param string the C++ std::string
   * @param treat_empty_as_null true if empty strings should be treated as null
   *
   * @return the Java UTF string, or nullptr if the provided string
   *     is null (or empty and treat_empty_as_null is set), or if an
   *     exception occurs allocating the Java String.
   */
  static jstring toJavaString(JNIEnv* env, const std::string* string,
                              const bool treat_empty_as_null = false) {
    if (string == nullptr) {
      return nullptr;
    }

    if (treat_empty_as_null && string->empty()) {
      return nullptr;
    }

    return env->NewStringUTF(string->c_str());
  }

  /**
   * Copies bytes to a new jByteArray with the check of java array size
   * limitation.
   *
   * @param bytes pointer to memory to copy to a new jByteArray
   * @param size number of bytes to copy
   *
   * @return the Java byte[], or nullptr if an exception occurs
   *
   * @throws RocksDBException thrown
   *   if memory size to copy exceeds general java array size limitation to
   * avoid overflow.
   */
  static jbyteArray createJavaByteArrayWithSizeCheck(JNIEnv* env,
                                                     const char* bytes,
                                                     const size_t size) {
    // Limitation for java array size is vm specific
    // In general it cannot exceed Integer.MAX_VALUE (2^31 - 1)
    // Current HotSpot VM limitation for array size is Integer.MAX_VALUE - 5
    // (2^31 - 1 - 5) It means that the next call to env->NewByteArray can still
    // end with OutOfMemoryError("Requested array size exceeds VM limit") coming
    // from VM
    static const size_t MAX_JARRAY_SIZE = (static_cast<size_t>(1)) << 31;
    if (size > MAX_JARRAY_SIZE) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, "Requested array size exceeds VM limit");
      return nullptr;
    }

    const jsize jlen = static_cast<jsize>(size);
    jbyteArray jbytes = env->NewByteArray(jlen);
    if (jbytes == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    env->SetByteArrayRegion(
        jbytes, 0, jlen,
        const_cast<jbyte*>(reinterpret_cast<const jbyte*>(bytes)));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jbytes);
      return nullptr;
    }

    return jbytes;
  }

  /**
   * Copies bytes from a ROCKSDB_NAMESPACE::Slice to a jByteArray
   *
   * @param env A pointer to the java environment
   * @param bytes The bytes to copy
   *
   * @return the Java byte[] or nullptr if an exception occurs
   *
   * @throws RocksDBException thrown
   *   if memory size to copy exceeds general java specific array size
   * limitation.
   */
  static jbyteArray copyBytes(JNIEnv* env, const Slice& bytes) {
    return createJavaByteArrayWithSizeCheck(env, bytes.data(), bytes.size());
  }

  /*
   * Helper for operations on a key and value
   * for example WriteBatch->Put
   *
   * TODO(AR) could be used for RocksDB->Put etc.
   */
  static std::unique_ptr<ROCKSDB_NAMESPACE::Status> kv_op(
      std::function<ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Slice,
                                              ROCKSDB_NAMESPACE::Slice)>
          op,
      JNIEnv* env, jbyteArray jkey, jint jkey_len, jbyteArray jvalue,
      jint jvalue_len) {
    jbyte* key = env->GetByteArrayElements(jkey, nullptr);
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    jbyte* value = env->GetByteArrayElements(jvalue, nullptr);
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      if (key != nullptr) {
        env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
      }
      return nullptr;
    }

    ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
    ROCKSDB_NAMESPACE::Slice value_slice(reinterpret_cast<char*>(value),
                                         jvalue_len);

    auto status = op(key_slice, value_slice);

    if (value != nullptr) {
      env->ReleaseByteArrayElements(jvalue, value, JNI_ABORT);
    }
    if (key != nullptr) {
      env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
    }

    return std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
        new ROCKSDB_NAMESPACE::Status(status));
  }

  /*
   * Helper for operations on a key
   * for example WriteBatch->Delete
   *
   * TODO(AR) could be used for RocksDB->Delete etc.
   */
  static std::unique_ptr<ROCKSDB_NAMESPACE::Status> k_op(
      std::function<ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Slice)> op,
      JNIEnv* env, jbyteArray jkey, jint jkey_len) {
    jbyte* key = env->GetByteArrayElements(jkey, nullptr);
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

    auto status = op(key_slice);

    if (key != nullptr) {
      env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
    }

    return std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
        new ROCKSDB_NAMESPACE::Status(status));
  }

  /*
   * Helper for operations on a key which is a region of an array
   * Used to extract the common code from seek/seekForPrev.
   * Possible that it can be generalised from that.
   *
   * We use GetByteArrayRegion to copy the key region of the whole array into
   * a char[] We suspect this is not much slower than GetByteArrayElements,
   * which probably copies anyway.
   */
  static void k_op_region(std::function<void(ROCKSDB_NAMESPACE::Slice&)> op,
                          JNIEnv* env, jbyteArray jkey, jint jkey_off,
                          jint jkey_len) {
    const std::unique_ptr<char[]> key(new char[jkey_len]);
    if (key == nullptr) {
      jclass oom_class = env->FindClass("/lang/java/OutOfMemoryError");
      env->ThrowNew(oom_class,
                    "Memory allocation failed in RocksDB JNI function");
      return;
    }
    env->GetByteArrayRegion(jkey, jkey_off, jkey_len,
                            reinterpret_cast<jbyte*>(key.get()));
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      return;
    }

    ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key.get()),
                                       jkey_len);
    op(key_slice);
  }

  /*
   * Helper for operations on a value
   * for example WriteBatchWithIndex->GetFromBatch
   */
  static jbyteArray v_op(std::function<ROCKSDB_NAMESPACE::Status(
                             ROCKSDB_NAMESPACE::Slice, std::string*)>
                             op,
                         JNIEnv* env, jbyteArray jkey, jint jkey_len) {
    jbyte* key = env->GetByteArrayElements(jkey, nullptr);
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

    std::string value;
    ROCKSDB_NAMESPACE::Status s = op(key_slice, &value);

    if (key != nullptr) {
      env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
    }

    if (s.IsNotFound()) {
      return nullptr;
    }

    if (s.ok()) {
      jbyteArray jret_value =
          env->NewByteArray(static_cast<jsize>(value.size()));
      if (jret_value == nullptr) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      env->SetByteArrayRegion(
          jret_value, 0, static_cast<jsize>(value.size()),
          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value.c_str())));
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        if (jret_value != nullptr) {
          env->DeleteLocalRef(jret_value);
        }
        return nullptr;
      }

      return jret_value;
    }

    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  /**
   * Creates a vector<T*> of C++ pointers from
   *     a Java array of C++ pointer addresses.
   *
   * @param env (IN) A pointer to the java environment
   * @param pointers (IN) A Java array of C++ pointer addresses
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an ArrayIndexOutOfBoundsException or OutOfMemoryError
   *     exception occurs.
   *
   * @return A vector of C++ pointers.
   */
  template <typename T>
  static std::vector<T*> fromJPointers(JNIEnv* env, jlongArray jptrs,
                                       jboolean* has_exception) {
    const jsize jptrs_len = env->GetArrayLength(jptrs);
    std::vector<T*> ptrs;
    jlong* jptr = env->GetLongArrayElements(jptrs, nullptr);
    if (jptr == nullptr) {
      // exception thrown: OutOfMemoryError
      *has_exception = JNI_TRUE;
      return ptrs;
    }
    ptrs.reserve(jptrs_len);
    for (jsize i = 0; i < jptrs_len; i++) {
      ptrs.push_back(reinterpret_cast<T*>(jptr[i]));
    }
    env->ReleaseLongArrayElements(jptrs, jptr, JNI_ABORT);
    return ptrs;
  }

  /**
   * Creates a Java array of C++ pointer addresses
   *     from a vector of C++ pointers.
   *
   * @param env (IN) A pointer to the java environment
   * @param pointers (IN) A vector of C++ pointers
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an ArrayIndexOutOfBoundsException or OutOfMemoryError
   *     exception occurs
   *
   * @return Java array of C++ pointer addresses.
   */
  template <typename T>
  static jlongArray toJPointers(JNIEnv* env, const std::vector<T*>& pointers,
                                jboolean* has_exception) {
    const jsize len = static_cast<jsize>(pointers.size());
    std::unique_ptr<jlong[]> results(new jlong[len]);
    std::transform(
        pointers.begin(), pointers.end(), results.get(),
        [](T* pointer) -> jlong { return GET_CPLUSPLUS_POINTER(pointer); });

    jlongArray jpointers = env->NewLongArray(len);
    if (jpointers == nullptr) {
      // exception thrown: OutOfMemoryError
      *has_exception = JNI_TRUE;
      return nullptr;
    }

    env->SetLongArrayRegion(jpointers, 0, len, results.get());
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      *has_exception = JNI_TRUE;
      env->DeleteLocalRef(jpointers);
      return nullptr;
    }

    *has_exception = JNI_FALSE;

    return jpointers;
  }

  /*
   * Helper for operations on a key and value
   * for example WriteBatch->Put
   *
   * TODO(AR) could be extended to cover returning ROCKSDB_NAMESPACE::Status
   * from `op` and used for RocksDB->Put etc.
   */
  static void kv_op_direct(
      std::function<void(ROCKSDB_NAMESPACE::Slice&, ROCKSDB_NAMESPACE::Slice&)>
          op,
      JNIEnv* env, jobject jkey, jint jkey_off, jint jkey_len, jobject jval,
      jint jval_off, jint jval_len) {
    char* key = reinterpret_cast<char*>(env->GetDirectBufferAddress(jkey));
    if (key == nullptr ||
        env->GetDirectBufferCapacity(jkey) < (jkey_off + jkey_len)) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env,
                                                       "Invalid key argument");
      return;
    }

    char* value = reinterpret_cast<char*>(env->GetDirectBufferAddress(jval));
    if (value == nullptr ||
        env->GetDirectBufferCapacity(jval) < (jval_off + jval_len)) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, "Invalid value argument");
      return;
    }

    key += jkey_off;
    value += jval_off;

    ROCKSDB_NAMESPACE::Slice key_slice(key, jkey_len);
    ROCKSDB_NAMESPACE::Slice value_slice(value, jval_len);

    op(key_slice, value_slice);
  }

  /*
   * Helper for operations on a key and value
   * for example WriteBatch->Delete
   *
   * TODO(AR) could be extended to cover returning ROCKSDB_NAMESPACE::Status
   * from `op` and used for RocksDB->Delete etc.
   */
  static void k_op_direct(std::function<void(ROCKSDB_NAMESPACE::Slice&)> op,
                          JNIEnv* env, jobject jkey, jint jkey_off,
                          jint jkey_len) {
    char* key = reinterpret_cast<char*>(env->GetDirectBufferAddress(jkey));
    if (key == nullptr ||
        env->GetDirectBufferCapacity(jkey) < (jkey_off + jkey_len)) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env,
                                                       "Invalid key argument");
      return;
    }

    key += jkey_off;

    ROCKSDB_NAMESPACE::Slice key_slice(key, jkey_len);

    return op(key_slice);
  }

  template <class T>
  static jint copyToDirect(JNIEnv* env, T& source, jobject jtarget,
                           jint jtarget_off, jint jtarget_len) {
    char* target =
        reinterpret_cast<char*>(env->GetDirectBufferAddress(jtarget));
    if (target == nullptr ||
        env->GetDirectBufferCapacity(jtarget) < (jtarget_off + jtarget_len)) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, "Invalid target argument");
      return 0;
    }

    target += jtarget_off;

    const jint cvalue_len = static_cast<jint>(source.size());
    const jint length = std::min(jtarget_len, cvalue_len);

    memcpy(target, source.data(), length);

    return cvalue_len;
  }
};
}  // namespace ROCKSDB_NAMESPACE
