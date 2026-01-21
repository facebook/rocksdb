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
#include "rocksjni/portal/java_class.h"

namespace ROCKSDB_NAMESPACE {
// The portal class for java.nio.ByteBuffer
class ByteBufferJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.nio.ByteBuffer
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/nio/ByteBuffer");
  }

  /**
   * Get the Java Method: ByteBuffer#allocate
   *
   * @param env A pointer to the Java environment
   * @param jbytebuffer_clazz if you have a reference to a ByteBuffer class, or
   * nullptr
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getAllocateMethodId(JNIEnv* env,
                                       jclass jbytebuffer_clazz = nullptr) {
    const jclass jclazz =
        jbytebuffer_clazz == nullptr ? getJClass(env) : jbytebuffer_clazz;
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetStaticMethodID(jclazz, "allocate", "(I)Ljava/nio/ByteBuffer;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: ByteBuffer#array
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getArrayMethodId(JNIEnv* env,
                                    jclass jbytebuffer_clazz = nullptr) {
    const jclass jclazz =
        jbytebuffer_clazz == nullptr ? getJClass(env) : jbytebuffer_clazz;
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "array", "()[B");
    assert(mid != nullptr);
    return mid;
  }

  static jobject construct(JNIEnv* env, const bool direct,
                           const size_t capacity,
                           jclass jbytebuffer_clazz = nullptr) {
    return constructWith(env, direct, nullptr, capacity, jbytebuffer_clazz);
  }

  static jobject constructWith(JNIEnv* env, const bool direct, const char* buf,
                               const size_t capacity,
                               jclass jbytebuffer_clazz = nullptr) {
    if (direct) {
      bool allocated = false;
      if (buf == nullptr) {
        buf = new char[capacity];
        allocated = true;
      }
      jobject jbuf = env->NewDirectByteBuffer(const_cast<char*>(buf),
                                              static_cast<jlong>(capacity));
      if (jbuf == nullptr) {
        // exception occurred
        if (allocated) {
          delete[] static_cast<const char*>(buf);
        }
        return nullptr;
      }
      return jbuf;
    } else {
      const jclass jclazz =
          jbytebuffer_clazz == nullptr ? getJClass(env) : jbytebuffer_clazz;
      if (jclazz == nullptr) {
        // exception occurred accessing class
        return nullptr;
      }
      const jmethodID jmid_allocate =
          getAllocateMethodId(env, jbytebuffer_clazz);
      if (jmid_allocate == nullptr) {
        // exception occurred accessing class, or NoSuchMethodException or
        // OutOfMemoryError
        return nullptr;
      }
      const jobject jbuf = env->CallStaticObjectMethod(
          jclazz, jmid_allocate, static_cast<jint>(capacity));
      if (env->ExceptionCheck()) {
        // exception occurred
        return nullptr;
      }

      // set buffer data?
      if (buf != nullptr) {
        jbyteArray jarray = array(env, jbuf, jbytebuffer_clazz);
        if (jarray == nullptr) {
          // exception occurred
          env->DeleteLocalRef(jbuf);
          return nullptr;
        }

        jboolean is_copy = JNI_FALSE;
        jbyte* ja = reinterpret_cast<jbyte*>(
            env->GetPrimitiveArrayCritical(jarray, &is_copy));
        if (ja == nullptr) {
          // exception occurred
          env->DeleteLocalRef(jarray);
          env->DeleteLocalRef(jbuf);
          return nullptr;
        }

        memcpy(ja, const_cast<char*>(buf), capacity);

        env->ReleasePrimitiveArrayCritical(jarray, ja, 0);

        env->DeleteLocalRef(jarray);
      }

      return jbuf;
    }
  }

  static jbyteArray array(JNIEnv* env, const jobject& jbyte_buffer,
                          jclass jbytebuffer_clazz = nullptr) {
    const jmethodID mid = getArrayMethodId(env, jbytebuffer_clazz);
    if (mid == nullptr) {
      // exception occurred accessing class, or NoSuchMethodException or
      // OutOfMemoryError
      return nullptr;
    }
    const jobject jarray = env->CallObjectMethod(jbyte_buffer, mid);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }
    return static_cast<jbyteArray>(jarray);
  }
};

}  // namespace ROCKSDB_NAMESPACE
