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
// The portal class for org.rocksdb.RocksDBException
class RocksDBExceptionJni : public JavaException<RocksDBExceptionJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.RocksDBException
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaException::getJClass(env, "org/rocksdb/RocksDBException");
  }

  /**
   * Create and throw a Java RocksDBException with the provided message
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg) {
    return JavaException::ThrowNew(env, msg);
  }

  /**
   * Create and throw a Java RocksDBException with the provided status
   *
   * If s->ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, std::unique_ptr<Status>& s) {
    return ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, *(s.get()));
  }

  /**
   * Create and throw a Java RocksDBException with the provided status
   *
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const Status& s) {
    if (s.ok()) {
      return false;
    }

    // get the RocksDBException class
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "RocksDBExceptionJni::ThrowNew/class - Error: unexpected "
                   "exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    // get the constructor of org.rocksdb.RocksDBException
    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(Lorg/rocksdb/Status;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      std::cerr
          << "RocksDBExceptionJni::ThrowNew/cstr - Error: unexpected exception!"
          << std::endl;
      return env->ExceptionCheck();
    }

    // get the Java status object
    jobject jstatus = StatusJni::construct(env, s);
    if (jstatus == nullptr) {
      // exception occcurred
      std::cerr << "RocksDBExceptionJni::ThrowNew/StatusJni - Error: "
                   "unexpected exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    // construct the RocksDBException
    jthrowable rocksdb_exception =
        reinterpret_cast<jthrowable>(env->NewObject(jclazz, mid, jstatus));
    if (env->ExceptionCheck()) {
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      std::cerr << "RocksDBExceptionJni::ThrowNew/NewObject - Error: "
                   "unexpected exception!"
                << std::endl;
      return true;
    }

    // throw the RocksDBException
    const jint rs = env->Throw(rocksdb_exception);
    if (rs != JNI_OK) {
      // exception could not be thrown
      std::cerr
          << "RocksDBExceptionJni::ThrowNew - Fatal: could not throw exception!"
          << std::endl;
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      return env->ExceptionCheck();
    }

    if (jstatus != nullptr) {
      env->DeleteLocalRef(jstatus);
    }
    if (rocksdb_exception != nullptr) {
      env->DeleteLocalRef(rocksdb_exception);
    }

    return true;
  }

  /**
   * Create and throw a Java RocksDBException with the provided message
   * and status
   *
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg, const Status& s) {
    assert(!s.ok());
    if (s.ok()) {
      return false;
    }

    // get the RocksDBException class
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "RocksDBExceptionJni::ThrowNew/class - Error: unexpected "
                   "exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    // get the constructor of org.rocksdb.RocksDBException
    jmethodID mid = env->GetMethodID(
        jclazz, "<init>", "(Ljava/lang/String;Lorg/rocksdb/Status;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      std::cerr
          << "RocksDBExceptionJni::ThrowNew/cstr - Error: unexpected exception!"
          << std::endl;
      return env->ExceptionCheck();
    }

    jstring jmsg = env->NewStringUTF(msg.c_str());
    if (jmsg == nullptr) {
      // exception thrown: OutOfMemoryError
      std::cerr
          << "RocksDBExceptionJni::ThrowNew/msg - Error: unexpected exception!"
          << std::endl;
      return env->ExceptionCheck();
    }

    // get the Java status object
    jobject jstatus = StatusJni::construct(env, s);
    if (jstatus == nullptr) {
      // exception occcurred
      std::cerr << "RocksDBExceptionJni::ThrowNew/StatusJni - Error: "
                   "unexpected exception!"
                << std::endl;
      if (jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      return env->ExceptionCheck();
    }

    // construct the RocksDBException
    jthrowable rocksdb_exception = reinterpret_cast<jthrowable>(
        env->NewObject(jclazz, mid, jmsg, jstatus));
    if (env->ExceptionCheck()) {
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      if (rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      std::cerr << "RocksDBExceptionJni::ThrowNew/NewObject - Error: "
                   "unexpected exception!"
                << std::endl;
      return true;
    }

    // throw the RocksDBException
    const jint rs = env->Throw(rocksdb_exception);
    if (rs != JNI_OK) {
      // exception could not be thrown
      std::cerr
          << "RocksDBExceptionJni::ThrowNew - Fatal: could not throw exception!"
          << std::endl;
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      if (rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      return env->ExceptionCheck();
    }

    if (jstatus != nullptr) {
      env->DeleteLocalRef(jstatus);
    }
    if (jmsg != nullptr) {
      env->DeleteLocalRef(jmsg);
    }
    if (rocksdb_exception != nullptr) {
      env->DeleteLocalRef(rocksdb_exception);
    }

    return true;
  }

  /**
   * Get the Java Method: RocksDBException#getStatus
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getStatusMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getStatus", "()Lorg/rocksdb/Status;");
    assert(mid != nullptr);
    return mid;
  }

  static std::unique_ptr<ROCKSDB_NAMESPACE::Status> toCppStatus(
      JNIEnv* env, jthrowable jrocksdb_exception) {
    if (!env->IsInstanceOf(jrocksdb_exception, getJClass(env))) {
      // not an instance of RocksDBException
      return nullptr;
    }

    // get the java status object
    jmethodID mid = getStatusMethod(env);
    if (mid == nullptr) {
      // exception occurred accessing class or method
      return nullptr;
    }

    jobject jstatus = env->CallObjectMethod(jrocksdb_exception, mid);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    if (jstatus == nullptr) {
      return nullptr;  // no status available
    }

    return ROCKSDB_NAMESPACE::StatusJni::toCppStatus(env, jstatus);
  }
};
}  // namespace ROCKSDB_NAMESPACE
