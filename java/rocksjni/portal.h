// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#ifndef JAVA_ROCKSJNI_PORTAL_H_
#define JAVA_ROCKSJNI_PORTAL_H_

#include <jni.h>
#include <functional>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/loggerjnicallback.h"
#include "rocksjni/writebatchhandlerjnicallback.h"

// Remove macro on windows
#ifdef DELETE
#undef DELETE
#endif

namespace rocksdb {

// Detect if jlong overflows size_t
inline Status check_if_jlong_fits_size_t(const jlong& jvalue) {
  Status s = Status::OK();
  if (static_cast<uint64_t>(jvalue) > std::numeric_limits<size_t>::max()) {
    s = Status::InvalidArgument(Slice("jlong overflows 32 bit value."));
  }
  return s;
}

class JavaClass {
 public:
  /**
   * Gets and initializes a Java Class
   *
   * @param env A pointer to the Java environment
   * @param jclazz_name The fully qualified JNI name of the Java Class
   *     e.g. "java/lang/String"
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env, const char* jclazz_name) {
    jclass jclazz = env->FindClass(jclazz_name);
    assert(jclazz != nullptr);
    return jclazz;
  }
};

// Native class template
template<class PTR, class DERIVED> class RocksDBNativeClass : public JavaClass {
};

// Native class template for sub-classes of RocksMutableObject
template<class PTR, class DERIVED> class NativeRocksMutableObject
    : public RocksDBNativeClass<PTR, DERIVED> {
 public:

  /**
   * Gets the Java Method ID for the
   * RocksMutableObject#setNativeHandle(long, boolean) method
   *
   * @param env A pointer to the Java environment
   * @return The Java Method ID or nullptr the RocksMutableObject class cannot
   *     be accessed, or if one of the NoSuchMethodError,
   *     ExceptionInInitializerError or OutOfMemoryError exceptions is thrown
   */
  static jmethodID getSetNativeHandleMethod(JNIEnv* env) {
    static jclass jclazz = DERIVED::getJClass(env);
    if(jclazz == nullptr) {
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "setNativeHandle", "(JZ)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Sets the C++ object pointer handle in the Java object
   *
   * @param env A pointer to the Java environment
   * @param jobj The Java object on which to set the pointer handle
   * @param ptr The C++ object pointer
   * @param java_owns_handle JNI_TRUE if ownership of the C++ object is
   *     managed by the Java object
   *
   * @return true if a Java exception is pending, false otherwise
   */
  static bool setHandle(JNIEnv* env, jobject jobj, PTR ptr,
      jboolean java_owns_handle) {
    assert(jobj != nullptr);
    static jmethodID mid = getSetNativeHandleMethod(env);
    if(mid == nullptr) {
      return true;  // signal exception
    }

    env->CallVoidMethod(jobj, mid, reinterpret_cast<jlong>(ptr), java_owns_handle);
    if(env->ExceptionCheck()) {
      return true;  // signal exception
    }

    return false;
  }
};

// Java Exception template
template<class DERIVED> class JavaException : public JavaClass {
 public:
  /**
   * Create and throw a java exception with the provided message
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg) {
    jclass jclazz = DERIVED::getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "JavaException::ThrowNew - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    const jint rs = env->ThrowNew(jclazz, msg.c_str());
    if(rs != JNI_OK) {
      // exception could not be thrown
      std::cerr << "JavaException::ThrowNew - Fatal: could not throw exception!" << std::endl;
      return env->ExceptionCheck();
    }

    return true;
  }
};

// The portal class for org.rocksdb.RocksDB
class RocksDBJni : public RocksDBNativeClass<rocksdb::DB*, RocksDBJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.RocksDB
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/RocksDB");
  }
};

// The portal class for org.rocksdb.Status
class StatusJni : public RocksDBNativeClass<rocksdb::Status*, StatusJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Status
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Status");
  }

  /**
   * Create a new Java org.rocksdb.Status object with the same properties as
   * the provided C++ rocksdb::Status object
   *
   * @param env A pointer to the Java environment
   * @param status The rocksdb::Status object
   *
   * @return A reference to a Java org.rocksdb.Status object, or nullptr
   *     if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const Status& status) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(BBLjava/lang/String;)V");
    if(mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    // convert the Status state for Java
    jstring jstate = nullptr;
    if (status.getState() != nullptr) {
      const char* const state = status.getState();
      jstate = env->NewStringUTF(state);
      if(env->ExceptionCheck()) {
        if(jstate != nullptr) {
          env->DeleteLocalRef(jstate);
        }
        return nullptr;
      }
    }

    jobject jstatus =
        env->NewObject(jclazz, mid, toJavaStatusCode(status.code()),
            toJavaStatusSubCode(status.subcode()), jstate);
    if(env->ExceptionCheck()) {
      // exception occurred
      if(jstate != nullptr) {
        env->DeleteLocalRef(jstate);
      }
      return nullptr;
    }

    if(jstate != nullptr) {
      env->DeleteLocalRef(jstate);
    }

    return jstatus;
  }

  // Returns the equivalent org.rocksdb.Status.Code for the provided
  // C++ rocksdb::Status::Code enum
  static jbyte toJavaStatusCode(const rocksdb::Status::Code& code) {
    switch (code) {
      case rocksdb::Status::Code::kOk:
        return 0x0;
      case rocksdb::Status::Code::kNotFound:
        return 0x1;
      case rocksdb::Status::Code::kCorruption:
        return 0x2;
      case rocksdb::Status::Code::kNotSupported:
        return 0x3;
      case rocksdb::Status::Code::kInvalidArgument:
        return 0x4;
      case rocksdb::Status::Code::kIOError:
        return 0x5;
      case rocksdb::Status::Code::kMergeInProgress:
        return 0x6;
      case rocksdb::Status::Code::kIncomplete:
        return 0x7;
      case rocksdb::Status::Code::kShutdownInProgress:
        return 0x8;
      case rocksdb::Status::Code::kTimedOut:
        return 0x9;
      case rocksdb::Status::Code::kAborted:
        return 0xA;
      case rocksdb::Status::Code::kBusy:
        return 0xB;
      case rocksdb::Status::Code::kExpired:
        return 0xC;
      case rocksdb::Status::Code::kTryAgain:
        return 0xD;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent org.rocksdb.Status.SubCode for the provided
  // C++ rocksdb::Status::SubCode enum
  static jbyte toJavaStatusSubCode(const rocksdb::Status::SubCode& subCode) {
    switch (subCode) {
      case rocksdb::Status::SubCode::kNone:
        return 0x0;
      case rocksdb::Status::SubCode::kMutexTimeout:
        return 0x1;
      case rocksdb::Status::SubCode::kLockTimeout:
        return 0x2;
      case rocksdb::Status::SubCode::kLockLimit:
        return 0x3;
      case rocksdb::Status::SubCode::kMaxSubCode:
        return 0x7E;
      default:
        return 0x7F;  // undefined
    }
  }
};

// The portal class for org.rocksdb.RocksDBException
class RocksDBExceptionJni :
    public JavaException<RocksDBExceptionJni> {
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
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const Status& s) {
    assert(!s.ok());
    if (s.ok()) {
      return false;
    }

    // get the RocksDBException class
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "RocksDBExceptionJni::ThrowNew/class - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    // get the constructor of org.rocksdb.RocksDBException
    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(Lorg/rocksdb/Status;)V");
    if(mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      std::cerr << "RocksDBExceptionJni::ThrowNew/cstr - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    // get the Java status object
    jobject jstatus = StatusJni::construct(env, s);
    if(jstatus == nullptr) {
      // exception occcurred
      std::cerr << "RocksDBExceptionJni::ThrowNew/StatusJni - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    // construct the RocksDBException
    jthrowable rocksdb_exception = reinterpret_cast<jthrowable>(env->NewObject(jclazz, mid, jstatus));
    if(env->ExceptionCheck()) {
      if(jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if(rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      std::cerr << "RocksDBExceptionJni::ThrowNew/NewObject - Error: unexpected exception!" << std::endl;
      return true;
    }

    // throw the RocksDBException
    const jint rs = env->Throw(rocksdb_exception);
    if(rs != JNI_OK) {
      // exception could not be thrown
      std::cerr << "RocksDBExceptionJni::ThrowNew - Fatal: could not throw exception!" << std::endl;
      if(jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if(rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      return env->ExceptionCheck();
    }

    if(jstatus != nullptr) {
      env->DeleteLocalRef(jstatus);
    }
    if(rocksdb_exception != nullptr) {
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
    if(jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "RocksDBExceptionJni::ThrowNew/class - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    // get the constructor of org.rocksdb.RocksDBException
    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(Ljava/lang/String;Lorg/rocksdb/Status;)V");
    if(mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      std::cerr << "RocksDBExceptionJni::ThrowNew/cstr - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    jstring jmsg = env->NewStringUTF(msg.c_str());
    if(jmsg == nullptr) {
      // exception thrown: OutOfMemoryError
      std::cerr << "RocksDBExceptionJni::ThrowNew/msg - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    // get the Java status object
    jobject jstatus = StatusJni::construct(env, s);
    if(jstatus == nullptr) {
      // exception occcurred
      std::cerr << "RocksDBExceptionJni::ThrowNew/StatusJni - Error: unexpected exception!" << std::endl;
      if(jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      return env->ExceptionCheck();
    }

    // construct the RocksDBException
    jthrowable rocksdb_exception = reinterpret_cast<jthrowable>(env->NewObject(jclazz, mid, jmsg, jstatus));
    if(env->ExceptionCheck()) {
      if(jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if(jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      if(rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      std::cerr << "RocksDBExceptionJni::ThrowNew/NewObject - Error: unexpected exception!" << std::endl;
      return true;
    }

    // throw the RocksDBException
    const jint rs = env->Throw(rocksdb_exception);
    if(rs != JNI_OK) {
      // exception could not be thrown
      std::cerr << "RocksDBExceptionJni::ThrowNew - Fatal: could not throw exception!" << std::endl;
      if(jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if(jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      if(rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      return env->ExceptionCheck();
    }

    if(jstatus != nullptr) {
      env->DeleteLocalRef(jstatus);
    }
    if(jmsg != nullptr) {
      env->DeleteLocalRef(jmsg);
    }
    if(rocksdb_exception != nullptr) {
      env->DeleteLocalRef(rocksdb_exception);
    }

    return true;
  }
};

// The portal class for java.lang.IllegalArgumentException
class IllegalArgumentExceptionJni :
    public JavaException<IllegalArgumentExceptionJni> {
 public:
  /**
   * Get the Java Class java.lang.IllegalArgumentException
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaException::getJClass(env, "java/lang/IllegalArgumentException");
  }

  /**
   * Create and throw a Java IllegalArgumentException with the provided status
   *
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const Status& s) {
    assert(!s.ok());
    if (s.ok()) {
      return false;
    }

    // get the IllegalArgumentException class
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "IllegalArgumentExceptionJni::ThrowNew/class - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    return JavaException::ThrowNew(env, s.ToString());
  }
};


// The portal class for org.rocksdb.Options
class OptionsJni : public RocksDBNativeClass<
    rocksdb::Options*, OptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Options
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Options");
  }
};

// The portal class for org.rocksdb.DBOptions
class DBOptionsJni : public RocksDBNativeClass<
    rocksdb::DBOptions*, DBOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.DBOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/DBOptions");
  }
};

class ColumnFamilyDescriptorJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.ColumnFamilyDescriptor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/ColumnFamilyDescriptor");
  }

  // Get the java method id of columnFamilyName
  static jmethodID getColumnFamilyNameMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getClass(env),
        "columnFamilyName", "()[B");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of columnFamilyOptions
  static jmethodID getColumnFamilyOptionsMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getClass(env),
        "columnFamilyOptions", "()Lorg/rocksdb/ColumnFamilyOptions;");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.ColumnFamilyOptions
class ColumnFamilyOptionsJni : public RocksDBNativeClass<
    rocksdb::ColumnFamilyOptions*, ColumnFamilyOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ColumnFamilyOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/ColumnFamilyOptions");
  }
};

// The portal class for org.rocksdb.WriteOptions
class WriteOptionsJni : public RocksDBNativeClass<
    rocksdb::WriteOptions*, WriteOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/WriteOptions");
  }
};

// The portal class for org.rocksdb.ReadOptions
class ReadOptionsJni : public RocksDBNativeClass<
    rocksdb::ReadOptions*, ReadOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ReadOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/ReadOptions");
  }
};

// The portal class for org.rocksdb.WriteBatch
class WriteBatchJni : public RocksDBNativeClass<
    rocksdb::WriteBatch*, WriteBatchJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatch
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/WriteBatch");
  }
};

// The portal class for org.rocksdb.WriteBatch.Handler
class WriteBatchHandlerJni : public RocksDBNativeClass<
    const rocksdb::WriteBatchHandlerJniCallback*,
    WriteBatchHandlerJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatch.Handler
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/WriteBatch$Handler");
  }

  // Get the java method `put` of org.rocksdb.WriteBatch.Handler.
  static jmethodID getPutMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "put", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `merge` of org.rocksdb.WriteBatch.Handler.
  static jmethodID getMergeMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "merge", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `delete` of org.rocksdb.WriteBatch.Handler.
  static jmethodID getDeleteMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "delete", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `logData` of org.rocksdb.WriteBatch.Handler.
  static jmethodID getLogDataMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "logData", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `shouldContinue` of org.rocksdb.WriteBatch.Handler.
  static jmethodID getContinueMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "shouldContinue", "()Z");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.WriteBatchWithIndex
class WriteBatchWithIndexJni : public RocksDBNativeClass<
    rocksdb::WriteBatchWithIndex*, WriteBatchWithIndexJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatchWithIndex
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/WriteBatchWithIndex");
  }
};

// The portal class for org.rocksdb.HistogramData
class HistogramDataJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.HistogramData
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/HistogramData");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(getJClass(env), "<init>", "(DDDDD)V");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.BackupableDBOptions
class BackupableDBOptionsJni : public RocksDBNativeClass<
    rocksdb::BackupableDBOptions*, BackupableDBOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.BackupableDBOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/BackupableDBOptions");
  }
};

// The portal class for org.rocksdb.BackupEngine
class BackupEngineJni : public RocksDBNativeClass<
    rocksdb::BackupEngine*, BackupEngineJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.BackupableEngine
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/BackupEngine");
  }
};

// The portal class for org.rocksdb.RocksIterator
class IteratorJni : public RocksDBNativeClass<
    rocksdb::Iterator*, IteratorJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.RocksIterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/RocksIterator");
  }
};

// The portal class for org.rocksdb.Filter
class FilterJni : public RocksDBNativeClass<
    std::shared_ptr<rocksdb::FilterPolicy>*, FilterJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Filter
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Filter");
  }
};

// The portal class for org.rocksdb.ColumnFamilyHandle
class ColumnFamilyHandleJni : public RocksDBNativeClass<
    rocksdb::ColumnFamilyHandle*, ColumnFamilyHandleJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ColumnFamilyHandle
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/ColumnFamilyHandle");
  }
};

// The portal class for org.rocksdb.FlushOptions
class FlushOptionsJni : public RocksDBNativeClass<
    rocksdb::FlushOptions*, FlushOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.FlushOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/FlushOptions");
  }
};

// The portal class for org.rocksdb.ComparatorOptions
class ComparatorOptionsJni : public RocksDBNativeClass<
    rocksdb::ComparatorJniCallbackOptions*, ComparatorOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ComparatorOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/ComparatorOptions");
  }
};

// The portal class for org.rocksdb.AbstractComparator
class AbstractComparatorJni : public RocksDBNativeClass<
    const rocksdb::BaseComparatorJniCallback*,
    AbstractComparatorJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractComparator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/AbstractComparator");
  }

  // Get the java method `name` of org.rocksdb.Comparator.
  static jmethodID getNameMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "name", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `compare` of org.rocksdb.Comparator.
  static jmethodID getCompareMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(getJClass(env),
      "compare",
      "(Lorg/rocksdb/AbstractSlice;Lorg/rocksdb/AbstractSlice;)I");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `findShortestSeparator` of org.rocksdb.Comparator.
  static jmethodID getFindShortestSeparatorMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(getJClass(env),
      "findShortestSeparator",
      "(Ljava/lang/String;Lorg/rocksdb/AbstractSlice;)Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method `findShortSuccessor` of org.rocksdb.Comparator.
  static jmethodID getFindShortSuccessorMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(getJClass(env),
      "findShortSuccessor",
      "(Ljava/lang/String;)Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.AbstractSlice
class AbstractSliceJni : public NativeRocksMutableObject<
    const rocksdb::Slice*, AbstractSliceJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractSlice
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/AbstractSlice");
  }
};

// The portal class for org.rocksdb.Slice
class SliceJni : public NativeRocksMutableObject<
    const rocksdb::Slice*, AbstractSliceJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Slice
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Slice");
  }

  static jobject construct0(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(getJClass(env), "<init>", "()V");
    assert(mid != nullptr);
    return env->NewObject(getJClass(env), mid);
  }
};

// The portal class for org.rocksdb.DirectSlice
class DirectSliceJni : public NativeRocksMutableObject<
    const rocksdb::Slice*, AbstractSliceJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.DirectSlice
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/DirectSlice");
  }

  static jobject construct0(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(getJClass(env), "<init>", "()V");
    assert(mid != nullptr);
    return env->NewObject(getJClass(env), mid);
  }
};

// The portal class for java.util.List
class ListJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.util.List
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getListClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/List");
  }

  /**
   * Get the Java Class java.util.ArrayList
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getArrayListClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/ArrayList");
  }

  /**
   * Get the Java Class java.util.Iterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getIteratorClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/Iterator");
  }

  // Get the java method id of java.util.List.iterator().
  static jmethodID getIteratorMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getListClass(env), "iterator", "()Ljava/util/Iterator;");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of java.util.Iterator.hasNext().
  static jmethodID getHasNextMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getIteratorClass(env), "hasNext", "()Z");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of java.util.Iterator.next().
  static jmethodID getNextMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getIteratorClass(env), "next", "()Ljava/lang/Object;");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of arrayList constructor.
  static jmethodID getArrayListConstructorMethodId(JNIEnv* env, jclass jclazz) {
    static jmethodID mid = env->GetMethodID(
        jclazz, "<init>", "(I)V");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of java.util.List.add().
  static jmethodID getListAddMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getListClass(env), "add", "(Ljava/lang/Object;)Z");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for java.lang.Byte
class ByteJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.lang.Byte
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/lang/Byte");
  }

  // Get the java method id of java.lang.Byte.byteValue.
  static jmethodID getByteValueMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "byteValue", "()B");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.BackupInfo
class BackupInfoJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.BackupInfo
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/BackupInfo");
  }

  static jobject construct0(JNIEnv* env, uint32_t backup_id, int64_t timestamp,
      uint64_t size, uint32_t number_files) {
    static jmethodID mid = env->GetMethodID(getJClass(env), "<init>",
        "(IJJI)V");
    assert(mid != nullptr);
    return env->NewObject(getJClass(env), mid,
        backup_id, timestamp, size, number_files);
  }
};

class BackupInfoListJni {
 public:
  static jobject getBackupInfo(JNIEnv* env,
      std::vector<BackupInfo> backup_infos) {
    jclass jclazz = rocksdb::ListJni::getArrayListClass(env);
    jmethodID mid = rocksdb::ListJni::getArrayListConstructorMethodId(
        env, jclazz);
    jobject jbackup_info_handle_list = env->NewObject(jclazz, mid,
        backup_infos.size());
    // insert in java list
    for (std::vector<rocksdb::BackupInfo>::size_type i = 0;
        i != backup_infos.size(); i++) {
      rocksdb::BackupInfo backup_info = backup_infos[i];
      jobject obj = rocksdb::BackupInfoJni::construct0(env,
          backup_info.backup_id,
          backup_info.timestamp,
          backup_info.size,
          backup_info.number_files);
      env->CallBooleanMethod(jbackup_info_handle_list,
          rocksdb::ListJni::getListAddMethodId(env), obj);
    }
    return jbackup_info_handle_list;
  }
};

// The portal class for org.rocksdb.WBWIRocksIterator
class WBWIRocksIteratorJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.WBWIRocksIterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WBWIRocksIterator");
  }

  static jfieldID getWriteEntryField(JNIEnv* env) {
    static jfieldID fid =
        env->GetFieldID(getJClass(env), "entry",
        "Lorg/rocksdb/WBWIRocksIterator$WriteEntry;");
    assert(fid != nullptr);
    return fid;
  }

  static jobject getWriteEntry(JNIEnv* env, jobject jwbwi_rocks_iterator) {
    jobject jwe =
        env->GetObjectField(jwbwi_rocks_iterator, getWriteEntryField(env));
    assert(jwe != nullptr);
    return jwe;
  }
};

// The portal class for org.rocksdb.WBWIRocksIterator.WriteType
class WriteTypeJni : public JavaClass {
 public:
    // Get the PUT enum field of WBWIRocksIterator.WriteType
    static jobject PUT(JNIEnv* env) {
      return getEnum(env, "PUT");
    }

    // Get the MERGE enum field of WBWIRocksIterator.WriteType
    static jobject MERGE(JNIEnv* env) {
      return getEnum(env, "MERGE");
    }

    // Get the DELETE enum field of WBWIRocksIterator.WriteType
    static jobject DELETE(JNIEnv* env) {
      return getEnum(env, "DELETE");
    }

    // Get the LOG enum field of WBWIRocksIterator.WriteType
    static jobject LOG(JNIEnv* env) {
      return getEnum(env, "LOG");
    }

 private:
  /**
   * Get the Java Class org.rocksdb.WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WBWIRocksIterator$WriteType");
  }

  // Get an enum field of org.rocksdb.WBWIRocksIterator.WriteType
  static jobject getEnum(JNIEnv* env, const char name[]) {
    jclass jclazz = getJClass(env);
    jfieldID jfid =
        env->GetStaticFieldID(jclazz, name,
            "Lorg/rocksdb/WBWIRocksIterator$WriteType;");
    assert(jfid != nullptr);
    return env->GetStaticObjectField(jclazz, jfid);
  }
};

// The portal class for org.rocksdb.WBWIRocksIterator.WriteEntry
class WriteEntryJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.WBWIRocksIterator.WriteEntry
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
    static jclass getJClass(JNIEnv* env) {
      return JavaClass::getJClass(env, "org/rocksdb/WBWIRocksIterator$WriteEntry");
    }
};

// The portal class for org.rocksdb.InfoLogLevel
class InfoLogLevelJni : public JavaClass {
 public:
    // Get the DEBUG_LEVEL enum field of org.rocksdb.InfoLogLevel
    static jobject DEBUG_LEVEL(JNIEnv* env) {
      return getEnum(env, "DEBUG_LEVEL");
    }

    // Get the INFO_LEVEL enum field of org.rocksdb.InfoLogLevel
    static jobject INFO_LEVEL(JNIEnv* env) {
      return getEnum(env, "INFO_LEVEL");
    }

    // Get the WARN_LEVEL enum field of org.rocksdb.InfoLogLevel
    static jobject WARN_LEVEL(JNIEnv* env) {
      return getEnum(env, "WARN_LEVEL");
    }

    // Get the ERROR_LEVEL enum field of org.rocksdb.InfoLogLevel
    static jobject ERROR_LEVEL(JNIEnv* env) {
      return getEnum(env, "ERROR_LEVEL");
    }

    // Get the FATAL_LEVEL enum field of org.rocksdb.InfoLogLevel
    static jobject FATAL_LEVEL(JNIEnv* env) {
      return getEnum(env, "FATAL_LEVEL");
    }

    // Get the HEADER_LEVEL enum field of org.rocksdb.InfoLogLevel
    static jobject HEADER_LEVEL(JNIEnv* env) {
      return getEnum(env, "HEADER_LEVEL");
    }

 private:
  /**
   * Get the Java Class org.rocksdb.InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/InfoLogLevel");
  }

  // Get an enum field of org.rocksdb.InfoLogLevel
  static jobject getEnum(JNIEnv* env, const char name[]) {
    jclass jclazz = getJClass(env);
    jfieldID jfid =
        env->GetStaticFieldID(jclazz, name,
        "Lorg/rocksdb/InfoLogLevel;");
    assert(jfid != nullptr);
    return env->GetStaticObjectField(jclazz, jfid);
  }
};

// The portal class for org.rocksdb.Logger
class LoggerJni : public RocksDBNativeClass<
    std::shared_ptr<rocksdb::LoggerJniCallback>*, LoggerJni> {
 public:
  /**
   * Get the Java Class org/rocksdb/Logger
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Logger");
  }

  // Get the java method `name` of org.rocksdb.Logger.
  static jmethodID getLogMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "log",
        "(Lorg/rocksdb/InfoLogLevel;Ljava/lang/String;)V");
    assert(mid != nullptr);
    return mid;
  }
};

// various utility functions for working with RocksDB and JNI
class JniUtil {
 public:
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

      JNIEnv *env;
      const jint env_rs = jvm->GetEnv(reinterpret_cast<void**>(&env),
          JNI_VERSION_1_2);

      if(env_rs == JNI_OK) {
        // current thread is already attached, return the JNIEnv
        *attached = JNI_FALSE;
        return env;
      } else if(env_rs == JNI_EDETACHED) {
        // current thread is not attached, attempt to attach
        const jint rs_attach = jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL);
        if(rs_attach == JNI_OK) {
          *attached = JNI_TRUE;
          return env;
        } else {
          // error, could not attach the thread
          std::cerr << "JniUtil::getJinEnv - Fatal: could not attach current thread to JVM!" << std::endl;
          return nullptr;
        }
      } else if(env_rs == JNI_EVERSION) {
        // error, JDK does not support JNI_VERSION_1_2+
        std::cerr << "JniUtil::getJinEnv - Fatal: JDK does not support JNI_VERSION_1_2" << std::endl;
        return nullptr;
      } else {
        std::cerr << "JniUtil::getJinEnv - Fatal: Unknown error: env_rs=" << env_rs << std::endl;
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
      if(attached == JNI_TRUE) {
        const jint rs_detach = jvm->DetachCurrentThread();
        assert(rs_detach == JNI_OK);
        if(rs_detach != JNI_OK) {
          std::cerr << "JniUtil::getJinEnv - Warn: Unable to detach current thread from JVM!" << std::endl;
        }
      }
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
    static std::string copyString(JNIEnv* env, jstring js,
        jboolean* has_exception) {
      const char *utf = env->GetStringUTFChars(js, NULL);
      if(utf == nullptr) {
        // exception thrown: OutOfMemoryError
        env->ExceptionCheck();
        *has_exception = JNI_TRUE;
        return std::string();
      } else if(env->ExceptionCheck()) {
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

    /*
     * Helper for operations on a key and value
     * for example WriteBatch->Put
     *
     * TODO(AR) could be extended to cover returning rocksdb::Status
     * from `op` and used for RocksDB->Put etc.
     */
    static void kv_op(
        std::function<void(rocksdb::Slice, rocksdb::Slice)> op,
        JNIEnv* env, jobject jobj,
        jbyteArray jkey, jint jkey_len,
        jbyteArray jentry_value, jint jentry_value_len) {
      jbyte* key = env->GetByteArrayElements(jkey, nullptr);
      jbyte* value = env->GetByteArrayElements(jentry_value, nullptr);
      rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
      rocksdb::Slice value_slice(reinterpret_cast<char*>(value),
          jentry_value_len);

      op(key_slice, value_slice);

      env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
      env->ReleaseByteArrayElements(jentry_value, value, JNI_ABORT);
    }

    /*
     * Helper for operations on a key
     * for example WriteBatch->Delete
     *
     * TODO(AR) could be extended to cover returning rocksdb::Status
     * from `op` and used for RocksDB->Delete etc.
     */
    static void k_op(
        std::function<void(rocksdb::Slice)> op,
        JNIEnv* env, jobject jobj,
        jbyteArray jkey, jint jkey_len) {
      jbyte* key = env->GetByteArrayElements(jkey, nullptr);
      rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

      op(key_slice);

      env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
    }

    /*
     * Helper for operations on a value
     * for example WriteBatchWithIndex->GetFromBatch
     */
    static jbyteArray v_op(
        std::function<rocksdb::Status(rocksdb::Slice, std::string*)> op,
        JNIEnv* env, jbyteArray jkey, jint jkey_len) {
      jboolean isCopy;
      jbyte* key = env->GetByteArrayElements(jkey, &isCopy);
      rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

      std::string value;
      rocksdb::Status s = op(key_slice, &value);

      env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);

      if (s.IsNotFound()) {
        return nullptr;
      }

      if (s.ok()) {
        jbyteArray jret_value =
            env->NewByteArray(static_cast<jsize>(value.size()));
        env->SetByteArrayRegion(jret_value, 0, static_cast<jsize>(value.size()),
                                reinterpret_cast<const jbyte*>(value.c_str()));
        return jret_value;
      }
      rocksdb::RocksDBExceptionJni::ThrowNew(env, s);

      return nullptr;
    }
};

}  // namespace rocksdb
#endif  // JAVA_ROCKSJNI_PORTAL_H_
