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
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/ColumnFamilyDescriptor");
  }

  /**
   * Get the Java Method: ColumnFamilyDescriptor#columnFamilyName
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getColumnFamilyNameMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "columnFamilyName", "()[B");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: ColumnFamilyDescriptor#columnFamilyOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getColumnFamilyOptionsMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "columnFamilyOptions",
            "()Lorg/rocksdb/ColumnFamilyOptions;");
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

  /**
   * Get the Java Method: WriteBatch.Handler#put
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getPutMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "put", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#merge
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMergeMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "merge", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#delete
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getDeleteMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "delete", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#deleteRange
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getDeleteRangeMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "deleteRange", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#logData
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getLogDataMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "logData", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#shouldContinue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getContinueMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "shouldContinue", "()Z");
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

  /**
   * Get the Java Method: HistogramData constructor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getConstructorMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "(DDDDD)V");
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

  /**
   * Get the Java Method: Comparator#name
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getNameMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "name", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Comparator#compare
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getCompareMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "compare",
            "(Lorg/rocksdb/AbstractSlice;Lorg/rocksdb/AbstractSlice;)I");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Comparator#findShortestSeparator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getFindShortestSeparatorMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "findShortestSeparator",
            "(Ljava/lang/String;Lorg/rocksdb/AbstractSlice;)Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Comparator#findShortSuccessor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getFindShortSuccessorMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "findShortSuccessor",
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

  /**
   * Constructs a Slice object
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java Slice object, or a nullptr if an
   *     exception occurs
   */
  static jobject construct0(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "()V");
    if(mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }
    
    jobject jslice = env->NewObject(jclazz, mid);
    if(env->ExceptionCheck()) {
      return nullptr;
    }

    return jslice;
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

  /**
   * Constructs a DirectSlice object
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java DirectSlice object, or a nullptr if an
   *     exception occurs
   */
  static jobject construct0(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "()V");
    if(mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jobject jdirect_slice = env->NewObject(jclazz, mid);
    if(env->ExceptionCheck()) {
      return nullptr;
    }

    return jdirect_slice;
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

  /**
   * Get the Java Method: List#iterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getIteratorMethod(JNIEnv* env) {
    jclass jlist_clazz = getListClass(env);
    if(jlist_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jlist_clazz, "iterator", "()Ljava/util/Iterator;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Iterator#hasNext
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getHasNextMethod(JNIEnv* env) {
    jclass jiterator_clazz = getIteratorClass(env);
    if(jiterator_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jiterator_clazz, "hasNext", "()Z");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Iterator#next
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getNextMethod(JNIEnv* env) {
    jclass jiterator_clazz = getIteratorClass(env);
    if(jiterator_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jiterator_clazz, "next", "()Ljava/lang/Object;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: ArrayList constructor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getArrayListConstructorMethodId(JNIEnv* env) {
    jclass jarray_list_clazz = getArrayListClass(env);
    if(jarray_list_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }
    static jmethodID mid =
        env->GetMethodID(jarray_list_clazz, "<init>", "(I)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: List#add
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getListAddMethodId(JNIEnv* env) {
    jclass jlist_clazz = getListClass(env);
    if(jlist_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jlist_clazz, "add", "(Ljava/lang/Object;)Z");
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

  /**
   * Get the Java Class byte[]
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getArrayJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "[B");
  }

  /**
   * Creates a new 2-dimensional Java Byte Array byte[][]
   *
   * @param env A pointer to the Java environment
   * @param len The size of the first dimension
   *
   * @return A reference to the Java byte[][] or nullptr if an exception occurs
   */
  static jobjectArray new2dByteArray(JNIEnv* env, const jsize len) {
    jclass clazz = getArrayJClass(env);
    if(clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    return env->NewObjectArray(len, clazz, nullptr);
  }

  /**
   * Get the Java Method: Byte#byteValue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getByteValueMethod(JNIEnv* env) {
    jclass clazz = getJClass(env);
    if(clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(clazz, "byteValue", "()B");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for java.lang.StringBuilder
class StringBuilderJni : public JavaClass {
  public:
  /**
   * Get the Java Class java.lang.StringBuilder
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/lang/StringBuilder");
  }

  /**
   * Get the Java Method: StringBuilder#append
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getListAddMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "append",
            "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Appends a C-style string to a StringBuilder
   *
   * @param env A pointer to the Java environment
   * @param jstring_builder Reference to a java.lang.StringBuilder
   * @param c_str A C-style string to append to the StringBuilder
   *
   * @return A reference to the updated StringBuilder, or a nullptr if
   *     an exception occurs
   */
  static jobject append(JNIEnv* env, jobject jstring_builder,
      const char* c_str) {
    jmethodID mid = getListAddMethodId(env);
    if(mid == nullptr) {
      // exception occurred accessing class or method
      return nullptr;
    }

    jstring new_value_str = env->NewStringUTF(c_str);
    if(new_value_str == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    jobject jresult_string_builder =
        env->CallObjectMethod(jstring_builder, mid, new_value_str);
    if(env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(new_value_str);
      return nullptr;
    }

    return jresult_string_builder;
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

  /**
   * Constructs a BackupInfo object
   *
   * @param env A pointer to the Java environment
   * @param backup_id id of the backup
   * @param timestamp timestamp of the backup
   * @param size size of the backup
   * @param number_files number of files related to the backup
   *
   * @return A reference to a Java BackupInfo object, or a nullptr if an
   *     exception occurs
   */
  static jobject construct0(JNIEnv* env, uint32_t backup_id, int64_t timestamp,
      uint64_t size, uint32_t number_files) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "(IJJI)V");
    if(mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jobject jbackup_info =
        env->NewObject(jclazz, mid, backup_id, timestamp, size, number_files);
    if(env->ExceptionCheck()) {
      return nullptr;
    }

    return jbackup_info;
  }
};

class BackupInfoListJni {
 public:
  /**
   * Converts a C++ std::vector<BackupInfo> object to
   * a Java ArrayList<org.rocksdb.BackupInfo> object
   *
   * @param env A pointer to the Java environment
   * @param backup_infos A vector of BackupInfo
   *
   * @return Either a reference to a Java ArrayList object, or a nullptr
   *     if an exception occurs
   */
  static jobject getBackupInfo(JNIEnv* env,
      std::vector<BackupInfo> backup_infos) {
    jclass jarray_list_clazz = rocksdb::ListJni::getArrayListClass(env);
    if(jarray_list_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID cstr_mid = rocksdb::ListJni::getArrayListConstructorMethodId(env);
    if(cstr_mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jmethodID add_mid = rocksdb::ListJni::getListAddMethodId(env);
    if(add_mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    // create java list
    jobject jbackup_info_handle_list =
        env->NewObject(jarray_list_clazz, cstr_mid, backup_infos.size());
    if(env->ExceptionCheck()) {
      // exception occurred constructing object
      return nullptr;
    }

    // insert in java list
    auto end = backup_infos.end();
    for (auto it = backup_infos.begin(); it != end; ++it) {
      auto backup_info = *it;

      jobject obj = rocksdb::BackupInfoJni::construct0(env,
          backup_info.backup_id,
          backup_info.timestamp,
          backup_info.size,
          backup_info.number_files);
      if(env->ExceptionCheck()) {
        // exception occurred constructing object
        if(obj != nullptr) {
          env->DeleteLocalRef(obj);
        }
        if(jbackup_info_handle_list != nullptr) {
          env->DeleteLocalRef(jbackup_info_handle_list);
        }
        return nullptr;
      }

      jboolean rs =
          env->CallBooleanMethod(jbackup_info_handle_list, add_mid, obj);
      if(env->ExceptionCheck() || rs == JNI_FALSE) {
        // exception occurred calling method, or could not add
        if(obj != nullptr) {
          env->DeleteLocalRef(obj);
        }
        if(jbackup_info_handle_list != nullptr) {
          env->DeleteLocalRef(jbackup_info_handle_list);
        }
        return nullptr;
      }
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

  /**
   * Get the Java Field: WBWIRocksIterator#entry
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Field ID or nullptr if the class or field id could not
   *     be retieved
   */
  static jfieldID getWriteEntryField(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jfieldID fid =
        env->GetFieldID(jclazz, "entry",
            "Lorg/rocksdb/WBWIRocksIterator$WriteEntry;");
    assert(fid != nullptr);
    return fid;
  }

  /**
   * Gets the value of the WBWIRocksIterator#entry
   *
   * @param env A pointer to the Java environment 
   * @param jwbwi_rocks_iterator A reference to a WBWIIterator
   *
   * @return A reference to a Java WBWIRocksIterator.WriteEntry object, or
   *     a nullptr if an exception occurs
   */
  static jobject getWriteEntry(JNIEnv* env, jobject jwbwi_rocks_iterator) {
    assert(jwbwi_rocks_iterator != nullptr);

    jfieldID jwrite_entry_field = getWriteEntryField(env);
    if(jwrite_entry_field == nullptr) {
      // exception occurred accessing the field
      return nullptr;
    }

    jobject jwe = env->GetObjectField(jwbwi_rocks_iterator, jwrite_entry_field);
    assert(jwe != nullptr);
    return jwe;
  }
};

// The portal class for org.rocksdb.WBWIRocksIterator.WriteType
class WriteTypeJni : public JavaClass {
 public:
    /**
     * Get the PUT enum field value of WBWIRocksIterator.WriteType
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject PUT(JNIEnv* env) {
      return getEnum(env, "PUT");
    }

    /**
     * Get the MERGE enum field value of WBWIRocksIterator.WriteType
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject MERGE(JNIEnv* env) {
      return getEnum(env, "MERGE");
    }

    /**
     * Get the DELETE enum field value of WBWIRocksIterator.WriteType
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject DELETE(JNIEnv* env) {
      return getEnum(env, "DELETE");
    }

    /**
     * Get the LOG enum field value of WBWIRocksIterator.WriteType
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
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

  /**
   * Get an enum field of org.rocksdb.WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   * @param name The name of the enum field
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject getEnum(JNIEnv* env, const char name[]) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jfieldID jfid =
        env->GetStaticFieldID(jclazz, name,
            "Lorg/rocksdb/WBWIRocksIterator$WriteType;");
    if(env->ExceptionCheck()) {
      // exception occurred while getting field
      return nullptr;
    } else if(jfid == nullptr) {
      return nullptr;
    }

    jobject jwrite_type = env->GetStaticObjectField(jclazz, jfid);
    assert(jwrite_type != nullptr);
    return jwrite_type;
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
    /**
     * Get the DEBUG_LEVEL enum field value of InfoLogLevel
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject DEBUG_LEVEL(JNIEnv* env) {
      return getEnum(env, "DEBUG_LEVEL");
    }

    /**
     * Get the INFO_LEVEL enum field value of InfoLogLevel
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject INFO_LEVEL(JNIEnv* env) {
      return getEnum(env, "INFO_LEVEL");
    }

    /**
     * Get the WARN_LEVEL enum field value of InfoLogLevel
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject WARN_LEVEL(JNIEnv* env) {
      return getEnum(env, "WARN_LEVEL");
    }

    /**
     * Get the ERROR_LEVEL enum field value of InfoLogLevel
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject ERROR_LEVEL(JNIEnv* env) {
      return getEnum(env, "ERROR_LEVEL");
    }

    /**
     * Get the FATAL_LEVEL enum field value of InfoLogLevel
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject FATAL_LEVEL(JNIEnv* env) {
      return getEnum(env, "FATAL_LEVEL");
    }

    /**
     * Get the HEADER_LEVEL enum field value of InfoLogLevel
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
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

  /**
   * Get an enum field of org.rocksdb.InfoLogLevel
   *
   * @param env A pointer to the Java environment
   * @param name The name of the enum field
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject getEnum(JNIEnv* env, const char name[]) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jfieldID jfid =
        env->GetStaticFieldID(jclazz, name, "Lorg/rocksdb/InfoLogLevel;");
    if(env->ExceptionCheck()) {
      // exception occurred while getting field
      return nullptr;
    } else if(jfid == nullptr) {
      return nullptr;
    }

    jobject jinfo_log_level = env->GetStaticObjectField(jclazz, jfid);
    assert(jinfo_log_level != nullptr);
    return jinfo_log_level;
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

  /**
   * Get the Java Method: Logger#log
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getLogMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "log",
            "(Lorg/rocksdb/InfoLogLevel;Ljava/lang/String;)V");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.TransactionLogIterator.BatchResult
class BatchResultJni : public JavaClass {
  public:
  /**
   * Get the Java Class org.rocksdb.TransactionLogIterator.BatchResult
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env,
        "org/rocksdb/TransactionLogIterator$BatchResult");
  }

  /**
   * Create a new Java org.rocksdb.TransactionLogIterator.BatchResult object
   * with the same properties as the provided C++ rocksdb::BatchResult object
   *
   * @param env A pointer to the Java environment
   * @param batch_result The rocksdb::BatchResult object
   *
   * @return A reference to a Java
   *     org.rocksdb.TransactionLogIterator.BatchResult object,
   *     or nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env,
      rocksdb::BatchResult& batch_result) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
      jclazz, "<init>", "(JJ)V");
    if(mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jbatch_result = env->NewObject(jclazz, mid,
      batch_result.sequence, batch_result.writeBatchPtr.get());
    if(jbatch_result == nullptr) {
      // exception thrown: InstantiationException or OutOfMemoryError
      return nullptr;
    }

    batch_result.writeBatchPtr.release();
    return jbatch_result;
  }
};

// The portal class for org.rocksdb.CompactionStopStyle
class CompactionStopStyleJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionStopStyle for the provided
  // C++ rocksdb::CompactionStopStyle enum
  static jbyte toJavaCompactionStopStyle(
      const rocksdb::CompactionStopStyle& compaction_stop_style) {
    switch(compaction_stop_style) {
      case rocksdb::CompactionStopStyle::kCompactionStopStyleSimilarSize:
        return 0x0;
      case rocksdb::CompactionStopStyle::kCompactionStopStyleTotalSize:
        return 0x1;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ rocksdb::CompactionStopStyle enum for the
  // provided Java org.rocksdb.CompactionStopStyle
  static rocksdb::CompactionStopStyle toCppCompactionStopStyle(
      jbyte jcompaction_stop_style) {
    switch(jcompaction_stop_style) {
      case 0x0:
        return rocksdb::CompactionStopStyle::kCompactionStopStyleSimilarSize;
      case 0x1:
        return rocksdb::CompactionStopStyle::kCompactionStopStyleTotalSize;
      default:
        // undefined/default
        return rocksdb::CompactionStopStyle::kCompactionStopStyleSimilarSize;
    }
  }
};

// The portal class for org.rocksdb.CompressionType
class CompressionTypeJni {
 public:
  // Returns the equivalent org.rocksdb.CompressionType for the provided
  // C++ rocksdb::CompressionType enum
  static jbyte toJavaCompressionType(
      const rocksdb::CompressionType& compression_type) {
    switch(compression_type) {
      case rocksdb::CompressionType::kNoCompression:
        return 0x0;
      case rocksdb::CompressionType::kSnappyCompression:
        return 0x1;
      case rocksdb::CompressionType::kZlibCompression:
        return 0x2;
      case rocksdb::CompressionType::kBZip2Compression:
        return 0x3;
      case rocksdb::CompressionType::kLZ4Compression:
        return 0x4;
      case rocksdb::CompressionType::kLZ4HCCompression:
        return 0x5;
      case rocksdb::CompressionType::kXpressCompression:
        return 0x6;
      case rocksdb::CompressionType::kZSTD:
        return 0x7;
      case rocksdb::CompressionType::kDisableCompressionOption:
      default:
        return 0x7F;
    }
  }

  // Returns the equivalent C++ rocksdb::CompressionType enum for the
  // provided Java org.rocksdb.CompressionType
  static rocksdb::CompressionType toCppCompressionType(
      jbyte jcompression_type) {
    switch(jcompression_type) {
      case 0x0:
        return rocksdb::CompressionType::kNoCompression;
      case 0x1:
        return rocksdb::CompressionType::kSnappyCompression;
      case 0x2:
        return rocksdb::CompressionType::kZlibCompression;
      case 0x3:
        return rocksdb::CompressionType::kBZip2Compression;
      case 0x4:
        return rocksdb::CompressionType::kLZ4Compression;
      case 0x5:
        return rocksdb::CompressionType::kLZ4HCCompression;
      case 0x6:
        return rocksdb::CompressionType::kXpressCompression;
      case 0x7:
        return rocksdb::CompressionType::kZSTD;
      case 0x7F:
      default:
        return rocksdb::CompressionType::kDisableCompressionOption;
    }
  }
};

// The portal class for org.rocksdb.CompactionPriority
class CompactionPriorityJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionPriority for the provided
  // C++ rocksdb::CompactionPri enum
  static jbyte toJavaCompactionPriority(
      const rocksdb::CompactionPri& compaction_priority) {
    switch(compaction_priority) {
      case rocksdb::CompactionPri::kByCompensatedSize:
        return 0x0;
      case rocksdb::CompactionPri::kOldestLargestSeqFirst:
        return 0x1;
      case rocksdb::CompactionPri::kOldestSmallestSeqFirst:
        return 0x2;
      case rocksdb::CompactionPri::kMinOverlappingRatio:
        return 0x3;
      default:
        return 0x0;  // undefined
    }
  }

  // Returns the equivalent C++ rocksdb::CompactionPri enum for the
  // provided Java org.rocksdb.CompactionPriority
  static rocksdb::CompactionPri toCppCompactionPriority(
      jbyte jcompaction_priority) {
    switch(jcompaction_priority) {
      case 0x0:
        return rocksdb::CompactionPri::kByCompensatedSize;
      case 0x1:
        return rocksdb::CompactionPri::kOldestLargestSeqFirst;
      case 0x2:
        return rocksdb::CompactionPri::kOldestSmallestSeqFirst;
      case 0x3:
        return rocksdb::CompactionPri::kMinOverlappingRatio;
      default:
        // undefined/default
        return rocksdb::CompactionPri::kByCompensatedSize;
    }
  }
};

// The portal class for org.rocksdb.AccessHint
class AccessHintJni {
 public:
  // Returns the equivalent org.rocksdb.AccessHint for the provided
  // C++ rocksdb::DBOptions::AccessHint enum
  static jbyte toJavaAccessHint(
      const rocksdb::DBOptions::AccessHint& access_hint) {
    switch(access_hint) {
      case rocksdb::DBOptions::AccessHint::NONE:
        return 0x0;
      case rocksdb::DBOptions::AccessHint::NORMAL:
        return 0x1;
      case rocksdb::DBOptions::AccessHint::SEQUENTIAL:
        return 0x2;
      case rocksdb::DBOptions::AccessHint::WILLNEED:
        return 0x3;
      default:
        // undefined/default
        return 0x1;
    }
  }

  // Returns the equivalent C++ rocksdb::DBOptions::AccessHint enum for the
  // provided Java org.rocksdb.AccessHint
  static rocksdb::DBOptions::AccessHint toCppAccessHint(jbyte jaccess_hint) {
    switch(jaccess_hint) {
      case 0x0:
        return rocksdb::DBOptions::AccessHint::NONE;
      case 0x1:
        return rocksdb::DBOptions::AccessHint::NORMAL;
      case 0x2:
        return rocksdb::DBOptions::AccessHint::SEQUENTIAL;
      case 0x3:
        return rocksdb::DBOptions::AccessHint::WILLNEED;
      default:
        // undefined/default
        return rocksdb::DBOptions::AccessHint::NORMAL;
    }
  }
};

// The portal class for org.rocksdb.WALRecoveryMode
class WALRecoveryModeJni {
 public:
  // Returns the equivalent org.rocksdb.WALRecoveryMode for the provided
  // C++ rocksdb::WALRecoveryMode enum
  static jbyte toJavaWALRecoveryMode(
      const rocksdb::WALRecoveryMode& wal_recovery_mode) {
    switch(wal_recovery_mode) {
      case rocksdb::WALRecoveryMode::kTolerateCorruptedTailRecords:
        return 0x0;
      case rocksdb::WALRecoveryMode::kAbsoluteConsistency:
        return 0x1;
      case rocksdb::WALRecoveryMode::kPointInTimeRecovery:
        return 0x2;
      case rocksdb::WALRecoveryMode::kSkipAnyCorruptedRecords:
        return 0x3;
      default:
        // undefined/default
        return 0x2;
    }
  }

  // Returns the equivalent C++ rocksdb::WALRecoveryMode enum for the
  // provided Java org.rocksdb.WALRecoveryMode
  static rocksdb::WALRecoveryMode toCppWALRecoveryMode(jbyte jwal_recovery_mode) {
    switch(jwal_recovery_mode) {
      case 0x0:
        return rocksdb::WALRecoveryMode::kTolerateCorruptedTailRecords;
      case 0x1:
        return rocksdb::WALRecoveryMode::kAbsoluteConsistency;
      case 0x2:
        return rocksdb::WALRecoveryMode::kPointInTimeRecovery;
      case 0x3:
        return rocksdb::WALRecoveryMode::kSkipAnyCorruptedRecords;
      default:
        // undefined/default
        return rocksdb::WALRecoveryMode::kPointInTimeRecovery;
    }
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
    static std::vector<std::string> copyStrings(JNIEnv* env,
        jobjectArray jss, jboolean* has_exception) {
          return rocksdb::JniUtil::copyStrings(env, jss,
              env->GetArrayLength(jss), has_exception);
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
    static std::vector<std::string> copyStrings(JNIEnv* env,
        jobjectArray jss, const jsize jss_len, jboolean* has_exception) {
      std::vector<std::string> strs;
      for (jsize i = 0; i < jss_len; i++) {
        jobject js = env->GetObjectArrayElement(jss, i);
        if(env->ExceptionCheck()) {
          // exception thrown: ArrayIndexOutOfBoundsException
          *has_exception = JNI_TRUE;
          return strs;
        }

        jstring jstr = static_cast<jstring>(js);
        const char* str = env->GetStringUTFChars(jstr, nullptr);
        if(str == nullptr) {
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
      const char *utf = env->GetStringUTFChars(js, nullptr);
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

    /**
     * Copies bytes from a std::string to a jByteArray
     *
     * @param env A pointer to the java environment
     * @param bytes The bytes to copy
     *
     * @return the Java byte[] or nullptr if an exception occurs
     */
    static jbyteArray copyBytes(JNIEnv* env, std::string bytes) {
      const jsize jlen = static_cast<jsize>(bytes.size());

      jbyteArray jbytes = env->NewByteArray(jlen);
      if(jbytes == nullptr) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      env->SetByteArrayRegion(jbytes, 0, jlen,
        const_cast<jbyte*>(reinterpret_cast<const jbyte*>(bytes.c_str())));
      if(env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jbytes);
        return nullptr;
      }

      return jbytes;
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
    template <typename T> static void byteStrings(JNIEnv* env,
        jobjectArray jbyte_strings,
        std::function<T(const char*, const size_t)> string_fn,
        std::function<void(size_t, T)> collector_fn,
        jboolean *has_exception) {
      const jsize jlen = env->GetArrayLength(jbyte_strings);

      for(jsize i = 0; i < jlen; i++) {
        jobject jbyte_string_obj = env->GetObjectArrayElement(jbyte_strings, i);
        if(env->ExceptionCheck()) {
          // exception thrown: ArrayIndexOutOfBoundsException
          *has_exception = JNI_TRUE;  // signal error
          return;
        }

        jbyteArray jbyte_string_ary =
            reinterpret_cast<jbyteArray>(jbyte_string_obj);
        T result = byteString(env, jbyte_string_ary, string_fn, has_exception);

        env->DeleteLocalRef(jbyte_string_obj);

        if(*has_exception == JNI_TRUE) {
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
    template <typename T> static T byteString(JNIEnv* env,
        jbyteArray jbyte_string_ary,
        std::function<T(const char*, const size_t)> string_fn,
        jboolean* has_exception) {
      const jsize jbyte_string_len = env->GetArrayLength(jbyte_string_ary);
      jbyte* jbyte_string =
          env->GetByteArrayElements(jbyte_string_ary, nullptr);
      if(jbyte_string == nullptr) {
        // exception thrown: OutOfMemoryError
        *has_exception = JNI_TRUE;
        return nullptr;  // signal error
      }

      T result =
          string_fn(reinterpret_cast<char *>(jbyte_string), jbyte_string_len);

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
     * @return A Java array of Strings expressed as bytes
     */
    static jobjectArray stringsBytes(JNIEnv* env, std::vector<std::string> strings) {
      jclass jcls_ba = ByteJni::getArrayJClass(env);
      if(jcls_ba == nullptr) {
        // exception occurred
        return nullptr;
      }

      const jsize len = static_cast<jsize>(strings.size());

      jobjectArray jbyte_strings = env->NewObjectArray(len, jcls_ba, nullptr);
      if(jbyte_strings == nullptr) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      for (jsize i = 0; i < len; i++) {
        std::string *str = &strings[i];
        const jsize str_len = static_cast<jsize>(str->size());

        jbyteArray jbyte_string_ary = env->NewByteArray(str_len);
        if(jbyte_string_ary == nullptr) {
          // exception thrown: OutOfMemoryError
          env->DeleteLocalRef(jbyte_strings);
          return nullptr;
        }

        env->SetByteArrayRegion(
          jbyte_string_ary, 0, str_len,
          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(str->c_str())));
        if(env->ExceptionCheck()) {
          // exception thrown: ArrayIndexOutOfBoundsException
          env->DeleteLocalRef(jbyte_string_ary);
          env->DeleteLocalRef(jbyte_strings);
          return nullptr;
        }

        env->SetObjectArrayElement(jbyte_strings, i, jbyte_string_ary);
        if(env->ExceptionCheck()) {
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
      if(env->ExceptionCheck()) {
        // exception thrown: OutOfMemoryError
        return;
      }

      jbyte* value = env->GetByteArrayElements(jentry_value, nullptr);
      if(env->ExceptionCheck()) {
        // exception thrown: OutOfMemoryError
        if(key != nullptr) {
          env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
        }
        return;
      }

      rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
      rocksdb::Slice value_slice(reinterpret_cast<char*>(value),
          jentry_value_len);

      op(key_slice, value_slice);

      if(value != nullptr) {
        env->ReleaseByteArrayElements(jentry_value, value, JNI_ABORT);
      }
      if(key != nullptr) {
        env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
      }
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
      if(env->ExceptionCheck()) {
        // exception thrown: OutOfMemoryError
        return;
      }

      rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

      op(key_slice);

      if(key != nullptr) {
        env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
      }
    }

    /*
     * Helper for operations on a value
     * for example WriteBatchWithIndex->GetFromBatch
     */
    static jbyteArray v_op(
        std::function<rocksdb::Status(rocksdb::Slice, std::string*)> op,
        JNIEnv* env, jbyteArray jkey, jint jkey_len) {
      jbyte* key = env->GetByteArrayElements(jkey, nullptr);
      if(env->ExceptionCheck()) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

      std::string value;
      rocksdb::Status s = op(key_slice, &value);

      if(key != nullptr) {
        env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
      }

      if (s.IsNotFound()) {
        return nullptr;
      }

      if (s.ok()) {
        jbyteArray jret_value =
            env->NewByteArray(static_cast<jsize>(value.size()));
        if(jret_value == nullptr) {
          // exception thrown: OutOfMemoryError
          return nullptr;
        }

        env->SetByteArrayRegion(jret_value, 0, static_cast<jsize>(value.size()),
                                const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value.c_str())));
        if(env->ExceptionCheck()) {
          // exception thrown: ArrayIndexOutOfBoundsException
          if(jret_value != nullptr) {
            env->DeleteLocalRef(jret_value);
          }
          return nullptr;
        }

        return jret_value;
      }

      rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
      return nullptr;
    }
};

}  // namespace rocksdb
#endif  // JAVA_ROCKSJNI_PORTAL_H_
