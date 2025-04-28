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
// The portal class for org.rocksdb.Status
class StatusJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::Status*, StatusJni> {
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
   * Get the Java Method: Status#getCode
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getCodeMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getCode", "()Lorg/rocksdb/Status$Code;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Status#getSubCode
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getSubCodeMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "getSubCode",
                                            "()Lorg/rocksdb/Status$SubCode;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Status#getState
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getStateMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getState", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Create a new Java org.rocksdb.Status object with the same properties as
   * the provided C++ ROCKSDB_NAMESPACE::Status object
   *
   * @param env A pointer to the Java environment
   * @param status The ROCKSDB_NAMESPACE::Status object
   *
   * @return A reference to a Java org.rocksdb.Status object, or nullptr
   *     if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const Status& status) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(BBLjava/lang/String;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    // convert the Status state for Java
    jstring jstate = nullptr;
    if (status.getState() != nullptr) {
      const char* const state = status.getState();
      jstate = env->NewStringUTF(state);
      if (env->ExceptionCheck()) {
        if (jstate != nullptr) {
          env->DeleteLocalRef(jstate);
        }
        return nullptr;
      }
    }

    jobject jstatus =
        env->NewObject(jclazz, mid, toJavaStatusCode(status.code()),
                       toJavaStatusSubCode(status.subcode()), jstate);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jstate != nullptr) {
        env->DeleteLocalRef(jstate);
      }
      return nullptr;
    }

    if (jstate != nullptr) {
      env->DeleteLocalRef(jstate);
    }

    return jstatus;
  }

  static jobject construct(JNIEnv* env, const Status* status) {
    return construct(env, *status);
  }

  // Returns the equivalent org.rocksdb.Status.Code for the provided
  // C++ ROCKSDB_NAMESPACE::Status::Code enum
  static jbyte toJavaStatusCode(const ROCKSDB_NAMESPACE::Status::Code& code) {
    switch (code) {
      case ROCKSDB_NAMESPACE::Status::Code::kOk:
        return 0x0;
      case ROCKSDB_NAMESPACE::Status::Code::kNotFound:
        return 0x1;
      case ROCKSDB_NAMESPACE::Status::Code::kCorruption:
        return 0x2;
      case ROCKSDB_NAMESPACE::Status::Code::kNotSupported:
        return 0x3;
      case ROCKSDB_NAMESPACE::Status::Code::kInvalidArgument:
        return 0x4;
      case ROCKSDB_NAMESPACE::Status::Code::kIOError:
        return 0x5;
      case ROCKSDB_NAMESPACE::Status::Code::kMergeInProgress:
        return 0x6;
      case ROCKSDB_NAMESPACE::Status::Code::kIncomplete:
        return 0x7;
      case ROCKSDB_NAMESPACE::Status::Code::kShutdownInProgress:
        return 0x8;
      case ROCKSDB_NAMESPACE::Status::Code::kTimedOut:
        return 0x9;
      case ROCKSDB_NAMESPACE::Status::Code::kAborted:
        return 0xA;
      case ROCKSDB_NAMESPACE::Status::Code::kBusy:
        return 0xB;
      case ROCKSDB_NAMESPACE::Status::Code::kExpired:
        return 0xC;
      case ROCKSDB_NAMESPACE::Status::Code::kTryAgain:
        return 0xD;
      case ROCKSDB_NAMESPACE::Status::Code::kColumnFamilyDropped:
        return 0xE;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent org.rocksdb.Status.SubCode for the provided
  // C++ ROCKSDB_NAMESPACE::Status::SubCode enum
  static jbyte toJavaStatusSubCode(
      const ROCKSDB_NAMESPACE::Status::SubCode& subCode) {
    switch (subCode) {
      case ROCKSDB_NAMESPACE::Status::SubCode::kNone:
        return 0x0;
      case ROCKSDB_NAMESPACE::Status::SubCode::kMutexTimeout:
        return 0x1;
      case ROCKSDB_NAMESPACE::Status::SubCode::kLockTimeout:
        return 0x2;
      case ROCKSDB_NAMESPACE::Status::SubCode::kLockLimit:
        return 0x3;
      case ROCKSDB_NAMESPACE::Status::SubCode::kNoSpace:
        return 0x4;
      case ROCKSDB_NAMESPACE::Status::SubCode::kDeadlock:
        return 0x5;
      case ROCKSDB_NAMESPACE::Status::SubCode::kStaleFile:
        return 0x6;
      case ROCKSDB_NAMESPACE::Status::SubCode::kMemoryLimit:
        return 0x7;
      default:
        return 0x7F;  // undefined
    }
  }

  static std::unique_ptr<ROCKSDB_NAMESPACE::Status> toCppStatus(
      const jbyte jcode_value, const jbyte jsub_code_value) {
    std::unique_ptr<ROCKSDB_NAMESPACE::Status> status;
    switch (jcode_value) {
      case 0x0:
        // Ok
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::OK()));
        break;
      case 0x1:
        // NotFound
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::NotFound(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x2:
        // Corruption
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::Corruption(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x3:
        // NotSupported
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(
                ROCKSDB_NAMESPACE::Status::NotSupported(
                    ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsub_code_value))));
        break;
      case 0x4:
        // InvalidArgument
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(
                ROCKSDB_NAMESPACE::Status::InvalidArgument(
                    ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsub_code_value))));
        break;
      case 0x5:
        // IOError
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::IOError(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x6:
        // MergeInProgress
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(
                ROCKSDB_NAMESPACE::Status::MergeInProgress(
                    ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsub_code_value))));
        break;
      case 0x7:
        // Incomplete
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::Incomplete(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x8:
        // ShutdownInProgress
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(
                ROCKSDB_NAMESPACE::Status::ShutdownInProgress(
                    ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsub_code_value))));
        break;
      case 0x9:
        // TimedOut
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::TimedOut(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xA:
        // Aborted
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::Aborted(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xB:
        // Busy
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::Busy(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xC:
        // Expired
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::Expired(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xD:
        // TryAgain
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Status::TryAgain(
                ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xE:
        // ColumnFamilyDropped
        status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(
                ROCKSDB_NAMESPACE::Status::ColumnFamilyDropped(
                    ROCKSDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsub_code_value))));
        break;
      case 0x7F:
      default:
        return nullptr;
    }
    return status;
  }

  // Returns the equivalent ROCKSDB_NAMESPACE::Status for the Java
  // org.rocksdb.Status
  static std::unique_ptr<ROCKSDB_NAMESPACE::Status> toCppStatus(
      JNIEnv* env, const jobject jstatus) {
    jmethodID mid_code = getCodeMethod(env);
    if (mid_code == nullptr) {
      // exception occurred
      return nullptr;
    }
    jobject jcode = env->CallObjectMethod(jstatus, mid_code);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    jmethodID mid_code_value = ROCKSDB_NAMESPACE::CodeJni::getValueMethod(env);
    if (mid_code_value == nullptr) {
      // exception occurred
      return nullptr;
    }
    jbyte jcode_value = env->CallByteMethod(jcode, mid_code_value);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jcode != nullptr) {
        env->DeleteLocalRef(jcode);
      }
      return nullptr;
    }

    jmethodID mid_subCode = getSubCodeMethod(env);
    if (mid_subCode == nullptr) {
      // exception occurred
      return nullptr;
    }
    jobject jsubCode = env->CallObjectMethod(jstatus, mid_subCode);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jcode != nullptr) {
        env->DeleteLocalRef(jcode);
      }
      return nullptr;
    }

    jbyte jsub_code_value = 0x0;  // None
    if (jsubCode != nullptr) {
      jmethodID mid_subCode_value =
          ROCKSDB_NAMESPACE::SubCodeJni::getValueMethod(env);
      if (mid_subCode_value == nullptr) {
        // exception occurred
        return nullptr;
      }
      jsub_code_value = env->CallByteMethod(jsubCode, mid_subCode_value);
      if (env->ExceptionCheck()) {
        // exception occurred
        if (jcode != nullptr) {
          env->DeleteLocalRef(jcode);
        }
        return nullptr;
      }
    }

    jmethodID mid_state = getStateMethod(env);
    if (mid_state == nullptr) {
      // exception occurred
      return nullptr;
    }
    jobject jstate = env->CallObjectMethod(jstatus, mid_state);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jsubCode != nullptr) {
        env->DeleteLocalRef(jsubCode);
      }
      if (jcode != nullptr) {
        env->DeleteLocalRef(jcode);
      }
      return nullptr;
    }

    std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
        toCppStatus(jcode_value, jsub_code_value);

    // delete all local refs
    if (jstate != nullptr) {
      env->DeleteLocalRef(jstate);
    }
    if (jsubCode != nullptr) {
      env->DeleteLocalRef(jsubCode);
    }
    if (jcode != nullptr) {
      env->DeleteLocalRef(jcode);
    }

    return status;
  }
};
}  // namespace ROCKSDB_NAMESPACE
