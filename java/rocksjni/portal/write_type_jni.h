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
  static jobject PUT(JNIEnv* env) { return getEnum(env, "PUT"); }

  /**
   * Get the MERGE enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject MERGE(JNIEnv* env) { return getEnum(env, "MERGE"); }

  /**
   * Get the DELETE enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject DELETE(JNIEnv* env) { return getEnum(env, "DELETE"); }

  /**
   * Get the LOG enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject LOG(JNIEnv* env) { return getEnum(env, "LOG"); }

  // Returns the equivalent org.rocksdb.WBWIRocksIterator.WriteType for the
  // provided C++ ROCKSDB_NAMESPACE::WriteType enum
  static jbyte toJavaWriteType(const ROCKSDB_NAMESPACE::WriteType& writeType) {
    switch (writeType) {
      case ROCKSDB_NAMESPACE::WriteType::kPutRecord:
        return 0x0;
      case ROCKSDB_NAMESPACE::WriteType::kMergeRecord:
        return 0x1;
      case ROCKSDB_NAMESPACE::WriteType::kDeleteRecord:
        return 0x2;
      case ROCKSDB_NAMESPACE::WriteType::kSingleDeleteRecord:
        return 0x3;
      case ROCKSDB_NAMESPACE::WriteType::kDeleteRangeRecord:
        return 0x4;
      case ROCKSDB_NAMESPACE::WriteType::kLogDataRecord:
        return 0x5;
      case ROCKSDB_NAMESPACE::WriteType::kXIDRecord:
        return 0x6;
      default:
        return 0x7F;  // undefined
    }
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
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jfieldID jfid = env->GetStaticFieldID(
        jclazz, name, "Lorg/rocksdb/WBWIRocksIterator$WriteType;");
    if (env->ExceptionCheck()) {
      // exception occurred while getting field
      return nullptr;
    } else if (jfid == nullptr) {
      return nullptr;
    }

    jobject jwrite_type = env->GetStaticObjectField(jclazz, jfid);
    assert(jwrite_type != nullptr);
    return jwrite_type;
  }
};

}  // namespace ROCKSDB_NAMESPACE
