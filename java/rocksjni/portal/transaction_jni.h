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
// The portal class for org.rocksdb.Transaction
class TransactionJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.Transaction
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/Transaction");
  }

  /**
   * Create a new Java org.rocksdb.Transaction.WaitingTransactions object
   *
   * @param env A pointer to the Java environment
   * @param jtransaction A Java org.rocksdb.Transaction object
   * @param column_family_id The id of the column family
   * @param key The key
   * @param transaction_ids The transaction ids
   *
   * @return A reference to a Java
   *     org.rocksdb.Transaction.WaitingTransactions object,
   *     or nullptr if an an exception occurs
   */
  static jobject newWaitingTransactions(
      JNIEnv* env, jobject jtransaction, const uint32_t column_family_id,
      const std::string& key,
      const std::vector<TransactionID>& transaction_ids) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
        jclazz, "newWaitingTransactions",
        "(JLjava/lang/String;[J)Lorg/rocksdb/Transaction$WaitingTransactions;");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jkey = env->NewStringUTF(key.c_str());
    if (jkey == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    const size_t len = transaction_ids.size();
    jlongArray jtransaction_ids = env->NewLongArray(static_cast<jsize>(len));
    if (jtransaction_ids == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jkey);
      return nullptr;
    }

    jboolean is_copy;
    jlong* body = env->GetLongArrayElements(jtransaction_ids, &is_copy);
    if (body == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jkey);
      env->DeleteLocalRef(jtransaction_ids);
      return nullptr;
    }
    for (size_t i = 0; i < len; ++i) {
      body[i] = static_cast<jlong>(transaction_ids[i]);
    }
    env->ReleaseLongArrayElements(jtransaction_ids, body,
                                  is_copy == JNI_TRUE ? 0 : JNI_ABORT);

    jobject jwaiting_transactions = env->CallObjectMethod(
        jtransaction, mid, static_cast<jlong>(column_family_id), jkey,
        jtransaction_ids);
    if (env->ExceptionCheck()) {
      // exception thrown: InstantiationException or OutOfMemoryError
      env->DeleteLocalRef(jkey);
      env->DeleteLocalRef(jtransaction_ids);
      return nullptr;
    }

    return jwaiting_transactions;
  }
};

}  // namespace ROCKSDB_NAMESPACE
