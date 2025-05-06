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
#include "rocksjni/portal/rocks_d_b_native_class.h"

namespace ROCKSDB_NAMESPACE {
enum FilterPolicyTypeJni {
  kUnknownFilterPolicy = 0x00,
  kBloomFilterPolicy = 0x01,
  kRibbonFilterPolicy = 0x02,
};
class FilterPolicyJni
    : public RocksDBNativeClass<
          std::shared_ptr<ROCKSDB_NAMESPACE::FilterPolicy>*, FilterPolicyJni> {
 private:
 public:
  /**
   * Get the Java Class org.rocksdb.FilterPolicy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/FilterPolicy");
  }

  static jbyte toJavaIndexType(const FilterPolicyTypeJni& filter_policy_type) {
    return static_cast<jbyte>(filter_policy_type);
  }

  static FilterPolicyTypeJni getFilterPolicyType(
      const std::string& policy_class_name) {
    if (policy_class_name == "rocksdb.BuiltinBloomFilter") {
      return kBloomFilterPolicy;
    }
    return kUnknownFilterPolicy;
  }
};

}  // namespace ROCKSDB_NAMESPACE
