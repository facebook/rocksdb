//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::CompactionFilter.

#ifndef JAVA_ROCKSJNI_COMPACTION_FILTER_JNICALLBACK_H_
#define JAVA_ROCKSJNI_COMPACTION_FILTER_JNICALLBACK_H_

#include <jni.h>

#include "rocksdb/compaction_filter.h"
#include "rocksjni/jnicallback.h"

namespace rocksdb {

/**
 * A JNI shim rocksdb::CompactionFilter that calls back into Java upon invocation of FilterV2.
 *
 * Heavily inspired by BaseComparatorJniCallback
 */
// TODO(benclay): Inherit from public JniCallback here if we can come up with an inheritance story on the Java side.
class CompactionFilterJniCallback : public CompactionFilter {

public:
  CompactionFilterJniCallback() {}
  ~CompactionFilterJniCallback();
  bool Initialize(JNIEnv *env, jobject jCompactionFilter);
  const char* Name() const override;
  rocksdb::CompactionFilter::Decision FilterV2(
      int level, const rocksdb::Slice &key, rocksdb::CompactionFilter::ValueType value_type,
      const rocksdb::Slice &existing_value, std::string *new_value,
      std::string *skip_until) const override;

private:
  // Cached name of the CompactionFilter
  std::string name_;
  // Reference to the JVM, which is necessary given multithreaded access (not using a factory)
  JavaVM* javaVM_;
  // Reference to our CompactionFilter object
  jobject jCompactionFilter_;
  // CompactionFilter#FilterV2 Java method reference
  jmethodID jCompactionFilterFilterV2InternalMethodId_;
  // CompactionOutput Java class reference
  jclass jCompactionOutputClass_;
  // CompactionOutput#decisionValue Java field reference
  jfieldID jCompactionOutputDecisionValueFieldId_;
  // CompactionOutput#newValue Java field reference
  jfieldID jCompactionOutputNewValueFieldId_;
  // CompactionOutput#skipUntil Java field reference
  jfieldID jCompactionOutputSkipUntilFieldId_;
};

/**
 * Exception class used in CompactionFilterJniCallback
 */

class CompactionFilterJniCallbackException : public std::runtime_error {
public:
  CompactionFilterJniCallbackException(const char * message) throw()
      : std::runtime_error(message) {}

};

}  // namespace rocksdb

#endif // JAVA_ROCKSJNI_COMPACTION_FILTER_JNICALLBACK_H_
