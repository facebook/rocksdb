//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_ASSOCIATIVE_MERGE_OPERATOR_JNICALLBACK_H
#define ROCKSDB_ASSOCIATIVE_MERGE_OPERATOR_JNICALLBACK_H

#include <iostream>
#include <jni.h>
#include <memory>
#include <string>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksjni/jnicallback.h"
#include "logging/logging.h"

namespace rocksdb {
  class AssociativeMergeOperatorJniCallback : public JniCallback, public AssociativeMergeOperator {
  public:
    AssociativeMergeOperatorJniCallback(JNIEnv* env, jobject jMergeOperator);

    virtual bool Merge(const Slice &key,
                       const Slice *existing_value,
                       const Slice &value,
                       std::string *new_value,
                       Logger */*logger*/) const override;

    virtual const char *Name() const override;

  private:
    std::unique_ptr<const char[]> m_name;
    jmethodID                     mergeMethodId;
    jclass                        returnTypeClass;
    jmethodID                     returnTypeInitMethodId;
    jfieldID                      returnTypeFieldId;
  };
}

#endif //ROCKSDB_ASSOCIATIVE_MERGE_OPERATOR_JNICALLBACK_H
