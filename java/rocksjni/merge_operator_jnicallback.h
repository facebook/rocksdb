//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_NOT_ASSOCIATIVE_MERGE_OPERATOR_JNICALLBACK_H
#define ROCKSDB_NOT_ASSOCIATIVE_MERGE_OPERATOR_JNICALLBACK_H

#include <iostream>
#include <jni.h>
#include <memory>
#include <string>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksjni/jnicallback.h"
#include "util/logging.h"

namespace rocksdb {
  class MergeOperatorJniCallback : public JniCallback, public MergeOperator {
  public:
    MergeOperatorJniCallback(JNIEnv* env, jobject jMergeOperator,
                             jboolean _allowSingleOperand, jboolean _allowShouldMerge,
                             jboolean _allowPartialMultiMerge);

    virtual bool AllowSingleOperand() const override;

    virtual bool PartialMerge(const Slice &key,
                              const Slice &lop,
                              const Slice &rop,
                              std::string *new_value,
                              Logger */*logger*/) const override;

    virtual bool PartialMergeMulti(const Slice &key,
                                   const std::deque <Slice> &operands,
                                   std::string *new_value,
                                   Logger */*logger*/) const override;

    virtual bool ShouldMerge(const std::vector <Slice> &operands) const override;

    virtual bool FullMergeV2(const MergeOperationInput &merge_in,
                             MergeOperationOutput *merge_out) const override;

    const char *Name() const override;

  private:
    std::unique_ptr<const char[]> m_name;
    jmethodID fullMergeMethodId;
    jmethodID partialMultiMergeMethodId;
    jmethodID partialMergeMethodId;
    jmethodID shouldMergeMethodId;
    jclass    byteArrayClass;

  protected:
    jboolean allowSingleOperand;
    jboolean allowShouldMerge;
    jboolean allowPartialMultiMerge;
  };
}


#endif //ROCKSDB_NOT_ASSOCIATIVE_MERGE_OPERATOR_JNICALLBACK_H
