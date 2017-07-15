// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::WriteBatch::Handler.

#ifndef JAVA_ROCKSJNI_WRITEBATCHHANDLERJNICALLBACK_H_
#define JAVA_ROCKSJNI_WRITEBATCHHANDLERJNICALLBACK_H_

#include <jni.h>
#include "rocksdb/write_batch.h"

namespace rocksdb {
/**
 * This class acts as a bridge between C++
 * and Java. The methods in this class will be
 * called back from the RocksDB storage engine (C++)
 * which calls the appropriate Java method.
 * This enables Write Batch Handlers to be implemented in Java.
 */
class WriteBatchHandlerJniCallback : public WriteBatch::Handler {
 public:
    WriteBatchHandlerJniCallback(
      JNIEnv* env, jobject jWriteBackHandler);
    ~WriteBatchHandlerJniCallback();
    void Put(const Slice& key, const Slice& value);
    void Merge(const Slice& key, const Slice& value);
    void Delete(const Slice& key);
    void DeleteRange(const Slice& beginKey, const Slice& endKey);
    void LogData(const Slice& blob);
    bool Continue();

 private:
    JNIEnv* m_env;
    jobject m_jWriteBatchHandler;
    jbyteArray sliceToJArray(const Slice& s);
    jmethodID m_jPutMethodId;
    jmethodID m_jMergeMethodId;
    jmethodID m_jDeleteMethodId;
    jmethodID m_jDeleteRangeMethodId;
    jmethodID m_jLogDataMethodId;
    jmethodID m_jContinueMethodId;
};
}  // namespace rocksdb

#endif  // JAVA_ROCKSJNI_WRITEBATCHHANDLERJNICALLBACK_H_
