//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::WalFilter.

#pragma once

#include <jni.h>

#include <map>
#include <memory>
#include <string>

#include "rocksdb/write_batch.h"
#include "rocksjni/portal.h"

namespace ROCKSDB_NAMESPACE {

class WriteBatchJavaNativeException : public std::exception {};
class WriteBatchJavaNative : public WriteBatch {
 public:
  // just copy the simplest WB constructor
  explicit WriteBatchJavaNative(size_t reserved_bytes = 0, size_t max_bytes = 0)
      : WriteBatch(reserved_bytes, max_bytes, 0, 0) {}
};

class WriteBatchJavaNativeBuffer {
 private:
  jbyte* buf;
  jlong buf_len;
  jint pos = 0;

  const static int ALIGN = sizeof(int) - 1;

 public:
  WriteBatchJavaNativeBuffer(jbyte* buf, jlong buf_len)
      : buf(buf), buf_len(buf_len) {};

  jint next_int() {
    jint result = *reinterpret_cast<jint*>(buf + pos);
    pos += sizeof(jint);
    return result;
  }

  /**
   * @brief 
   * 
   * @param bytes_to_skip 
   */
  void skip(const jint bytes_to_skip) {
    if (pos + bytes_to_skip > buf_len) {
      throw ROCKSDB_NAMESPACE::WriteBatchJavaNativeException();
    }
    pos += bytes_to_skip;
  }

  void skip_aligned(const jint bytes_to_skip) {
    return skip(align(bytes_to_skip));
  };

  bool has_next() { return pos < buf_len; };

  ROCKSDB_NAMESPACE::Slice slice(jint slice_len) {
    jbyte* slice_ptr = ptr();
    skip_aligned(slice_len);
    return ROCKSDB_NAMESPACE::Slice(reinterpret_cast<char*>(slice_ptr),
                                    slice_len);
  }

  jbyte* ptr() { return buf + pos; };

  jint align(const jint val) { return val + ALIGN & ~ALIGN; }
};

}  // namespace ROCKSDB_NAMESPACE