// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_ROCKSDB_IOSTATS_CONTEXT_H_
#define INCLUDE_ROCKSDB_IOSTATS_CONTEXT_H_

#include <stdint.h>
#include <string>

// A thread local context for gathering io-stats efficiently and transparently.
namespace rocksdb {

struct IOStatsContext {
  // reset all io-stats counter to zero
  void Reset();

  std::string ToString() const;

  // the thread pool id
  uint64_t thread_pool_id;

  // number of bytes that has been written.
  uint64_t bytes_written;
  // number of bytes that has been read.
  uint64_t bytes_read;
};

#ifndef IOS_CROSS_COMPILE
extern __thread IOStatsContext iostats_context;
#endif  // IOS_CROSS_COMPILE

}  // namespace rocksdb

#endif  // INCLUDE_ROCKSDB_IOSTATS_CONTEXT_H_
