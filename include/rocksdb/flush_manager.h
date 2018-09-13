//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <vector>

#include "rocksdb/status.h"

namespace rocksdb {

// Argument for FlushManager to communicate with DB
struct FlushCallbackArg {
  // IN: IDs of column families that are candidates for flush
  std::vector<uint32_t> candidates;
  // OUT: vectors of column families that must be flushed sequentially. For
  // example, {{1, 2}, {1, 3, 4}} specifies that cf1 and cf2 must be flushed
  // sequentially with cf1 first. Cf1, cf3 and cf4 must be flushed in order.
  std::vector<std::vector<uint32_t>> to_flush;
};

class FlushManager {
 public:
  virtual ~FlushManager() {}

  virtual Status OnHandleWriteBufferFull(FlushCallbackArg* /* arg */) {
    return Status::NotSupported("Not implemented");
  }

  virtual Status OnSwitchWAL(FlushCallbackArg* /* arg */) {
    return Status::NotSupported("Not implemented");
  }

  virtual Status OnScheduleFlushes(FlushCallbackArg* /* arg */) {
    return Status::NotSupported("Not implemented");
  }
};

}  // namespace rocksdb
