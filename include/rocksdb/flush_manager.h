//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <vector>

#include "rocksdb/status.h"

namespace rocksdb {

struct FlushCallbackArg {
  std::vector<uint32_t> candidates;
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
