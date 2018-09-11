//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <functional>
#include <vector>

namespace rocksdb {

class ExternalFlushManager {
 public:
  virtual ~ExternalFlushManager() {}

  std::function<void(uint32_t, std::vector<std::vector<uint32_t>>*)>
      OnManualFlush;

  std::function<void(std::vector<std::vector<uint32_t>>*)>
      OnHandleWriteBufferFull;

  std::function<void(std::vector<std::vector<uint32_t>>*)> OnSwitchWAL;

  std::function<void(const std::vector<uint32_t>&,
                     std::vector<std::vector<uint32_t>>*)>
      OnScheduleFlushes;
};

class FlushManager;

extern FlushManager* NewDefaultFlushManager();

extern FlushManager* NewFlushManager(ExternalFlushManager* mgr);

}  // namespace rocksdb
