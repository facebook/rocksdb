//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include "util/event_logger.h"
#include "rocksdb/table_properties.h"

namespace rocksdb {

class EventLoggerHelpers {
 public:
  static void LogTableFileCreation(EventLogger* event_logger, int job_id,
                                   uint64_t file_number, uint64_t file_size,
                                   const TableProperties& table_properties);
};
}  // namespace rocksdb
