//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "util/event_logger.h"

#include <inttypes.h>
#include <cassert>
#include <sstream>
#include <string>

#include "util/string_util.h"

namespace rocksdb {

const char* kEventLoggerPrefix = "EVENT_LOG_v1";

EventLoggerStream::EventLoggerStream(Logger* logger)
    : logger_(logger), log_buffer_(nullptr), json_writer_(nullptr) {}

EventLoggerStream::EventLoggerStream(LogBuffer* log_buffer)
    : logger_(nullptr), log_buffer_(log_buffer), json_writer_(nullptr) {}

EventLoggerStream::~EventLoggerStream() {
  if (json_writer_) {
    json_writer_->EndObject();
#ifdef ROCKSDB_PRINT_EVENTS_TO_STDOUT
    printf("%s\n", json_writer_->Get().c_str());
#else
    if (logger_) {
      Log(logger_, "%s %s", kEventLoggerPrefix, json_writer_->Get().c_str());
    } else if (log_buffer_) {
      LogToBuffer(log_buffer_, "%s %s", kEventLoggerPrefix,
                  json_writer_->Get().c_str());
    }
#endif
    delete json_writer_;
  }
}

}  // namespace rocksdb
