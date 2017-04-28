//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once

#include <stdarg.h>
#include <stdio.h>

#include "rocksdb/env.h"

namespace rocksdb {

// Prints logs to stderr for faster debugging
class StderrLogger : public Logger {
 public:
  explicit StderrLogger(const InfoLogLevel log_level = InfoLogLevel::INFO_LEVEL)
      : Logger(log_level) {}

  // Brings overloaded Logv()s into scope so they're not hidden when we override
  // a subset of them.
  using Logger::Logv;

  virtual void Logv(const char* format, va_list ap) override {
    vfprintf(stderr, format, ap);
    fprintf(stderr, "\n");
  }
};

}  // namespace rocksdb
