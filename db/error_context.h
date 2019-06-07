//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <string>

#include "rocksdb/listener.h"

namespace rocksdb {

struct ErrorContext {
  explicit ErrorContext() {}
  ErrorContext(BackgroundErrorReason r) : reason(r) {}

  BackgroundErrorReason reason;
  std::string file_name;
};
}  // namespace rocksdb
