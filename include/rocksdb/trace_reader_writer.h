// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#include <string>
#include "rocksdb/status.h"

namespace rocksdb {

class TraceWriter {
 public:
  TraceWriter() {}
  virtual ~TraceWriter() {};

  virtual Status AddRecord(const std::string& key, const std::string& value) = 0;

  virtual void Close() {};
};

class TraceReader {
 public:
  TraceReader() {}
  virtual ~TraceReader() {};

  virtual Status ReadRecord(std::string* key, std::string* value) = 0;

  virtual Status ReadLastRecord(std::string* key, std::string* value) {
    return Status::NotSupported("ReadLastRecord is not supported");
  };

  virtual void Close() {};
};

}
