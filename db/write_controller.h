//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <stdint.h>

#include <memory>

namespace rocksdb {

class WriteControllerToken;

// WriteController is controlling write stalls in our write code-path. Write
// stalls happen when compaction can't keep up with write rate.
// All of the methods here (including WriteControllerToken's destructors) need
// to be called while holding DB mutex
class WriteController {
 public:
  WriteController() : total_stopped_(0), total_delay_us_(0) {}
  ~WriteController() = default;

  // When an actor (column family) requests a stop token, all writes will be
  // stopped until the stop token is released (deleted)
  std::unique_ptr<WriteControllerToken> GetStopToken();
  // When an actor (column family) requests a delay token, total delay for all
  // writes will be increased by delay_us. The delay will last until delay token
  // is released
  std::unique_ptr<WriteControllerToken> GetDelayToken(uint64_t delay_us);

  // these two metods are querying the state of the WriteController
  bool IsStopped() const;
  uint64_t GetDelay() const;

 private:
  friend class WriteControllerToken;
  friend class StopWriteToken;
  friend class DelayWriteToken;

  int total_stopped_;
  uint64_t total_delay_us_;
};

class WriteControllerToken {
 public:
  explicit WriteControllerToken(WriteController* controller)
      : controller_(controller) {}
  virtual ~WriteControllerToken() {}

 protected:
  WriteController* controller_;

 private:
  // no copying allowed
  WriteControllerToken(const WriteControllerToken&) = delete;
  void operator=(const WriteControllerToken&) = delete;
};

class StopWriteToken : public WriteControllerToken {
 public:
  explicit StopWriteToken(WriteController* controller)
      : WriteControllerToken(controller) {}
  virtual ~StopWriteToken();
};

class DelayWriteToken : public WriteControllerToken {
 public:
  DelayWriteToken(WriteController* controller, uint64_t delay_us)
      : WriteControllerToken(controller), delay_us_(delay_us) {}
  virtual ~DelayWriteToken();

 private:
  uint64_t delay_us_;
};

}  // namespace rocksdb
