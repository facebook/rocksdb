// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/status.h"

namespace rocksdb {

class DB;

class PreReleaseCallback {
 public:
  virtual ~PreReleaseCallback() {}

  // Will be called while on the write thread after the write to the WAL and
  // before the write to memtable. This is useful if any operation needs to be
  // done before the write gets visible to the readers, or if we want to reduce
  // the overhead of locking by updating something sequentially while we are on
  // the write thread. If the callback fails, this function returns a non-OK
  // status, the sequence number will not be released, and same status will be
  // propagated to all the writers in the write group.
  // seq is the sequence number that is used for this write and will be
  // released.
  // is_mem_disabled is currently used for debugging purposes to assert that
  // the callback is done from the right write queue.
  // If non-zero, log_number indicates the WAL log to which we wrote.
  virtual Status Callback(SequenceNumber seq, bool is_mem_disabled,
                          uint64_t log_number) = 0;
};

}  //  namespace rocksdb
