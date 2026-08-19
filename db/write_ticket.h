//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "db/write_thread.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"

namespace ROCKSDB_NAMESPACE {

enum class TicketState {
  kQueued,
  kWalWriting,
  kWalDone,
  kApplyingToMemtable,
  kFinished,
};

struct WriteTicket {
  WriteBatch* batch;  // Or an owned immutable WAL payload
  TicketState state;
  Status result;

  // Information needed to wake the blocked DB::Write() caller.
  WriteThread::Writer* writer;
};

}  // namespace ROCKSDB_NAMESPACE
