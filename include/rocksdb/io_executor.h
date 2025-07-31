//  Copyright (c) Meta Platforms, Inc. and affiliates.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct PrefetchJob {};

// IOExecutor controls the execution of async IO operations. It is used to
// control the amount specific operation (i.e., scans) prefetch.

class IOExecutor {
 public:
  using Ticket = int64_t;
  using Priority = int;

  IOExecutor(){};
  ~IOExecutor() = default;

  // I propose we do a ticket system for IO, meaning submitters will submit
  // their jobs, in which they will recieve a ticket for each IO job they
  // submitted (which will be continuous), if they want to know if their IO is
  // done, they can query IOExecutor
  virtual Ticket submit(Priority pri, std::vector<PrefetchJob> jobs) = 0;

  virtual bool job_done(Ticket job) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
