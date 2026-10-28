//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// EXPERIMENTAL. AutoTuneDriver runs the auto-tuning loop for a DB (see
// rocksdb/auto_tune.h): capture a workload profile every trace_period_sec,
// hand it to the tuner, check and apply its recommendations, watch and revert.
// Setting DBOptions::auto_tune makes the DB run its own driver;
// AutoTuneDriver::Start() starts one for an already open DB instead.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/auto_tune.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;
class DB;

// Finds the column family `name` for an apply or revert. Sets `*handle`, and
// sets `*holder` when the handle was created for this call and must be freed
// after it.
using AutoTuneColumnFamilyResolver = std::function<Status(
    const std::string& name, std::unique_ptr<ColumnFamilyHandle>* holder,
    ColumnFamilyHandle** handle)>;

// Runs auto-tuning for one DB on its own thread.
class AutoTuneDriver {
 public:
  virtual ~AutoTuneDriver() = default;

  // Validates `options` against `db` (see AutoTuneOptions) and starts a
  // driver that captures a window every options.trace_period_sec. `db` and
  // `column_families` must outlive the driver. Only the column families in
  // `column_families` can be changed.
  static Status Start(DB* db, const AutoTuneOptions& options,
                      const std::vector<ColumnFamilyHandle*>& column_families,
                      std::unique_ptr<AutoTuneDriver>* driver);

  // Like Start(), with a resolver for column families. When
  // `self_scheduled` is false, nothing runs until Trigger() is called, so the
  // caller schedules the windows (the DB does this with its periodic task
  // scheduler).
  static Status Start(DB* db, const AutoTuneOptions& options,
                      AutoTuneColumnFamilyResolver resolver,
                      bool self_scheduled,
                      std::unique_ptr<AutoTuneDriver>* driver);

  // Checks `options` without a DB: required fields, window and period, the
  // format of allowed_options and the limits.
  static Status ValidateOptions(const AutoTuneOptions& options);

  // Asks for one window. Returns right away; the window runs on the driver's
  // thread. A request made while a window runs is dropped.
  virtual void Trigger() = 0;

  // Stops the driver: ends a window in progress (the workload profile is not
  // handed to the tuner), joins the thread, then calls
  // AutoTuner::OnDriverStop(). Idempotent.
  virtual void Stop() = 0;

  // Runs one window on the calling thread and acts on the tuner's answer.
  // For tests and callers that schedule their own windows; must not be called
  // while the driver's thread could run a window.
  virtual Status RunOnce() = 0;
};

}  // namespace ROCKSDB_NAMESPACE
