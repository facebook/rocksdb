//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>

#include "rocksdb/auto_tune.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/auto_tune.h"

namespace ROCKSDB_NAMESPACE {

class DB;
class Env;

namespace auto_tune {

struct Bounds {
  double min = 0;
  double max = 0;
};

// Parses AutoTuneOptions::allowed_options.
Status ParseAllowedOptions(const std::string& allowed,
                           std::map<std::string, Bounds>* out);

// Parses a whole string as a finite double.
bool ParseNumber(const std::string& s, double* out);

// Writes `text` to `path` through a temporary file and a rename, so a reader
// sees the old or the new content, never a partial file.
Status WriteFileAtomically(Env* env, const std::string& path,
                           const std::string& text);

const char* AutoTuneModeToString(AutoTuneMode mode);

// Option name -> value, for one column family and the DB.
struct OptionChanges {
  std::string column_family;
  std::unordered_map<std::string, std::string> cf;
  std::unordered_map<std::string, std::string> db;
};

// What a driver remembers between windows: the recommendations it acted on,
// a watch in progress and the last apply. A DB keeps it across driver
// restarts while the tuner object stays the same.
struct DriverState {
  explicit DriverState(const std::shared_ptr<AutoTuner>& owner)
      : tuner(owner) {}

  // The tuner this state belongs to.
  std::weak_ptr<AutoTuner> tuner;
  std::set<std::string> handled_ids;
  bool watching = false;
  TuningRecommendation watched;
  // The values the watched change replaced, and the values it set, as the
  // options serialize them.
  OptionChanges watched_previous;
  OptionChanges watched_applied;
  double watched_confidence = 0;
  uint32_t watch_windows_left = 0;
  bool applied_before = false;
  uint64_t last_apply_micros = 0;
};

// AutoTuneDriver::Start() with the state of an earlier driver: `*state` is
// kept if it belongs to options.tuner and replaced otherwise. Only one driver
// at a time may use a state.
Status StartDriver(DB* db, const AutoTuneOptions& options,
                   AutoTuneColumnFamilyResolver resolver, bool self_scheduled,
                   std::shared_ptr<DriverState>* state,
                   std::unique_ptr<AutoTuneDriver>* driver);

}  // namespace auto_tune
}  // namespace ROCKSDB_NAMESPACE
