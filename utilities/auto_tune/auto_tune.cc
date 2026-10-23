//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/auto_tune.h"

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdlib>

#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/auto_tune.h"
#include "rocksdb/utilities/customizable_util.h"
#include "utilities/auto_tune/auto_tune_internal.h"

namespace ROCKSDB_NAMESPACE {

const char* TuningOutcomeToString(TuningOutcome outcome) {
  switch (outcome) {
    case TuningOutcome::kRejected:
      return "rejected";
    case TuningOutcome::kRecommended:
      return "recommended";
    case TuningOutcome::kApplied:
      return "applied";
    case TuningOutcome::kKept:
      return "kept";
    case TuningOutcome::kReverted:
      return "reverted";
    case TuningOutcome::kViolationNotReverted:
      return "violation_not_reverted";
    case TuningOutcome::kRevertFailed:
      return "revert_failed";
  }
  return "unknown";
}

double TuningConfidence(uint64_t wins, uint64_t n) {
  if (n == 0) {
    return 0.0;
  }
  constexpr double kZ = 1.645;
  const double nd = static_cast<double>(n);
  const double p = static_cast<double>(wins) / nd;
  const double z2 = kZ * kZ;
  const double center = p + z2 / (2 * nd);
  const double margin = kZ * std::sqrt(p * (1 - p) / nd + z2 / (4 * nd * nd));
  return std::max(0.0, (center - margin) / (1 + z2 / nd));
}

Status AutoTuner::CreateFromString(const ConfigOptions& config_options,
                                   const std::string& id,
                                   std::shared_ptr<AutoTuner>* result) {
  return LoadSharedObject<AutoTuner>(config_options, id, result);
}

namespace auto_tune {

const char* AutoTuneModeToString(AutoTuneMode mode) {
  switch (mode) {
    case AutoTuneMode::kOff:
      return "kOff";
    case AutoTuneMode::kObserve:
      return "kObserve";
    case AutoTuneMode::kRecommend:
      return "kRecommend";
    case AutoTuneMode::kApply:
      return "kApply";
  }
  return "unknown";
}

bool ParseNumber(const std::string& s, double* out) {
  if (s.empty()) {
    return false;
  }
  errno = 0;
  char* end = nullptr;
  const double v = std::strtod(s.c_str(), &end);
  if (errno != 0 || end != s.c_str() + s.size() || !std::isfinite(v)) {
    return false;
  }
  *out = v;
  return true;
}

Status ParseAllowedOptions(const std::string& allowed,
                           std::map<std::string, Bounds>* out) {
  out->clear();
  std::unordered_map<std::string, std::string> entries;
  Status s = StringToMap(allowed, &entries);
  if (!s.ok()) {
    return Status::InvalidArgument("auto_tune.allowed_options", s.ToString());
  }
  for (const auto& [name, range] : entries) {
    const size_t colon = range.find(':');
    Bounds b;
    if (colon == std::string::npos ||
        !ParseNumber(range.substr(0, colon), &b.min) ||
        !ParseNumber(range.substr(colon + 1), &b.max) || b.min > b.max) {
      return Status::InvalidArgument(
          "auto_tune.allowed_options: expected <option>=<min>:<max> with "
          "numeric min <= max for " +
          name);
    }
    (*out)[name] = b;
  }
  return Status::OK();
}

Status WriteFileAtomically(Env* env, const std::string& path,
                           const std::string& text) {
  const std::string tmp = path + ".tmp";
  Status s = WriteStringToFile(env, text, tmp, /*should_sync=*/true);
  if (s.ok()) {
    s = env->RenameFile(tmp, path);
  }
  return s;
}

}  // namespace auto_tune

Status AutoTuneDriver::ValidateOptions(const AutoTuneOptions& options) {
  if (options.mode == AutoTuneMode::kOff) {
    return Status::OK();
  }
  if (options.tuner == nullptr) {
    return Status::InvalidArgument("auto_tune.tuner is required");
  }
  if (options.tuning_dir == "." || options.tuning_dir == "..") {
    return Status::InvalidArgument(
        "auto_tune.tuning_dir must be a directory name or a path");
  }
  if (options.trace_window_sec == 0 ||
      options.trace_window_sec >= options.trace_period_sec) {
    return Status::InvalidArgument(
        "auto_tune needs 0 < trace_window_sec < trace_period_sec");
  }
  if (options.trace_sampling_frequency == 0) {
    return Status::InvalidArgument(
        "auto_tune.trace_sampling_frequency must be at least 1");
  }
  if (!(options.max_step_ratio >= 1.0)) {
    return Status::InvalidArgument("auto_tune.max_step_ratio must be >= 1");
  }
  if (!(options.min_confidence > 0.0 && options.min_confidence < 1.0)) {
    return Status::InvalidArgument(
        "auto_tune.min_confidence must be in (0, 1)");
  }
  if (options.max_confirm_windows == 0 || options.watch_windows == 0) {
    return Status::InvalidArgument(
        "auto_tune.max_confirm_windows and watch_windows must be at least 1");
  }
  std::map<std::string, auto_tune::Bounds> allowed;
  Status s = auto_tune::ParseAllowedOptions(options.allowed_options, &allowed);
  if (!s.ok()) {
    return s;
  }
  // Changing these restarts the driver from its own thread.
  for (const auto& entry : allowed) {
    if (entry.first == "auto_tune" || entry.first.rfind("auto_tune.", 0) == 0) {
      return Status::InvalidArgument(
          "auto_tune.allowed_options cannot include " + entry.first);
    }
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
