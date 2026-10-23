//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// EXPERIMENTAL. Auto-tuning: RocksDB traces short windows of live traffic,
// hands each window to a tuner, and applies the options the tuner recommends
// only after checks that RocksDB owns. The tuner decides what to try; RocksDB
// decides what may be applied. See AutoTuneOptions.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class AutoTuner;
class Env;

// The default workload profile directory inside the DB directory (see
// AutoTuneOptions::tuning_dir).
inline constexpr char kAutoTuneDirName[] = "auto_tune";

// What auto-tuning does. Each mode includes the work of the modes before it.
enum class AutoTuneMode : uint8_t {
  // Nothing runs.
  kOff = 0,
  // Every trace_period_sec, capture a workload profile (see WorkloadProfile)
  // and hand it to the tuner.
  kObserve,
  // Also check the tuner's recommendations and report the ones that pass
  // through EventListener::OnTuningRecommendation(). Nothing is changed. Use
  // this when a service owns option values in its own config system, which
  // would overwrite a change made inside RocksDB.
  kRecommend,
  // Also apply a recommendation that passes with DB::SetOptions() /
  // DB::SetDBOptions(), watch the next watch_windows windows, and revert if
  // the tuner reports a broken constraint.
  kApply,
};

// EXPERIMENTAL
struct AutoTuneOptions {
  AutoTuneMode mode = AutoTuneMode::kOff;

  // What the user wants, for the tuner: objectives, constraints and anything
  // else the tuner understands. RocksDB does not interpret it; it is written
  // into every workload profile.
  std::string intent;

  // The options a recommendation may change and their bounds, checked by
  // RocksDB whatever the tuner returns. Format:
  //   "<option>=<min>:<max>;<option>=<min>:<max>;..."
  // Values must be numeric. Each option must be a mutable column family
  // option (see DB::SetOptions()) or a mutable DB option (see
  // DB::SetDBOptions()) when mode is kApply. A field of a nested option is
  // named with a dot, e.g. compaction_options_universal.size_ratio or
  // block_based_table_factory.block_size (table options only affect files
  // written after the change). auto_tune and its fields cannot be listed.
  // A value the option reads as a different number (8e6 or 1.5 for an
  // integer option) is rejected.
  // Empty means nothing may change.
  std::string allowed_options;

  // Directory for workload profiles; created if missing. Each workload profile
  // is a subdirectory.
  // - Empty: "<DB directory>/auto_tune" (kAutoTuneDirName).
  // - A name without a path separator: "<DB directory>/<name>".
  // - Anything else: used as given.
  // A directory inside the DB directory is per DB, is on the DB's filesystem
  // (so checkpoints are hard links, not copies) and is deleted by DestroyDB().
  // A directory given as a path is not deleted by RocksDB.
  // RocksDB does not delete workload profiles otherwise; the tuner deletes what
  // it no longer needs. A workload profile with a checkpoint holds hard links
  // (or copies) of the DB's files, so workload profiles can use as much space
  // as the DB.
  std::string tuning_dir;

  // A window of live traffic is traced for trace_window_sec, once every
  // trace_period_sec. trace_window_sec must be less than trace_period_sec.
  uint64_t trace_window_sec = 0;
  uint64_t trace_period_sec = 0;

  // Passed to DB::StartTrace() (see TraceOptions). A sampled trace
  // (trace_sampling_frequency > 1) replays a different load than the live one.
  uint64_t trace_max_file_size = uint64_t{64} * 1024 * 1024 * 1024;
  uint64_t trace_sampling_frequency = 1;
  uint64_t trace_filter = 0;

  // Also take a block cache trace (DB::StartBlockCacheTrace()) over the
  // window.
  bool trace_block_cache = false;

  // Take a checkpoint (Checkpoint::CreateCheckpoint(), which flushes the
  // memtables) just before the trace starts, so a replay starts from the data
  // the traced operations ran against. Writes that land between the checkpoint
  // and the start of the trace are in neither.
  bool checkpoint_at_trace_start = true;

  // The tuner. Required unless mode is kOff. See AutoTuner.
  std::shared_ptr<AutoTuner> tuner;

  // Limits checked inside RocksDB, whatever the tuner returns. A
  // recommendation that breaks one is rejected.
  //
  // At most this many option changes per recommendation.
  uint32_t max_changes_per_recommendation = 3;
  // A value may move by at most this factor per step, in either direction.
  // Not checked when the current or new value is zero.
  double max_step_ratio = 2.0;
  // Apply or report only at or above this confidence: the one-sided 95%
  // Wilson lower bound of the win rate over the recommendation's confirmation
  // windows (TuningRecommendation::confirmation_wins), computed by RocksDB.
  // With every window a win, 0.8 needs 11 windows.
  double min_confidence = 0.8;
  // A recommendation with more confirmation windows than this is rejected.
  uint32_t max_confirm_windows = 20;
  // Windows watched after an apply (kApply only). One more window is marked
  // as watched and the verdict asked once more, because a tuner that answers
  // asynchronously (for example, one that runs a separate process per
  // workload profile) reports on a window only after RocksDB has asked about
  // it. Only windows that were captured count.
  uint32_t watch_windows = 3;
  // At least this long between two applies (kApply only). Kept in memory with
  // the recommendations acted on and a watch in progress: they start over
  // when the DB is reopened or SetDBOptions() replaces the tuner object, and
  // are kept across other auto_tune changes.
  uint64_t min_apply_interval_sec = 86400;
  // Put the previous values back when the tuner reports a broken constraint
  // during the watch (kApply only). An option that another caller changed
  // during the watch is left as is.
  bool revert_on_constraint_violation = true;
};

// One traced window, handed to AutoTuner::OnWorkloadProfileReady(). File names
// are relative to `dir`; an empty name means the file was not written.
struct WorkloadProfile {
  std::string workload_profile_id;
  // <tuning_dir>/<workload_profile_id>
  std::string dir;
  // The DB::StartTrace() output for the window.
  std::string trace_file;
  // The block cache trace, if trace_block_cache.
  std::string block_cache_trace_file;
  // The checkpoint taken just before the trace, if checkpoint_at_trace_start.
  std::string checkpoint_dir;
  // The OPTIONS file in effect at the end of the window.
  std::string options_file;
  // Statistics::ToString() and the "rocksdb.levelstats" property of every
  // column family, at the start and at the end of the window. Tickers can be
  // subtracted; histograms are cumulative since the statistics were created.
  std::string stats_start_file;
  std::string stats_end_file;
  // AutoTuneOptions::intent, also written to the file "intent".
  std::string intent;
  uint64_t start_micros = 0;
  uint64_t end_micros = 0;
  // Set for the windows that follow an apply: the id of the recommendation
  // being watched. Empty otherwise.
  std::string watched_recommendation_id;
  // The DB's Env. The files above (and the checkpoint) were written through
  // it, so read them through it: it may not be the local file system, e.g.
  // when the DB's files are on remote storage. Valid while the DB is open.
  Env* env = nullptr;
};

// Option changes proposed by a tuner, with the evidence RocksDB checks.
struct TuningRecommendation {
  // Unique per recommendation; RocksDB acts on an id at most once.
  std::string id;
  // The column family for cf_changes. Empty means the default column family.
  std::string column_family;
  // Option name -> new value, applied with DB::SetOptions().
  std::unordered_map<std::string, std::string> cf_changes;
  // Option name -> new value, applied with DB::SetDBOptions().
  std::unordered_map<std::string, std::string> db_changes;
  // One entry per confirmation window: true if the change won in that window.
  // The window the change was chosen on must not be included. RocksDB
  // computes the confidence from these.
  std::vector<bool> confirmation_wins;
  // Free-form evidence from the tuner, for logs and listeners.
  std::string evidence;
};

// The tuner's verdict on a window watched after an apply.
enum class TuningWatchVerdict : uint8_t {
  // No verdict yet; keep watching.
  kPending = 0,
  // The constraints held; keep the change and stop watching.
  kKeep,
  // A constraint broke.
  kRevert,
};

// What RocksDB did with a recommendation. Reported to the tuner
// (AutoTuner::OnOutcome()) and to listeners.
enum class TuningOutcome : uint8_t {
  // A check failed; see the reason.
  kRejected = 0,
  // Passed the checks in kRecommend mode; reported, not applied.
  kRecommended,
  // Passed the checks and was applied (kApply); the watch starts.
  kApplied,
  // The watch ended without a broken constraint.
  kKept,
  // A constraint broke during the watch and the previous values were put
  // back.
  kReverted,
  // A constraint broke during the watch, but revert_on_constraint_violation
  // is false.
  kViolationNotReverted,
  // A constraint broke during the watch and some of the previous values were
  // not put back (see the reason): setting them failed, or another caller
  // changed the option during the watch. The others were put back.
  kRevertFailed,
};

const char* TuningOutcomeToString(TuningOutcome outcome);

// EXPERIMENTAL. The tuning engine: turns workload profiles into
// recommendations. RocksDB has no built-in tuner: implement this class, and
// register it with the ObjectRegistry to configure it by name (for example,
// "tuner={id=MyTuner;...}" in an options string).
//
// RocksDB calls these methods from one background thread per DB, never
// concurrently for the same DB. A tuner shared by several DBs must be
// thread-safe.
class AutoTuner : public Customizable {
 public:
  ~AutoTuner() override = default;

  static const char* Type() { return "AutoTuner"; }
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& id,
                                 std::shared_ptr<AutoTuner>* result);

  // Called before each window is captured. Returning false skips the window:
  // no checkpoint, no trace, no workload profile. A tuner still busy with an
  // earlier window returns false; this call is also where it can collect that
  // work's result. Must return quickly.
  virtual bool ReadyForWorkloadProfile() { return true; }

  // A window was captured. Must return quickly: slow work (replays, model
  // calls) belongs on the tuner's own threads or processes. The workload
  // profile's files stay until the tuner deletes them.
  virtual Status OnWorkloadProfileReady(
      const WorkloadProfile& workload_profile) = 0;

  // The latest recommendation. Status::NotFound() when there is none.
  // Called after each window outside a watch, in modes kRecommend and kApply.
  virtual Status GetRecommendation(TuningRecommendation* recommendation) = 0;

  // The verdict for the watch of `recommendation_id`, called after each
  // watched window (watch_windows + 1 of them), and also when a window during
  // the watch is skipped or cannot be captured, so that a broken constraint
  // does not wait for a capture. Leaving it kPending through the last one
  // keeps the change.
  virtual Status GetWatchVerdict(const std::string& recommendation_id,
                                 TuningWatchVerdict* verdict) = 0;

  // What RocksDB did with a recommendation, with a reason for kRejected and
  // the confidence RocksDB computed (0 when not computed).
  virtual void OnOutcome(const std::string& /*recommendation_id*/,
                         TuningOutcome /*outcome*/,
                         const std::string& /*reason*/, double /*confidence*/) {
  }
};

// The confidence RocksDB checks against AutoTuneOptions::min_confidence: the
// one-sided 95% (z = 1.645) Wilson score lower bound of wins / n. 0 when n is
// 0.
double TuningConfidence(uint64_t wins, uint64_t n);

}  // namespace ROCKSDB_NAMESPACE
