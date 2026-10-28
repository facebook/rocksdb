//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "rocksdb/block_cache_trace_writer.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"
#include "rocksdb/statistics.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/table.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/utilities/auto_tune.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/options_util.h"
#include "test_util/sync_point.h"
#include "utilities/auto_tune/auto_tune_internal.h"

namespace ROCKSDB_NAMESPACE {

namespace {

using auto_tune::Bounds;

constexpr char kDefaultColumnFamily[] = "default";

enum class OptionScope { kColumnFamily, kDB };

std::string Join(const std::string& dir, const std::string& name) {
  return dir + "/" + name;
}

std::string ColumnFamilyOrDefault(const std::string& name) {
  return name.empty() ? kDefaultColumnFamily : name;
}

// Adds "<name>.<field>" for every field of a braced (struct or object)
// value, recursively, so an allowed option can name a field such as
// compaction_options_universal.size_ratio or
// block_based_table_factory.block_size.
// SetOptions() and SetDBOptions() accept the same dotted names.
void AddNestedOptions(std::unordered_map<std::string, std::string>* options) {
  std::vector<std::pair<std::string, std::string>> pending(options->begin(),
                                                           options->end());
  while (!pending.empty()) {
    const auto [name, value] = pending.back();
    pending.pop_back();
    if (value.size() < 2 || value.front() != '{' || value.back() != '}') {
      continue;
    }
    std::unordered_map<std::string, std::string> fields;
    if (!StringToMap(value, &fields).ok()) {
      continue;
    }
    for (const auto& [field, field_value] : fields) {
      const std::string dotted = name + "." + field;
      if (options->emplace(dotted, field_value).second) {
        pending.emplace_back(dotted, field_value);
      }
    }
  }
}

// Option name -> serialized value, for column family or DB options,
// including the fields of nested options under dotted names.
Status SerializeColumnFamilyOptions(
    const ColumnFamilyOptions& options,
    std::unordered_map<std::string, std::string>* out) {
  ConfigOptions config;
  std::string serialized;
  Status s = GetStringFromColumnFamilyOptions(config, options, &serialized);
  if (s.ok()) {
    s = StringToMap(serialized, out);
  }
  // table_factory serializes as its name only; add the block-based table
  // options under the name SetOptions() accepts for them.
  if (s.ok() && options.table_factory != nullptr &&
      options.table_factory->IsInstanceOf(
          TableFactory::kBlockBasedTableName())) {
    std::string table;
    s = options.table_factory->GetOptionString(config, &table);
    std::unordered_map<std::string, std::string> fields;
    if (s.ok()) {
      s = StringToMap(table, &fields);
    }
    for (const auto& [field, value] : fields) {
      out->emplace("block_based_table_factory." + field, value);
    }
  }
  if (s.ok()) {
    AddNestedOptions(out);
  }
  return s;
}

Status SerializeDBOptions(const DBOptions& options,
                          std::unordered_map<std::string, std::string>* out) {
  ConfigOptions config;
  std::string serialized;
  Status s = GetStringFromDBOptions(config, options, &serialized);
  if (s.ok()) {
    s = StringToMap(serialized, out);
  }
  if (s.ok()) {
    AddNestedOptions(out);
  }
  return s;
}

// Whether `name` can be changed with SetOptions() (kColumnFamily) or
// SetDBOptions() (kDB), checked by parsing its current value with
// mutable_options_only.
bool IsMutable(OptionScope scope, const std::string& name,
               const std::string& value) {
  ConfigOptions config;
  config.mutable_options_only = true;
  config.ignore_unknown_options = false;
  config.invoke_prepare_options = false;
  if (scope == OptionScope::kColumnFamily) {
    ColumnFamilyOptions base;
    ColumnFamilyOptions parsed;
    return GetColumnFamilyOptionsFromMap(config, base, {{name, value}}, &parsed)
        .ok();
  }
  DBOptions base;
  DBOptions parsed;
  return GetDBOptionsFromMap(config, base, {{name, value}}, &parsed).ok();
}

// `value` as option `name` reads it: parsed with the option's type and
// serialized back.
Status NormalizeValue(OptionScope scope, const std::string& name,
                      const std::string& value, std::string* normalized) {
  ConfigOptions config;
  config.ignore_unknown_options = false;
  config.invoke_prepare_options = false;
  std::unordered_map<std::string, std::string> serialized;
  Status s;
  if (scope == OptionScope::kColumnFamily) {
    ColumnFamilyOptions parsed;
    s = GetColumnFamilyOptionsFromMap(config, ColumnFamilyOptions(),
                                      {{name, value}}, &parsed);
    if (s.ok()) {
      s = SerializeColumnFamilyOptions(parsed, &serialized);
    }
  } else {
    DBOptions parsed;
    s = GetDBOptionsFromMap(config, DBOptions(), {{name, value}}, &parsed);
    if (s.ok()) {
      s = SerializeDBOptions(parsed, &serialized);
    }
  }
  if (!s.ok()) {
    return s;
  }
  const auto it = serialized.find(name);
  if (it == serialized.end()) {
    return Status::NotFound("no serialized value for " + name);
  }
  *normalized = it->second;
  return Status::OK();
}

using auto_tune::DriverState;
using auto_tune::OptionChanges;

class AutoTuneDriverImpl : public AutoTuneDriver {
 public:
  AutoTuneDriverImpl(DB* db, const AutoTuneOptions& options,
                     AutoTuneColumnFamilyResolver resolver,
                     std::map<std::string, Bounds> allowed,
                     std::map<std::string, OptionScope> scopes,
                     std::shared_ptr<DriverState> state)
      : db_(db),
        env_(db->GetEnv()),
        clock_(env_->GetSystemClock().get()),
        info_log_(db->GetDBOptions().info_log),
        options_(options),
        tuning_dir_(AutoTuneDir(db->GetName(), options.tuning_dir)),
        resolver_(std::move(resolver)),
        allowed_(std::move(allowed)),
        scopes_(std::move(scopes)),
        state_(std::move(state)) {}

  ~AutoTuneDriverImpl() override { Stop(); }

  void StartThread(bool self_scheduled) {
    self_scheduled_ = self_scheduled;
    thread_ = std::thread([this]() { ThreadMain(); });
  }

  void Trigger() override {
    {
      std::lock_guard<std::mutex> l(mu_);
      if (!in_window_) {
        triggered_ = true;
      }
    }
    cv_.notify_all();
  }

  void Stop() override {
    {
      std::lock_guard<std::mutex> l(mu_);
      stop_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) {
      thread_.join();
    }
    if (!tuner_stopped_) {
      tuner_stopped_ = true;
      options_.tuner->OnDriverStop();
    }
  }

  Status RunOnce() override;

 private:
  void ThreadMain();
  // Waits up to `micros`; false if the driver was stopped.
  bool Wait(uint64_t micros);
  Status CaptureWorkloadProfile(WorkloadProfile* workload_profile);
  Status WriteStats(const std::string& path);
  Status WriteWorkloadProfileFile(const WorkloadProfile& workload_profile);
  Status ConsiderRecommendation();
  // `captured` is false for a window that was skipped or failed: a verdict
  // is still acted on, but the window does not count toward the watch.
  Status ContinueWatch(bool captured);
  Status ContinueWatchWithoutWindow();
  // Empty if `rec` passes; the reason otherwise. Fills the values the change
  // replaces, the values it sets (serialized as the options read them) and
  // the confidence.
  std::string Check(const TuningRecommendation& rec, OptionChanges* previous,
                    OptionChanges* applied, double* confidence);
  // Counts in `*unchanged` the changes to the value the option already has.
  std::string CheckChanges(
      const std::unordered_map<std::string, std::string>& changes,
      OptionScope scope,
      const std::unordered_map<std::string, std::string>& current,
      std::unordered_map<std::string, std::string>* previous,
      std::unordered_map<std::string, std::string>* applied,
      size_t* unchanged) const;
  Status ApplyColumnFamilyChanges(const OptionChanges& changes);
  // Stops at the first part that fails.
  Status Apply(const OptionChanges& changes);
  // Puts back the DB options even if the column family's fail.
  Status Restore(const OptionChanges& previous);
  // Removes from `previous` the options whose value is no longer the one the
  // watched change set: another caller changed them. Returns their names.
  std::string DropOptionsChangedSinceApply(OptionChanges* previous);
  void Report(const TuningRecommendation& rec, TuningOutcome outcome,
              const std::string& reason, double confidence);

  DB* const db_;
  Env* const env_;
  SystemClock* const clock_;
  const std::shared_ptr<Logger> info_log_;
  const AutoTuneOptions options_;
  const std::string tuning_dir_;
  const AutoTuneColumnFamilyResolver resolver_;
  const std::map<std::string, Bounds> allowed_;
  const std::map<std::string, OptionScope> scopes_;
  // Used only by the thread running windows.
  const std::shared_ptr<DriverState> state_;

  std::mutex mu_;
  std::condition_variable cv_;
  bool stop_ = false;
  bool triggered_ = false;
  bool in_window_ = false;
  bool self_scheduled_ = false;
  std::thread thread_;
  // Used only by the thread that calls Stop().
  bool tuner_stopped_ = false;

  // Used only by the thread running windows.
  uint64_t workload_profile_seq_ = 0;
};

void AutoTuneDriverImpl::ThreadMain() {
  const auto period = std::chrono::seconds(options_.trace_period_sec);
  std::unique_lock<std::mutex> l(mu_);
  while (true) {
    if (self_scheduled_) {
      cv_.wait_for(l, period, [this]() { return stop_ || triggered_; });
    } else {
      cv_.wait(l, [this]() { return stop_ || triggered_; });
    }
    if (stop_) {
      break;
    }
    triggered_ = false;
    in_window_ = true;
    l.unlock();
    Status s = RunOnce();
    if (!s.ok() && !s.IsAborted()) {
      ROCKS_LOG_WARN(info_log_, "[auto_tune] window failed: %s",
                     s.ToString().c_str());
    }
    TEST_SYNC_POINT("AutoTuneDriverImpl::ThreadMain:WindowDone");
    l.lock();
    in_window_ = false;
  }
}

bool AutoTuneDriverImpl::Wait(uint64_t micros) {
  std::unique_lock<std::mutex> l(mu_);
  return !cv_.wait_for(l, std::chrono::microseconds(micros),
                       [this]() { return stop_; });
}

Status AutoTuneDriverImpl::RunOnce() {
  if (!options_.tuner->ReadyForWorkloadProfile()) {
    ROCKS_LOG_INFO(info_log_,
                   "[auto_tune] window skipped: the tuner is not ready for a "
                   "workload profile");
    return ContinueWatchWithoutWindow();
  }
  WorkloadProfile workload_profile;
  Status s = CaptureWorkloadProfile(&workload_profile);
  if (!s.ok()) {
    // The tuner never sees an incomplete workload profile, so nothing else
    // deletes it.
    if (!workload_profile.dir.empty()) {
      DestroyDir(env_, workload_profile.dir).PermitUncheckedError();
    }
    if (!s.IsAborted()) {
      Status watch = ContinueWatchWithoutWindow();
      if (!watch.ok()) {
        ROCKS_LOG_WARN(info_log_, "[auto_tune] watch failed: %s",
                       watch.ToString().c_str());
      }
    }
    return s;
  }
  ROCKS_LOG_INFO(info_log_, "[auto_tune] workload profile %s ready",
                 workload_profile.workload_profile_id.c_str());
  s = options_.tuner->OnWorkloadProfileReady(workload_profile);
  if (!s.ok()) {
    ROCKS_LOG_WARN(
        info_log_, "[auto_tune] tuner did not take workload profile %s: %s",
        workload_profile.workload_profile_id.c_str(), s.ToString().c_str());
  }
  if (options_.mode < AutoTuneMode::kRecommend) {
    return s;
  }
  return state_->watching ? ContinueWatch(/*captured=*/true)
                          : ConsiderRecommendation();
}

// A broken constraint is acted on without waiting for a window that can be
// captured.
Status AutoTuneDriverImpl::ContinueWatchWithoutWindow() {
  if (options_.mode < AutoTuneMode::kRecommend || !state_->watching) {
    return Status::OK();
  }
  return ContinueWatch(/*captured=*/false);
}

Status AutoTuneDriverImpl::WriteStats(const std::string& path) {
  std::string text;
  const DBOptions db_options = db_->GetDBOptions();
  if (db_options.statistics != nullptr) {
    text += "STATISTICS:\n" + db_options.statistics->ToString() + "\n";
  }
  std::vector<std::string> names;
  Status s = DB::ListColumnFamilies(db_options, db_->GetName(), &names);
  if (!s.ok()) {
    names = {kDefaultColumnFamily};
  }
  for (const std::string& name : names) {
    std::unique_ptr<ColumnFamilyHandle> holder;
    ColumnFamilyHandle* cf = nullptr;
    std::string levelstats;
    if (resolver_(name, &holder, &cf).ok() &&
        db_->GetProperty(cf, "rocksdb.levelstats", &levelstats)) {
      text += "** levelstats column_family=" + name + " **\n" + levelstats;
    }
  }
  return WriteStringToFile(env_, text, path);
}

Status AutoTuneDriverImpl::CaptureWorkloadProfile(
    WorkloadProfile* workload_profile) {
  const uint64_t now_sec = clock_->NowMicros() / 1000000;
  workload_profile->intent = options_.intent;
  if (state_->watching) {
    workload_profile->watched_recommendation_id = state_->watched.id;
  }
  workload_profile->env = env_;
  Status s = env_->CreateDirIfMissing(tuning_dir_);
  // Another driver (another DB with the same tuning_dir, or an earlier open of
  // the same DB) may use the same second and sequence number, also between
  // FileExists() and CreateDir().
  int collisions = 0;
  while (s.ok()) {
    workload_profile->workload_profile_id =
        "p" + std::to_string(now_sec) + "-" +
        std::to_string(++workload_profile_seq_);
    const std::string dir =
        Join(tuning_dir_, workload_profile->workload_profile_id);
    if (env_->FileExists(dir).ok()) {
      continue;
    }
    s = env_->CreateDir(dir);
    if (s.ok()) {
      // Set only once created: RunOnce() deletes it if the capture fails.
      workload_profile->dir = dir;
      break;
    }
    if (env_->FileExists(dir).ok() && ++collisions < 10) {
      s = Status::OK();
    }
  }
  if (s.ok() && options_.checkpoint_at_trace_start) {
    Checkpoint* raw = nullptr;
    s = Checkpoint::Create(db_, &raw);
    std::unique_ptr<Checkpoint> checkpoint(raw);
    if (s.ok()) {
      workload_profile->checkpoint_dir = "checkpoint";
      s = checkpoint->CreateCheckpoint(
          Join(workload_profile->dir, workload_profile->checkpoint_dir));
    }
  }
  if (!s.ok()) {
    return s;
  }

  TraceOptions trace_options;
  trace_options.max_trace_file_size = options_.trace_max_file_size;
  trace_options.sampling_frequency = options_.trace_sampling_frequency;
  trace_options.filter = options_.trace_filter;
  // Replays then run each call with its own options, e.g. disableWAL.
  trace_options.include_read_write_options = true;
  std::unique_ptr<TraceWriter> trace_writer;
  workload_profile->trace_file = "trace";
  s = NewFileTraceWriter(
      env_, EnvOptions(),
      Join(workload_profile->dir, workload_profile->trace_file), &trace_writer);
  // A trace someone else started is left alone: StartTraceIfIdle() returns
  // Busy while one is in progress, and EndTraceIfOwner() does not end one that
  // replaced this one.
  DBImpl* const db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());
  uint64_t trace_id = 0;
  if (s.ok()) {
    s = db_impl->StartTraceIfIdle(trace_options, std::move(trace_writer),
                                  &trace_id);
  }
  if (!s.ok()) {
    return s;
  }
  bool block_cache_tracing = false;
  if (options_.trace_block_cache) {
    std::unique_ptr<TraceWriter> writer;
    workload_profile->block_cache_trace_file = "block_cache_trace";
    s = NewFileTraceWriter(
        env_, EnvOptions(),
        Join(workload_profile->dir, workload_profile->block_cache_trace_file),
        &writer);
    if (s.ok()) {
      BlockCacheTraceWriterOptions writer_options;
      writer_options.max_trace_file_size = options_.trace_max_file_size;
      BlockCacheTraceOptions block_cache_options;
      block_cache_options.sampling_frequency =
          options_.trace_sampling_frequency;
      s = db_->StartBlockCacheTrace(
          block_cache_options,
          NewBlockCacheTraceWriter(clock_, writer_options, std::move(writer)));
      block_cache_tracing = s.ok();
    }
  }

  // After the trace starts, so the difference between the two snapshots
  // counts the traced window, not the checkpoint's flush.
  if (s.ok()) {
    workload_profile->stats_start_file = "stats_start.txt";
    s = WriteStats(
        Join(workload_profile->dir, workload_profile->stats_start_file));
  }
  workload_profile->start_micros = clock_->NowMicros();
  uint64_t window_micros = options_.trace_window_sec * 1000000;
  TEST_SYNC_POINT_CALLBACK(
      "AutoTuneDriver::CaptureWorkloadProfile:WindowMicros", &window_micros);
  const bool completed = s.ok() && Wait(window_micros);
  workload_profile->end_micros = clock_->NowMicros();

  Status end = db_impl->EndTraceIfOwner(trace_id);
  if (block_cache_tracing) {
    Status end_block_cache = db_->EndBlockCacheTrace();
    if (end.ok()) {
      end = end_block_cache;
    }
  }
  if (!s.ok()) {
    end.PermitUncheckedError();
    return s;
  }
  if (!end.ok()) {
    return end;
  }
  if (!completed) {
    return Status::Aborted("auto_tune driver stopped during a window");
  }

  std::string options_file_name;
  s = GetLatestOptionsFileName(db_->GetName(), env_, &options_file_name);
  std::string options_text;
  if (s.ok()) {
    s = ReadFileToString(env_, Join(db_->GetName(), options_file_name),
                         &options_text);
  }
  if (s.ok()) {
    workload_profile->options_file = "OPTIONS";
    s = WriteStringToFile(
        env_, options_text,
        Join(workload_profile->dir, workload_profile->options_file));
  }
  if (s.ok()) {
    workload_profile->stats_end_file = "stats_end.txt";
    s = WriteStats(
        Join(workload_profile->dir, workload_profile->stats_end_file));
  }
  if (s.ok()) {
    s = WriteStringToFile(env_, options_.intent,
                          Join(workload_profile->dir, "intent"));
  }
  if (s.ok()) {
    s = WriteWorkloadProfileFile(*workload_profile);
  }
  return s;
}

// Written last: a workload profile directory without this file is incomplete.
Status AutoTuneDriverImpl::WriteWorkloadProfileFile(
    const WorkloadProfile& workload_profile) {
  std::vector<std::string> names;
  DB::ListColumnFamilies(db_->GetDBOptions(), db_->GetName(), &names)
      .PermitUncheckedError();
  std::string cfs;
  for (const std::string& name : names) {
    cfs += (cfs.empty() ? "" : ",") + name;
  }
  std::string text;
  auto add = [&text](const std::string& key, const std::string& value) {
    text += key + "=" + value + "\n";
  };
  add("workload_profile_id", workload_profile.workload_profile_id);
  add("start_micros", std::to_string(workload_profile.start_micros));
  add("end_micros", std::to_string(workload_profile.end_micros));
  add("trace_file", workload_profile.trace_file);
  add("block_cache_trace_file", workload_profile.block_cache_trace_file);
  add("checkpoint_dir", workload_profile.checkpoint_dir);
  add("options_file", workload_profile.options_file);
  add("stats_start_file", workload_profile.stats_start_file);
  add("stats_end_file", workload_profile.stats_end_file);
  add("intent_file", "intent");
  add("watched_recommendation_id", workload_profile.watched_recommendation_id);
  add("column_families", cfs);
  add("mode", auto_tune::AutoTuneModeToString(options_.mode));
  add("allowed_options", options_.allowed_options);
  add("max_changes_per_recommendation",
      std::to_string(options_.max_changes_per_recommendation));
  add("max_step_ratio", std::to_string(options_.max_step_ratio));
  add("min_confidence", std::to_string(options_.min_confidence));
  add("max_confirm_windows", std::to_string(options_.max_confirm_windows));
  add("watch_windows", std::to_string(options_.watch_windows));
  return auto_tune::WriteFileAtomically(
      env_, Join(workload_profile.dir, "WORKLOAD_PROFILE"), text);
}

std::string AutoTuneDriverImpl::CheckChanges(
    const std::unordered_map<std::string, std::string>& changes,
    OptionScope scope,
    const std::unordered_map<std::string, std::string>& current,
    std::unordered_map<std::string, std::string>* previous,
    std::unordered_map<std::string, std::string>* applied,
    size_t* unchanged) const {
  for (const auto& [name, value] : changes) {
    const auto bounds = allowed_.find(name);
    if (bounds == allowed_.end()) {
      return name + " is not in allowed_options";
    }
    const auto scope_it = scopes_.find(name);
    if (scope_it == scopes_.end() || scope_it->second != scope) {
      return name + (scope == OptionScope::kDB
                         ? " is not a DB option"
                         : " is not a column family option");
    }
    double to = 0;
    if (!auto_tune::ParseNumber(value, &to)) {
      return name + "=" + value + " is not a number";
    }
    // The option's own parser may read the value differently: an integer
    // option reads 8e6 as 8. Doubles are serialized with 6 decimals.
    std::string normalized;
    Status s = NormalizeValue(scope, name, value, &normalized);
    if (!s.ok()) {
      return name + "=" + value + ": " + s.ToString();
    }
    double read = 0;
    if (!auto_tune::ParseNumber(normalized, &read) ||
        std::abs(read - to) > 1e-6) {
      return name + "=" + value + " is read as " + normalized;
    }
    if (to < bounds->second.min || to > bounds->second.max) {
      return name + "=" + value + " is outside allowed_options";
    }
    const auto cur = current.find(name);
    double from = 0;
    if (cur == current.end() || !auto_tune::ParseNumber(cur->second, &from)) {
      return "cannot read the current value of " + name;
    }
    if (from > 0 && to > 0 &&
        std::max(to / from, from / to) > options_.max_step_ratio) {
      return name + " " + cur->second + " -> " + value +
             " exceeds max_step_ratio";
    }
    if (normalized == cur->second) {
      ++*unchanged;
    }
    (*previous)[name] = cur->second;
    (*applied)[name] = normalized;
  }
  return "";
}

std::string AutoTuneDriverImpl::Check(const TuningRecommendation& rec,
                                      OptionChanges* previous,
                                      OptionChanges* applied,
                                      double* confidence) {
  *confidence = 0;
  const size_t n_changes = rec.cf_changes.size() + rec.db_changes.size();
  if (n_changes == 0) {
    return "no changes";
  }
  if (n_changes > options_.max_changes_per_recommendation) {
    return std::to_string(n_changes) +
           " changes exceed max_changes_per_recommendation";
  }
  const size_t n = rec.confirmation_wins.size();
  if (n == 0) {
    return "no confirmation windows";
  }
  if (n > options_.max_confirm_windows) {
    return std::to_string(n) +
           " confirmation windows exceed "
           "max_confirm_windows";
  }
  const size_t wins = static_cast<size_t>(std::count(
      rec.confirmation_wins.begin(), rec.confirmation_wins.end(), true));
  *confidence = TuningConfidence(wins, n);
  if (*confidence < options_.min_confidence) {
    return "confidence " + std::to_string(*confidence) +
           " is below min_confidence";
  }

  previous->column_family = ColumnFamilyOrDefault(rec.column_family);
  applied->column_family = previous->column_family;
  std::string reason;
  size_t unchanged = 0;
  if (!rec.cf_changes.empty()) {
    std::unique_ptr<ColumnFamilyHandle> holder;
    ColumnFamilyHandle* cf = nullptr;
    std::unordered_map<std::string, std::string> current;
    Status s = resolver_(previous->column_family, &holder, &cf);
    if (s.ok()) {
      s = SerializeColumnFamilyOptions(db_->GetOptions(cf), &current);
    }
    if (!s.ok()) {
      return "column family " + previous->column_family + ": " + s.ToString();
    }
    reason = CheckChanges(rec.cf_changes, OptionScope::kColumnFamily, current,
                          &previous->cf, &applied->cf, &unchanged);
  }
  if (reason.empty() && !rec.db_changes.empty()) {
    std::unordered_map<std::string, std::string> current;
    Status s = SerializeDBOptions(db_->GetDBOptions(), &current);
    if (!s.ok()) {
      return "DB options: " + s.ToString();
    }
    reason = CheckChanges(rec.db_changes, OptionScope::kDB, current,
                          &previous->db, &applied->db, &unchanged);
  }
  if (reason.empty() && unchanged == n_changes) {
    return "changes nothing: every option already has the recommended value";
  }
  return reason;
}

Status AutoTuneDriverImpl::ApplyColumnFamilyChanges(
    const OptionChanges& changes) {
  if (changes.cf.empty()) {
    return Status::OK();
  }
  std::unique_ptr<ColumnFamilyHandle> holder;
  ColumnFamilyHandle* cf = nullptr;
  Status s = resolver_(changes.column_family, &holder, &cf);
  if (s.ok()) {
    s = db_->SetOptions(cf, changes.cf);
  }
  return s;
}

Status AutoTuneDriverImpl::Apply(const OptionChanges& changes) {
  Status s = ApplyColumnFamilyChanges(changes);
  if (s.ok() && !changes.db.empty()) {
    s = db_->SetDBOptions(changes.db);
  }
  return s;
}

Status AutoTuneDriverImpl::Restore(const OptionChanges& previous) {
  Status s = ApplyColumnFamilyChanges(previous);
  if (!previous.db.empty()) {
    Status db = db_->SetDBOptions(previous.db);
    if (s.ok()) {
      s = db;
    } else {
      db.PermitUncheckedError();
    }
  }
  return s;
}

std::string AutoTuneDriverImpl::DropOptionsChangedSinceApply(
    OptionChanges* previous) {
  const OptionChanges& applied = state_->watched_applied;
  std::string changed;
  auto drop = [&changed](
                  const std::unordered_map<std::string, std::string>& current,
                  const std::unordered_map<std::string, std::string>& set,
                  std::unordered_map<std::string, std::string>* values) {
    for (auto it = values->begin(); it != values->end();) {
      const auto cur = current.find(it->first);
      const auto was_set = set.find(it->first);
      if (cur != current.end() && was_set != set.end() &&
          cur->second != was_set->second) {
        changed += (changed.empty() ? "" : ", ") + it->first;
        it = values->erase(it);
      } else {
        ++it;
      }
    }
  };
  // Options that cannot be read are left to Restore(), which reports them.
  if (!previous->cf.empty()) {
    std::unique_ptr<ColumnFamilyHandle> holder;
    ColumnFamilyHandle* cf = nullptr;
    std::unordered_map<std::string, std::string> current;
    if (resolver_(previous->column_family, &holder, &cf).ok() &&
        SerializeColumnFamilyOptions(db_->GetOptions(cf), &current).ok()) {
      drop(current, applied.cf, &previous->cf);
    }
  }
  if (!previous->db.empty()) {
    std::unordered_map<std::string, std::string> current;
    if (SerializeDBOptions(db_->GetDBOptions(), &current).ok()) {
      drop(current, applied.db, &previous->db);
    }
  }
  return changed;
}

void AutoTuneDriverImpl::Report(const TuningRecommendation& rec,
                                TuningOutcome outcome,
                                const std::string& reason, double confidence) {
  ROCKS_LOG_INFO(info_log_,
                 "[auto_tune] recommendation %s: %s (confidence %.3f) %s",
                 rec.id.c_str(), TuningOutcomeToString(outcome), confidence,
                 reason.c_str());
  options_.tuner->OnOutcome(rec.id, outcome, reason, confidence);
  TuningRecommendationInfo info;
  info.db_name = db_->GetName();
  info.recommendation = rec;
  info.outcome = outcome;
  info.reason = reason;
  info.confidence = confidence;
  for (const auto& listener : db_->GetDBOptions().listeners) {
    listener->OnTuningRecommendation(info);
  }
}

Status AutoTuneDriverImpl::ConsiderRecommendation() {
  TuningRecommendation rec;
  Status s = options_.tuner->GetRecommendation(&rec);
  if (s.IsNotFound()) {
    return Status::OK();
  }
  if (!s.ok()) {
    return s;
  }
  if (rec.id.empty() || state_->Handled(rec.id)) {
    return Status::OK();
  }
  const uint64_t now = clock_->NowMicros();
  if (options_.mode == AutoTuneMode::kApply && state_->applied_before &&
      now - state_->last_apply_micros <
          options_.min_apply_interval_sec * 1000000) {
    // Not handled yet: considered again after the interval.
    return Status::OK();
  }
  state_->MarkHandled(rec.id);

  OptionChanges previous;
  OptionChanges applied;
  double confidence = 0;
  const std::string reason = Check(rec, &previous, &applied, &confidence);
  if (!reason.empty()) {
    Report(rec, TuningOutcome::kRejected, reason, confidence);
    return Status::OK();
  }
  if (options_.mode == AutoTuneMode::kRecommend) {
    Report(rec, TuningOutcome::kRecommended, "", confidence);
    return Status::OK();
  }

  OptionChanges changes;
  changes.column_family = previous.column_family;
  changes.cf = rec.cf_changes;
  changes.db = rec.db_changes;
  s = Apply(changes);
  if (!s.ok()) {
    // Put back whatever part was applied.
    Restore(previous).PermitUncheckedError();
    Report(rec, TuningOutcome::kRejected, "apply failed: " + s.ToString(),
           confidence);
    return s;
  }
  state_->applied_before = true;
  state_->last_apply_micros = now;
  state_->watching = true;
  state_->watched = rec;
  state_->watched_previous = std::move(previous);
  state_->watched_applied = std::move(applied);
  state_->watched_confidence = confidence;
  // One extra window: see AutoTuneOptions::watch_windows.
  state_->watch_windows_left = options_.watch_windows + 1;
  Report(rec, TuningOutcome::kApplied, "", confidence);
  return Status::OK();
}

Status AutoTuneDriverImpl::ContinueWatch(bool captured) {
  const TuningRecommendation& watched = state_->watched;
  const double confidence = state_->watched_confidence;
  TuningWatchVerdict verdict = TuningWatchVerdict::kPending;
  Status s = options_.tuner->GetWatchVerdict(watched.id, &verdict);
  if (!s.ok()) {
    ROCKS_LOG_WARN(info_log_, "[auto_tune] no watch verdict for %s: %s",
                   watched.id.c_str(), s.ToString().c_str());
    verdict = TuningWatchVerdict::kPending;
  }
  if (captured) {
    --state_->watch_windows_left;
  }
  if (verdict == TuningWatchVerdict::kRevert) {
    state_->watching = false;
    if (!options_.revert_on_constraint_violation) {
      Report(watched, TuningOutcome::kViolationNotReverted, "", confidence);
      return Status::OK();
    }
    OptionChanges previous = state_->watched_previous;
    const std::string changed = DropOptionsChangedSinceApply(&previous);
    s = Restore(previous);
    if (s.ok() && changed.empty()) {
      Report(watched, TuningOutcome::kReverted, "", confidence);
      return s;
    }
    std::string reason = s.ok() ? "" : s.ToString();
    if (!changed.empty()) {
      reason += (reason.empty() ? "" : "; ") +
                std::string(
                    "changed by another caller during the watch, "
                    "left as is: ") +
                changed;
    }
    Report(watched, TuningOutcome::kRevertFailed, reason, confidence);
    return s;
  }
  if (verdict == TuningWatchVerdict::kKeep || state_->watch_windows_left == 0) {
    state_->watching = false;
    Report(watched, TuningOutcome::kKept, "", confidence);
  }
  return Status::OK();
}

// Fills the scope (column family or DB) of every allowed option and checks
// that it exists, and, for kApply, that it can change at runtime.
Status ResolveScopes(DB* db, const AutoTuneOptions& options,
                     const AutoTuneColumnFamilyResolver& resolver,
                     const std::map<std::string, Bounds>& allowed,
                     std::map<std::string, OptionScope>* scopes) {
  std::unordered_map<std::string, std::string> cf_current;
  std::unordered_map<std::string, std::string> db_current;
  std::unique_ptr<ColumnFamilyHandle> holder;
  ColumnFamilyHandle* cf = nullptr;
  Status s = resolver(kDefaultColumnFamily, &holder, &cf);
  if (s.ok()) {
    s = SerializeColumnFamilyOptions(db->GetOptions(cf), &cf_current);
  }
  if (s.ok()) {
    s = SerializeDBOptions(db->GetDBOptions(), &db_current);
  }
  if (!s.ok()) {
    return s;
  }
  const bool apply = options.mode == AutoTuneMode::kApply;
  for (const auto& entry : allowed) {
    const std::string& name = entry.first;
    OptionScope scope;
    std::string value;
    if (cf_current.count(name) > 0) {
      scope = OptionScope::kColumnFamily;
      value = cf_current[name];
    } else if (db_current.count(name) > 0) {
      scope = OptionScope::kDB;
      value = db_current[name];
    } else {
      return Status::InvalidArgument(
          "auto_tune.allowed_options: unknown option " + name);
    }
    if (apply && !IsMutable(scope, name, value)) {
      return Status::InvalidArgument("auto_tune.allowed_options: " + name +
                                     " cannot be changed while the DB is "
                                     "open, so kApply cannot apply it");
    }
    (*scopes)[name] = scope;
  }
  return Status::OK();
}

}  // namespace

namespace auto_tune {

Status StartDriver(DB* db, const AutoTuneOptions& options,
                   AutoTuneColumnFamilyResolver resolver, bool self_scheduled,
                   std::shared_ptr<DriverState>* state,
                   std::unique_ptr<AutoTuneDriver>* driver) {
  driver->reset();
  if (options.mode == AutoTuneMode::kOff) {
    return Status::InvalidArgument("auto_tune.mode is kOff");
  }
  Status s = AutoTuneDriver::ValidateOptions(options);
  std::map<std::string, Bounds> allowed;
  if (s.ok()) {
    s = ParseAllowedOptions(options.allowed_options, &allowed);
  }
  std::map<std::string, OptionScope> scopes;
  if (s.ok()) {
    s = ResolveScopes(db, options, resolver, allowed, &scopes);
  }
  if (!s.ok()) {
    return s;
  }
  if (*state == nullptr || (*state)->tuner.lock() != options.tuner) {
    *state = std::make_shared<DriverState>(options.tuner);
  }
  auto impl = std::make_unique<AutoTuneDriverImpl>(
      db, options, std::move(resolver), std::move(allowed), std::move(scopes),
      *state);
  impl->StartThread(self_scheduled);
  *driver = std::move(impl);
  return Status::OK();
}

}  // namespace auto_tune

Status AutoTuneDriver::Start(DB* db, const AutoTuneOptions& options,
                             AutoTuneColumnFamilyResolver resolver,
                             bool self_scheduled,
                             std::unique_ptr<AutoTuneDriver>* driver) {
  std::shared_ptr<auto_tune::DriverState> state;
  return auto_tune::StartDriver(db, options, std::move(resolver),
                                self_scheduled, &state, driver);
}

Status AutoTuneDriver::Start(
    DB* db, const AutoTuneOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::unique_ptr<AutoTuneDriver>* driver) {
  std::map<std::string, ColumnFamilyHandle*> by_name;
  for (ColumnFamilyHandle* cf : column_families) {
    by_name[cf->GetName()] = cf;
  }
  if (by_name.count(kDefaultColumnFamily) == 0) {
    by_name[kDefaultColumnFamily] = db->DefaultColumnFamily();
  }
  AutoTuneColumnFamilyResolver resolver =
      [by_name](const std::string& name,
                std::unique_ptr<ColumnFamilyHandle>* /*holder*/,
                ColumnFamilyHandle** handle) {
        const auto it = by_name.find(name);
        if (it == by_name.end()) {
          return Status::NotFound("column family " + name +
                                  " was not given to the auto_tune driver");
        }
        *handle = it->second;
        return Status::OK();
      };
  return Start(db, options, std::move(resolver), /*self_scheduled=*/true,
               driver);
}

}  // namespace ROCKSDB_NAMESPACE
