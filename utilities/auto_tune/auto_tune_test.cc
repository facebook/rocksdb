//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/auto_tune.h"

#include <chrono>
#include <functional>
#include <mutex>
#include <optional>
#include <sstream>
#include <thread>

#include "db/db_test_util.h"
#include "file/filename.h"
#include "port/stack_trace.h"
#include "rocksdb/auto_tune.h"
#include "rocksdb/convenience.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/replayer.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "utilities/auto_tune/auto_tune_internal.h"

namespace ROCKSDB_NAMESPACE {

namespace {

struct Outcome {
  std::string id;
  TuningOutcome outcome;
  std::string reason;
};

class FakeTuner : public AutoTuner {
 public:
  const char* Name() const override { return "FakeTuner"; }

  bool ReadyForWorkloadProfile() override {
    std::lock_guard<std::mutex> l(mu_);
    return ready_;
  }

  Status OnWorkloadProfileReady(
      const WorkloadProfile& workload_profile) override {
    std::lock_guard<std::mutex> l(mu_);
    workload_profiles_.push_back(workload_profile);
    return Status::OK();
  }

  Status GetRecommendation(TuningRecommendation* rec) override {
    std::lock_guard<std::mutex> l(mu_);
    if (!rec_.has_value()) {
      return Status::NotFound();
    }
    *rec = *rec_;
    return Status::OK();
  }

  Status GetWatchVerdict(const std::string& /*id*/,
                         TuningWatchVerdict* verdict) override {
    std::lock_guard<std::mutex> l(mu_);
    *verdict = verdict_;
    return Status::OK();
  }

  void OnOutcome(const std::string& id, TuningOutcome outcome,
                 const std::string& reason, double /*confidence*/) override {
    std::lock_guard<std::mutex> l(mu_);
    outcomes_.push_back({id, outcome, reason});
  }

  void SetRecommendation(const TuningRecommendation& rec) {
    std::lock_guard<std::mutex> l(mu_);
    rec_ = rec;
  }
  void SetVerdict(TuningWatchVerdict v) {
    std::lock_guard<std::mutex> l(mu_);
    verdict_ = v;
  }
  void SetReady(bool ready) {
    std::lock_guard<std::mutex> l(mu_);
    ready_ = ready;
  }
  std::vector<WorkloadProfile> workload_profiles() {
    std::lock_guard<std::mutex> l(mu_);
    return workload_profiles_;
  }
  std::vector<Outcome> outcomes() {
    std::lock_guard<std::mutex> l(mu_);
    return outcomes_;
  }

 private:
  std::mutex mu_;
  std::vector<WorkloadProfile> workload_profiles_;
  std::optional<TuningRecommendation> rec_;
  TuningWatchVerdict verdict_ = TuningWatchVerdict::kPending;
  std::vector<Outcome> outcomes_;
  bool ready_ = true;
};

// A FakeTuner with an option, `label`.
class LabeledTuner : public FakeTuner {
 public:
  LabeledTuner() {
    static std::unordered_map<std::string, OptionTypeInfo> type_info = {
        {"label",
         {0, OptionType::kString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}}};
    RegisterOptions("LabeledTunerOptions", &label_, &type_info);
  }
  const char* Name() const override { return "LabeledTuner"; }

  std::string label_;
};

// Takes the profile directory name another driver took first: CreateDir()
// makes the directory, writes that driver's file into it, and fails.
class CollidingEnv : public EnvWrapper {
 public:
  CollidingEnv(Env* base, const std::string& prefix)
      : EnvWrapper(base), prefix_(prefix) {}
  static const char* kClassName() { return "CollidingEnv"; }
  const char* Name() const override { return kClassName(); }

  Status CreateDir(const std::string& dir) override {
    if (dir.compare(0, prefix_.size(), prefix_) != 0) {
      return target()->CreateDir(dir);
    }
    taken_dirs.push_back(dir);
    Status s = target()->CreateDir(dir);
    if (s.ok()) {
      s = WriteStringToFile(target(), "another driver's", dir + "/other");
    }
    return s.ok() ? Status::IOError("taken by another driver") : s;
  }

  std::vector<std::string> taken_dirs;

 private:
  const std::string prefix_;
};

class RecordingListener : public EventListener {
 public:
  const char* Name() const override { return "RecordingListener"; }
  void OnTuningRecommendation(const TuningRecommendationInfo& info) override {
    std::lock_guard<std::mutex> l(mu_);
    infos_.push_back(info);
  }
  std::vector<TuningRecommendationInfo> infos() {
    std::lock_guard<std::mutex> l(mu_);
    return infos_;
  }

 private:
  std::mutex mu_;
  std::vector<TuningRecommendationInfo> infos_;
};

// "key=value" lines, as in the WORKLOAD_PROFILE file.
std::unordered_map<std::string, std::string> ParseKeyValueLines(
    const std::string& text) {
  std::unordered_map<std::string, std::string> out;
  std::istringstream lines(text);
  std::string line;
  while (std::getline(lines, line)) {
    const size_t eq = line.find('=');
    if (eq != std::string::npos) {
      out[line.substr(0, eq)] = line.substr(eq + 1);
    }
  }
  return out;
}

TuningRecommendation WriteBufferRecommendation(const std::string& id,
                                               const std::string& value,
                                               size_t windows, size_t wins) {
  TuningRecommendation rec;
  rec.id = id;
  rec.cf_changes = {{"write_buffer_size", value}};
  for (size_t i = 0; i < windows; ++i) {
    rec.confirmation_wins.push_back(i < wins);
  }
  return rec;
}

}  // namespace

TEST(AutoTuneConfidenceTest, WilsonLowerBound) {
  // All wins: n / (n + z^2) with z = 1.645.
  ASSERT_NEAR(TuningConfidence(11, 11), 11 / (11 + 1.645 * 1.645), 1e-9);
  ASSERT_GE(TuningConfidence(11, 11), 0.8);
  ASSERT_LT(TuningConfidence(10, 10), 0.8);
  ASSERT_GE(TuningConfidence(19, 20), 0.8);
  ASSERT_LT(TuningConfidence(18, 20), 0.8);
  ASSERT_EQ(TuningConfidence(0, 0), 0.0);
  ASSERT_EQ(TuningConfidence(0, 5), 0.0);
}

TEST(AutoTuneOptionsTest, Validate) {
  AutoTuneOptions o;
  ASSERT_OK(AutoTuneDriver::ValidateOptions(o));  // kOff
  o.mode = AutoTuneMode::kObserve;
  ASSERT_TRUE(AutoTuneDriver::ValidateOptions(o).IsInvalidArgument());
  o.tuner = std::make_shared<FakeTuner>();
  o.tuning_dir = "/tmp/x";
  o.trace_window_sec = 5;
  o.trace_period_sec = 5;
  ASSERT_TRUE(AutoTuneDriver::ValidateOptions(o).IsInvalidArgument());
  o.trace_period_sec = 60;
  ASSERT_OK(AutoTuneDriver::ValidateOptions(o));
  o.allowed_options = "write_buffer_size=8";
  ASSERT_TRUE(AutoTuneDriver::ValidateOptions(o).IsInvalidArgument());
  o.allowed_options = "write_buffer_size=16:8";
  ASSERT_TRUE(AutoTuneDriver::ValidateOptions(o).IsInvalidArgument());
  o.allowed_options = "write_buffer_size=8:16;max_background_jobs=2:8";
  ASSERT_OK(AutoTuneDriver::ValidateOptions(o));
  for (const char* allowed :
       {"auto_tune.trace_window_sec=1:2", "auto_tune=1:2"}) {
    o.allowed_options = allowed;
    ASSERT_TRUE(AutoTuneDriver::ValidateOptions(o).IsInvalidArgument())
        << allowed;
  }
  o.allowed_options = "";
  o.min_confidence = 1.0;
  ASSERT_TRUE(AutoTuneDriver::ValidateOptions(o).IsInvalidArgument());
  o.min_confidence = 0.8;
  o.tuning_dir = "";
  ASSERT_OK(AutoTuneDriver::ValidateOptions(o));
  o.tuning_dir = "..";
  ASSERT_TRUE(AutoTuneDriver::ValidateOptions(o).IsInvalidArgument());
}

TEST(AutoTuneOptionsTest, TuningDir) {
  ASSERT_EQ(AutoTuneDir("/db", ""), "/db/auto_tune");
  ASSERT_EQ(AutoTuneDir("/db", "tune"), "/db/tune");
  ASSERT_EQ(AutoTuneDir("/db", "/data/tune"), "/data/tune");
  ASSERT_EQ(AutoTuneDir("/db", "data/tune"), "data/tune");
  ASSERT_EQ(AutoTuneDirName("/data/tune"), "");
}

TEST(AutoTuneOptionsTest, StringRoundTrip) {
  ObjectLibrary::Default()->AddFactory<AutoTuner>(
      "FakeTuner", [](const std::string& /*uri*/,
                      std::unique_ptr<AutoTuner>* guard, std::string* /*err*/) {
        guard->reset(new FakeTuner());
        return guard->get();
      });
  ConfigOptions config;
  config.ignore_unknown_options = false;
  DBOptions base;
  DBOptions parsed;
  const std::string intent =
      "{\"objectives\": [{\"metric\": \"ops_per_sec\", \"goal\": "
      "\"MAXIMIZE\"}]}";
  ASSERT_OK(GetDBOptionsFromMap(
      config, base,
      {{"auto_tune",
        "{mode=kRecommend;tuning_dir=/tmp/t;trace_window_sec=5;"
        "trace_period_sec=600;allowed_options={write_buffer_size=1:2;"
        "max_background_jobs=2:8};min_confidence=0.9;watch_windows=4;"
        "tuner={id=FakeTuner};"
        "revert_on_constraint_violation=false}"}},
      &parsed));
  parsed.auto_tune.intent = intent;
  ASSERT_EQ(parsed.auto_tune.mode, AutoTuneMode::kRecommend);
  ASSERT_EQ(parsed.auto_tune.trace_period_sec, 600);
  ASSERT_EQ(parsed.auto_tune.allowed_options,
            "write_buffer_size=1:2;max_background_jobs=2:8");
  ASSERT_EQ(parsed.auto_tune.min_confidence, 0.9);
  ASSERT_EQ(parsed.auto_tune.watch_windows, 4u);
  ASSERT_FALSE(parsed.auto_tune.revert_on_constraint_violation);
  ASSERT_NE(parsed.auto_tune.tuner, nullptr);
  ASSERT_STREQ(parsed.auto_tune.tuner->Name(), "FakeTuner");

  std::string serialized;
  ASSERT_OK(GetStringFromDBOptions(config, parsed, &serialized));
  DBOptions again;
  ASSERT_OK(GetDBOptionsFromString(config, base, serialized, &again));
  ASSERT_EQ(again.auto_tune.intent, intent);
  ASSERT_EQ(again.auto_tune.allowed_options, parsed.auto_tune.allowed_options);
  ASSERT_EQ(again.auto_tune.mode, AutoTuneMode::kRecommend);
  ASSERT_NE(again.auto_tune.tuner, nullptr);
  ASSERT_EQ(again.auto_tune.tuner->ToString(config),
            parsed.auto_tune.tuner->ToString(config));
}

class AutoTuneDriverTest : public DBTestBase {
 public:
  AutoTuneDriverTest() : DBTestBase("auto_tune_test", /*env_do_fsync=*/false) {
    tuning_dir_ = dbname_ + "_tuning";
  }

  void SetUp() override {
    SyncPoint::GetInstance()->SetCallBack(
        "AutoTuneDriver::CaptureWorkloadProfile:WindowMicros",
        [](void* arg) { *static_cast<uint64_t*>(arg) = 50 * 1000; });
    SyncPoint::GetInstance()->EnableProcessing();
  }

  void TearDown() override {
    if (driver_ != nullptr) {
      driver_->Stop();
      driver_.reset();
    }
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    DestroyDir(env_, tuning_dir_).PermitUncheckedError();
  }

  Options BaseOptions() {
    Options options = CurrentOptions();
    options.write_buffer_size = 4 << 20;
    options.statistics = CreateDBStatistics();
    options.listeners.push_back(listener_);
    return options;
  }

  AutoTuneOptions TuneOptions(AutoTuneMode mode) {
    AutoTuneOptions o;
    o.mode = mode;
    o.tuner = tuner_;
    o.tuning_dir = tuning_dir_;
    o.trace_window_sec = 1;
    o.trace_period_sec = 3600;  // Windows run only through RunOnce().
    o.intent = "{\"objectives\": []}";
    o.allowed_options =
        "write_buffer_size=1048576:67108864;max_background_jobs=1:16";
    o.min_apply_interval_sec = 0;
    return o;
  }

  void StartDriver(const AutoTuneOptions& o) {
    ASSERT_OK(AutoTuneDriver::Start(db_.get(), o, {}, &driver_));
  }

  uint64_t WriteBufferSize() { return db_->GetOptions().write_buffer_size; }

  size_t CountOutcomes(const std::string& id, TuningOutcome outcome) {
    size_t n = 0;
    for (const Outcome& o : tuner_->outcomes()) {
      n += (o.id == id && o.outcome == outcome) ? 1 : 0;
    }
    return n;
  }

  void StartOtherTrace() {
    std::unique_ptr<TraceWriter> writer;
    ASSERT_OK(NewFileTraceWriter(env_, EnvOptions(), dbname_ + "/other_trace",
                                 &writer));
    ASSERT_OK(db_->StartTrace(TraceOptions(), std::move(writer)));
  }

  // The tuner got no workload profile and none is left on disk.
  void AssertNoWorkloadProfile() {
    ASSERT_TRUE(tuner_->workload_profiles().empty());
    std::vector<std::string> children;
    const Status s = env_->GetChildren(tuning_dir_, &children);
    if (s.IsNotFound()) {
      return;
    }
    ASSERT_OK(s);
    for (const std::string& c : children) {
      ASSERT_TRUE(c == "." || c == "..") << c;
    }
  }

  std::string tuning_dir_;
  std::shared_ptr<FakeTuner> tuner_ = std::make_shared<FakeTuner>();
  std::shared_ptr<RecordingListener> listener_ =
      std::make_shared<RecordingListener>();
  std::unique_ptr<AutoTuneDriver> driver_;
};

TEST_F(AutoTuneDriverTest, CapturesWorkloadProfile) {
  Reopen(BaseOptions());
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "v"));
  }
  StartDriver(TuneOptions(AutoTuneMode::kObserve));
  ASSERT_OK(driver_->RunOnce());

  const std::vector<WorkloadProfile> workload_profiles =
      tuner_->workload_profiles();
  ASSERT_EQ(workload_profiles.size(), 1u);
  const WorkloadProfile& b = workload_profiles[0];
  ASSERT_EQ(b.dir, tuning_dir_ + "/" + b.workload_profile_id);
  ASSERT_EQ(b.env, db_->GetEnv());
  for (const std::string& f :
       {b.trace_file, b.options_file, b.stats_start_file, b.stats_end_file,
        std::string("intent"), std::string("WORKLOAD_PROFILE")}) {
    ASSERT_OK(env_->FileExists(b.dir + "/" + f)) << f;
  }
  ASSERT_OK(env_->FileExists(b.dir + "/" + b.checkpoint_dir + "/CURRENT"));
  ASSERT_TRUE(b.watched_recommendation_id.empty());
  ASSERT_LT(b.start_micros, b.end_micros);

  std::string text;
  ASSERT_OK(ReadFileToString(env_, b.dir + "/WORKLOAD_PROFILE", &text));
  auto kv = ParseKeyValueLines(text);
  ASSERT_EQ(kv["workload_profile_id"], b.workload_profile_id);
  ASSERT_EQ(kv["mode"], "kObserve");
  ASSERT_EQ(kv["column_families"], "default");
  ASSERT_EQ(kv["allowed_options"],
            "write_buffer_size=1048576:67108864;max_background_jobs=1:16");
  ASSERT_OK(ReadFileToString(env_, b.dir + "/" + b.stats_end_file, &text));
  ASSERT_NE(text.find("STATISTICS:"), std::string::npos);
  ASSERT_NE(text.find("** levelstats column_family=default **"),
            std::string::npos);
  // kObserve never asks for a recommendation.
  tuner_->SetRecommendation(WriteBufferRecommendation("r1", "8388608", 11, 11));
  ASSERT_OK(driver_->RunOnce());
  ASSERT_TRUE(tuner_->outcomes().empty());
}

TEST_F(AutoTuneDriverTest, TraceKeepsEachCallsOptions) {
  Reopen(BaseOptions());
  StartDriver(TuneOptions(AutoTuneMode::kObserve));
  SyncPoint::GetInstance()->SetCallBack(
      "AutoTuneDriver::CaptureWorkloadProfile:WindowMicros", [&](void* arg) {
        *static_cast<uint64_t*>(arg) = 50 * 1000;
        WriteOptions write_options;
        write_options.disableWAL = true;
        ASSERT_OK(db_->Put(write_options, "k", "v"));
      });
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(tuner_->workload_profiles().size(), 1u);
  const WorkloadProfile b = tuner_->workload_profiles()[0];

  std::unique_ptr<TraceReader> reader;
  ASSERT_OK(NewFileTraceReader(env_, EnvOptions(), b.dir + "/" + b.trace_file,
                               &reader));
  std::unique_ptr<Replayer> replayer;
  ASSERT_OK(db_->NewDefaultReplayer({db_->DefaultColumnFamily()},
                                    std::move(reader), &replayer));
  ASSERT_OK(replayer->Prepare());
  int writes = 0;
  std::unique_ptr<TraceRecord> record;
  while (replayer->Next(&record).ok()) {
    if (record->GetTraceType() != kTraceWrite) {
      continue;
    }
    const WriteOptions* recorded =
        static_cast<WriteQueryTraceRecord*>(record.get())->GetWriteOptions();
    ASSERT_NE(recorded, nullptr);
    ASSERT_TRUE(recorded->disableWAL);
    ++writes;
  }
  ASSERT_EQ(writes, 1);
}

TEST_F(AutoTuneDriverTest, WorkloadProfilesInsideDbDirectory) {
  for (const std::string& name : {std::string(), std::string("tune")}) {
    Options options = BaseOptions();
    options.auto_tune.tuning_dir = name;
    DestroyAndReopen(options);
    ASSERT_OK(Put("k", "v"));
    AutoTuneOptions o = TuneOptions(AutoTuneMode::kObserve);
    o.tuning_dir = name;
    StartDriver(o);
    ASSERT_OK(driver_->RunOnce());
    driver_->Stop();
    driver_.reset();
    const WorkloadProfile b = tuner_->workload_profiles().back();
    const std::string expected_dir =
        dbname_ + "/" + (name.empty() ? kAutoTuneDirName : name);
    ASSERT_EQ(b.dir, expected_dir + "/" + b.workload_profile_id);

    // Reopening the DB leaves the workload profiles alone.
    Reopen(options);
    ASSERT_OK(env_->FileExists(b.dir + "/WORKLOAD_PROFILE"));
    ASSERT_EQ(Get("k"), "v");

    // DestroyDB() deletes them with the DB.
    Close();
    ASSERT_OK(DestroyDB(dbname_, options));
    ASSERT_TRUE(env_->FileExists(dbname_).IsNotFound());
  }
}

TEST_F(AutoTuneDriverTest, StopDuringWindowRemovesWorkloadProfile) {
  Reopen(BaseOptions());
  StartDriver(TuneOptions(AutoTuneMode::kObserve));
  // Stop the driver while the window waits: the workload profile is dropped.
  std::thread stopper;
  SyncPoint::GetInstance()->SetCallBack(
      "AutoTuneDriver::CaptureWorkloadProfile:WindowMicros", [&](void* arg) {
        *static_cast<uint64_t*>(arg) = 60 * 1000 * 1000;
        stopper = std::thread([this]() {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          driver_->Stop();
        });
      });
  ASSERT_TRUE(driver_->RunOnce().IsAborted());
  stopper.join();
  AssertNoWorkloadProfile();
}

TEST_F(AutoTuneDriverTest, NotReadyTunerSkipsTheWindow) {
  Reopen(BaseOptions());
  StartDriver(TuneOptions(AutoTuneMode::kObserve));
  tuner_->SetReady(false);

  ASSERT_OK(driver_->RunOnce());

  AssertNoWorkloadProfile();
  tuner_->SetReady(true);
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(tuner_->workload_profiles().size(), 1u);
}

TEST_F(AutoTuneDriverTest, AnotherTraceIsNotReplaced) {
  Reopen(BaseOptions());
  StartOtherTrace();
  StartDriver(TuneOptions(AutoTuneMode::kObserve));

  const Status s = driver_->RunOnce();

  ASSERT_TRUE(s.IsBusy()) << s.ToString();
  AssertNoWorkloadProfile();
  // Still the other trace's: the driver did not end it.
  ASSERT_OK(db_->EndTrace());
}

TEST_F(AutoTuneDriverTest, ATraceStartedDuringTheWindowIsNotEnded) {
  Reopen(BaseOptions());
  StartDriver(TuneOptions(AutoTuneMode::kObserve));
  // Someone else starts a trace while the window runs.
  SyncPoint::GetInstance()->SetCallBack(
      "AutoTuneDriver::CaptureWorkloadProfile:WindowMicros", [&](void* arg) {
        *static_cast<uint64_t*>(arg) = 50 * 1000;
        std::unique_ptr<TraceWriter> writer;
        ASSERT_OK(NewFileTraceWriter(env_, EnvOptions(),
                                     dbname_ + "/other_trace", &writer));
        ASSERT_OK(db_->StartTrace(TraceOptions(), std::move(writer)));
      });

  const Status s = driver_->RunOnce();

  ASSERT_TRUE(s.IsBusy()) << s.ToString();
  AssertNoWorkloadProfile();
  ASSERT_OK(db_->EndTrace());
}

TEST_F(AutoTuneDriverTest, ACollidingDirectoryIsNotDeleted) {
  CollidingEnv colliding_env(env_, tuning_dir_ + "/p");
  Options options = BaseOptions();
  options.env = &colliding_env;
  Reopen(options);
  StartDriver(TuneOptions(AutoTuneMode::kObserve));

  const Status s = driver_->RunOnce();

  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_TRUE(tuner_->workload_profiles().empty());
  // Tried other names, and left every taken directory to its owner.
  ASSERT_GT(colliding_env.taken_dirs.size(), 1u);
  for (const std::string& dir : colliding_env.taken_dirs) {
    ASSERT_OK(env_->FileExists(dir + "/other")) << dir;
  }
  driver_->Stop();
  driver_.reset();
  // Off colliding_env before it goes away.
  Reopen(BaseOptions());
}

TEST_F(AutoTuneDriverTest, ApplyWatchRevert) {
  Reopen(BaseOptions());
  StartDriver(TuneOptions(AutoTuneMode::kApply));
  tuner_->SetRecommendation(WriteBufferRecommendation("r1", "8388608", 11, 11));

  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(WriteBufferSize(), 8u << 20);
  ASSERT_EQ(tuner_->outcomes().back().outcome, TuningOutcome::kApplied);
  ASSERT_EQ(listener_->infos().back().outcome, TuningOutcome::kApplied);
  ASSERT_GE(listener_->infos().back().confidence, 0.8);

  // The next window is a watch window; the tuner reports a broken constraint.
  tuner_->SetVerdict(TuningWatchVerdict::kRevert);
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(tuner_->workload_profiles().back().watched_recommendation_id, "r1");
  ASSERT_EQ(WriteBufferSize(), 4u << 20);
  ASSERT_EQ(tuner_->outcomes().back().outcome, TuningOutcome::kReverted);

  // r1 is not applied again.
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(WriteBufferSize(), 4u << 20);
  ASSERT_EQ(tuner_->outcomes().size(), 2u);
}

TEST_F(AutoTuneDriverTest, WatchEndsKept) {
  Reopen(BaseOptions());
  AutoTuneOptions o = TuneOptions(AutoTuneMode::kApply);
  o.watch_windows = 2;
  StartDriver(o);
  tuner_->SetRecommendation(WriteBufferRecommendation("r1", "8388608", 11, 11));
  ASSERT_OK(driver_->RunOnce());  // applied
  ASSERT_OK(driver_->RunOnce());  // watch 1, pending
  ASSERT_OK(driver_->RunOnce());  // watch 2, pending
  ASSERT_EQ(tuner_->outcomes().size(), 1u);
  // The extra window for asynchronous tuners, still pending: kept.
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(tuner_->workload_profiles().back().watched_recommendation_id, "r1");
  ASSERT_EQ(tuner_->outcomes().back().outcome, TuningOutcome::kKept);
  ASSERT_EQ(WriteBufferSize(), 8u << 20);
  ASSERT_OK(driver_->RunOnce());
  ASSERT_TRUE(
      tuner_->workload_profiles().back().watched_recommendation_id.empty());
}

TEST_F(AutoTuneDriverTest, ViolationNotReverted) {
  Reopen(BaseOptions());
  AutoTuneOptions o = TuneOptions(AutoTuneMode::kApply);
  o.revert_on_constraint_violation = false;
  StartDriver(o);
  tuner_->SetRecommendation(WriteBufferRecommendation("r1", "8388608", 11, 11));
  tuner_->SetVerdict(TuningWatchVerdict::kRevert);
  ASSERT_OK(driver_->RunOnce());
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(tuner_->outcomes().back().outcome,
            TuningOutcome::kViolationNotReverted);
  ASSERT_EQ(WriteBufferSize(), 8u << 20);
}

// A broken constraint is acted on while windows cannot be captured, here
// because another trace runs.
TEST_F(AutoTuneDriverTest, RevertDoesNotWaitForACapture) {
  Reopen(BaseOptions());
  StartDriver(TuneOptions(AutoTuneMode::kApply));
  tuner_->SetRecommendation(WriteBufferRecommendation("r1", "8388608", 11, 11));
  ASSERT_OK(driver_->RunOnce());
  StartOtherTrace();
  tuner_->SetVerdict(TuningWatchVerdict::kRevert);

  ASSERT_TRUE(driver_->RunOnce().IsBusy());

  ASSERT_EQ(tuner_->outcomes().back().outcome, TuningOutcome::kReverted);
  ASSERT_EQ(WriteBufferSize(), 4u << 20);
  ASSERT_OK(db_->EndTrace());
}

TEST_F(AutoTuneDriverTest, OnlyCapturedWindowsCountTowardTheWatch) {
  Reopen(BaseOptions());
  AutoTuneOptions o = TuneOptions(AutoTuneMode::kApply);
  o.watch_windows = 1;
  StartDriver(o);
  tuner_->SetRecommendation(WriteBufferRecommendation("r1", "8388608", 11, 11));
  ASSERT_OK(driver_->RunOnce());  // applied
  tuner_->SetReady(false);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(driver_->RunOnce());  // skipped
  }
  ASSERT_EQ(tuner_->outcomes().size(), 1u);

  tuner_->SetReady(true);
  ASSERT_OK(driver_->RunOnce());  // watch 1
  ASSERT_OK(driver_->RunOnce());  // the extra watch window: kept

  ASSERT_EQ(tuner_->workload_profiles().back().watched_recommendation_id, "r1");
  ASSERT_EQ(tuner_->outcomes().back().outcome, TuningOutcome::kKept);
}

TEST_F(AutoTuneDriverTest, RevertLeavesAnOptionChangedDuringTheWatch) {
  Options options = BaseOptions();
  options.max_background_jobs = 4;
  Reopen(options);
  StartDriver(TuneOptions(AutoTuneMode::kApply));
  TuningRecommendation rec = WriteBufferRecommendation("r1", "8388608", 11, 11);
  rec.db_changes = {{"max_background_jobs", "6"}};
  tuner_->SetRecommendation(rec);
  ASSERT_OK(driver_->RunOnce());
  // Someone else sets write_buffer_size during the watch.
  ASSERT_OK(db_->SetOptions({{"write_buffer_size", "33554432"}}));
  tuner_->SetVerdict(TuningWatchVerdict::kRevert);

  ASSERT_OK(driver_->RunOnce());

  ASSERT_EQ(WriteBufferSize(), 32u << 20);
  ASSERT_EQ(db_->GetDBOptions().max_background_jobs, 4);
  const Outcome last = tuner_->outcomes().back();
  ASSERT_EQ(last.outcome, TuningOutcome::kRevertFailed);
  ASSERT_NE(last.reason.find("write_buffer_size"), std::string::npos)
      << last.reason;
}

TEST_F(AutoTuneDriverTest, RevertRestoresDBOptionsWhenTheColumnFamilyIsGone) {
  Options options = BaseOptions();
  options.max_background_jobs = 4;
  CreateAndReopenWithCF({"pikachu"}, options);
  // Like the DB's own resolver: a dropped column family is not found.
  bool dropped = false;
  AutoTuneColumnFamilyResolver resolver =
      [this, &dropped](const std::string& name,
                       std::unique_ptr<ColumnFamilyHandle>* /*holder*/,
                       ColumnFamilyHandle** handle) {
        for (ColumnFamilyHandle* h : handles_) {
          if (h->GetName() == name && !(dropped && name == "pikachu")) {
            *handle = h;
            return Status::OK();
          }
        }
        return Status::NotFound("column family " + name);
      };
  ASSERT_OK(AutoTuneDriver::Start(db_.get(), TuneOptions(AutoTuneMode::kApply),
                                  std::move(resolver),
                                  /*self_scheduled=*/false, &driver_));
  TuningRecommendation rec = WriteBufferRecommendation("r1", "8388608", 11, 11);
  rec.column_family = "pikachu";
  rec.db_changes = {{"max_background_jobs", "6"}};
  tuner_->SetRecommendation(rec);
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(db_->GetDBOptions().max_background_jobs, 6);

  ASSERT_OK(db_->DropColumnFamily(handles_[1]));
  dropped = true;
  tuner_->SetVerdict(TuningWatchVerdict::kRevert);
  const Status s = driver_->RunOnce();

  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_EQ(db_->GetDBOptions().max_background_jobs, 4);
  ASSERT_EQ(tuner_->outcomes().back().outcome, TuningOutcome::kRevertFailed);
  ASSERT_NE(tuner_->outcomes().back().reason.find("pikachu"),
            std::string::npos);
}

TEST_F(AutoTuneDriverTest, ChecksRejectRecommendations) {
  Reopen(BaseOptions());
  StartDriver(TuneOptions(AutoTuneMode::kApply));
  struct Case {
    TuningRecommendation rec;
    std::string reason;
  };
  TuningRecommendation not_allowed =
      WriteBufferRecommendation("not_allowed", "8388608", 11, 11);
  not_allowed.cf_changes = {{"max_write_buffer_number", "3"}};
  TuningRecommendation too_many =
      WriteBufferRecommendation("too_many", "8388608", 11, 11);
  too_many.db_changes = {{"max_background_jobs", "3"}};
  AutoTuneOptions o = TuneOptions(AutoTuneMode::kApply);
  TuningRecommendation wrong_scope =
      WriteBufferRecommendation("wrong_scope", "8388608", 11, 11);
  wrong_scope.cf_changes = {{"max_background_jobs", "3"}};
  const std::vector<Case> cases = {
      {not_allowed, "not in allowed_options"},
      {WriteBufferRecommendation("big_step", "16777216", 11, 11),
       "max_step_ratio"},
      {WriteBufferRecommendation("out_of_bounds", "524288", 11, 11),
       "outside allowed_options"},
      {WriteBufferRecommendation("not_a_number", "8MB", 11, 11),
       "not a number"},
      // An integer option reads these as 8 and 6291456.
      {WriteBufferRecommendation("exponent", "8e6", 11, 11), "is read as 8"},
      {WriteBufferRecommendation("fraction", "6291456.5", 11, 11),
       "is read as 6291456"},
      {WriteBufferRecommendation("low_confidence", "8388608", 10, 10),
       "below min_confidence"},
      {WriteBufferRecommendation("too_many_windows", "8388608", 21, 21),
       "max_confirm_windows"},
      {WriteBufferRecommendation("no_windows", "8388608", 0, 0),
       "no confirmation windows"},
      {WriteBufferRecommendation("no_change", "4194304", 11, 11),
       "changes nothing"},
      {wrong_scope, "not a column family option"},
  };
  for (const Case& c : cases) {
    tuner_->SetRecommendation(c.rec);
    ASSERT_OK(driver_->RunOnce());
    const Outcome last = tuner_->outcomes().back();
    ASSERT_EQ(last.id, c.rec.id);
    ASSERT_EQ(last.outcome, TuningOutcome::kRejected) << c.rec.id;
    ASSERT_NE(last.reason.find(c.reason), std::string::npos)
        << c.rec.id << ": " << last.reason;
    ASSERT_EQ(WriteBufferSize(), 4u << 20);
  }
  // Two changes pass the default limit of 3; with a limit of 1 they do not.
  driver_->Stop();
  o.max_changes_per_recommendation = 1;
  StartDriver(o);
  tuner_->SetRecommendation(too_many);
  ASSERT_OK(driver_->RunOnce());
  ASSERT_NE(
      tuner_->outcomes().back().reason.find("max_changes_per_recommendation"),
      std::string::npos);
}

TEST_F(AutoTuneDriverTest, AppliesDBOption) {
  Options options = BaseOptions();
  options.max_background_jobs = 4;
  Reopen(options);
  StartDriver(TuneOptions(AutoTuneMode::kApply));
  TuningRecommendation rec = WriteBufferRecommendation("r1", "8388608", 11, 11);
  rec.db_changes = {{"max_background_jobs", "6"}};
  tuner_->SetRecommendation(rec);
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(tuner_->outcomes().back().outcome, TuningOutcome::kApplied);
  ASSERT_EQ(db_->GetDBOptions().max_background_jobs, 6);
  ASSERT_EQ(WriteBufferSize(), 8u << 20);
}

TEST_F(AutoTuneDriverTest, AppliesDoubleOptionAsGiven) {
  Options options = BaseOptions();
  options.max_bytes_for_level_multiplier = 10;
  Reopen(options);
  AutoTuneOptions o = TuneOptions(AutoTuneMode::kApply);
  o.allowed_options = "max_bytes_for_level_multiplier=5:20";
  StartDriver(o);
  // More digits than RocksDB serializes a double with.
  TuningRecommendation rec = WriteBufferRecommendation("r1", "8388608", 11, 11);
  rec.cf_changes = {{"max_bytes_for_level_multiplier", "12.3456789"}};
  tuner_->SetRecommendation(rec);
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(tuner_->outcomes().back().outcome, TuningOutcome::kApplied)
      << tuner_->outcomes().back().reason;
  ASSERT_EQ(db_->GetOptions().max_bytes_for_level_multiplier, 12.3456789);
}

TEST_F(AutoTuneDriverTest, AppliesNestedOptions) {
  Options options = BaseOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.compaction_options_universal.size_ratio = 10;
  BlockBasedTableOptions table_options;
  table_options.block_size = 4096;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  AutoTuneOptions o = TuneOptions(AutoTuneMode::kApply);
  o.allowed_options =
      "compaction_options_universal.size_ratio=1:100;"
      "block_based_table_factory.block_size=1024:65536";
  StartDriver(o);
  // max_step_ratio (2) applies to nested fields too.
  TuningRecommendation rec = WriteBufferRecommendation("r1", "8388608", 11, 11);
  rec.cf_changes = {{"compaction_options_universal.size_ratio", "80"}};
  tuner_->SetRecommendation(rec);
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(tuner_->outcomes().back().outcome, TuningOutcome::kRejected);
  ASSERT_EQ(db_->GetOptions().compaction_options_universal.size_ratio, 10u);

  rec.id = "r2";
  rec.cf_changes = {{"compaction_options_universal.size_ratio", "20"},
                    {"block_based_table_factory.block_size", "8192"}};
  tuner_->SetRecommendation(rec);
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(tuner_->outcomes().back().outcome, TuningOutcome::kApplied)
      << tuner_->outcomes().back().reason;
  const Options now = db_->GetOptions();
  ASSERT_EQ(now.compaction_options_universal.size_ratio, 20u);
  ASSERT_EQ(now.table_factory->GetOptions<BlockBasedTableOptions>()->block_size,
            8192u);
}

TEST_F(AutoTuneDriverTest, RecommendModeDoesNotApply) {
  Reopen(BaseOptions());
  StartDriver(TuneOptions(AutoTuneMode::kRecommend));
  tuner_->SetRecommendation(WriteBufferRecommendation("r1", "8388608", 11, 11));
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(WriteBufferSize(), 4u << 20);
  ASSERT_EQ(listener_->infos().back().outcome, TuningOutcome::kRecommended);
  ASSERT_EQ(listener_->infos().back().recommendation.cf_changes.at(
                "write_buffer_size"),
            "8388608");
}

TEST_F(AutoTuneDriverTest, MinApplyInterval) {
  Reopen(BaseOptions());
  AutoTuneOptions o = TuneOptions(AutoTuneMode::kApply);
  o.min_apply_interval_sec = 3600;
  o.watch_windows = 1;
  StartDriver(o);
  tuner_->SetRecommendation(WriteBufferRecommendation("r1", "8388608", 11, 11));
  ASSERT_OK(driver_->RunOnce());  // applied
  ASSERT_OK(driver_->RunOnce());  // watch 1
  ASSERT_OK(driver_->RunOnce());  // extra watch window: kept
  tuner_->SetRecommendation(
      WriteBufferRecommendation("r2", "16777216", 11, 11));
  ASSERT_OK(driver_->RunOnce());
  // r2 waits for the interval: no outcome, no change.
  ASSERT_EQ(tuner_->outcomes().size(), 2u);
  ASSERT_EQ(WriteBufferSize(), 8u << 20);
}

TEST_F(AutoTuneDriverTest, StartChecksAllowedOptions) {
  Reopen(BaseOptions());
  AutoTuneOptions o = TuneOptions(AutoTuneMode::kApply);
  o.allowed_options = "min_write_buffer_number_to_merge=1:4";
  Status s = AutoTuneDriver::Start(db_.get(), o, {}, &driver_);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_NE(s.ToString().find("cannot be changed"), std::string::npos);
  // A service can still apply it itself, so kRecommend allows it.
  o.mode = AutoTuneMode::kRecommend;
  ASSERT_OK(AutoTuneDriver::Start(db_.get(), o, {}, &driver_));
  driver_->Stop();
  o.allowed_options = "no_such_option=1:4";
  s = AutoTuneDriver::Start(db_.get(), o, {}, &driver_);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST_F(AutoTuneDriverTest, ColumnFamilyChange) {
  Options options = BaseOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  StartDriver(TuneOptions(AutoTuneMode::kApply));
  // The standalone driver can change only the column families it was given.
  TuningRecommendation rec = WriteBufferRecommendation("r1", "8388608", 11, 11);
  rec.column_family = "pikachu";
  tuner_->SetRecommendation(rec);
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(tuner_->outcomes().back().outcome, TuningOutcome::kRejected);
  driver_->Stop();
  ASSERT_OK(AutoTuneDriver::Start(db_.get(), TuneOptions(AutoTuneMode::kApply),
                                  handles_, &driver_));
  rec.id = "r2";
  tuner_->SetRecommendation(rec);
  ASSERT_OK(driver_->RunOnce());
  ASSERT_EQ(tuner_->outcomes().back().outcome, TuningOutcome::kApplied);
  ASSERT_EQ(db_->GetOptions(handles_[1]).write_buffer_size, 8u << 20);
  ASSERT_EQ(db_->GetOptions(handles_[0]).write_buffer_size, 4u << 20);
}

TEST_F(AutoTuneDriverTest, DBRunsItsOwnDriver) {
  Options options = BaseOptions();
  options.auto_tune = TuneOptions(AutoTuneMode::kApply);
  options.auto_tune.trace_period_sec = 2;
  tuner_->SetRecommendation(WriteBufferRecommendation("r1", "8388608", 11, 11));
  ASSERT_OK(TryReopen(options));
  for (int i = 0; i < 100 && WriteBufferSize() != (8u << 20); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(WriteBufferSize(), 8u << 20);
  ASSERT_GE(tuner_->workload_profiles().size(), 1u);
  // The OPTIONS file keeps the auto_tune settings; the fake tuner has no
  // registered id, so it is not loaded back.
  ConfigOptions config;
  config.env = env_;
  config.ignore_unknown_options = true;
  config.ignore_unsupported_options = true;
  DBOptions loaded;
  std::vector<ColumnFamilyDescriptor> cfs;
  ASSERT_OK(LoadLatestOptions(config, dbname_, &loaded, &cfs));
  ASSERT_EQ(loaded.auto_tune.mode, AutoTuneMode::kApply);
  ASSERT_EQ(loaded.auto_tune.tuning_dir, tuning_dir_);
  ASSERT_EQ(loaded.auto_tune.allowed_options,
            options.auto_tune.allowed_options);
  Close();
  // No window runs after close.
  const size_t workload_profiles = tuner_->workload_profiles().size();
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  ASSERT_EQ(tuner_->workload_profiles().size(), workload_profiles);
}

TEST_F(AutoTuneDriverTest, DBOpenRunsWithoutBadAutoTuneOptions) {
  std::vector<AutoTuneOptions> bad;
  // The window is not shorter than the period.
  bad.push_back(TuneOptions(AutoTuneMode::kApply));
  bad.back().trace_period_sec = 1;
  bad.back().trace_window_sec = 1;
  // Not a mutable option.
  bad.push_back(TuneOptions(AutoTuneMode::kApply));
  bad.back().allowed_options = "min_write_buffer_number_to_merge=1:4";
  bad.back().trace_period_sec = 2;
  // No tuner.
  bad.push_back(TuneOptions(AutoTuneMode::kApply));
  bad.back().tuner = nullptr;
  bad.back().trace_period_sec = 2;
  // A field of auto_tune itself: applying it would restart the driver from
  // its own thread.
  bad.push_back(TuneOptions(AutoTuneMode::kApply));
  bad.back().allowed_options = "auto_tune.trace_window_sec=1:2";
  bad.back().trace_period_sec = 3;
  tuner_->SetRecommendation([] {
    TuningRecommendation rec;
    rec.id = "restart";
    rec.db_changes = {{"auto_tune.trace_window_sec", "2"}};
    rec.confirmation_wins.assign(11, true);
    return rec;
  }());
  for (const AutoTuneOptions& o : bad) {
    Options options = BaseOptions();
    options.auto_tune = o;
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("k", "v"));
    // No window runs.
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    ASSERT_TRUE(tuner_->workload_profiles().empty());
    ASSERT_EQ(Get("k"), "v");
  }
}

// SetDBOptions() parses the tuner by id, so the tests that change auto_tune
// at runtime register FakeTuner and reach the object the DB made through
// GetDBOptions().
void RegisterFakeTuner() {
  static std::once_flag once;
  std::call_once(once, [] {
    ObjectLibrary::Default()->AddFactory<AutoTuner>(
        "FakeTuner",
        [](const std::string& /*uri*/, std::unique_ptr<AutoTuner>* guard,
           std::string* /*err*/) {
          guard->reset(new FakeTuner());
          return guard->get();
        });
    ObjectLibrary::Default()->AddFactory<AutoTuner>(
        "LabeledTuner",
        [](const std::string& /*uri*/, std::unique_ptr<AutoTuner>* guard,
           std::string* /*err*/) {
          guard->reset(new LabeledTuner());
          return guard->get();
        });
  });
}

class AutoTuneSetDBOptionsTest : public AutoTuneDriverTest {
 public:
  void SetUp() override {
    AutoTuneDriverTest::SetUp();
    RegisterFakeTuner();
  }

  // An auto_tune value for SetDBOptions(): windows every 2 s (each 50 ms, see
  // the fixture).
  std::string Setting(const std::string& mode,
                      const std::string& allowed = "write_buffer_size=1:2") {
    return "{mode=" + mode + ";tuner={id=FakeTuner};tuning_dir=" + tuning_dir_ +
           ";trace_window_sec=1;trace_period_sec=2;intent={{}};"
           "allowed_options={" +
           allowed + "}}";
  }

  // Shared: a replaced driver drops the tuner it ran.
  std::shared_ptr<FakeTuner> CurrentTuner() {
    return std::static_pointer_cast<FakeTuner>(
        db_->GetDBOptions().auto_tune.tuner);
  }

  // Waits up to 10 s for `tuner` to get a workload profile.
  bool GetsWindows(const std::shared_ptr<FakeTuner>& tuner) {
    return WaitFor([&] { return !tuner->workload_profiles().empty(); });
  }

  // Waits up to 10 s for `done`.
  static bool WaitFor(const std::function<bool()>& done) {
    for (int i = 0; i < 100; ++i) {
      if (done()) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
  }
};

TEST_F(AutoTuneSetDBOptionsTest, TurnedOnAndOffWithoutReopening) {
  Reopen(BaseOptions());
  ASSERT_EQ(db_->GetDBOptions().auto_tune.mode, AutoTuneMode::kOff);

  ASSERT_OK(db_->SetDBOptions({{"auto_tune", Setting("kObserve")}}));
  std::shared_ptr<FakeTuner> tuner = CurrentTuner();
  ASSERT_NE(tuner, nullptr);
  ASSERT_TRUE(GetsWindows(tuner));

  ASSERT_OK(db_->SetDBOptions({{"auto_tune", Setting("kOff")}}));
  ASSERT_EQ(db_->GetDBOptions().auto_tune.mode, AutoTuneMode::kOff);
  const size_t windows = tuner->workload_profiles().size();
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  ASSERT_EQ(tuner->workload_profiles().size(), windows);
}

TEST_F(AutoTuneSetDBOptionsTest, TheSameSettingKeepsTheRunningTuner) {
  Reopen(BaseOptions());
  ASSERT_OK(db_->SetDBOptions({{"auto_tune", Setting("kObserve")}}));
  std::shared_ptr<FakeTuner> tuner = CurrentTuner();

  // Alone, and with another option that changes.
  ASSERT_OK(db_->SetDBOptions({{"auto_tune", Setting("kObserve")}}));
  ASSERT_OK(db_->SetDBOptions(
      {{"auto_tune", Setting("kObserve")}, {"max_background_jobs", "7"}}));

  ASSERT_EQ(CurrentTuner(), tuner);
  ASSERT_EQ(db_->GetDBOptions().max_background_jobs, 7);
  ASSERT_TRUE(GetsWindows(tuner));
}

// The tuner set at open (here tuner_, as a service would wire one) stays when
// later values leave out `tuner`: only the mode and windows change.
TEST_F(AutoTuneSetDBOptionsTest, ValuesWithoutATunerKeepTheTunerSetAtOpen) {
  Options options = BaseOptions();
  options.auto_tune = TuneOptions(AutoTuneMode::kOff);
  Reopen(options);

  ASSERT_OK(
      db_->SetDBOptions({{"auto_tune", "{mode=kObserve;trace_period_sec=2}"}}));
  ASSERT_EQ(CurrentTuner(), tuner_);
  ASSERT_TRUE(GetsWindows(tuner_));

  ASSERT_OK(db_->SetDBOptions({{"auto_tune", "{mode=kOff}"}}));
  ASSERT_EQ(CurrentTuner(), tuner_);
  ASSERT_EQ(db_->GetDBOptions().auto_tune.mode, AutoTuneMode::kOff);
}

TEST_F(AutoTuneSetDBOptionsTest, ANewSettingRestartsWithANewTuner) {
  Reopen(BaseOptions());
  ASSERT_OK(db_->SetDBOptions({{"auto_tune", Setting("kObserve")}}));
  std::shared_ptr<FakeTuner> first = CurrentTuner();
  ASSERT_TRUE(GetsWindows(first));

  ASSERT_OK(db_->SetDBOptions(
      {{"auto_tune", Setting("kObserve", "write_buffer_size=1:4")}}));

  std::shared_ptr<FakeTuner> second = CurrentTuner();
  ASSERT_NE(second, first);
  ASSERT_EQ(db_->GetDBOptions().auto_tune.allowed_options,
            "write_buffer_size=1:4");
  ASSERT_TRUE(GetsWindows(second));
}

TEST_F(AutoTuneSetDBOptionsTest, AnotherTunerRestartsWithIt) {
  Reopen(BaseOptions());
  ASSERT_OK(db_->SetDBOptions({{"auto_tune", Setting("kObserve")}}));
  ASSERT_STREQ(CurrentTuner()->Name(), "FakeTuner");

  // Only the tuner's type changes.
  ASSERT_OK(
      db_->SetDBOptions({{"auto_tune", "{tuner={id=LabeledTuner;label=a}}"}}));
  std::shared_ptr<FakeTuner> labeled = CurrentTuner();
  ASSERT_STREQ(labeled->Name(), "LabeledTuner");
  ASSERT_TRUE(GetsWindows(labeled));

  // The same options keep the running object; other options replace it.
  ASSERT_OK(
      db_->SetDBOptions({{"auto_tune", "{tuner={id=LabeledTuner;label=a}}"}}));
  ASSERT_EQ(CurrentTuner(), labeled);
  ASSERT_OK(
      db_->SetDBOptions({{"auto_tune", "{tuner={id=LabeledTuner;label=b}}"}}));
  std::shared_ptr<FakeTuner> relabeled = CurrentTuner();
  ASSERT_NE(relabeled, labeled);
  ASSERT_EQ(std::static_pointer_cast<LabeledTuner>(relabeled)->label_, "b");
  ASSERT_TRUE(GetsWindows(relabeled));
}

// A new setting that keeps the tuner keeps the watch: r1 is not applied
// again, and the revert puts back the value r1 replaced.
TEST_F(AutoTuneSetDBOptionsTest, ANewSettingKeepsTheWatch) {
  Options options = BaseOptions();
  options.auto_tune = TuneOptions(AutoTuneMode::kApply);
  options.auto_tune.trace_period_sec = 2;
  tuner_->SetRecommendation(WriteBufferRecommendation("r1", "8388608", 11, 11));
  Reopen(options);
  ASSERT_TRUE(
      WaitFor([&] { return CountOutcomes("r1", TuningOutcome::kApplied); }));

  ASSERT_OK(db_->SetDBOptions({{"auto_tune", "{watch_windows=5}"}}));
  ASSERT_EQ(CurrentTuner(), tuner_);
  const size_t windows = tuner_->workload_profiles().size();
  ASSERT_TRUE(
      WaitFor([&] { return tuner_->workload_profiles().size() > windows; }));
  ASSERT_EQ(tuner_->workload_profiles().back().watched_recommendation_id, "r1");
  tuner_->SetVerdict(TuningWatchVerdict::kRevert);

  ASSERT_TRUE(
      WaitFor([&] { return CountOutcomes("r1", TuningOutcome::kReverted); }));
  ASSERT_EQ(WriteBufferSize(), 4u << 20);
  ASSERT_EQ(CountOutcomes("r1", TuningOutcome::kApplied), 1u);
}

TEST_F(AutoTuneSetDBOptionsTest, AnInvalidSettingIsRejected) {
  Reopen(BaseOptions());
  ASSERT_OK(db_->SetDBOptions({{"auto_tune", Setting("kObserve")}}));
  std::shared_ptr<FakeTuner> tuner = CurrentTuner();

  // The window is not shorter than the period.
  ASSERT_TRUE(db_->SetDBOptions({{"auto_tune",
                                  "{mode=kObserve;tuner={id=FakeTuner};"
                                  "trace_window_sec=2;trace_period_sec=2}"}})
                  .IsInvalidArgument());

  ASSERT_EQ(CurrentTuner(), tuner);
  ASSERT_EQ(db_->GetDBOptions().auto_tune.trace_period_sec, 2u);
  ASSERT_TRUE(GetsWindows(tuner));
}

TEST_F(AutoTuneSetDBOptionsTest, ASettingThatCannotStartIsRetried) {
  Reopen(BaseOptions());
  // Valid on its own, but the DB has no such option: logged, not started.
  ASSERT_OK(db_->SetDBOptions(
      {{"auto_tune", Setting("kObserve", "no_such_option=1:2")}}));
  std::shared_ptr<FakeTuner> unused = CurrentTuner();
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  ASSERT_TRUE(unused->workload_profiles().empty());

  ASSERT_OK(db_->SetDBOptions({{"auto_tune", Setting("kObserve")}}));

  ASSERT_TRUE(GetsWindows(CurrentTuner()));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
