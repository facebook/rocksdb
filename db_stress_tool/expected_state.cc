//  Copyright (c) 2021-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <iomanip>
#include <sstream>
#ifdef GFLAGS

#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "db_stress_tool/db_stress_common.h"
#include "db_stress_tool/db_stress_shared_state.h"
#include "db_stress_tool/expected_state.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record_result.h"

namespace ROCKSDB_NAMESPACE {
ExpectedState::ExpectedState(size_t max_key, size_t num_column_families)
    : max_key_(max_key),
      num_column_families_(num_column_families),
      values_(nullptr) {}

void ExpectedState::ClearColumnFamily(int cf) {
  const uint32_t del_mask = ExpectedValue::GetDelMask();
  std::fill(&Value(cf, 0 /* key */), &Value(cf + 1, 0 /* key */), del_mask);
}

void ExpectedState::Precommit(int cf, int64_t key, const ExpectedValue& value) {
  Value(cf, key).store(value.Read());
  // To prevent low-level instruction reordering that results
  // in db write happens before setting pending state in expected value
  std::atomic_thread_fence(std::memory_order_release);
}

PendingExpectedValue ExpectedState::PreparePut(int cf, int64_t key) {
  ExpectedValue expected_value = Load(cf, key);

  // Calculate the original expected value
  const ExpectedValue orig_expected_value = expected_value;

  // Calculate the pending expected value
  expected_value.Put(true /* pending */);
  const ExpectedValue pending_expected_value = expected_value;

  // Calculate the final expected value
  expected_value.Put(false /* pending */);
  const ExpectedValue final_expected_value = expected_value;

  // Precommit
  Precommit(cf, key, pending_expected_value);
  return PendingExpectedValue(&Value(cf, key), orig_expected_value,
                              final_expected_value);
}

ExpectedValue ExpectedState::Get(int cf, int64_t key) { return Load(cf, key); }

PendingExpectedValue ExpectedState::PrepareDelete(int cf, int64_t key) {
  ExpectedValue expected_value = Load(cf, key);

  // Calculate the original expected value
  const ExpectedValue orig_expected_value = expected_value;

  // Calculate the pending expected value
  bool res = expected_value.Delete(true /* pending */);
  if (!res) {
    PendingExpectedValue ret = PendingExpectedValue(
        &Value(cf, key), orig_expected_value, orig_expected_value);
    return ret;
  }
  const ExpectedValue pending_expected_value = expected_value;

  // Calculate the final expected value
  expected_value.Delete(false /* pending */);
  const ExpectedValue final_expected_value = expected_value;

  // Precommit
  Precommit(cf, key, pending_expected_value);
  return PendingExpectedValue(&Value(cf, key), orig_expected_value,
                              final_expected_value);
}

PendingExpectedValue ExpectedState::PrepareSingleDelete(int cf, int64_t key) {
  return PrepareDelete(cf, key);
}

std::vector<PendingExpectedValue> ExpectedState::PrepareDeleteRange(
    int cf, int64_t begin_key, int64_t end_key) {
  std::vector<PendingExpectedValue> pending_expected_values;

  for (int64_t key = begin_key; key < end_key; ++key) {
    pending_expected_values.push_back(PrepareDelete(cf, key));
  }

  return pending_expected_values;
}

bool ExpectedState::Exists(int cf, int64_t key) {
  return Load(cf, key).Exists();
}

void ExpectedState::Reset() {
  const uint32_t del_mask = ExpectedValue::GetDelMask();
  for (size_t i = 0; i < num_column_families_; ++i) {
    for (size_t j = 0; j < max_key_; ++j) {
      Value(static_cast<int>(i), j).store(del_mask, std::memory_order_relaxed);
    }
  }
}

void ExpectedState::SyncPut(int cf, int64_t key, uint32_t value_base) {
  ExpectedValue expected_value = Load(cf, key);
  expected_value.SyncPut(value_base);
  Value(cf, key).store(expected_value.Read());
}

void ExpectedState::SyncPendingPut(int cf, int64_t key) {
  ExpectedValue expected_value = Load(cf, key);
  expected_value.SyncPendingPut();
  Value(cf, key).store(expected_value.Read());
}

void ExpectedState::SyncDelete(int cf, int64_t key) {
  ExpectedValue expected_value = Load(cf, key);
  expected_value.SyncDelete();
  Value(cf, key).store(expected_value.Read());
}

void ExpectedState::SyncDeleteRange(int cf, int64_t begin_key,
                                    int64_t end_key) {
  for (int64_t key = begin_key; key < end_key; ++key) {
    SyncDelete(cf, key);
  }
}

FileExpectedState::FileExpectedState(
    const std::string& expected_state_file_path,
    const std::string& expected_persisted_seqno_file_path, size_t max_key,
    size_t num_column_families)
    : ExpectedState(max_key, num_column_families),
      expected_state_file_path_(expected_state_file_path),
      expected_persisted_seqno_file_path_(expected_persisted_seqno_file_path) {}

Status FileExpectedState::Open(bool create) {
  size_t expected_values_size = GetValuesLen();

  Env* default_env = Env::Default();

  Status status;
  if (create) {
    status = CreateFile(default_env, EnvOptions(), expected_state_file_path_,
                        std::string(expected_values_size, '\0'));
    if (!status.ok()) {
      return status;
    }

    status = CreateFile(default_env, EnvOptions(),
                        expected_persisted_seqno_file_path_,
                        std::string(sizeof(std::atomic<SequenceNumber>), '\0'));

    if (!status.ok()) {
      return status;
    }
  }

  status = MemoryMappedFile(default_env, expected_state_file_path_,
                            expected_state_mmap_buffer_, expected_values_size);
  if (!status.ok()) {
    assert(values_ == nullptr);
    return status;
  }

  values_ = static_cast<std::atomic<uint32_t>*>(
      expected_state_mmap_buffer_->GetBase());
  assert(values_ != nullptr);
  if (create) {
    Reset();
  }

  // TODO(hx235): Find a way to mmap persisted seqno and expected state into the
  // same LATEST file so we can obselete the logic to handle this extra file for
  // persisted seqno
  status = MemoryMappedFile(default_env, expected_persisted_seqno_file_path_,
                            expected_persisted_seqno_mmap_buffer_,
                            sizeof(std::atomic<SequenceNumber>));
  if (!status.ok()) {
    assert(persisted_seqno_ == nullptr);
    return status;
  }

  persisted_seqno_ = static_cast<std::atomic<SequenceNumber>*>(
      expected_persisted_seqno_mmap_buffer_->GetBase());
  assert(persisted_seqno_ != nullptr);
  if (create) {
    persisted_seqno_->store(0, std::memory_order_relaxed);
  }

  return status;
}

AnonExpectedState::AnonExpectedState(size_t max_key, size_t num_column_families)
    : ExpectedState(max_key, num_column_families) {}

#ifndef NDEBUG
Status AnonExpectedState::Open(bool create) {
#else
Status AnonExpectedState::Open(bool /* create */) {
#endif
  // AnonExpectedState only supports being freshly created.
  assert(create);
  values_allocation_.reset(
      new std::atomic<uint32_t>[GetValuesLen() /
                                sizeof(std::atomic<uint32_t>)]);
  values_ = &values_allocation_[0];
  persisted_seqno_allocation_.reset(new std::atomic<SequenceNumber>(0));
  persisted_seqno_ = persisted_seqno_allocation_.get();
  Reset();
  return Status::OK();
}

ExpectedStateManager::ExpectedStateManager(size_t max_key,
                                           size_t num_column_families)
    : max_key_(max_key),
      num_column_families_(num_column_families),
      latest_(nullptr) {}

ExpectedStateManager::~ExpectedStateManager() = default;

const std::string FileExpectedStateManager::kLatestBasename = "LATEST";
const std::string FileExpectedStateManager::kStateFilenameSuffix = ".state";
const std::string FileExpectedStateManager::kTraceFilenameSuffix = ".trace";
const std::string FileExpectedStateManager::kPersistedSeqnoBasename = "PERSIST";
const std::string FileExpectedStateManager::kPersistedSeqnoFilenameSuffix =
    ".seqno";
const std::string FileExpectedStateManager::kTempFilenamePrefix = ".";
const std::string FileExpectedStateManager::kTempFilenameSuffix = ".tmp";

FileExpectedStateManager::FileExpectedStateManager(
    size_t max_key, size_t num_column_families,
    std::string expected_state_dir_path)
    : ExpectedStateManager(max_key, num_column_families),
      expected_state_dir_path_(std::move(expected_state_dir_path)) {
  assert(!expected_state_dir_path_.empty());
}

Status FileExpectedStateManager::Open() {
  // Before doing anything, sync directory state with ours. That is, determine
  // `saved_seqno_`, and create any necessary missing files.
  std::vector<std::string> expected_state_dir_children;
  Status s = Env::Default()->GetChildren(expected_state_dir_path_,
                                         &expected_state_dir_children);
  bool found_trace = false;
  if (s.ok()) {
    for (size_t i = 0; i < expected_state_dir_children.size(); ++i) {
      const auto& filename = expected_state_dir_children[i];
      if (filename.size() >= kStateFilenameSuffix.size() &&
          filename.rfind(kStateFilenameSuffix) ==
              filename.size() - kStateFilenameSuffix.size() &&
          filename.rfind(kLatestBasename, 0) == std::string::npos) {
        SequenceNumber found_seqno = ParseUint64(
            filename.substr(0, filename.size() - kStateFilenameSuffix.size()));
        if (saved_seqno_ == kMaxSequenceNumber || found_seqno > saved_seqno_) {
          saved_seqno_ = found_seqno;
        }
      }
    }
    // Check if crash happened after creating state file but before creating
    // trace file.
    if (saved_seqno_ != kMaxSequenceNumber) {
      std::string saved_seqno_trace_path = GetPathForFilename(
          std::to_string(saved_seqno_) + kTraceFilenameSuffix);
      Status exists_status = Env::Default()->FileExists(saved_seqno_trace_path);
      if (exists_status.ok()) {
        found_trace = true;
      } else if (exists_status.IsNotFound()) {
        found_trace = false;
      } else {
        s = exists_status;
      }
    }
  }
  if (s.ok() && saved_seqno_ != kMaxSequenceNumber && !found_trace) {
    // Create an empty trace file so later logic does not need to distinguish
    // missing vs. empty trace file.
    std::unique_ptr<WritableFile> wfile;
    const EnvOptions soptions;
    std::string saved_seqno_trace_path =
        GetPathForFilename(std::to_string(saved_seqno_) + kTraceFilenameSuffix);
    s = Env::Default()->NewWritableFile(saved_seqno_trace_path, &wfile,
                                        soptions);
  }

  if (s.ok()) {
    s = Clean();
  }

  std::string expected_state_file_path =
      GetPathForFilename(kLatestBasename + kStateFilenameSuffix);
  std::string expected_persisted_seqno_file_path = GetPathForFilename(
      kPersistedSeqnoBasename + kPersistedSeqnoFilenameSuffix);
  bool found = false;
  if (s.ok()) {
    Status exists_status = Env::Default()->FileExists(expected_state_file_path);
    if (exists_status.ok()) {
      found = true;
    } else if (exists_status.IsNotFound()) {
      assert(Env::Default()
                 ->FileExists(expected_persisted_seqno_file_path)
                 .IsNotFound());
    } else {
      s = exists_status;
    }
  }

  if (!found) {
    // Initialize the file in a temp path and then rename it. That way, in case
    // this process is killed during setup, `Clean()` will take care of removing
    // the incomplete expected values file.
    std::string temp_expected_state_file_path =
        GetTempPathForFilename(kLatestBasename + kStateFilenameSuffix);
    std::string temp_expected_persisted_seqno_file_path =
        GetTempPathForFilename(kPersistedSeqnoBasename +
                               kPersistedSeqnoFilenameSuffix);
    FileExpectedState temp_expected_state(
        temp_expected_state_file_path, temp_expected_persisted_seqno_file_path,
        max_key_, num_column_families_);
    if (s.ok()) {
      s = temp_expected_state.Open(true /* create */);
    }
    if (s.ok()) {
      s = Env::Default()->RenameFile(temp_expected_state_file_path,
                                     expected_state_file_path);
    }
    if (s.ok()) {
      s = Env::Default()->RenameFile(temp_expected_persisted_seqno_file_path,
                                     expected_persisted_seqno_file_path);
    }
  }

  if (s.ok()) {
    latest_.reset(
        new FileExpectedState(std::move(expected_state_file_path),
                              std::move(expected_persisted_seqno_file_path),
                              max_key_, num_column_families_));
    s = latest_->Open(false /* create */);
  }
  return s;
}

Status FileExpectedStateManager::SaveAtAndAfter(DB* db) {
  SequenceNumber seqno = db->GetLatestSequenceNumber();

  std::string state_filename = std::to_string(seqno) + kStateFilenameSuffix;
  std::string state_file_temp_path = GetTempPathForFilename(state_filename);
  std::string state_file_path = GetPathForFilename(state_filename);

  std::string latest_file_path =
      GetPathForFilename(kLatestBasename + kStateFilenameSuffix);

  std::string trace_filename = std::to_string(seqno) + kTraceFilenameSuffix;
  std::string trace_file_path = GetPathForFilename(trace_filename);

  // Populate a tempfile and then rename it to atomically create "<seqno>.state"
  // with contents from "LATEST.state"
  Status s =
      CopyFile(FileSystem::Default(), latest_file_path, Temperature::kUnknown,
               state_file_temp_path, Temperature::kUnknown, 0 /* size */,
               false /* use_fsync */, nullptr /* io_tracer */);
  if (s.ok()) {
    s = FileSystem::Default()->RenameFile(state_file_temp_path, state_file_path,
                                          IOOptions(), nullptr /* dbg */);
  }
  SequenceNumber old_saved_seqno = 0;
  if (s.ok()) {
    old_saved_seqno = saved_seqno_;
    saved_seqno_ = seqno;
  }

  // If there is a crash now, i.e., after "<seqno>.state" was created but before
  // "<seqno>.trace" is created, it will be treated as if "<seqno>.trace" were
  // present but empty.

  // Create "<seqno>.trace" directly. It is initially empty so no need for
  // tempfile.
  std::unique_ptr<TraceWriter> trace_writer;
  if (s.ok()) {
    EnvOptions soptions;
    // Disable buffering so traces will not get stuck in application buffer.
    soptions.writable_file_max_buffer_size = 0;
    s = NewFileTraceWriter(Env::Default(), soptions, trace_file_path,
                           &trace_writer);
  }
  if (s.ok()) {
    TraceOptions trace_opts;
    trace_opts.filter |= kTraceFilterGet;
    trace_opts.filter |= kTraceFilterMultiGet;
    trace_opts.filter |= kTraceFilterIteratorSeek;
    trace_opts.filter |= kTraceFilterIteratorSeekForPrev;
    // Expected-state restore replays by recovered DB sequence count rather than
    // by trace-side commit acknowledgement. This trace therefore needs to be an
    // ordered superset of writes that could survive recovery: missing trace
    // entries are fatal, while extra suffix entries are tolerated.
    trace_opts.preserve_write_order = true;
    s = db->StartTrace(trace_opts, std::move(trace_writer));
  }

  // Delete old state/trace files. Deletion order does not matter since we only
  // delete after successfully saving new files, so old files will never be used
  // again, even if we crash.
  if (s.ok() && old_saved_seqno != kMaxSequenceNumber &&
      old_saved_seqno != saved_seqno_) {
    s = Env::Default()->DeleteFile(GetPathForFilename(
        std::to_string(old_saved_seqno) + kStateFilenameSuffix));
  }
  if (s.ok() && old_saved_seqno != kMaxSequenceNumber &&
      old_saved_seqno != saved_seqno_) {
    s = Env::Default()->DeleteFile(GetPathForFilename(
        std::to_string(old_saved_seqno) + kTraceFilenameSuffix));
  }
  return s;
}

bool FileExpectedStateManager::HasHistory() {
  return saved_seqno_ != kMaxSequenceNumber;
}

namespace {

std::string DescribeExpectedValue(const ExpectedValue& value) {
  std::ostringstream oss;
  oss << "{raw=0x" << std::hex << std::setw(8) << std::setfill('0')
      << value.Read() << std::dec << std::setfill(' ')
      << " value_base=" << value.GetValueBase()
      << " next_value_base=" << value.NextValueBase()
      << " del_counter=" << value.GetDelCounter()
      << " pending_write=" << value.PendingWrite()
      << " pending_delete=" << value.PendingDelete()
      << " deleted=" << value.IsDeleted() << "}";
  return oss.str();
}

size_t CountTrailingXs(const std::string& key) {
  size_t trailing_xs = 0;
  while (trailing_xs < key.size() && key[key.size() - trailing_xs - 1] == 'x') {
    ++trailing_xs;
  }
  return trailing_xs;
}

struct TraceKeyDebugInfo {
  std::string raw_key;
  std::string raw_key_hex;
  bool parse_ok = false;
  uint64_t parsed_key_id = 0;
  std::string roundtrip_key;
  std::string roundtrip_key_hex;
  bool roundtrip_matches_raw = false;
  bool raw_matches_focus_key = false;
  bool parsed_matches_focus_key = false;
  bool roundtrip_matches_focus_key = false;
  size_t trailing_bytes = 0;
  size_t trailing_xs = 0;

  bool MatchesFocusKey() const {
    return raw_matches_focus_key || parsed_matches_focus_key ||
           roundtrip_matches_focus_key;
  }
};

std::string DescribeTraceKeyDebugInfo(const TraceKeyDebugInfo& info) {
  std::ostringstream oss;
  oss << "{raw_key=" << info.raw_key_hex << " size=" << info.raw_key.size()
      << " trailing_bytes=" << info.trailing_bytes
      << " trailing_xs=" << info.trailing_xs << " parse_ok=" << info.parse_ok;
  if (info.parse_ok) {
    oss << " parsed_key=" << info.parsed_key_id
        << " roundtrip_key=" << info.roundtrip_key_hex
        << " roundtrip_matches_raw=" << info.roundtrip_matches_raw;
  }
  if (FLAGS_expected_state_trace_debug_key >= 0) {
    oss << " focus_key=" << FLAGS_expected_state_trace_debug_key
        << " raw_matches_focus_key=" << info.raw_matches_focus_key
        << " parsed_matches_focus_key=" << info.parsed_matches_focus_key
        << " roundtrip_matches_focus_key=" << info.roundtrip_matches_focus_key;
  }
  oss << "}";
  return oss.str();
}

// An `ExpectedStateTraceRecordHandler` applies a configurable number of traced
// write operations to the configured expected state. It is used in
// `FileExpectedStateManager::Restore()` to sync the expected state with the
// DB's post-recovery state.
class ExpectedStateTraceRecordHandler : public TraceRecord::Handler,
                                        public WriteBatch::Handler {
 public:
  ExpectedStateTraceRecordHandler(uint64_t max_write_ops, ExpectedState* state)
      : max_write_ops_(max_write_ops),
        state_(state),
        debug_enabled_(FLAGS_expected_state_trace_debug),
        debug_focus_key_(FLAGS_expected_state_trace_debug_key),
        debug_focus_key_raw_(debug_focus_key_ >= 0 ? Key(debug_focus_key_)
                                                   : std::string()),
        debug_max_logs_(static_cast<uint64_t>(
            std::max(0, FLAGS_expected_state_trace_debug_max_logs))),
        buffered_writes_(nullptr) {}

  // True if we have already reached the limit on write operations to apply.
  bool IsDone() const { return num_write_ops_ >= max_write_ops_; }

  uint64_t NumWriteOps() const { return num_write_ops_; }
  uint64_t NumKeyDecodeFailures() const { return key_decode_failures_; }
  uint64_t NumKeyRoundtripMismatches() const {
    return key_roundtrip_mismatches_;
  }
  uint64_t NumFocusKeyOpHits() const { return focus_key_op_hits_; }
  uint64_t NumLogsEmitted() const { return emitted_debug_logs_; }
  uint64_t NumLogsSuppressed() const { return suppressed_debug_logs_; }

  bool Continue() override { return !IsDone(); }

  Status Handle(const WriteQueryTraceRecord& record,
                std::unique_ptr<TraceRecordResult>* /* result */) override {
    if (IsDone()) {
      return Status::OK();
    }
    WriteBatch batch(record.GetWriteBatchRep().ToString());
    return batch.Iterate(this);
  }

  // Ignore reads.
  Status Handle(const GetQueryTraceRecord& /* record */,
                std::unique_ptr<TraceRecordResult>* /* result */) override {
    return Status::OK();
  }

  // Ignore reads.
  Status Handle(const IteratorSeekQueryTraceRecord& /* record */,
                std::unique_ptr<TraceRecordResult>* /* result */) override {
    return Status::OK();
  }

  // Ignore reads.
  Status Handle(const MultiGetQueryTraceRecord& /* record */,
                std::unique_ptr<TraceRecordResult>* /* result */) override {
    return Status::OK();
  }

  // Below are the WriteBatch::Handler overrides. We could use a separate
  // object, but it's convenient and works to share state with the
  // `TraceRecord::Handler`.

  Status PutCF(uint32_t column_family_id, const Slice& key_with_ts,
               const Slice& value) override {
    Slice key =
        StripTimestampFromUserKey(key_with_ts, FLAGS_user_timestamp_size);
    uint64_t key_id = 0;
    TraceKeyDebugInfo key_info;
    Status status =
        ParseTracedKey(key, "unable to parse key", &key_id, &key_info);
    if (status.ok()) {
      const int64_t expected_key_id = static_cast<int64_t>(key_id);
      const uint32_t value_base = GetValueBase(value);

      bool should_buffer_write = !(buffered_writes_ == nullptr);
      if (should_buffer_write) {
        MaybeLogKeyOperation("PutCF", column_family_id, true /* buffered */,
                             key_info,
                             "value_base=" + std::to_string(value_base) +
                                 " value_size=" + std::to_string(value.size()));
        return WriteBatchInternal::Put(buffered_writes_.get(), column_family_id,
                                       key, value);
      }

      const ExpectedValue before =
          state_->Get(column_family_id, expected_key_id);
      state_->SyncPut(column_family_id, expected_key_id, value_base);
      const ExpectedValue after =
          state_->Get(column_family_id, expected_key_id);
      NoteWriteOpApplied();
      MaybeLogKeyOperation("PutCF", column_family_id, false /* buffered */,
                           key_info,
                           "value_base=" + std::to_string(value_base) +
                               " value_size=" + std::to_string(value.size()),
                           &before, &after);
      status = Status::OK();
    }
    return status;
  }

  Status TimedPutCF(uint32_t column_family_id, const Slice& key_with_ts,
                    const Slice& value, uint64_t write_unix_time) override {
    Slice key =
        StripTimestampFromUserKey(key_with_ts, FLAGS_user_timestamp_size);
    uint64_t key_id = 0;
    TraceKeyDebugInfo key_info;
    Status status =
        ParseTracedKey(key, "unable to parse key", &key_id, &key_info);
    if (status.ok()) {
      const int64_t expected_key_id = static_cast<int64_t>(key_id);
      const uint32_t value_base = GetValueBase(value);

      bool should_buffer_write = !(buffered_writes_ == nullptr);
      if (should_buffer_write) {
        MaybeLogKeyOperation(
            "TimedPutCF", column_family_id, true /* buffered */, key_info,
            "value_base=" + std::to_string(value_base) +
                " value_size=" + std::to_string(value.size()) +
                " write_unix_time=" + std::to_string(write_unix_time));
        return WriteBatchInternal::TimedPut(buffered_writes_.get(),
                                            column_family_id, key, value,
                                            write_unix_time);
      }

      const ExpectedValue before =
          state_->Get(column_family_id, expected_key_id);
      state_->SyncPut(column_family_id, expected_key_id, value_base);
      const ExpectedValue after =
          state_->Get(column_family_id, expected_key_id);
      NoteWriteOpApplied();
      MaybeLogKeyOperation(
          "TimedPutCF", column_family_id, false /* buffered */, key_info,
          "value_base=" + std::to_string(value_base) +
              " value_size=" + std::to_string(value.size()) +
              " write_unix_time=" + std::to_string(write_unix_time),
          &before, &after);
      status = Status::OK();
    }
    return status;
  }

  Status PutEntityCF(uint32_t column_family_id, const Slice& key_with_ts,
                     const Slice& entity) override {
    Slice key =
        StripTimestampFromUserKey(key_with_ts, FLAGS_user_timestamp_size);

    uint64_t key_id = 0;
    TraceKeyDebugInfo key_info;
    Status status =
        ParseTracedKey(key, "Unable to parse key", &key_id, &key_info);
    if (status.ok()) {
      const int64_t expected_key_id = static_cast<int64_t>(key_id);

      Slice entity_copy = entity;
      WideColumns columns;
      if (!WideColumnSerialization::Deserialize(entity_copy, columns).ok()) {
        return Status::Corruption("Unable to deserialize entity",
                                  entity.ToString(/* hex */ true));
      }

      if (!VerifyWideColumns(columns)) {
        return Status::Corruption("Wide columns in entity inconsistent",
                                  entity.ToString(/* hex */ true));
      }

      if (buffered_writes_) {
        MaybeLogKeyOperation(
            "PutEntityCF", column_family_id, true /* buffered */, key_info,
            "entity_size=" + std::to_string(entity.size()) +
                " num_columns=" + std::to_string(columns.size()));
        return WriteBatchInternal::PutEntity(buffered_writes_.get(),
                                             column_family_id, key, columns);
      }

      const uint32_t value_base =
          GetValueBase(WideColumnsHelper::GetDefaultColumn(columns));

      const ExpectedValue before =
          state_->Get(column_family_id, expected_key_id);
      state_->SyncPut(column_family_id, expected_key_id, value_base);
      const ExpectedValue after =
          state_->Get(column_family_id, expected_key_id);
      NoteWriteOpApplied();
      MaybeLogKeyOperation(
          "PutEntityCF", column_family_id, false /* buffered */, key_info,
          "entity_size=" + std::to_string(entity.size()) +
              " num_columns=" + std::to_string(columns.size()) +
              " default_value_base=" + std::to_string(value_base),
          &before, &after);

      status = Status::OK();
    }
    return status;
  }

  Status DeleteCF(uint32_t column_family_id,
                  const Slice& key_with_ts) override {
    Slice key =
        StripTimestampFromUserKey(key_with_ts, FLAGS_user_timestamp_size);
    uint64_t key_id = 0;
    TraceKeyDebugInfo key_info;
    Status status =
        ParseTracedKey(key, "unable to parse key", &key_id, &key_info);
    if (status.ok()) {
      const int64_t expected_key_id = static_cast<int64_t>(key_id);

      bool should_buffer_write = !(buffered_writes_ == nullptr);
      if (should_buffer_write) {
        MaybeLogKeyOperation("DeleteCF", column_family_id, true /* buffered */,
                             key_info, "");
        return WriteBatchInternal::Delete(buffered_writes_.get(),
                                          column_family_id, key);
      }

      const ExpectedValue before =
          state_->Get(column_family_id, expected_key_id);
      state_->SyncDelete(column_family_id, expected_key_id);
      const ExpectedValue after =
          state_->Get(column_family_id, expected_key_id);
      NoteWriteOpApplied();
      MaybeLogKeyOperation("DeleteCF", column_family_id, false /* buffered */,
                           key_info, "", &before, &after);
      status = Status::OK();
    }
    return status;
  }

  Status SingleDeleteCF(uint32_t column_family_id,
                        const Slice& key_with_ts) override {
    bool should_buffer_write = !(buffered_writes_ == nullptr);
    if (should_buffer_write) {
      Slice key =
          StripTimestampFromUserKey(key_with_ts, FLAGS_user_timestamp_size);
      Slice ts =
          ExtractTimestampFromUserKey(key_with_ts, FLAGS_user_timestamp_size);
      std::array<Slice, 2> key_with_ts_arr{{key, ts}};
      return WriteBatchInternal::SingleDelete(
          buffered_writes_.get(), column_family_id,
          SliceParts(key_with_ts_arr.data(), 2));
    }

    return DeleteCF(column_family_id, key_with_ts);
  }

  Status DeleteRangeCF(uint32_t column_family_id,
                       const Slice& begin_key_with_ts,
                       const Slice& end_key_with_ts) override {
    Slice begin_key =
        StripTimestampFromUserKey(begin_key_with_ts, FLAGS_user_timestamp_size);
    Slice end_key =
        StripTimestampFromUserKey(end_key_with_ts, FLAGS_user_timestamp_size);
    uint64_t begin_key_id = 0;
    uint64_t end_key_id = 0;
    TraceKeyDebugInfo begin_info;
    TraceKeyDebugInfo end_info;
    Status status = ParseTracedKey(begin_key, "unable to parse begin key",
                                   &begin_key_id, &begin_info);
    if (status.ok()) {
      status = ParseTracedKey(end_key, "unable to parse end key", &end_key_id,
                              &end_info);
    }
    if (status.ok()) {
      bool should_buffer_write = !(buffered_writes_ == nullptr);
      if (should_buffer_write) {
        const uint64_t affected_keys =
            end_key_id > begin_key_id ? end_key_id - begin_key_id : 0;
        MaybeLogRangeOperation(
            "DeleteRangeCF", column_family_id, true /* buffered */, begin_info,
            end_info,
            "affected_keys=" + std::to_string(affected_keys) +
                " inverted_range=" +
                std::to_string(end_key_id < begin_key_id ? 1 : 0));
        return WriteBatchInternal::DeleteRange(
            buffered_writes_.get(), column_family_id, begin_key, end_key);
      }

      const bool focus_in_range = FocusKeyInRange(begin_key_id, end_key_id);
      const uint64_t affected_keys =
          end_key_id > begin_key_id ? end_key_id - begin_key_id : 0;
      ExpectedValue focus_before;
      ExpectedValue focus_after;
      if (focus_in_range) {
        focus_before = state_->Get(column_family_id, debug_focus_key_);
      }
      state_->SyncDeleteRange(column_family_id,
                              static_cast<int64_t>(begin_key_id),
                              static_cast<int64_t>(end_key_id));
      if (focus_in_range) {
        focus_after = state_->Get(column_family_id, debug_focus_key_);
      }
      NoteWriteOpApplied();
      MaybeLogRangeOperation(
          "DeleteRangeCF", column_family_id, false /* buffered */, begin_info,
          end_info,
          "affected_keys=" + std::to_string(affected_keys) +
              " inverted_range=" +
              std::to_string(end_key_id < begin_key_id ? 1 : 0) +
              " focus_in_range=" + std::to_string(focus_in_range ? 1 : 0),
          focus_in_range ? &focus_before : nullptr,
          focus_in_range ? &focus_after : nullptr);
      status = Status::OK();
    }
    return status;
  }

  Status MergeCF(uint32_t column_family_id, const Slice& key_with_ts,
                 const Slice& value) override {
    Slice key =
        StripTimestampFromUserKey(key_with_ts, FLAGS_user_timestamp_size);

    bool should_buffer_write = !(buffered_writes_ == nullptr);
    if (should_buffer_write) {
      return WriteBatchInternal::Merge(buffered_writes_.get(), column_family_id,
                                       key, value);
    }

    return PutCF(column_family_id, key, value);
  }

  Status PutBlobIndexCF(uint32_t column_family_id, const Slice& key_with_ts,
                        const Slice& value) override {
    Slice key =
        StripTimestampFromUserKey(key_with_ts, FLAGS_user_timestamp_size);
    uint64_t key_id = 0;
    TraceKeyDebugInfo key_info;
    Status status =
        ParseTracedKey(key, "unable to parse key", &key_id, &key_info);
    if (status.ok()) {
      const int64_t expected_key_id = static_cast<int64_t>(key_id);

      bool should_buffer_write = !(buffered_writes_ == nullptr);
      if (should_buffer_write) {
        MaybeLogKeyOperation("PutBlobIndexCF", column_family_id,
                             true /* buffered */, key_info,
                             "blob_index_size=" + std::to_string(value.size()));
        return WriteBatchInternal::PutBlobIndex(buffered_writes_.get(),
                                                column_family_id, key, value);
      }

      // Blob direct-write traces record the transformed BlobIndex write rather
      // than the original value bytes. For expected-state replay we only need
      // the logical effect of "another put to this key", and db_stress values
      // advance deterministically by one value_base per committed write.
      const ExpectedValue before =
          state_->Get(column_family_id, expected_key_id);
      const uint32_t value_base = before.NextValueBase();
      state_->SyncPut(column_family_id, expected_key_id, value_base);
      const ExpectedValue after =
          state_->Get(column_family_id, expected_key_id);
      NoteWriteOpApplied();
      MaybeLogKeyOperation(
          "PutBlobIndexCF", column_family_id, false /* buffered */, key_info,
          "blob_index_size=" + std::to_string(value.size()) +
              " derived_value_base=" + std::to_string(value_base),
          &before, &after);
      status = Status::OK();
    }
    return status;
  }

  Status MarkBeginPrepare(bool = false) override {
    assert(!buffered_writes_);
    buffered_writes_.reset(new WriteBatch());
    return Status::OK();
  }

  Status MarkEndPrepare(const Slice& xid) override {
    assert(buffered_writes_);
    std::string xid_str = xid.ToString();
    assert(xid_to_buffered_writes_.find(xid_str) ==
           xid_to_buffered_writes_.end());

    xid_to_buffered_writes_[xid_str].swap(buffered_writes_);

    buffered_writes_.reset();

    return Status::OK();
  }

  Status MarkCommit(const Slice& xid) override {
    std::string xid_str = xid.ToString();
    assert(xid_to_buffered_writes_.find(xid_str) !=
           xid_to_buffered_writes_.end());
    assert(xid_to_buffered_writes_.at(xid_str));

    Status s = xid_to_buffered_writes_.at(xid_str)->Iterate(this);
    xid_to_buffered_writes_.erase(xid_str);

    return s;
  }

  Status MarkRollback(const Slice& xid) override {
    std::string xid_str = xid.ToString();
    assert(xid_to_buffered_writes_.find(xid_str) !=
           xid_to_buffered_writes_.end());
    assert(xid_to_buffered_writes_.at(xid_str));
    xid_to_buffered_writes_.erase(xid_str);

    return Status::OK();
  }

 private:
  bool HasFocusKey() const { return debug_focus_key_ >= 0; }

  bool FocusKeyInRange(uint64_t begin_key_id, uint64_t end_key_id) const {
    return HasFocusKey() &&
           begin_key_id <= static_cast<uint64_t>(debug_focus_key_) &&
           static_cast<uint64_t>(debug_focus_key_) < end_key_id;
  }

  void MaybeNoteFocusKeyHit(bool hit) {
    if (hit) {
      ++focus_key_op_hits_;
    }
  }

  void MaybeEmitDebugLog(const std::string& line) {
    if (!debug_enabled_) {
      return;
    }
    if (emitted_debug_logs_ >= debug_max_logs_) {
      ++suppressed_debug_logs_;
      return;
    }
    ++emitted_debug_logs_;
    fprintf(stdout, "[expected_state_trace_debug] %s\n", line.c_str());
    fflush(stdout);
  }

  TraceKeyDebugInfo BuildTraceKeyDebugInfo(const std::string& raw_key,
                                           bool parse_ok,
                                           uint64_t parsed_key_id) {
    TraceKeyDebugInfo info;
    if (!debug_enabled_) {
      return info;
    }

    info.raw_key = raw_key;
    info.raw_key_hex = Slice(raw_key).ToString(/* hex */ true);
    info.parse_ok = parse_ok;
    info.trailing_bytes = raw_key.size() % sizeof(uint64_t);
    info.trailing_xs = CountTrailingXs(raw_key);
    info.raw_matches_focus_key =
        HasFocusKey() && raw_key == debug_focus_key_raw_;

    if (!parse_ok) {
      ++key_decode_failures_;
      return info;
    }

    info.parsed_key_id = parsed_key_id;
    info.roundtrip_key = Key(static_cast<int64_t>(parsed_key_id));
    info.roundtrip_key_hex = Slice(info.roundtrip_key).ToString(/* hex */ true);
    info.roundtrip_matches_raw = raw_key == info.roundtrip_key;
    info.parsed_matches_focus_key =
        HasFocusKey() &&
        parsed_key_id == static_cast<uint64_t>(debug_focus_key_);
    info.roundtrip_matches_focus_key =
        HasFocusKey() && info.roundtrip_key == debug_focus_key_raw_;

    if (!info.roundtrip_matches_raw) {
      ++key_roundtrip_mismatches_;
    }

    return info;
  }

  Status ParseTracedKey(const Slice& key, const char* error_msg,
                        uint64_t* key_id, TraceKeyDebugInfo* debug_info) {
    const std::string raw_key = key.ToString();
    const bool parse_ok = GetIntVal(raw_key, key_id);
    if (debug_enabled_) {
      *debug_info =
          BuildTraceKeyDebugInfo(raw_key, parse_ok, parse_ok ? *key_id : 0);
      if (!parse_ok && (!HasFocusKey() || debug_info->MatchesFocusKey())) {
        std::ostringstream oss;
        oss << "parse_failure error=\"" << error_msg << "\" "
            << DescribeTraceKeyDebugInfo(*debug_info);
        MaybeEmitDebugLog(oss.str());
      }
    }
    if (!parse_ok) {
      return Status::Corruption(error_msg, raw_key);
    }
    return Status::OK();
  }

  bool ShouldLogKeyOperation(const TraceKeyDebugInfo& info) {
    if (!debug_enabled_) {
      return false;
    }
    const bool focus_hit = info.MatchesFocusKey();
    MaybeNoteFocusKeyHit(focus_hit);
    return !HasFocusKey() || focus_hit;
  }

  bool ShouldLogRangeOperation(const TraceKeyDebugInfo& begin_info,
                               const TraceKeyDebugInfo& end_info) {
    if (!debug_enabled_) {
      return false;
    }
    const bool focus_hit =
        begin_info.MatchesFocusKey() || end_info.MatchesFocusKey() ||
        (begin_info.parse_ok && end_info.parse_ok &&
         FocusKeyInRange(begin_info.parsed_key_id, end_info.parsed_key_id));
    MaybeNoteFocusKeyHit(focus_hit);
    return !HasFocusKey() || focus_hit;
  }

  void MaybeLogKeyOperation(const char* op, uint32_t column_family_id,
                            bool buffered, const TraceKeyDebugInfo& key_info,
                            const std::string& details,
                            const ExpectedValue* before = nullptr,
                            const ExpectedValue* after = nullptr) {
    if (!ShouldLogKeyOperation(key_info)) {
      return;
    }
    std::ostringstream oss;
    oss << op << " cf=" << column_family_id << " buffered=" << buffered << " "
        << DescribeTraceKeyDebugInfo(key_info);
    if (!details.empty()) {
      oss << " " << details;
    }
    if (before != nullptr) {
      oss << " before=" << DescribeExpectedValue(*before);
    }
    if (after != nullptr) {
      oss << " after=" << DescribeExpectedValue(*after);
    }
    MaybeEmitDebugLog(oss.str());
  }

  void MaybeLogRangeOperation(const char* op, uint32_t column_family_id,
                              bool buffered,
                              const TraceKeyDebugInfo& begin_info,
                              const TraceKeyDebugInfo& end_info,
                              const std::string& details,
                              const ExpectedValue* focus_before = nullptr,
                              const ExpectedValue* focus_after = nullptr) {
    if (!ShouldLogRangeOperation(begin_info, end_info)) {
      return;
    }
    std::ostringstream oss;
    oss << op << " cf=" << column_family_id << " buffered=" << buffered
        << " begin=" << DescribeTraceKeyDebugInfo(begin_info)
        << " end=" << DescribeTraceKeyDebugInfo(end_info);
    if (!details.empty()) {
      oss << " " << details;
    }
    if (focus_before != nullptr) {
      oss << " focus_before=" << DescribeExpectedValue(*focus_before);
    }
    if (focus_after != nullptr) {
      oss << " focus_after=" << DescribeExpectedValue(*focus_after);
    }
    MaybeEmitDebugLog(oss.str());
  }

  void NoteWriteOpApplied() {
    ++num_write_ops_;
    assert(num_write_ops_ <= max_write_ops_);
  }

  uint64_t num_write_ops_ = 0;
  uint64_t max_write_ops_;
  ExpectedState* state_;
  bool debug_enabled_;
  int64_t debug_focus_key_;
  std::string debug_focus_key_raw_;
  uint64_t debug_max_logs_;
  uint64_t key_decode_failures_ = 0;
  uint64_t key_roundtrip_mismatches_ = 0;
  uint64_t focus_key_op_hits_ = 0;
  uint64_t emitted_debug_logs_ = 0;
  uint64_t suppressed_debug_logs_ = 0;
  std::unordered_map<std::string, std::unique_ptr<WriteBatch>>
      xid_to_buffered_writes_;
  std::unique_ptr<WriteBatch> buffered_writes_;
};

}  // anonymous namespace

Status FileExpectedStateManager::Restore(DB* db) {
  assert(HasHistory());
  SequenceNumber seqno = db->GetLatestSequenceNumber();
  if (seqno < saved_seqno_) {
    return Status::Corruption("DB is older than any restorable expected state");
  }
  const bool trace_debug = FLAGS_expected_state_trace_debug;
  const uint64_t replay_write_ops = seqno - saved_seqno_;

  std::string state_filename =
      std::to_string(saved_seqno_) + kStateFilenameSuffix;
  std::string state_file_path = GetPathForFilename(state_filename);

  std::string latest_file_temp_path =
      GetTempPathForFilename(kLatestBasename + kStateFilenameSuffix);
  std::string latest_file_path =
      GetPathForFilename(kLatestBasename + kStateFilenameSuffix);

  std::string trace_filename =
      std::to_string(saved_seqno_) + kTraceFilenameSuffix;
  std::string trace_file_path = GetPathForFilename(trace_filename);

  if (trace_debug) {
    std::string focus_key_hex = "<unset>";
    if (FLAGS_expected_state_trace_debug_key >= 0) {
      focus_key_hex = Slice(Key(FLAGS_expected_state_trace_debug_key))
                          .ToString(/* hex */ true);
    }
    fprintf(stdout,
            "[expected_state_trace_debug] restore_begin saved_seqno=%" PRIu64
            " db_seqno=%" PRIu64 " replay_write_ops=%" PRIu64
            " state_path=%s trace_path=%s focus_key=%" PRIi64
            " focus_key_hex=%s max_logs=%d\n",
            static_cast<uint64_t>(saved_seqno_), static_cast<uint64_t>(seqno),
            replay_write_ops, state_file_path.c_str(), trace_file_path.c_str(),
            FLAGS_expected_state_trace_debug_key, focus_key_hex.c_str(),
            FLAGS_expected_state_trace_debug_max_logs);
    fflush(stdout);
  }

  std::unique_ptr<TraceReader> trace_reader;
  Status s = NewFileTraceReader(Env::Default(), EnvOptions(), trace_file_path,
                                &trace_reader);

  std::string persisted_seqno_file_path = GetPathForFilename(
      kPersistedSeqnoBasename + kPersistedSeqnoFilenameSuffix);

  uint64_t replayed_write_ops = 0;
  uint64_t key_decode_failures = 0;
  uint64_t key_roundtrip_mismatches = 0;
  uint64_t focus_key_op_hits = 0;
  uint64_t logs_emitted = 0;
  uint64_t logs_suppressed = 0;

  if (s.ok()) {
    // We are going to replay on top of "`seqno`.state" to create a new
    // "LATEST.state". Start off by creating a tempfile so we can later make the
    // new "LATEST.state" appear atomically using `RenameFile()`.
    s = CopyFile(FileSystem::Default(), state_file_path, Temperature::kUnknown,
                 latest_file_temp_path, Temperature::kUnknown, 0 /* size */,
                 false /* use_fsync */, nullptr /* io_tracer */);
  }

  {
    std::unique_ptr<Replayer> replayer;
    std::unique_ptr<ExpectedState> state;
    std::unique_ptr<ExpectedStateTraceRecordHandler> handler;
    if (s.ok()) {
      state.reset(new FileExpectedState(latest_file_temp_path,
                                        persisted_seqno_file_path, max_key_,
                                        num_column_families_));
      s = state->Open(false /* create */);
    }
    if (s.ok()) {
      handler.reset(new ExpectedStateTraceRecordHandler(seqno - saved_seqno_,
                                                        state.get()));
      // TODO(ajkr): An API limitation requires we provide `handles` although
      // they will be unused since we only use the replayer for reading records.
      // Just give a default CFH for now to satisfy the requirement.
      s = db->NewDefaultReplayer({db->DefaultColumnFamily()} /* handles */,
                                 std::move(trace_reader), &replayer);
    }

    if (s.ok()) {
      s = replayer->Prepare();
    }
    for (; s.ok();) {
      std::unique_ptr<TraceRecord> record;
      s = replayer->Next(&record);
      if (!s.ok()) {
        if (trace_debug) {
          fprintf(stdout,
                  "[expected_state_trace_debug] restore_replay_next status=%s "
                  "handler_done=%d\n",
                  s.ToString().c_str(),
                  handler != nullptr && handler->IsDone());
          fflush(stdout);
        }
        if (s.IsCorruption() && handler->IsDone()) {
          // There could be a corruption reading the tail record of the trace
          // due to `db_stress` crashing while writing it. It shouldn't matter
          // as long as we already found all the write ops we need to catch up
          // the expected state.
          s = Status::OK();
        }
        if (s.IsIncomplete()) {
          // OK because `Status::Incomplete` is expected upon finishing all the
          // trace records.
          s = Status::OK();
        }
        break;
      }
      std::unique_ptr<TraceRecordResult> res;
      s = record->Accept(handler.get(), &res);
    }
    if (s.ok() && !handler->IsDone()) {
      s = Status::Corruption(
          "Trace ended before replaying all expected write ops",
          std::to_string(handler->NumWriteOps()) + " < " +
              std::to_string(seqno - saved_seqno_));
    }
    if (handler) {
      replayed_write_ops = handler->NumWriteOps();
      key_decode_failures = handler->NumKeyDecodeFailures();
      key_roundtrip_mismatches = handler->NumKeyRoundtripMismatches();
      focus_key_op_hits = handler->NumFocusKeyOpHits();
      logs_emitted = handler->NumLogsEmitted();
      logs_suppressed = handler->NumLogsSuppressed();
    }
  }

  if (trace_debug) {
    fprintf(stdout,
            "[expected_state_trace_debug] restore_replay_summary status=%s "
            "replayed_write_ops=%" PRIu64 "/%" PRIu64
            " key_decode_failures=%" PRIu64 " key_roundtrip_mismatches=%" PRIu64
            " focus_key_op_hits=%" PRIu64 " logs_emitted=%" PRIu64
            " logs_suppressed=%" PRIu64 "\n",
            s.ToString().c_str(), replayed_write_ops, replay_write_ops,
            key_decode_failures, key_roundtrip_mismatches, focus_key_op_hits,
            logs_emitted, logs_suppressed);
    fflush(stdout);
  }

  if (s.ok()) {
    s = FileSystem::Default()->RenameFile(latest_file_temp_path,
                                          latest_file_path, IOOptions(),
                                          nullptr /* dbg */);
  }
  if (s.ok()) {
    latest_.reset(new FileExpectedState(latest_file_path,
                                        persisted_seqno_file_path, max_key_,
                                        num_column_families_));
    s = latest_->Open(false /* create */);
  }

  // Delete old state/trace files. We must delete the state file first.
  // Otherwise, a crash-recovery immediately after deleting the trace file could
  // lead to `Restore()` unable to replay to `seqno`.
  if (s.ok()) {
    s = Env::Default()->DeleteFile(state_file_path);
  }
  if (s.ok()) {
    std::vector<std::string> expected_state_dir_children;
    s = Env::Default()->GetChildren(expected_state_dir_path_,
                                    &expected_state_dir_children);
    if (s.ok()) {
      for (size_t i = 0; i < expected_state_dir_children.size(); ++i) {
        const auto& filename = expected_state_dir_children[i];
        if (filename.size() >= kTraceFilenameSuffix.size() &&
            filename.rfind(kTraceFilenameSuffix) ==
                filename.size() - kTraceFilenameSuffix.size()) {
          SequenceNumber found_seqno = ParseUint64(filename.substr(
              0, filename.size() - kTraceFilenameSuffix.size()));
          // Delete older trace files, but keep the one we just replayed for
          // debugging purposes
          if (found_seqno < saved_seqno_) {
            s = Env::Default()->DeleteFile(GetPathForFilename(filename));
          }
        }
        if (!s.ok()) {
          break;
        }
      }
    }
    if (s.ok()) {
      saved_seqno_ = kMaxSequenceNumber;
    }
  }
  if (trace_debug) {
    fprintf(stdout, "[expected_state_trace_debug] restore_end status=%s\n",
            s.ToString().c_str());
    fflush(stdout);
  }
  return s;
}

Status FileExpectedStateManager::Clean() {
  std::vector<std::string> expected_state_dir_children;
  Status s = Env::Default()->GetChildren(expected_state_dir_path_,
                                         &expected_state_dir_children);
  // An incomplete `Open()` or incomplete `SaveAtAndAfter()` could have left
  // behind invalid temporary files. An incomplete `SaveAtAndAfter()` could have
  // also left behind stale state/trace files. An incomplete `Restore()` could
  // have left behind stale trace files.
  for (size_t i = 0; s.ok() && i < expected_state_dir_children.size(); ++i) {
    const auto& filename = expected_state_dir_children[i];
    if (filename.rfind(kTempFilenamePrefix, 0 /* pos */) == 0 &&
        filename.size() >= kTempFilenameSuffix.size() &&
        filename.rfind(kTempFilenameSuffix) ==
            filename.size() - kTempFilenameSuffix.size()) {
      // Delete all temp files.
      s = Env::Default()->DeleteFile(GetPathForFilename(filename));
    } else if (filename.size() >= kStateFilenameSuffix.size() &&
               filename.rfind(kStateFilenameSuffix) ==
                   filename.size() - kStateFilenameSuffix.size() &&
               filename.rfind(kLatestBasename, 0) == std::string::npos &&
               ParseUint64(filename.substr(
                   0, filename.size() - kStateFilenameSuffix.size())) <
                   saved_seqno_) {
      assert(saved_seqno_ != kMaxSequenceNumber);
      // Delete stale state files.
      s = Env::Default()->DeleteFile(GetPathForFilename(filename));
    } else if (filename.size() >= kTraceFilenameSuffix.size() &&
               filename.rfind(kTraceFilenameSuffix) ==
                   filename.size() - kTraceFilenameSuffix.size() &&
               ParseUint64(filename.substr(
                   0, filename.size() - kTraceFilenameSuffix.size())) <
                   saved_seqno_) {
      // Delete stale trace files.
      s = Env::Default()->DeleteFile(GetPathForFilename(filename));
    }
  }
  return s;
}

std::string FileExpectedStateManager::GetTempPathForFilename(
    const std::string& filename) {
  assert(!expected_state_dir_path_.empty());
  std::string expected_state_dir_path_slash =
      expected_state_dir_path_.back() == '/' ? expected_state_dir_path_
                                             : expected_state_dir_path_ + "/";
  return expected_state_dir_path_slash + kTempFilenamePrefix + filename +
         kTempFilenameSuffix;
}

std::string FileExpectedStateManager::GetPathForFilename(
    const std::string& filename) {
  assert(!expected_state_dir_path_.empty());
  std::string expected_state_dir_path_slash =
      expected_state_dir_path_.back() == '/' ? expected_state_dir_path_
                                             : expected_state_dir_path_ + "/";
  return expected_state_dir_path_slash + filename;
}

AnonExpectedStateManager::AnonExpectedStateManager(size_t max_key,
                                                   size_t num_column_families)
    : ExpectedStateManager(max_key, num_column_families) {}

Status AnonExpectedStateManager::Open() {
  latest_.reset(new AnonExpectedState(max_key_, num_column_families_));
  return latest_->Open(true /* create */);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
