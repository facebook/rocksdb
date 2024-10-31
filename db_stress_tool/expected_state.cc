//  Copyright (c) 2021-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
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

FileExpectedState::FileExpectedState(std::string expected_state_file_path,
                                     size_t max_key, size_t num_column_families)
    : ExpectedState(max_key, num_column_families),
      expected_state_file_path_(expected_state_file_path) {}

Status FileExpectedState::Open(bool create) {
  size_t expected_values_size = GetValuesLen();

  Env* default_env = Env::Default();

  Status status;
  if (create) {
    std::unique_ptr<WritableFile> wfile;
    const EnvOptions soptions;
    status = default_env->NewWritableFile(expected_state_file_path_, &wfile,
                                          soptions);
    if (status.ok()) {
      std::string buf(expected_values_size, '\0');
      status = wfile->Append(buf);
    }
  }
  if (status.ok()) {
    status = default_env->NewMemoryMappedFileBuffer(
        expected_state_file_path_, &expected_state_mmap_buffer_);
  }
  if (status.ok()) {
    assert(expected_state_mmap_buffer_->GetLen() == expected_values_size);
    values_ = static_cast<std::atomic<uint32_t>*>(
        expected_state_mmap_buffer_->GetBase());
    assert(values_ != nullptr);
    if (create) {
      Reset();
    }
  } else {
    assert(values_ == nullptr);
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
  bool found = false;
  if (s.ok()) {
    Status exists_status = Env::Default()->FileExists(expected_state_file_path);
    if (exists_status.ok()) {
      found = true;
    } else if (exists_status.IsNotFound()) {
      found = false;
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
    FileExpectedState temp_expected_state(temp_expected_state_file_path,
                                          max_key_, num_column_families_);
    if (s.ok()) {
      s = temp_expected_state.Open(true /* create */);
    }
    if (s.ok()) {
      s = Env::Default()->RenameFile(temp_expected_state_file_path,
                                     expected_state_file_path);
    }
  }

  if (s.ok()) {
    latest_.reset(new FileExpectedState(std::move(expected_state_file_path),
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

// An `ExpectedStateTraceRecordHandler` applies a configurable number of
// write operation trace records to the configured expected state. It is used in
// `FileExpectedStateManager::Restore()` to sync the expected state with the
// DB's post-recovery state.
class ExpectedStateTraceRecordHandler : public TraceRecord::Handler,
                                        public WriteBatch::Handler {
 public:
  ExpectedStateTraceRecordHandler(uint64_t max_write_ops, ExpectedState* state)
      : max_write_ops_(max_write_ops),
        state_(state),
        buffered_writes_(nullptr) {}

  ~ExpectedStateTraceRecordHandler() { assert(IsDone()); }

  // True if we have already reached the limit on write operations to apply.
  bool IsDone() { return num_write_ops_ == max_write_ops_; }

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
    uint64_t key_id;
    if (!GetIntVal(key.ToString(), &key_id)) {
      return Status::Corruption("unable to parse key", key.ToString());
    }
    uint32_t value_base = GetValueBase(value);

    bool should_buffer_write = !(buffered_writes_ == nullptr);
    if (should_buffer_write) {
      return WriteBatchInternal::Put(buffered_writes_.get(), column_family_id,
                                     key, value);
    }

    state_->SyncPut(column_family_id, static_cast<int64_t>(key_id), value_base);
    ++num_write_ops_;
    return Status::OK();
  }

  Status TimedPutCF(uint32_t column_family_id, const Slice& key_with_ts,
                    const Slice& value, uint64_t write_unix_time) override {
    Slice key =
        StripTimestampFromUserKey(key_with_ts, FLAGS_user_timestamp_size);
    uint64_t key_id;
    if (!GetIntVal(key.ToString(), &key_id)) {
      return Status::Corruption("unable to parse key", key.ToString());
    }
    uint32_t value_base = GetValueBase(value);

    bool should_buffer_write = !(buffered_writes_ == nullptr);
    if (should_buffer_write) {
      return WriteBatchInternal::TimedPut(buffered_writes_.get(),
                                          column_family_id, key, value,
                                          write_unix_time);
    }

    state_->SyncPut(column_family_id, static_cast<int64_t>(key_id), value_base);
    ++num_write_ops_;
    return Status::OK();
  }

  Status PutEntityCF(uint32_t column_family_id, const Slice& key_with_ts,
                     const Slice& entity) override {
    Slice key =
        StripTimestampFromUserKey(key_with_ts, FLAGS_user_timestamp_size);

    uint64_t key_id = 0;
    if (!GetIntVal(key.ToString(), &key_id)) {
      return Status::Corruption("Unable to parse key", key.ToString());
    }

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
      return WriteBatchInternal::PutEntity(buffered_writes_.get(),
                                           column_family_id, key, columns);
    }

    const uint32_t value_base =
        GetValueBase(WideColumnsHelper::GetDefaultColumn(columns));

    state_->SyncPut(column_family_id, static_cast<int64_t>(key_id), value_base);

    ++num_write_ops_;

    return Status::OK();
  }

  Status DeleteCF(uint32_t column_family_id,
                  const Slice& key_with_ts) override {
    Slice key =
        StripTimestampFromUserKey(key_with_ts, FLAGS_user_timestamp_size);
    uint64_t key_id;
    if (!GetIntVal(key.ToString(), &key_id)) {
      return Status::Corruption("unable to parse key", key.ToString());
    }

    bool should_buffer_write = !(buffered_writes_ == nullptr);
    if (should_buffer_write) {
      return WriteBatchInternal::Delete(buffered_writes_.get(),
                                        column_family_id, key);
    }

    state_->SyncDelete(column_family_id, static_cast<int64_t>(key_id));
    ++num_write_ops_;
    return Status::OK();
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
    uint64_t begin_key_id, end_key_id;
    if (!GetIntVal(begin_key.ToString(), &begin_key_id)) {
      return Status::Corruption("unable to parse begin key",
                                begin_key.ToString());
    }
    if (!GetIntVal(end_key.ToString(), &end_key_id)) {
      return Status::Corruption("unable to parse end key", end_key.ToString());
    }

    bool should_buffer_write = !(buffered_writes_ == nullptr);
    if (should_buffer_write) {
      return WriteBatchInternal::DeleteRange(
          buffered_writes_.get(), column_family_id, begin_key, end_key);
    }

    state_->SyncDeleteRange(column_family_id,
                            static_cast<int64_t>(begin_key_id),
                            static_cast<int64_t>(end_key_id));
    ++num_write_ops_;
    return Status::OK();
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
  uint64_t num_write_ops_ = 0;
  uint64_t max_write_ops_;
  ExpectedState* state_;
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

  std::unique_ptr<TraceReader> trace_reader;
  Status s = NewFileTraceReader(Env::Default(), EnvOptions(), trace_file_path,
                                &trace_reader);

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
      state.reset(new FileExpectedState(latest_file_temp_path, max_key_,
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
  }

  if (s.ok()) {
    s = FileSystem::Default()->RenameFile(latest_file_temp_path,
                                          latest_file_path, IOOptions(),
                                          nullptr /* dbg */);
  }
  if (s.ok()) {
    latest_.reset(new FileExpectedState(latest_file_path, max_key_,
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
