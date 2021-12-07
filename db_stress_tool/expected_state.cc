//  Copyright (c) 2021-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS

#include "db_stress_tool/expected_state.h"

#include "db_stress_tool/db_stress_shared_state.h"
#include "rocksdb/trace_reader_writer.h"

namespace ROCKSDB_NAMESPACE {

ExpectedState::ExpectedState(size_t max_key, size_t num_column_families)
    : max_key_(max_key),
      num_column_families_(num_column_families),
      values_(nullptr) {}

void ExpectedState::ClearColumnFamily(int cf) {
  std::fill(&Value(cf, 0 /* key */), &Value(cf + 1, 0 /* key */),
            SharedState::DELETION_SENTINEL);
}

void ExpectedState::Put(int cf, int64_t key, uint32_t value_base,
                        bool pending) {
  if (!pending) {
    // prevent expected-value update from reordering before Write
    std::atomic_thread_fence(std::memory_order_release);
  }
  Value(cf, key).store(pending ? SharedState::UNKNOWN_SENTINEL : value_base,
                       std::memory_order_relaxed);
  if (pending) {
    // prevent Write from reordering before expected-value update
    std::atomic_thread_fence(std::memory_order_release);
  }
}

uint32_t ExpectedState::Get(int cf, int64_t key) const {
  return Value(cf, key);
}

bool ExpectedState::Delete(int cf, int64_t key, bool pending) {
  if (Value(cf, key) == SharedState::DELETION_SENTINEL) {
    return false;
  }
  Put(cf, key, SharedState::DELETION_SENTINEL, pending);
  return true;
}

bool ExpectedState::SingleDelete(int cf, int64_t key, bool pending) {
  return Delete(cf, key, pending);
}

int ExpectedState::DeleteRange(int cf, int64_t begin_key, int64_t end_key,
                               bool pending) {
  int covered = 0;
  for (int64_t key = begin_key; key < end_key; ++key) {
    if (Delete(cf, key, pending)) {
      ++covered;
    }
  }
  return covered;
}

bool ExpectedState::Exists(int cf, int64_t key) {
  // UNKNOWN_SENTINEL counts as exists. That assures a key for which overwrite
  // is disallowed can't be accidentally added a second time, in which case
  // SingleDelete wouldn't be able to properly delete the key. It does allow
  // the case where a SingleDelete might be added which covers nothing, but
  // that's not a correctness issue.
  uint32_t expected_value = Value(cf, key).load();
  return expected_value != SharedState::DELETION_SENTINEL;
}

void ExpectedState::Reset() {
  for (size_t i = 0; i < num_column_families_; ++i) {
    for (size_t j = 0; j < max_key_; ++j) {
      Delete(static_cast<int>(i), j, false /* pending */);
    }
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

ExpectedStateManager::~ExpectedStateManager() {}

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
      std::string saved_seqno_trace_path =
          GetPathForFilename(ToString(saved_seqno_) + kTraceFilenameSuffix);
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
        GetPathForFilename(ToString(saved_seqno_) + kTraceFilenameSuffix);
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

#ifndef ROCKSDB_LITE
Status FileExpectedStateManager::SaveAtAndAfter(DB* db) {
  SequenceNumber seqno = db->GetLatestSequenceNumber();

  std::string state_filename = ToString(seqno) + kStateFilenameSuffix;
  std::string state_file_temp_path = GetTempPathForFilename(state_filename);
  std::string state_file_path = GetPathForFilename(state_filename);

  std::string latest_file_path =
      GetPathForFilename(kLatestBasename + kStateFilenameSuffix);

  std::string trace_filename = ToString(seqno) + kTraceFilenameSuffix;
  std::string trace_file_path = GetPathForFilename(trace_filename);

  // Populate a tempfile and then rename it to atomically create "<seqno>.state"
  // with contents from "LATEST.state"
  Status s =
      CopyFile(FileSystem::Default(), latest_file_path, state_file_temp_path,
               0 /* size */, false /* use_fsync */);
  if (s.ok()) {
    s = FileSystem::Default()->RenameFile(state_file_temp_path, state_file_path,
                                          IOOptions(), nullptr /* dbg */);
  }
  SequenceNumber old_saved_seqno;
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
    s = db->StartTrace(TraceOptions(), std::move(trace_writer));
  }

  // Delete old state/trace files. Deletion order does not matter since we only
  // delete after successfully saving new files, so old files will never be used
  // again, even if we crash.
  if (s.ok() && old_saved_seqno != kMaxSequenceNumber &&
      old_saved_seqno != saved_seqno_) {
    s = Env::Default()->DeleteFile(
        GetPathForFilename(ToString(old_saved_seqno) + kStateFilenameSuffix));
  }
  if (s.ok() && old_saved_seqno != kMaxSequenceNumber &&
      old_saved_seqno != saved_seqno_) {
    s = Env::Default()->DeleteFile(
        GetPathForFilename(ToString(old_saved_seqno) + kTraceFilenameSuffix));
  }
  return s;
}
#else   // ROCKSDB_LITE
Status FileExpectedStateManager::SaveAtAndAfter(DB* /* db */) {
  return Status::NotSupported();
}
#endif  // ROCKSDB_LITE

Status FileExpectedStateManager::Clean() {
  std::vector<std::string> expected_state_dir_children;
  Status s = Env::Default()->GetChildren(expected_state_dir_path_,
                                         &expected_state_dir_children);
  // An incomplete `Open()` or incomplete `SaveAtAndAfter()` could have left
  // behind invalid temporary files. An incomplete `SaveAtAndAfter()` could have
  // also left behind stale state/trace files.
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
      assert(saved_seqno_ != kMaxSequenceNumber);
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
