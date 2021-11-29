//  Copyright (c) 2021-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS

#include "db_stress_tool/expected_state.h"

#include "db_stress_tool/db_stress_shared_state.h"

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

const std::string FileExpectedStateManager::kLatestFilename = "LATEST.state";

FileExpectedStateManager::FileExpectedStateManager(
    size_t max_key, size_t num_column_families,
    std::string expected_state_dir_path)
    : ExpectedStateManager(max_key, num_column_families),
      expected_state_dir_path_(std::move(expected_state_dir_path)) {
  assert(!expected_state_dir_path_.empty());
}

Status FileExpectedStateManager::Open() {
  Status s = Clean();

  std::string expected_state_file_path = GetPathForFilename(kLatestFilename);
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
        GetTempPathForFilename(kLatestFilename);
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

Status FileExpectedStateManager::Clean() {
  // An incomplete `Open()` could have left behind an invalid temporary file.
  std::string temp_path = GetTempPathForFilename(kLatestFilename);
  Status s = Env::Default()->FileExists(temp_path);
  if (s.ok()) {
    s = Env::Default()->DeleteFile(temp_path);
  } else if (s.IsNotFound()) {
    s = Status::OK();
  }
  return s;
}

std::string FileExpectedStateManager::GetTempPathForFilename(
    const std::string& filename) {
  static const std::string kTempFilenamePrefix = ".";
  static const std::string kTempFilenameSuffix = ".tmp";

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
