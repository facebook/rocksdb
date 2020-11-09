//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This test uses a custom FileSystem to keep track of the state of a file
// system the last "Sync". The data being written is cached in a "buffer".
// Only when "Sync" is called, the data will be persistent. It can similate
// file data loss (or entire files) not protected by a "Sync". For any of the
// FileSystem related operations, by specify the "IOStatus Error", a specific
// error can be returned when file system is not activated.

#include "utilities/fault_injection_fs.h"

#include <functional>
#include <utility>

#include "env/composite_env_wrapper.h"
#include "port/lang.h"
#include "port/stack_trace.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

// Assume a filename, and not a directory name like "/foo/bar/"
std::string TestFSGetDirName(const std::string filename) {
  size_t found = filename.find_last_of("/\\");
  if (found == std::string::npos) {
    return "";
  } else {
    return filename.substr(0, found);
  }
}

// Trim the tailing "/" in the end of `str`
std::string TestFSTrimDirname(const std::string& str) {
  size_t found = str.find_last_not_of("/");
  if (found == std::string::npos) {
    return str;
  }
  return str.substr(0, found + 1);
}

// Return pair <parent directory name, file name> of a full path.
std::pair<std::string, std::string> TestFSGetDirAndName(
    const std::string& name) {
  std::string dirname = TestFSGetDirName(name);
  std::string fname = name.substr(dirname.size() + 1);
  return std::make_pair(dirname, fname);
}

IOStatus FSFileState::DropUnsyncedData() {
  buffer_.resize(0);
  return IOStatus::OK();
}

IOStatus FSFileState::DropRandomUnsyncedData(Random* rand) {
  int range = static_cast<int>(buffer_.size());
  size_t truncated_size = static_cast<size_t>(rand->Uniform(range));
  buffer_.resize(truncated_size);
  return IOStatus::OK();
}

IOStatus TestFSDirectory::Fsync(const IOOptions& options, IODebugContext* dbg) {
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  fs_->SyncDir(dirname_);
  return dir_->Fsync(options, dbg);
}

TestFSWritableFile::TestFSWritableFile(const std::string& fname,
                                       std::unique_ptr<FSWritableFile>&& f,
                                       FaultInjectionTestFS* fs)
    : state_(fname),
      target_(std::move(f)),
      writable_file_opened_(true),
      fs_(fs) {
  assert(target_ != nullptr);
  state_.pos_ = 0;
}

TestFSWritableFile::~TestFSWritableFile() {
  if (writable_file_opened_) {
    Close(IOOptions(), nullptr).PermitUncheckedError();
  }
}

IOStatus TestFSWritableFile::Append(const Slice& data, const IOOptions&,
                                    IODebugContext*) {
  MutexLock l(&mutex_);
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  state_.buffer_.append(data.data(), data.size());
  state_.pos_ += data.size();
  fs_->WritableFileAppended(state_);
  IOStatus io_s = fs_->InjectWriteError(state_.filename_);
  return io_s;
}

IOStatus TestFSWritableFile::Close(const IOOptions& options,
                                   IODebugContext* dbg) {
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  writable_file_opened_ = false;
  IOStatus io_s;
  io_s = target_->Append(state_.buffer_, options, dbg);
  if (io_s.ok()) {
    state_.buffer_.resize(0);
    // Ignore sync errors
    target_->Sync(options, dbg).PermitUncheckedError();
    io_s = target_->Close(options, dbg);
  }
  if (io_s.ok()) {
    fs_->WritableFileClosed(state_);
  }
  return io_s;
}

IOStatus TestFSWritableFile::Flush(const IOOptions&, IODebugContext*) {
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  if (fs_->IsFilesystemActive()) {
    state_.pos_at_last_flush_ = state_.pos_;
  }
  return IOStatus::OK();
}

IOStatus TestFSWritableFile::Sync(const IOOptions& options,
                                  IODebugContext* dbg) {
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  IOStatus io_s = target_->Append(state_.buffer_, options, dbg);
  state_.buffer_.resize(0);
  // Ignore sync errors
  target_->Sync(options, dbg).PermitUncheckedError();
  state_.pos_at_last_sync_ = state_.pos_;
  fs_->WritableFileSynced(state_);
  return io_s;
}

TestFSRandomRWFile::TestFSRandomRWFile(const std::string& /*fname*/,
                                       std::unique_ptr<FSRandomRWFile>&& f,
                                       FaultInjectionTestFS* fs)
    : target_(std::move(f)), file_opened_(true), fs_(fs) {
  assert(target_ != nullptr);
}

TestFSRandomRWFile::~TestFSRandomRWFile() {
  if (file_opened_) {
    Close(IOOptions(), nullptr).PermitUncheckedError();
  }
}

IOStatus TestFSRandomRWFile::Write(uint64_t offset, const Slice& data,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  return target_->Write(offset, data, options, dbg);
}

IOStatus TestFSRandomRWFile::Read(uint64_t offset, size_t n,
                                  const IOOptions& options, Slice* result,
                                  char* scratch, IODebugContext* dbg) const {
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  return target_->Read(offset, n, options, result, scratch, dbg);
}

IOStatus TestFSRandomRWFile::Close(const IOOptions& options,
                                   IODebugContext* dbg) {
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  file_opened_ = false;
  return target_->Close(options, dbg);
}

IOStatus TestFSRandomRWFile::Flush(const IOOptions& options,
                                   IODebugContext* dbg) {
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  return target_->Flush(options, dbg);
}

IOStatus TestFSRandomRWFile::Sync(const IOOptions& options,
                                  IODebugContext* dbg) {
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  return target_->Sync(options, dbg);
}

TestFSRandomAccessFile::TestFSRandomAccessFile(const std::string& /*fname*/,
                                       std::unique_ptr<FSRandomAccessFile>&& f,
                                       FaultInjectionTestFS* fs)
    : target_(std::move(f)), fs_(fs) {
  assert(target_ != nullptr);
}

IOStatus TestFSRandomAccessFile::Read(uint64_t offset, size_t n,
                                  const IOOptions& options, Slice* result,
                                  char* scratch, IODebugContext* dbg) const {
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  IOStatus s = target_->Read(offset, n, options, result, scratch, dbg);
  if (s.ok()) {
    s = fs_->InjectError(FaultInjectionTestFS::ErrorOperation::kRead, result,
                         use_direct_io(), scratch);
  }
  return s;
}

IOStatus FaultInjectionTestFS::NewDirectory(
    const std::string& name, const IOOptions& options,
    std::unique_ptr<FSDirectory>* result, IODebugContext* dbg) {
  std::unique_ptr<FSDirectory> r;
  IOStatus io_s = target()->NewDirectory(name, options, &r, dbg);
  if (!io_s.ok()) {
    return io_s;
  }
  result->reset(
      new TestFSDirectory(this, TestFSTrimDirname(name), r.release()));
  return IOStatus::OK();
}

IOStatus FaultInjectionTestFS::NewWritableFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  if (IsFilesystemDirectWritable()) {
    return target()->NewWritableFile(fname, file_opts, result, dbg);
  }

  IOStatus io_s = target()->NewWritableFile(fname, file_opts, result, dbg);
  if (io_s.ok()) {
    result->reset(new TestFSWritableFile(fname, std::move(*result), this));
    // WritableFileWriter* file is opened
    // again then it will be truncated - so forget our saved state.
    UntrackFile(fname);
    MutexLock l(&mutex_);
    open_files_.insert(fname);
    auto dir_and_name = TestFSGetDirAndName(fname);
    auto& list = dir_to_new_files_since_last_sync_[dir_and_name.first];
    list.insert(dir_and_name.second);
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::ReopenWritableFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  if (IsFilesystemDirectWritable()) {
    return target()->ReopenWritableFile(fname, file_opts, result, dbg);
  }
  IOStatus io_s = target()->ReopenWritableFile(fname, file_opts, result, dbg);
  if (io_s.ok()) {
    result->reset(new TestFSWritableFile(fname, std::move(*result), this));
    // WritableFileWriter* file is opened
    // again then it will be truncated - so forget our saved state.
    UntrackFile(fname);
    MutexLock l(&mutex_);
    open_files_.insert(fname);
    auto dir_and_name = TestFSGetDirAndName(fname);
    auto& list = dir_to_new_files_since_last_sync_[dir_and_name.first];
    list.insert(dir_and_name.second);
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::NewRandomRWFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSRandomRWFile>* result, IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  if (IsFilesystemDirectWritable()) {
    return target()->NewRandomRWFile(fname, file_opts, result, dbg);
  }
  IOStatus io_s = target()->NewRandomRWFile(fname, file_opts, result, dbg);
  if (io_s.ok()) {
    result->reset(new TestFSRandomRWFile(fname, std::move(*result), this));
    // WritableFileWriter* file is opened
    // again then it will be truncated - so forget our saved state.
    UntrackFile(fname);
    MutexLock l(&mutex_);
    open_files_.insert(fname);
    auto dir_and_name = TestFSGetDirAndName(fname);
    auto& list = dir_to_new_files_since_last_sync_[dir_and_name.first];
    list.insert(dir_and_name.second);
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::NewRandomAccessFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s = InjectError(ErrorOperation::kOpen, nullptr, false, nullptr);
  if (io_s.ok()) {
    io_s = target()->NewRandomAccessFile(fname, file_opts, result, dbg);
  }
  if (io_s.ok()) {
    result->reset(new TestFSRandomAccessFile(fname, std::move(*result), this));
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::DeleteFile(const std::string& f,
                                          const IOOptions& options,
                                          IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s = FileSystemWrapper::DeleteFile(f, options, dbg);
  if (io_s.ok()) {
    UntrackFile(f);
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::RenameFile(const std::string& s,
                                          const std::string& t,
                                          const IOOptions& options,
                                          IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s = FileSystemWrapper::RenameFile(s, t, options, dbg);

  if (io_s.ok()) {
    MutexLock l(&mutex_);
    if (db_file_state_.find(s) != db_file_state_.end()) {
      db_file_state_[t] = db_file_state_[s];
      db_file_state_.erase(s);
    }

    auto sdn = TestFSGetDirAndName(s);
    auto tdn = TestFSGetDirAndName(t);
    if (dir_to_new_files_since_last_sync_[sdn.first].erase(sdn.second) != 0) {
      auto& tlist = dir_to_new_files_since_last_sync_[tdn.first];
      assert(tlist.find(tdn.second) == tlist.end());
      tlist.insert(tdn.second);
    }
  }

  return io_s;
}

void FaultInjectionTestFS::WritableFileClosed(const FSFileState& state) {
  MutexLock l(&mutex_);
  if (open_files_.find(state.filename_) != open_files_.end()) {
    db_file_state_[state.filename_] = state;
    open_files_.erase(state.filename_);
  }
}

void FaultInjectionTestFS::WritableFileSynced(const FSFileState& state) {
  MutexLock l(&mutex_);
  if (open_files_.find(state.filename_) != open_files_.end()) {
    if (db_file_state_.find(state.filename_) == db_file_state_.end()) {
      db_file_state_.insert(std::make_pair(state.filename_, state));
    } else {
      db_file_state_[state.filename_] = state;
    }
  }
}

void FaultInjectionTestFS::WritableFileAppended(const FSFileState& state) {
  MutexLock l(&mutex_);
  if (open_files_.find(state.filename_) != open_files_.end()) {
    if (db_file_state_.find(state.filename_) == db_file_state_.end()) {
      db_file_state_.insert(std::make_pair(state.filename_, state));
    } else {
      db_file_state_[state.filename_] = state;
    }
  }
}

IOStatus FaultInjectionTestFS::DropUnsyncedFileData() {
  IOStatus io_s;
  MutexLock l(&mutex_);
  for (std::map<std::string, FSFileState>::iterator it = db_file_state_.begin();
       io_s.ok() && it != db_file_state_.end(); ++it) {
    FSFileState& fs_state = it->second;
    if (!fs_state.IsFullySynced()) {
      io_s = fs_state.DropUnsyncedData();
    }
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::DropRandomUnsyncedFileData(Random* rnd) {
  IOStatus io_s;
  MutexLock l(&mutex_);
  for (std::map<std::string, FSFileState>::iterator it = db_file_state_.begin();
       io_s.ok() && it != db_file_state_.end(); ++it) {
    FSFileState& fs_state = it->second;
    if (!fs_state.IsFullySynced()) {
      io_s = fs_state.DropRandomUnsyncedData(rnd);
    }
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::DeleteFilesCreatedAfterLastDirSync(
    const IOOptions& options, IODebugContext* dbg) {
  // Because DeleteFile access this container make a copy to avoid deadlock
  std::map<std::string, std::set<std::string>> map_copy;
  {
    MutexLock l(&mutex_);
    map_copy.insert(dir_to_new_files_since_last_sync_.begin(),
                    dir_to_new_files_since_last_sync_.end());
  }

  for (auto& pair : map_copy) {
    for (std::string name : pair.second) {
      IOStatus io_s = DeleteFile(pair.first + "/" + name, options, dbg);
      if (!io_s.ok()) {
        return io_s;
      }
    }
  }
  return IOStatus::OK();
}

void FaultInjectionTestFS::ResetState() {
  MutexLock l(&mutex_);
  db_file_state_.clear();
  dir_to_new_files_since_last_sync_.clear();
  SetFilesystemActiveNoLock(true);
}

void FaultInjectionTestFS::UntrackFile(const std::string& f) {
  MutexLock l(&mutex_);
  auto dir_and_name = TestFSGetDirAndName(f);
  dir_to_new_files_since_last_sync_[dir_and_name.first].erase(
      dir_and_name.second);
  db_file_state_.erase(f);
  open_files_.erase(f);
}

IOStatus FaultInjectionTestFS::InjectError(ErrorOperation op,
                                           Slice* result,
                                           bool direct_io,
                                           char* scratch) {
  ErrorContext* ctx =
        static_cast<ErrorContext*>(thread_local_error_->Get());
  if (ctx == nullptr || !ctx->enable_error_injection || !ctx->one_in) {
    return IOStatus::OK();
  }

  if (ctx->rand.OneIn(ctx->one_in)) {
    ctx->count++;
    if (ctx->callstack) {
      free(ctx->callstack);
    }
    ctx->callstack = port::SaveStack(&ctx->frames);
    switch (op) {
      case kRead:
      {
        if (!direct_io) {
          ctx->type =
            static_cast<ErrorType>(ctx->rand.Uniform(ErrorType::kErrorTypeMax));
        } else {
          // In Direct IO mode, the actual read will read extra data due to
          // alignment restrictions. So don't inject corruption or
          // truncated reads as we don't know if it will actually cause a
          // detectable error
          ctx->type = ErrorType::kErrorTypeStatus;
        }
        switch (ctx->type) {
          // Inject IO error
          case ErrorType::kErrorTypeStatus:
            return IOStatus::IOError();
          // Inject random corruption
          case ErrorType::kErrorTypeCorruption:
          {
            if (result->data() == scratch) {
              uint64_t offset = ctx->rand.Uniform((uint32_t)result->size());
              uint64_t len =
                std::min<uint64_t>(result->size() - offset, 64UL);
              assert(offset < result->size());
              assert(offset + len <= result->size());
              std::string str;
              // The randomly generated string could be identical to the
              // original one, so retry
              do {
                str = ctx->rand.RandomString(static_cast<int>(len));
              } while (str == std::string(scratch + offset, len));
              memcpy(scratch + offset, str.data(), len);
              break;
            } else {
              FALLTHROUGH_INTENDED;
            }
          }
          // Truncate the result
          case ErrorType::kErrorTypeTruncated:
          {
            assert(result->size() > 0);
            uint64_t offset = ctx->rand.Uniform((uint32_t)result->size());
            assert(offset < result->size());
            *result = Slice(result->data(), offset);
            break;
          }
          default:
            assert(false);
        }
        break;
      }
      case kOpen:
        return IOStatus::IOError();
      default:
        assert(false);
    }
  }
  return IOStatus::OK();
}

IOStatus FaultInjectionTestFS::InjectWriteError(const std::string& file_name) {
  MutexLock l(&mutex_);
  if (!enable_write_error_injection_ || !write_error_one_in_) {
    return IOStatus::OK();
  }
  bool allowed_type = false;

  uint64_t number;
  FileType c_type;
  if (ParseFileName(file_name, &number, &c_type)) {
    for (const auto& type : write_error_allowed_types_) {
      if (c_type == type) {
        allowed_type = true;
      }
    }
  }

  if (allowed_type) {
    if (write_error_rand_.OneIn(write_error_one_in_)) {
      return error_;
    }
  }
  return IOStatus::OK();
}

void FaultInjectionTestFS::PrintFaultBacktrace() {
#if defined(OS_LINUX)
  ErrorContext* ctx =
        static_cast<ErrorContext*>(thread_local_error_->Get());
  if (ctx == nullptr) {
    return;
  }
  fprintf(stderr, "Injected error type = %d\n", ctx->type);
  port::PrintAndFreeStack(ctx->callstack, ctx->frames);
  ctx->callstack = nullptr;
#endif
}

}  // namespace ROCKSDB_NAMESPACE
