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
// Only when "Sync" is called, the data will be persistent. It can simulate
// file data loss (or entire files) not protected by a "Sync". For any of the
// FileSystem related operations, by specify the "IOStatus Error", a specific
// error can be returned when file system is not activated.

#include "utilities/fault_injection_fs.h"

#include <algorithm>
#include <cstdio>
#include <functional>
#include <utility>

#include "env/composite_env_wrapper.h"
#include "port/lang.h"
#include "port/stack_trace.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/types.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {

const std::string kNewFileNoOverwrite;

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
  size_t found = str.find_last_not_of('/');
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

// Calculate the checksum of the data with corresponding checksum
// type. If name does not match, no checksum is returned.
void CalculateTypedChecksum(const ChecksumType& checksum_type, const char* data,
                            size_t size, std::string* checksum) {
  if (checksum_type == ChecksumType::kCRC32c) {
    uint32_t v_crc32c = crc32c::Extend(0, data, size);
    PutFixed32(checksum, v_crc32c);
    return;
  } else if (checksum_type == ChecksumType::kxxHash) {
    uint32_t v = XXH32(data, size, 0);
    PutFixed32(checksum, v);
  }
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

  IOStatus s = fs_->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kMetadataWrite, options);
  if (!s.ok()) {
    return s;
  }

  fs_->SyncDir(dirname_);
  s = dir_->Fsync(options, dbg);
  return s;
}

IOStatus TestFSDirectory::Close(const IOOptions& options, IODebugContext* dbg) {
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }

  IOStatus s = fs_->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kMetadataWrite, options);
  if (!s.ok()) {
    return s;
  }

  s = dir_->Close(options, dbg);
  return s;
}

IOStatus TestFSDirectory::FsyncWithDirOptions(
    const IOOptions& options, IODebugContext* dbg,
    const DirFsyncOptions& dir_fsync_options) {
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  IOStatus s = fs_->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kMetadataWrite, options);
  if (!s.ok()) {
    return s;
  }

  fs_->SyncDir(dirname_);
  s = dir_->FsyncWithDirOptions(options, dbg, dir_fsync_options);
  return s;
}

TestFSWritableFile::TestFSWritableFile(const std::string& fname,
                                       const FileOptions& file_opts,
                                       std::unique_ptr<FSWritableFile>&& f,
                                       FaultInjectionTestFS* fs)
    : state_(fname),
      file_opts_(file_opts),
      target_(std::move(f)),
      writable_file_opened_(true),
      fs_(fs),
      unsync_data_loss_(fs_->InjectUnsyncedDataLoss()) {
  assert(target_ != nullptr);
  assert(state_.pos_at_last_append_ == 0);
  assert(state_.pos_at_last_sync_ == 0);
}

TestFSWritableFile::~TestFSWritableFile() {
  if (writable_file_opened_) {
    Close(IOOptions(), nullptr).PermitUncheckedError();
  }
}

IOStatus TestFSWritableFile::Append(const Slice& data, const IOOptions& options,
                                    IODebugContext* dbg) {
  MutexLock l(&mutex_);
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }

  IOStatus s = fs_->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kWrite, options, state_.filename_,
      FaultInjectionTestFS::ErrorOperation::kAppend);
  if (!s.ok()) {
    return s;
  }

  if (target_->use_direct_io() || !unsync_data_loss_) {
    // TODO(hx235): buffer data for direct IO write to simulate data loss like
    // non-direct IO write
    s = target_->Append(data, options, dbg);
  } else {
    state_.buffer_.append(data.data(), data.size());
  }

  if (s.ok()) {
    state_.pos_at_last_append_ += data.size();
    fs_->WritableFileAppended(state_);
  }

  return s;
}

// By setting the IngestDataCorruptionBeforeWrite(), the data corruption is
// simulated.
IOStatus TestFSWritableFile::Append(
    const Slice& data, const IOOptions& options,
    const DataVerificationInfo& verification_info, IODebugContext* dbg) {
  MutexLock l(&mutex_);
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  if (fs_->ShouldDataCorruptionBeforeWrite()) {
    return IOStatus::Corruption("Data is corrupted!");
  }

  IOStatus s = fs_->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kWrite, options, state_.filename_,
      FaultInjectionTestFS::ErrorOperation::kAppend);
  if (!s.ok()) {
    return s;
  }

  // Calculate the checksum
  std::string checksum;
  CalculateTypedChecksum(fs_->GetChecksumHandoffFuncType(), data.data(),
                         data.size(), &checksum);
  if (fs_->GetChecksumHandoffFuncType() != ChecksumType::kNoChecksum &&
      checksum != verification_info.checksum.ToString()) {
    std::string msg =
        "Data is corrupted! Origin data checksum: " +
        verification_info.checksum.ToString(true) +
        "current data checksum: " + Slice(checksum).ToString(true);
    return IOStatus::Corruption(msg);
  }

  if (target_->use_direct_io() || !unsync_data_loss_) {
    // TODO(hx235): buffer data for direct IO write to simulate data loss like
    // non-direct IO write
    s = target_->Append(data, options, dbg);
  } else {
    state_.buffer_.append(data.data(), data.size());
  }
  if (s.ok()) {
    state_.pos_at_last_append_ += data.size();
    fs_->WritableFileAppended(state_);
  }
  return s;
}

IOStatus TestFSWritableFile::Truncate(uint64_t size, const IOOptions& options,
                                      IODebugContext* dbg) {
  MutexLock l(&mutex_);
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  IOStatus s = fs_->MaybeInjectThreadLocalError(FaultInjectionIOType::kWrite,
                                                options, state_.filename_);
  if (!s.ok()) {
    return s;
  }

  s = target_->Truncate(size, options, dbg);
  if (s.ok()) {
    state_.pos_at_last_append_ = size;
  }
  return s;
}

IOStatus TestFSWritableFile::PositionedAppend(const Slice& data,
                                              uint64_t offset,
                                              const IOOptions& options,
                                              IODebugContext* dbg) {
  MutexLock l(&mutex_);
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  if (fs_->ShouldDataCorruptionBeforeWrite()) {
    return IOStatus::Corruption("Data is corrupted!");
  }
  IOStatus s = fs_->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kWrite, options, state_.filename_,
      FaultInjectionTestFS::ErrorOperation::kPositionedAppend);
  if (!s.ok()) {
    return s;
  }

  // TODO(hx235): buffer data for direct IO write to simulate data loss like
  // non-direct IO write
  s = target_->PositionedAppend(data, offset, options, dbg);
  if (s.ok()) {
    state_.pos_at_last_append_ = offset + data.size();
    fs_->WritableFileAppended(state_);
  }
  return s;
}

IOStatus TestFSWritableFile::PositionedAppend(
    const Slice& data, uint64_t offset, const IOOptions& options,
    const DataVerificationInfo& verification_info, IODebugContext* dbg) {
  MutexLock l(&mutex_);
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  if (fs_->ShouldDataCorruptionBeforeWrite()) {
    return IOStatus::Corruption("Data is corrupted!");
  }
  IOStatus s = fs_->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kWrite, options, state_.filename_,
      FaultInjectionTestFS::ErrorOperation::kPositionedAppend);
  if (!s.ok()) {
    return s;
  }

  // Calculate the checksum
  std::string checksum;
  CalculateTypedChecksum(fs_->GetChecksumHandoffFuncType(), data.data(),
                         data.size(), &checksum);
  if (fs_->GetChecksumHandoffFuncType() != ChecksumType::kNoChecksum &&
      checksum != verification_info.checksum.ToString()) {
    std::string msg =
        "Data is corrupted! Origin data checksum: " +
        verification_info.checksum.ToString(true) +
        "current data checksum: " + Slice(checksum).ToString(true);
    return IOStatus::Corruption(msg);
  }
  // TODO(hx235): buffer data for direct IO write to simulate data loss like
  // non-direct IO write
  s = target_->PositionedAppend(data, offset, options, dbg);
  if (s.ok()) {
    state_.pos_at_last_append_ = offset + data.size();
    fs_->WritableFileAppended(state_);
  }
  return s;
}

IOStatus TestFSWritableFile::Close(const IOOptions& options,
                                   IODebugContext* dbg) {
  MutexLock l(&mutex_);
  fs_->WritableFileClosed(state_);
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  IOStatus io_s = fs_->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kMetadataWrite, options);
  if (!io_s.ok()) {
    return io_s;
  }
  writable_file_opened_ = false;

  // Drop buffered data that was never synced because close is not a syncing
  // mechanism in POSIX file semantics.
  state_.buffer_.resize(0);
  io_s = target_->Close(options, dbg);
  return io_s;
}

IOStatus TestFSWritableFile::Flush(const IOOptions&, IODebugContext*) {
  MutexLock l(&mutex_);
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  return IOStatus::OK();
}

IOStatus TestFSWritableFile::Sync(const IOOptions& options,
                                  IODebugContext* dbg) {
  MutexLock l(&mutex_);
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  if (target_->use_direct_io()) {
    // For Direct IO mode, we don't buffer anything in TestFSWritableFile.
    // So just return
    return IOStatus::OK();
  }
  IOStatus io_s = target_->Append(state_.buffer_, options, dbg);
  state_.buffer_.resize(0);
  // Ignore sync errors
  target_->Sync(options, dbg).PermitUncheckedError();
  state_.pos_at_last_sync_ = state_.pos_at_last_append_;
  fs_->WritableFileSynced(state_);
  return io_s;
}

IOStatus TestFSWritableFile::RangeSync(uint64_t offset, uint64_t nbytes,
                                       const IOOptions& options,
                                       IODebugContext* dbg) {
  MutexLock l(&mutex_);
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  // Assumes caller passes consecutive byte ranges.
  uint64_t sync_limit = offset + nbytes;

  IOStatus io_s;
  if (sync_limit < state_.pos_at_last_sync_) {
    return io_s;
  }
  uint64_t num_to_sync = std::min(static_cast<uint64_t>(state_.buffer_.size()),
                                  sync_limit - state_.pos_at_last_sync_);
  Slice buf_to_sync(state_.buffer_.data(), num_to_sync);
  io_s = target_->Append(buf_to_sync, options, dbg);
  state_.buffer_ = state_.buffer_.substr(num_to_sync);
  // Ignore sync errors
  target_->RangeSync(offset, nbytes, options, dbg).PermitUncheckedError();
  state_.pos_at_last_sync_ = offset + num_to_sync;
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
  // TODO (low priority): fs_->ReadUnsyncedData()
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

TestFSRandomAccessFile::TestFSRandomAccessFile(
    const std::string& /*fname*/, std::unique_ptr<FSRandomAccessFile>&& f,
    FaultInjectionTestFS* fs)
    : target_(std::move(f)), fs_(fs) {
  assert(target_ != nullptr);
}

IOStatus TestFSRandomAccessFile::Read(uint64_t offset, size_t n,
                                      const IOOptions& options, Slice* result,
                                      char* scratch,
                                      IODebugContext* dbg) const {
  TEST_SYNC_POINT("FaultInjectionTestFS::RandomRead");
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  IOStatus s = fs_->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kRead, options, "",
      FaultInjectionTestFS::ErrorOperation::kRead, result, use_direct_io(),
      scratch, /*need_count_increase=*/true,
      /*fault_injected=*/nullptr);
  if (!s.ok()) {
    return s;
  }

  s = target_->Read(offset, n, options, result, scratch, dbg);
  // TODO (low priority): fs_->ReadUnsyncedData()
  return s;
}

IOStatus TestFSRandomAccessFile::ReadAsync(
    FSReadRequest& req, const IOOptions& opts,
    std::function<void(FSReadRequest&, void*)> cb, void* cb_arg,
    void** io_handle, IOHandleDeleter* del_fn, IODebugContext* /*dbg*/) {
  IOStatus res_status;
  FSReadRequest res;
  IOStatus s;
  if (!fs_->IsFilesystemActive()) {
    res_status = fs_->GetError();
  }
  if (res_status.ok()) {
    res_status = fs_->MaybeInjectThreadLocalError(
        FaultInjectionIOType::kRead, opts, "",
        FaultInjectionTestFS::ErrorOperation::kRead, &res.result,
        use_direct_io(), req.scratch, /*need_count_increase=*/true,
        /*fault_injected=*/nullptr);
  }
  if (res_status.ok()) {
    s = target_->ReadAsync(req, opts, cb, cb_arg, io_handle, del_fn, nullptr);
    // TODO (low priority): fs_->ReadUnsyncedData()
  } else {
    // If there's no injected error, then cb will be called asynchronously when
    // target_ actually finishes the read. But if there's an injected error, it
    // needs to immediately call cb(res, cb_arg) s since target_->ReadAsync()
    // isn't invoked at all.
    res.status = res_status;
    cb(res, cb_arg);
  }
  // We return ReadAsync()'s status intead of injected error status here since
  // the return status is not supposed to be the status of the actual IO (i.e,
  // the actual async read). The actual status of the IO will be passed to cb()
  // callback upon the actual read finishes or like above when injected error
  // happens.
  return s;
}

IOStatus TestFSRandomAccessFile::MultiRead(FSReadRequest* reqs, size_t num_reqs,
                                           const IOOptions& options,
                                           IODebugContext* dbg) {
  if (!fs_->IsFilesystemActive()) {
    return fs_->GetError();
  }
  IOStatus s = target_->MultiRead(reqs, num_reqs, options, dbg);
  // TODO (low priority): fs_->ReadUnsyncedData()
  bool injected_error = false;
  for (size_t i = 0; i < num_reqs; i++) {
    if (!reqs[i].status.ok()) {
      // Already seeing an error.
      break;
    }
    bool this_injected_error;
    reqs[i].status = fs_->MaybeInjectThreadLocalError(
        FaultInjectionIOType::kRead, options, "",
        FaultInjectionTestFS::ErrorOperation::kRead, &(reqs[i].result),
        use_direct_io(), reqs[i].scratch,
        /*need_count_increase=*/true,
        /*fault_injected=*/&this_injected_error);
    injected_error |= this_injected_error;
  }
  if (s.ok()) {
    s = fs_->MaybeInjectThreadLocalError(
        FaultInjectionIOType::kRead, options, "",
        FaultInjectionTestFS::ErrorOperation::kMultiRead, nullptr,
        use_direct_io(), nullptr, /*need_count_increase=*/!injected_error,
        /*fault_injected=*/nullptr);
  }
  return s;
}

size_t TestFSRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  if (fs_->ShouldFailGetUniqueId()) {
    return 0;
  } else {
    return target_->GetUniqueId(id, max_size);
  }
}

namespace {
// Modifies `result` to start at the beginning of `scratch` if not already,
// copying data there if needed.
void MoveToScratchIfNeeded(Slice* result, char* scratch) {
  if (result->data() != scratch) {
    // NOTE: might overlap, where result is later in scratch
    std::copy(result->data(), result->data() + result->size(), scratch);
    *result = Slice(scratch, result->size());
  }
}
}  // namespace

void FaultInjectionTestFS::ReadUnsynced(const std::string& fname,
                                        uint64_t offset, size_t n,
                                        Slice* result, char* scratch,
                                        int64_t* pos_at_last_sync) {
  *result = Slice(scratch, 0);      // default empty result
  assert(*pos_at_last_sync == -1);  // default "unknown"

  MutexLock l(&mutex_);
  auto it = db_file_state_.find(fname);
  if (it != db_file_state_.end()) {
    auto& st = it->second;
    *pos_at_last_sync = static_cast<int64_t>(st.pos_at_last_sync_);
    // Find overlap between [offset, offset + n) and
    // [*pos_at_last_sync, *pos_at_last_sync + st.buffer_.size())
    int64_t begin = std::max(static_cast<int64_t>(offset), *pos_at_last_sync);
    int64_t end =
        std::min(static_cast<int64_t>(offset + n),
                 *pos_at_last_sync + static_cast<int64_t>(st.buffer_.size()));

    // Copy and return overlap if there is any
    if (begin < end) {
      size_t offset_in_buffer = static_cast<size_t>(begin - *pos_at_last_sync);
      size_t offset_in_scratch = static_cast<size_t>(begin - offset);
      std::copy_n(st.buffer_.data() + offset_in_buffer, end - begin,
                  scratch + offset_in_scratch);
      *result = Slice(scratch + offset_in_scratch, end - begin);
    }
  }
}

IOStatus TestFSSequentialFile::Read(size_t n, const IOOptions& options,
                                    Slice* result, char* scratch,
                                    IODebugContext* dbg) {
  IOStatus s = fs_->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kRead, options, "",
      FaultInjectionTestFS::ErrorOperation::kRead, result, use_direct_io(),
      scratch, true /*need_count_increase=*/, nullptr /* fault_injected*/);
  if (!s.ok()) {
    return s;
  }

  // Some complex logic is needed to deal with concurrent write to the same
  // file, while keeping good performance (e.g. not holding FS mutex during
  // I/O op), especially in common cases.

  if (read_pos_ == target_read_pos_) {
    // Normal case: start by reading from underlying file
    s = target()->Read(n, options, result, scratch, dbg);
    if (!s.ok()) {
      return s;
    }
    target_read_pos_ += result->size();
  } else {
    // We must have previously read buffered data (unsynced) not written to
    // target. Deal with this case (and more) below.
    *result = {};
  }

  if (fs_->ReadUnsyncedData() && result->size() < n) {
    // We need to check if there's unsynced data to fill out the rest of the
    // read.

    // First, ensure target read data is in scratch for easy handling.
    MoveToScratchIfNeeded(result, scratch);
    assert(result->data() == scratch);

    // If we just did a target Read, we only want unsynced data after it
    // (target_read_pos_). Otherwise (e.g. if target is behind because of
    // unsynced data) we want unsynced data starting at the current read pos
    // (read_pos_, not yet updated).
    const uint64_t unsynced_read_pos = std::max(target_read_pos_, read_pos_);
    const size_t offset_from_read_pos =
        static_cast<size_t>(unsynced_read_pos - read_pos_);
    Slice unsynced_result;
    int64_t pos_at_last_sync = -1;
    fs_->ReadUnsynced(fname_, unsynced_read_pos, n - offset_from_read_pos,
                      &unsynced_result, scratch + offset_from_read_pos,
                      &pos_at_last_sync);
    assert(unsynced_result.data() >= scratch + offset_from_read_pos);
    assert(unsynced_result.data() < scratch + n);
    // Now, there are several cases to consider (some grouped together):
    if (pos_at_last_sync <= static_cast<int64_t>(unsynced_read_pos)) {
      // 1. We didn't get any unsynced data because nothing has been written
      // to the file beyond unsynced_read_pos (including untracked
      // pos_at_last_sync == -1)
      // 2. We got some unsynced data starting at unsynced_read_pos (possibly
      // on top of some synced data from target). We don't need to try reading
      // any more from target because we established a "point in time" for
      // completing this Read in which we read as much tail data (unsynced) as
      // we could.

      // We got pos_at_last_sync info if we got any unsynced data.
      assert(pos_at_last_sync >= 0 || unsynced_result.size() == 0);

      // Combined data is already lined up in scratch.
      assert(result->data() + result->size() == unsynced_result.data());
      assert(result->size() + unsynced_result.size() <= n);
      // Combine results
      *result = Slice(result->data(), result->size() + unsynced_result.size());
    } else {
      // 3. Any unsynced data we got was after unsynced_read_pos because the
      // file was synced some time since our last target Read (either from this
      // Read or a prior Read). We need to read more data from target to ensure
      // this Read is filled out, even though we might have already read some
      // (but not all due to a race). This code handles:
      //
      // * Catching up target after prior read(s) of unsynced data
      // * Racing Sync in another thread since we called target Read above
      //
      // And merging potentially three results together for this Read:
      // * The original target Read above
      // * The following (non-throw-away) target Read
      // * The ReadUnsynced above, which is always last if it returned data,
      // so that we have a "point in time" for completing this Read in which we
      // read as much tail data (unsynced) as we could.
      //
      // Deeper note about the race: we cannot just treat the original target
      // Read as a "point in time" view of available data in the file, because
      // there might have been unsynced data at that time, which became synced
      // data by the time we read unsynced data. That is the race we are
      // resolving with this "double check"-style code.
      const size_t supplemental_read_pos = unsynced_read_pos;

      // First, if there's any data from target that we know we would need to
      // throw away to catch up, try to do it.
      if (target_read_pos_ < supplemental_read_pos) {
        Slice throw_away_result;
        size_t throw_away_n = supplemental_read_pos - target_read_pos_;
        std::unique_ptr<char[]> throw_away_scratch{new char[throw_away_n]};
        s = target()->Read(throw_away_n, options, &throw_away_result,
                           throw_away_scratch.get(), dbg);
        if (!s.ok()) {
          read_pos_ += result->size();
          return s;
        }
        target_read_pos_ += throw_away_result.size();
        if (target_read_pos_ < supplemental_read_pos) {
          // Because of pos_at_last_sync > supplemental_read_pos, we should
          // have been able to catch up
          read_pos_ += result->size();
          return IOStatus::IOError(
              "Unexpected truncation or short read of file " + fname_);
        }
      }
      // Now we can do a productive supplemental Read from target
      assert(target_read_pos_ == supplemental_read_pos);
      Slice supplemental_result;
      size_t supplemental_n =
          unsynced_result.size() == 0
              ? n - offset_from_read_pos
              : unsynced_result.data() - (scratch + offset_from_read_pos);
      s = target()->Read(supplemental_n, options, &supplemental_result,
                         scratch + offset_from_read_pos, dbg);
      if (!s.ok()) {
        read_pos_ += result->size();
        return s;
      }
      target_read_pos_ += supplemental_result.size();
      MoveToScratchIfNeeded(&supplemental_result,
                            scratch + offset_from_read_pos);

      // Combined data is already lined up in scratch.
      assert(result->data() + result->size() == supplemental_result.data());
      assert(unsynced_result.size() == 0 ||
             supplemental_result.data() + supplemental_result.size() ==
                 unsynced_result.data());
      assert(result->size() + supplemental_result.size() +
                 unsynced_result.size() <=
             n);
      // Combine results
      *result =
          Slice(result->data(), result->size() + supplemental_result.size() +
                                    unsynced_result.size());
    }
  }
  read_pos_ += result->size();

  return s;
}

IOStatus TestFSSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                              const IOOptions& options,
                                              Slice* result, char* scratch,
                                              IODebugContext* dbg) {
  IOStatus s = fs_->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kRead, options, "",
      FaultInjectionTestFS::ErrorOperation::kRead, result, use_direct_io(),
      scratch, true /*need_count_increase=*/, nullptr /* fault_injected */);
  if (!s.ok()) {
    return s;
  }

  s = target()->PositionedRead(offset, n, options, result, scratch, dbg);
  // TODO (low priority): fs_->ReadUnsyncedData()
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

IOStatus FaultInjectionTestFS::FileExists(const std::string& fname,
                                          const IOOptions& options,
                                          IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }

  IOStatus io_s =
      MaybeInjectThreadLocalError(FaultInjectionIOType::kMetadataRead, options);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->FileExists(fname, options, dbg);
  return io_s;
}

IOStatus FaultInjectionTestFS::GetChildren(const std::string& dir,
                                           const IOOptions& options,
                                           std::vector<std::string>* result,
                                           IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }

  IOStatus io_s =
      MaybeInjectThreadLocalError(FaultInjectionIOType::kMetadataRead, options);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->GetChildren(dir, options, result, dbg);
  return io_s;
}

IOStatus FaultInjectionTestFS::GetChildrenFileAttributes(
    const std::string& dir, const IOOptions& options,
    std::vector<FileAttributes>* result, IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }

  IOStatus io_s =
      MaybeInjectThreadLocalError(FaultInjectionIOType::kMetadataRead, options);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->GetChildrenFileAttributes(dir, options, result, dbg);
  return io_s;
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

  IOStatus io_s = MaybeInjectThreadLocalError(
      FaultInjectionIOType::kWrite, file_opts.io_options, fname,
      FaultInjectionTestFS::ErrorOperation::kOpen);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->NewWritableFile(fname, file_opts, result, dbg);
  if (io_s.ok()) {
    result->reset(
        new TestFSWritableFile(fname, file_opts, std::move(*result), this));
    // WritableFileWriter* file is opened
    // again then it will be truncated - so forget our saved state.
    UntrackFile(fname);
    {
      MutexLock l(&mutex_);
      open_managed_files_.insert(fname);
      auto dir_and_name = TestFSGetDirAndName(fname);
      auto& list = dir_to_new_files_since_last_sync_[dir_and_name.first];
      // The new file could overwrite an old one. Here we simplify
      // the implementation by assuming no file of this name after
      // dropping unsynced files.
      list[dir_and_name.second] = kNewFileNoOverwrite;
    }
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
  IOStatus io_s = MaybeInjectThreadLocalError(FaultInjectionIOType::kWrite,
                                              file_opts.io_options, fname);
  if (!io_s.ok()) {
    return io_s;
  }

  bool exists;
  IOStatus exists_s =
      target()->FileExists(fname, IOOptions(), nullptr /* dbg */);
  if (exists_s.IsNotFound()) {
    exists = false;
  } else if (exists_s.ok()) {
    exists = true;
  } else {
    io_s = exists_s;
    exists = false;
  }

  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->ReopenWritableFile(fname, file_opts, result, dbg);

  // Only track files we created. Files created outside of this
  // `FaultInjectionTestFS` are not eligible for tracking/data dropping
  // (for example, they may contain data a previous db_stress run expects to
  // be recovered). This could be extended to track/drop data appended once
  // the file is under `FaultInjectionTestFS`'s control.
  if (io_s.ok()) {
    bool should_track;
    {
      MutexLock l(&mutex_);
      if (db_file_state_.find(fname) != db_file_state_.end()) {
        // It was written by this `FileSystem` earlier.
        assert(exists);
        should_track = true;
      } else if (!exists) {
        // It was created by this `FileSystem` just now.
        should_track = true;
        open_managed_files_.insert(fname);
        auto dir_and_name = TestFSGetDirAndName(fname);
        auto& list = dir_to_new_files_since_last_sync_[dir_and_name.first];
        list[dir_and_name.second] = kNewFileNoOverwrite;
      } else {
        should_track = false;
      }
    }
    if (should_track) {
      result->reset(
          new TestFSWritableFile(fname, file_opts, std::move(*result), this));
    }
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    const FileOptions& file_opts, std::unique_ptr<FSWritableFile>* result,
    IODebugContext* dbg) {
  IOStatus s = RenameFile(old_fname, fname, file_opts.io_options, dbg);
  if (!s.ok()) {
    return s;
  }
  return NewWritableFile(fname, file_opts, result, dbg);
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
  IOStatus io_s = MaybeInjectThreadLocalError(FaultInjectionIOType::kWrite,
                                              file_opts.io_options, fname);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->NewRandomRWFile(fname, file_opts, result, dbg);

  if (io_s.ok()) {
    result->reset(new TestFSRandomRWFile(fname, std::move(*result), this));
    // WritableFileWriter* file is opened
    // again then it will be truncated - so forget our saved state.
    UntrackFile(fname);
    {
      MutexLock l(&mutex_);
      open_managed_files_.insert(fname);
      auto dir_and_name = TestFSGetDirAndName(fname);
      auto& list = dir_to_new_files_since_last_sync_[dir_and_name.first];
      // It could be overwriting an old file, but we simplify the
      // implementation by ignoring it.
      list[dir_and_name.second] = kNewFileNoOverwrite;
    }
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::NewRandomAccessFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s = MaybeInjectThreadLocalError(
      FaultInjectionIOType::kRead, file_opts.io_options, fname,
      ErrorOperation::kOpen, nullptr /* result */, false /* direct_io */,
      nullptr /* scratch */, true /*need_count_increase*/,
      nullptr /*fault_injected*/);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->NewRandomAccessFile(fname, file_opts, result, dbg);

  if (io_s.ok()) {
    result->reset(new TestFSRandomAccessFile(fname, std::move(*result), this));
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::NewSequentialFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s = MaybeInjectThreadLocalError(
      FaultInjectionIOType::kRead, file_opts.io_options, fname,
      ErrorOperation::kOpen, nullptr /* result */, false /* direct_io */,
      nullptr /* scratch */, true /*need_count_increase*/,
      nullptr /*fault_injected*/);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->NewSequentialFile(fname, file_opts, result, dbg);

  if (io_s.ok()) {
    result->reset(new TestFSSequentialFile(std::move(*result), this, fname));
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::DeleteFile(const std::string& f,
                                          const IOOptions& options,
                                          IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s = MaybeInjectThreadLocalError(
      FaultInjectionIOType::kMetadataWrite, options);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = FileSystemWrapper::DeleteFile(f, options, dbg);

  if (io_s.ok()) {
    UntrackFile(f);
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::GetFileSize(const std::string& f,
                                           const IOOptions& options,
                                           uint64_t* file_size,
                                           IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s =
      MaybeInjectThreadLocalError(FaultInjectionIOType::kMetadataRead, options);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->GetFileSize(f, options, file_size, dbg);
  if (!io_s.ok()) {
    return io_s;
  }

  if (ReadUnsyncedData()) {
    // Need to report flushed size, not synced size
    MutexLock l(&mutex_);
    auto it = db_file_state_.find(f);
    if (it != db_file_state_.end()) {
      *file_size = it->second.pos_at_last_append_;
    }
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::GetFileModificationTime(const std::string& fname,
                                                       const IOOptions& options,
                                                       uint64_t* file_mtime,
                                                       IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s =
      MaybeInjectThreadLocalError(FaultInjectionIOType::kMetadataRead, options);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->GetFileModificationTime(fname, options, file_mtime, dbg);
  return io_s;
}

IOStatus FaultInjectionTestFS::RenameFile(const std::string& s,
                                          const std::string& t,
                                          const IOOptions& options,
                                          IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s = MaybeInjectThreadLocalError(
      FaultInjectionIOType::kMetadataWrite, options);
  if (!io_s.ok()) {
    return io_s;
  }

  // We preserve contents of overwritten files up to a size threshold.
  // We could keep previous file in another name, but we need to worry about
  // garbage collect the those files. We do it if it is needed later.
  // We ignore I/O errors here for simplicity.
  std::string previous_contents = kNewFileNoOverwrite;
  if (target()->FileExists(t, IOOptions(), nullptr).ok()) {
    uint64_t file_size;
    if (target()->GetFileSize(t, IOOptions(), &file_size, nullptr).ok() &&
        file_size < 1024) {
      ReadFileToString(target(), t, &previous_contents).PermitUncheckedError();
    }
  }
  io_s = FileSystemWrapper::RenameFile(s, t, options, dbg);

  if (io_s.ok()) {
    {
      MutexLock l(&mutex_);
      if (db_file_state_.find(s) != db_file_state_.end()) {
        db_file_state_[t] = db_file_state_[s];
        db_file_state_.erase(s);
      }

      auto sdn = TestFSGetDirAndName(s);
      auto tdn = TestFSGetDirAndName(t);
      if (dir_to_new_files_since_last_sync_[sdn.first].erase(sdn.second) != 0) {
        auto& tlist = dir_to_new_files_since_last_sync_[tdn.first];
        tlist[tdn.second] = previous_contents;
      }
    }
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::LinkFile(const std::string& s,
                                        const std::string& t,
                                        const IOOptions& options,
                                        IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s = MaybeInjectThreadLocalError(
      FaultInjectionIOType::kMetadataWrite, options);
  if (!io_s.ok()) {
    return io_s;
  }

  // Using the value in `dir_to_new_files_since_last_sync_` for the source file
  // may be a more reasonable choice.
  std::string previous_contents = kNewFileNoOverwrite;

  io_s = FileSystemWrapper::LinkFile(s, t, options, dbg);

  if (io_s.ok()) {
    {
      MutexLock l(&mutex_);
      if (!allow_link_open_file_ &&
          open_managed_files_.find(s) != open_managed_files_.end()) {
        fprintf(stderr, "Attempt to LinkFile while open for write: %s\n",
                s.c_str());
        abort();
      }
      if (db_file_state_.find(s) != db_file_state_.end()) {
        db_file_state_[t] = db_file_state_[s];
      }

      auto sdn = TestFSGetDirAndName(s);
      auto tdn = TestFSGetDirAndName(t);
      if (dir_to_new_files_since_last_sync_[sdn.first].find(sdn.second) !=
          dir_to_new_files_since_last_sync_[sdn.first].end()) {
        auto& tlist = dir_to_new_files_since_last_sync_[tdn.first];
        assert(tlist.find(tdn.second) == tlist.end());
        tlist[tdn.second] = previous_contents;
      }
    }
  }
  return io_s;
}

IOStatus FaultInjectionTestFS::NumFileLinks(const std::string& fname,
                                            const IOOptions& options,
                                            uint64_t* count,
                                            IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s =
      MaybeInjectThreadLocalError(FaultInjectionIOType::kMetadataRead, options);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->NumFileLinks(fname, options, count, dbg);
  return io_s;
}

IOStatus FaultInjectionTestFS::AreFilesSame(const std::string& first,
                                            const std::string& second,
                                            const IOOptions& options, bool* res,
                                            IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s =
      MaybeInjectThreadLocalError(FaultInjectionIOType::kMetadataRead, options);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->AreFilesSame(first, second, options, res, dbg);
  return io_s;
}

IOStatus FaultInjectionTestFS::GetAbsolutePath(const std::string& db_path,
                                               const IOOptions& options,
                                               std::string* output_path,
                                               IODebugContext* dbg) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s =
      MaybeInjectThreadLocalError(FaultInjectionIOType::kMetadataRead, options);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->GetAbsolutePath(db_path, options, output_path, dbg);
  return io_s;
}

IOStatus FaultInjectionTestFS::IsDirectory(const std::string& path,
                                           const IOOptions& options,
                                           bool* is_dir, IODebugContext* dgb) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  IOStatus io_s =
      MaybeInjectThreadLocalError(FaultInjectionIOType::kMetadataRead, options);
  if (!io_s.ok()) {
    return io_s;
  }

  io_s = target()->IsDirectory(path, options, is_dir, dgb);
  return io_s;
}

IOStatus FaultInjectionTestFS::Poll(std::vector<void*>& io_handles,
                                    size_t min_completions) {
  return target()->Poll(io_handles, min_completions);
}

IOStatus FaultInjectionTestFS::AbortIO(std::vector<void*>& io_handles) {
  return target()->AbortIO(io_handles);
}

void FaultInjectionTestFS::WritableFileClosed(const FSFileState& state) {
  MutexLock l(&mutex_);
  if (open_managed_files_.find(state.filename_) != open_managed_files_.end()) {
    db_file_state_[state.filename_] = state;
    open_managed_files_.erase(state.filename_);
  }
}

void FaultInjectionTestFS::WritableFileSynced(const FSFileState& state) {
  MutexLock l(&mutex_);
  if (open_managed_files_.find(state.filename_) != open_managed_files_.end()) {
    if (db_file_state_.find(state.filename_) == db_file_state_.end()) {
      db_file_state_.insert(std::make_pair(state.filename_, state));
    } else {
      db_file_state_[state.filename_] = state;
    }
  }
}

void FaultInjectionTestFS::WritableFileAppended(const FSFileState& state) {
  MutexLock l(&mutex_);
  if (open_managed_files_.find(state.filename_) != open_managed_files_.end()) {
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
  std::map<std::string, std::map<std::string, std::string>> map_copy;
  {
    MutexLock l(&mutex_);
    map_copy.insert(dir_to_new_files_since_last_sync_.begin(),
                    dir_to_new_files_since_last_sync_.end());
  }

  for (auto& pair : map_copy) {
    for (auto& file_pair : pair.second) {
      if (file_pair.second == kNewFileNoOverwrite) {
        IOStatus io_s =
            DeleteFile(pair.first + "/" + file_pair.first, options, dbg);
        if (!io_s.ok()) {
          return io_s;
        }
      } else {
        IOOptions opts;
        IOStatus io_s =
            WriteStringToFile(target(), file_pair.second,
                              pair.first + "/" + file_pair.first, true, opts);
        if (!io_s.ok()) {
          return io_s;
        }
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
  open_managed_files_.erase(f);
}

IOStatus FaultInjectionTestFS::MaybeInjectThreadLocalReadError(
    const IOOptions& io_options, ErrorOperation op, Slice* result,
    bool direct_io, char* scratch, bool need_count_increase,
    bool* fault_injected) {
  bool dummy_bool;
  bool& ret_fault_injected = fault_injected ? *fault_injected : dummy_bool;
  ret_fault_injected = false;
  ErrorContext* ctx =
      static_cast<ErrorContext*>(injected_thread_local_read_error_.Get());
  if (ctx == nullptr || !ctx->enable_error_injection || !ctx->one_in ||
      ShouldIOActivtiesExcludedFromFaultInjection(io_options.io_activity)) {
    return IOStatus::OK();
  }

  IOStatus ret;
  if (ctx->rand.OneIn(ctx->one_in)) {
    if (ctx->count == 0) {
      ctx->message = "";
    }
    if (need_count_increase) {
      ctx->count++;
    }
    if (ctx->callstack) {
      free(ctx->callstack);
    }
    ctx->callstack = port::SaveStack(&ctx->frames);

    std::stringstream msg;
    msg << FaultInjectionTestFS::kInjected << " ";
    if (op != ErrorOperation::kMultiReadSingleReq) {
      // Likely non-per read status code for MultiRead
      msg << "read error";
      ctx->message = msg.str();
      ret_fault_injected = true;
      ret = IOStatus::IOError(ctx->message);
    } else if (Random::GetTLSInstance()->OneIn(8)) {
      assert(result);
      // For a small chance, set the failure to status but turn the
      // result to be empty, which is supposed to be caught for a check.
      *result = Slice();
      msg << "empty result";
      ctx->message = msg.str();
      ret_fault_injected = true;
    } else if (!direct_io && Random::GetTLSInstance()->OneIn(7) &&
               scratch != nullptr && result->data() == scratch) {
      assert(result);
      // With direct I/O, many extra bytes might be read so corrupting
      // one byte might not cause checksum mismatch. Skip checksum
      // corruption injection.
      // We only corrupt data if the result is filled to `scratch`. For other
      // cases, the data might not be able to be modified (e.g mmaped files)
      // or has unintended side effects.
      // For a small chance, set the failure to status but corrupt the
      // result in a way that checksum checking is supposed to fail.
      // Corrupt the last byte, which is supposed to be a checksum byte
      // It would work for CRC. Not 100% sure for xxhash and will adjust
      // if it is not the case.
      const_cast<char*>(result->data())[result->size() - 1]++;
      msg << "corrupt last byte";
      ctx->message = msg.str();
      ret_fault_injected = true;
    } else {
      msg << "error result multiget single";
      ctx->message = msg.str();
      ret_fault_injected = true;
      ret = IOStatus::IOError(ctx->message);
    }
  }

  ret.SetRetryable(ctx->retryable);
  ret.SetDataLoss(ctx->has_data_loss);
  return ret;
}

bool FaultInjectionTestFS::TryParseFileName(const std::string& file_name,
                                            uint64_t* number, FileType* type) {
  std::size_t found = file_name.find_last_of('/');
  std::string file = file_name.substr(found);
  return ParseFileName(file, number, type);
}

IOStatus FaultInjectionTestFS::MaybeInjectThreadLocalError(
    FaultInjectionIOType type, const IOOptions& io_options,
    const std::string& file_name, ErrorOperation op, Slice* result,
    bool direct_io, char* scratch, bool need_count_increase,
    bool* fault_injected) {
  if (type == FaultInjectionIOType::kRead) {
    return MaybeInjectThreadLocalReadError(io_options, op, result, direct_io,
                                           scratch, need_count_increase,
                                           fault_injected);
  }

  ErrorContext* ctx = GetErrorContextFromFaultInjectionIOType(type);
  if (ctx == nullptr || !ctx->enable_error_injection || !ctx->one_in ||
      ShouldIOActivtiesExcludedFromFaultInjection(io_options.io_activity) ||
      (type == FaultInjectionIOType::kWrite &&
       ShouldExcludeFromWriteFaultInjection(file_name))) {
    return IOStatus::OK();
  }

  IOStatus ret;
  if (ctx->rand.OneIn(ctx->one_in)) {
    ctx->count++;
    if (ctx->callstack) {
      free(ctx->callstack);
    }
    ctx->callstack = port::SaveStack(&ctx->frames);
    ctx->message = GetErrorMessage(type, file_name, op);
    ret = IOStatus::IOError(ctx->message);
    ret.SetRetryable(ctx->retryable);
    ret.SetDataLoss(ctx->has_data_loss);
    if (type == FaultInjectionIOType::kWrite) {
      TEST_SYNC_POINT(
          "FaultInjectionTestFS::InjectMetadataWriteError:Injected");
    }
  }
  return ret;
}

void FaultInjectionTestFS::PrintInjectedThreadLocalErrorBacktrace(
    FaultInjectionIOType type) {
#if defined(OS_LINUX)
  ErrorContext* ctx = GetErrorContextFromFaultInjectionIOType(type);
  if (ctx) {
    if (type == FaultInjectionIOType::kRead) {
      fprintf(stderr, "Injected read error type = %d\n", ctx->type);
    }
    fprintf(stderr, "Message: %s\n", ctx->message.c_str());
    port::PrintAndFreeStack(ctx->callstack, ctx->frames);
    ctx->callstack = nullptr;
  }
#else
  (void)type;
#endif
}
}  // namespace ROCKSDB_NAMESPACE
