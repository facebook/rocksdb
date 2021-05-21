//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "tools/simulated_hybrid_file_system.h"

#include <algorithm>
#include <sstream>
#include <string>

#include "rocksdb/rate_limiter.h"

namespace ROCKSDB_NAMESPACE {

const int kLatencyAddedPerRequestUs = 15000;
const int64_t kRequestPerSec = 100;
const int64_t kDummyBytesPerRequest = 1024 * 1024;

// The metadata file format: each line is a full filename of a file which is
// warm
SimulatedHybridFileSystem::SimulatedHybridFileSystem(
    const std::shared_ptr<FileSystem>& base,
    const std::string& metadata_file_name)
    : FileSystemWrapper(base),
      // Limit to 100 requests per second.
      rate_limiter_(NewGenericRateLimiter(
          kDummyBytesPerRequest * kRequestPerSec /* rate_bytes_per_sec */,
          1000 /* refill_period_us */)),
      metadata_file_name_(metadata_file_name),
      name_("SimulatedHybridFileSystem: " + std::string(target()->Name())) {
  IOStatus s = base->FileExists(metadata_file_name, IOOptions(), nullptr);
  if (s.IsNotFound()) {
    return;
  }
  std::string metadata;
  s = ReadFileToString(base.get(), metadata_file_name, &metadata);
  if (!s.ok()) {
    fprintf(stderr, "Error reading from file %s: %s",
            metadata_file_name.c_str(), s.ToString().c_str());
    // Exit rather than assert as this file system is built to run with
    // benchmarks, which usually run on release mode.
    std::exit(1);
  }
  std::istringstream input;
  input.str(metadata);
  std::string line;
  while (std::getline(input, line)) {
    fprintf(stderr, "Warm file %s\n", line.c_str());
    warm_file_set_.insert(line);
  }
}

// Need to write out the metadata file to file. See comment of
// SimulatedHybridFileSystem::SimulatedHybridFileSystem() for format of the
// file.
SimulatedHybridFileSystem::~SimulatedHybridFileSystem() {
  std::string metadata;
  for (const auto& f : warm_file_set_) {
    metadata += f;
    metadata += "\n";
  }
  IOStatus s = WriteStringToFile(target(), metadata, metadata_file_name_, true);
  if (!s.ok()) {
    fprintf(stderr, "Error writing to file %s: %s", metadata_file_name_.c_str(),
            s.ToString().c_str());
  }
}

IOStatus SimulatedHybridFileSystem::NewRandomAccessFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  Temperature temperature = Temperature::kUnknown;
  {
    const std::lock_guard<std::mutex> lock(mutex_);
    if (warm_file_set_.find(fname) != warm_file_set_.end()) {
      temperature = Temperature::kWarm;
    }
  }
  IOStatus s = target()->NewRandomAccessFile(fname, file_opts, result, dbg);
  result->reset(
      new SimulatedHybridRaf(result->release(), rate_limiter_, temperature));
  return s;
}

IOStatus SimulatedHybridFileSystem::NewWritableFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  if (file_opts.temperature == Temperature::kWarm) {
    const std::lock_guard<std::mutex> lock(mutex_);
    fprintf(stderr, "warm file %s\n", fname.c_str());
    warm_file_set_.insert(fname);
  }
  return target()->NewWritableFile(fname, file_opts, result, dbg);
}

IOStatus SimulatedHybridFileSystem::DeleteFile(const std::string& fname,
                                               const IOOptions& options,
                                               IODebugContext* dbg) {
  {
    const std::lock_guard<std::mutex> lock(mutex_);
    warm_file_set_.erase(fname);
  }
  return target()->DeleteFile(fname, options, dbg);
}

IOStatus SimulatedHybridRaf::Read(uint64_t offset, size_t n,
                                  const IOOptions& options, Slice* result,
                                  char* scratch, IODebugContext* dbg) const {
  if (temperature_ == Temperature::kWarm) {
    Env::Default()->SleepForMicroseconds(kLatencyAddedPerRequestUs);
    RequestRateLimit(1);
  }
  return target()->Read(offset, n, options, result, scratch, dbg);
}

IOStatus SimulatedHybridRaf::MultiRead(FSReadRequest* reqs, size_t num_reqs,
                                       const IOOptions& options,
                                       IODebugContext* dbg) {
  if (temperature_ == Temperature::kWarm) {
    RequestRateLimit(static_cast<int64_t>(num_reqs));
    Env::Default()->SleepForMicroseconds(kLatencyAddedPerRequestUs *
                                         static_cast<int>(num_reqs));
  }
  return target()->MultiRead(reqs, num_reqs, options, dbg);
}

IOStatus SimulatedHybridRaf::Prefetch(uint64_t offset, size_t n,
                                      const IOOptions& options,
                                      IODebugContext* dbg) {
  if (temperature_ == Temperature::kWarm) {
    RequestRateLimit(1);
    Env::Default()->SleepForMicroseconds(kLatencyAddedPerRequestUs);
  }
  return target()->Prefetch(offset, n, options, dbg);
}

void SimulatedHybridRaf::RequestRateLimit(int64_t num_requests) const {
  int64_t left = num_requests * kDummyBytesPerRequest;
  const int64_t kMaxToRequest = kDummyBytesPerRequest / 100;
  while (left > 0) {
    int64_t to_request = std::min(kMaxToRequest, left);
    rate_limiter_->Request(to_request, Env::IOPriority::IO_LOW, nullptr);
    left -= to_request;
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
