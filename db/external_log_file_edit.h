//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "logging/event_logger.h"
#include "rocksdb/external_log_file.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter;
class Slice;
class Status;

using ExternalLogFileNumber = uint64_t;

class ExternalLogFileMetadata {
 public:
  ExternalLogFileMetadata() = default;

  ExternalLogFileMetadata(std::string name, ExternalLogFilePathType path_type,
                          ExternalLogFileState state, uint64_t durable_size,
                          std::string file_checksum,
                          std::string file_checksum_func_name,
                          Temperature temperature)
      : name_(std::move(name)),
        path_type_(path_type),
        state_(state),
        durable_size_(durable_size),
        file_checksum_(std::move(file_checksum)),
        file_checksum_func_name_(std::move(file_checksum_func_name)),
        temperature_(temperature) {}

  const std::string& GetName() const { return name_; }
  ExternalLogFilePathType GetPathType() const { return path_type_; }
  ExternalLogFileState GetState() const { return state_; }
  uint64_t GetDurableSize() const { return durable_size_; }
  const std::string& GetFileChecksum() const { return file_checksum_; }
  const std::string& GetFileChecksumFuncName() const {
    return file_checksum_func_name_;
  }
  Temperature GetTemperature() const { return temperature_; }

  void SetState(ExternalLogFileState state) { state_ = state; }
  void SetDurableSize(uint64_t durable_size) { durable_size_ = durable_size; }
  void SetFileChecksum(std::string file_checksum,
                       std::string file_checksum_func_name) {
    file_checksum_ = std::move(file_checksum);
    file_checksum_func_name_ = std::move(file_checksum_func_name);
  }

  std::string DebugString() const;

 private:
  friend bool operator==(const ExternalLogFileMetadata& lhs,
                         const ExternalLogFileMetadata& rhs);
  friend bool operator!=(const ExternalLogFileMetadata& lhs,
                         const ExternalLogFileMetadata& rhs);

  std::string name_;
  ExternalLogFilePathType path_type_ = ExternalLogFilePathType::kDBRelativePath;
  ExternalLogFileState state_ = ExternalLogFileState::kUnsealed;
  uint64_t durable_size_ = 0;
  std::string file_checksum_ = kUnknownFileChecksum;
  std::string file_checksum_func_name_ = kUnknownFileChecksumFuncName;
  Temperature temperature_ = Temperature::kUnknown;
};

inline bool operator==(const ExternalLogFileMetadata& lhs,
                       const ExternalLogFileMetadata& rhs) {
  return lhs.name_ == rhs.name_ && lhs.path_type_ == rhs.path_type_ &&
         lhs.state_ == rhs.state_ && lhs.durable_size_ == rhs.durable_size_ &&
         lhs.file_checksum_ == rhs.file_checksum_ &&
         lhs.file_checksum_func_name_ == rhs.file_checksum_func_name_ &&
         lhs.temperature_ == rhs.temperature_;
}

inline bool operator!=(const ExternalLogFileMetadata& lhs,
                       const ExternalLogFileMetadata& rhs) {
  return !(lhs == rhs);
}

enum class ExternalLogFileAdditionTag : uint32_t {
  kTerminate = 1,
  kName = 2,
  kState = 3,
  kDurableSize = 4,
  kFileChecksum = 5,
  kFileChecksumFuncName = 6,
  kTemperature = 7,
  kPathType = 8,
  kSafeIgnoreMask = 1 << 16,
};

class ExternalLogFileAddition {
 public:
  ExternalLogFileAddition() = default;

  ExternalLogFileAddition(ExternalLogFileNumber number,
                          ExternalLogFileMetadata metadata)
      : number_(number), metadata_(std::move(metadata)) {}

  ExternalLogFileNumber GetFileNumber() const { return number_; }
  const ExternalLogFileMetadata& GetMetadata() const { return metadata_; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  std::string DebugString() const;

 private:
  ExternalLogFileNumber number_ = 0;
  ExternalLogFileMetadata metadata_;
};

std::ostream& operator<<(std::ostream& os, const ExternalLogFileAddition& file);
JSONWriter& operator<<(JSONWriter& jw, const ExternalLogFileAddition& file);

using ExternalLogFileAdditions = std::vector<ExternalLogFileAddition>;

class ExternalLogFileDeletion {
 public:
  ExternalLogFileDeletion() = default;
  explicit ExternalLogFileDeletion(std::string name) : name_(std::move(name)) {}

  const std::string& GetName() const { return name_; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  std::string DebugString() const;

 private:
  std::string name_;
};

std::ostream& operator<<(std::ostream& os, const ExternalLogFileDeletion& file);
JSONWriter& operator<<(JSONWriter& jw, const ExternalLogFileDeletion& file);

using ExternalLogFileDeletions = std::vector<ExternalLogFileDeletion>;

// Current MANIFEST-tracked external log files.
//
// Not thread safe. Callers must hold the DB mutex, or be in single-threaded
// MANIFEST recovery.
class ExternalLogFileSet {
 public:
  Status AddFile(const ExternalLogFileAddition& file);
  Status AddFiles(const ExternalLogFileAdditions& files);

  void DeleteFile(const ExternalLogFileDeletion& file);
  Status DeleteFiles(const ExternalLogFileDeletions& files);

  void Reset();

  const std::map<ExternalLogFileNumber, ExternalLogFileMetadata>& GetFiles()
      const {
    return files_;
  }

  const ExternalLogFileMetadata* FindFile(const std::string& name,
                                          ExternalLogFileNumber* number) const;

 private:
  std::map<ExternalLogFileNumber, ExternalLogFileMetadata> files_;
  std::unordered_map<std::string, ExternalLogFileNumber> name_to_number_;
};

}  // namespace ROCKSDB_NAMESPACE
