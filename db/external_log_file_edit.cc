//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/external_log_file_edit.h"

#include <sstream>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

namespace {

void PutLengthPrefixedVarint32Field(std::string* dst, uint32_t tag,
                                    uint32_t value) {
  PutVarint32(dst, tag);
  std::string field;
  PutVarint32(&field, value);
  PutLengthPrefixedSlice(dst, field);
}

void PutLengthPrefixedVarint64Field(std::string* dst, uint32_t tag,
                                    uint64_t value) {
  PutVarint32(dst, tag);
  std::string field;
  PutVarint64(&field, value);
  PutLengthPrefixedSlice(dst, field);
}

const char* ExternalLogFileStateToString(ExternalLogFileState state) {
  switch (state) {
    case ExternalLogFileState::kUnsealed:
      return "unsealed";
    case ExternalLogFileState::kSealed:
      return "sealed";
  }
  return "unknown";
}

const char* ExternalLogFilePathTypeToString(ExternalLogFilePathType path_type) {
  switch (path_type) {
    case ExternalLogFilePathType::kDBRelativePath:
      return "db_relative";
    case ExternalLogFilePathType::kExternalPath:
      return "external";
  }
  return "unknown";
}

}  // namespace

std::string ExternalLogFileMetadata::DebugString() const {
  std::ostringstream oss;
  oss << "name: " << name_
      << " path_type: " << ExternalLogFilePathTypeToString(path_type_)
      << " state: " << ExternalLogFileStateToString(state_)
      << " durable_size: " << durable_size_ << " file_checksum: ";
  oss << Slice(file_checksum_).ToString(true);
  oss << " file_checksum_func_name: " << file_checksum_func_name_
      << " temperature: " << static_cast<int>(temperature_);
  return oss.str();
}

void ExternalLogFileAddition::EncodeTo(std::string* dst) const {
  PutVarint64(dst, number_);

  PutVarint32(dst, static_cast<uint32_t>(ExternalLogFileAdditionTag::kName));
  PutLengthPrefixedSlice(dst, metadata_.GetName());

  if (metadata_.GetPathType() != ExternalLogFilePathType::kDBRelativePath) {
    PutLengthPrefixedVarint32Field(
        dst, static_cast<uint32_t>(ExternalLogFileAdditionTag::kPathType),
        static_cast<uint32_t>(metadata_.GetPathType()));
  }

  PutLengthPrefixedVarint32Field(
      dst, static_cast<uint32_t>(ExternalLogFileAdditionTag::kState),
      static_cast<uint32_t>(metadata_.GetState()));

  PutLengthPrefixedVarint64Field(
      dst, static_cast<uint32_t>(ExternalLogFileAdditionTag::kDurableSize),
      metadata_.GetDurableSize());

  PutVarint32(dst,
              static_cast<uint32_t>(ExternalLogFileAdditionTag::kFileChecksum));
  PutLengthPrefixedSlice(dst, metadata_.GetFileChecksum());

  PutVarint32(dst, static_cast<uint32_t>(
                       ExternalLogFileAdditionTag::kFileChecksumFuncName));
  PutLengthPrefixedSlice(dst, metadata_.GetFileChecksumFuncName());

  PutLengthPrefixedVarint32Field(
      dst, static_cast<uint32_t>(ExternalLogFileAdditionTag::kTemperature),
      static_cast<uint32_t>(metadata_.GetTemperature()));

  PutVarint32(dst,
              static_cast<uint32_t>(ExternalLogFileAdditionTag::kTerminate));
}

Status ExternalLogFileAddition::DecodeFrom(Slice* src) {
  constexpr char class_name[] = "ExternalLogFileAddition";

  if (!GetVarint64(src, &number_)) {
    return Status::Corruption(class_name,
                              "Error decoding external log file number");
  }

  std::string name;
  ExternalLogFilePathType path_type = ExternalLogFilePathType::kDBRelativePath;
  ExternalLogFileState state = ExternalLogFileState::kUnsealed;
  uint64_t durable_size = 0;
  std::string file_checksum = kUnknownFileChecksum;
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;
  Temperature temperature = Temperature::kUnknown;

  while (true) {
    uint32_t tag_value = 0;
    if (!GetVarint32(src, &tag_value)) {
      return Status::Corruption(class_name, "Error decoding tag");
    }

    ExternalLogFileAdditionTag tag =
        static_cast<ExternalLogFileAdditionTag>(tag_value);
    if (tag == ExternalLogFileAdditionTag::kTerminate) {
      if (number_ == 0 || name.empty()) {
        return Status::Corruption(class_name,
                                  "Missing external log file number or name");
      }
      if (file_checksum_func_name.empty()) {
        return Status::Corruption(class_name,
                                  "Missing file checksum function name");
      }
      if (file_checksum_func_name == kUnknownFileChecksumFuncName &&
          !file_checksum.empty()) {
        return Status::Corruption(
            class_name,
            "Unknown file checksum function has non-empty checksum");
      }
      if (file_checksum_func_name != kUnknownFileChecksumFuncName &&
          file_checksum.empty()) {
        return Status::Corruption(class_name,
                                  "File checksum function has empty checksum");
      }
      metadata_ = ExternalLogFileMetadata(
          std::move(name), path_type, state, durable_size,
          std::move(file_checksum), std::move(file_checksum_func_name),
          temperature);
      return Status::OK();
    }

    Slice field;
    if (!GetLengthPrefixedSlice(src, &field)) {
      return Status::Corruption(class_name,
                                "Error decoding length-prefixed field");
    }

    switch (tag) {
      case ExternalLogFileAdditionTag::kName:
        name = field.ToString();
        break;
      case ExternalLogFileAdditionTag::kPathType: {
        uint32_t path_type_value = 0;
        if (!GetVarint32(&field, &path_type_value)) {
          return Status::Corruption(class_name,
                                    "Error decoding external log file path "
                                    "type");
        }
        if (path_type_value >
            static_cast<uint32_t>(ExternalLogFilePathType::kExternalPath)) {
          return Status::Corruption(class_name,
                                    "Invalid external log file path type");
        }
        path_type = static_cast<ExternalLogFilePathType>(path_type_value);
        break;
      }
      case ExternalLogFileAdditionTag::kState: {
        uint32_t state_value = 0;
        if (!GetVarint32(&field, &state_value)) {
          return Status::Corruption(class_name,
                                    "Error decoding external log file state");
        }
        if (state_value >
            static_cast<uint32_t>(ExternalLogFileState::kSealed)) {
          return Status::Corruption(class_name,
                                    "Invalid external log file state");
        }
        state = static_cast<ExternalLogFileState>(state_value);
        break;
      }
      case ExternalLogFileAdditionTag::kDurableSize:
        if (!GetVarint64(&field, &durable_size)) {
          return Status::Corruption(class_name,
                                    "Error decoding durable file size");
        }
        break;
      case ExternalLogFileAdditionTag::kFileChecksum:
        file_checksum = field.ToString();
        break;
      case ExternalLogFileAdditionTag::kFileChecksumFuncName:
        file_checksum_func_name = field.ToString();
        break;
      case ExternalLogFileAdditionTag::kTemperature: {
        uint32_t temperature_value = 0;
        if (!GetVarint32(&field, &temperature_value)) {
          return Status::Corruption(class_name, "Error decoding temperature");
        }
        if (temperature_value >=
            static_cast<uint32_t>(Temperature::kLastTemperature)) {
          return Status::Corruption(class_name, "Invalid temperature");
        }
        temperature = static_cast<Temperature>(temperature_value);
        break;
      }
      case ExternalLogFileAdditionTag::kTerminate:
        assert(false);
        break;
      default:
        if ((tag_value & static_cast<uint32_t>(
                             ExternalLogFileAdditionTag::kSafeIgnoreMask)) ==
            0) {
          std::stringstream ss;
          ss << "Unknown external log file addition tag " << tag_value;
          return Status::Corruption(class_name, ss.str());
        }
        break;
    }
  }
}

JSONWriter& operator<<(JSONWriter& jw, const ExternalLogFileAddition& file) {
  jw << "FileNumber" << file.GetFileNumber() << "Name"
     << file.GetMetadata().GetName() << "PathType"
     << ExternalLogFilePathTypeToString(file.GetMetadata().GetPathType())
     << "State" << ExternalLogFileStateToString(file.GetMetadata().GetState())
     << "DurableSize" << file.GetMetadata().GetDurableSize() << "FileChecksum"
     << Slice(file.GetMetadata().GetFileChecksum()).ToString(true)
     << "FileChecksumFuncName" << file.GetMetadata().GetFileChecksumFuncName()
     << "Temperature" << static_cast<int>(file.GetMetadata().GetTemperature());
  return jw;
}

std::ostream& operator<<(std::ostream& os,
                         const ExternalLogFileAddition& file) {
  os << "file_number: " << file.GetFileNumber() << " "
     << file.GetMetadata().DebugString();
  return os;
}

std::string ExternalLogFileAddition::DebugString() const {
  std::ostringstream oss;
  oss << *this;
  return oss.str();
}

void ExternalLogFileDeletion::EncodeTo(std::string* dst) const {
  PutLengthPrefixedSlice(dst, name_);
}

Status ExternalLogFileDeletion::DecodeFrom(Slice* src) {
  Slice name;
  if (!GetLengthPrefixedSlice(src, &name)) {
    return Status::Corruption("ExternalLogFileDeletion",
                              "Error decoding external log file name");
  }
  name_ = name.ToString();
  if (name_.empty()) {
    return Status::Corruption("ExternalLogFileDeletion",
                              "External log file name is empty");
  }
  return Status::OK();
}

JSONWriter& operator<<(JSONWriter& jw, const ExternalLogFileDeletion& file) {
  jw << "Name" << file.GetName();
  return jw;
}

std::ostream& operator<<(std::ostream& os,
                         const ExternalLogFileDeletion& file) {
  os << "name: " << file.GetName();
  return os;
}

std::string ExternalLogFileDeletion::DebugString() const {
  std::ostringstream oss;
  oss << *this;
  return oss.str();
}

Status ExternalLogFileSet::AddFile(const ExternalLogFileAddition& file) {
  const ExternalLogFileNumber number = file.GetFileNumber();
  const ExternalLogFileMetadata& metadata = file.GetMetadata();
  const std::string& name = metadata.GetName();

  if (number == 0 || name.empty()) {
    return Status::Corruption("ExternalLogFileSet::AddFile",
                              "external log file number or name is empty");
  }

  auto name_iter = name_to_number_.find(name);
  if (name_iter != name_to_number_.end() && name_iter->second != number) {
    std::stringstream ss;
    ss << "External log file name " << name << " already maps to file "
       << name_iter->second << " but manifest added file " << number;
    return Status::Corruption("ExternalLogFileSet::AddFile", ss.str());
  }
  auto file_iter = files_.find(number);
  if (file_iter == files_.end()) {
    files_.emplace(number, metadata);
    name_to_number_.emplace(name, number);
    return Status::OK();
  }

  if (file_iter->second.GetName() != name) {
    std::stringstream ss;
    ss << "External log file number " << number << " already maps to name "
       << file_iter->second.GetName() << " but manifest added name " << name;
    return Status::Corruption("ExternalLogFileSet::AddFile", ss.str());
  }

  if (file_iter->second.GetPathType() != metadata.GetPathType()) {
    std::stringstream ss;
    ss << "External log file " << name << " changed path type from "
       << ExternalLogFilePathTypeToString(file_iter->second.GetPathType())
       << " to " << ExternalLogFilePathTypeToString(metadata.GetPathType());
    return Status::Corruption("ExternalLogFileSet::AddFile", ss.str());
  }

  if (metadata.GetDurableSize() < file_iter->second.GetDurableSize()) {
    std::stringstream ss;
    ss << "Manifest attempted to shrink external log file " << name << " from "
       << file_iter->second.GetDurableSize() << " bytes to "
       << metadata.GetDurableSize() << " bytes";
    return Status::Corruption("ExternalLogFileSet::AddFile", ss.str());
  }

  if (file_iter->second.GetState() == ExternalLogFileState::kSealed &&
      file_iter->second != metadata) {
    std::stringstream ss;
    ss << "Manifest attempted to modify sealed external log file " << name;
    return Status::Corruption("ExternalLogFileSet::AddFile", ss.str());
  }

  if (file_iter->second.GetState() == ExternalLogFileState::kUnsealed &&
      metadata.GetState() == ExternalLogFileState::kUnsealed &&
      file_iter->second != metadata) {
    std::stringstream ss;
    ss << "Manifest attempted to modify unsealed external log file " << name
       << " without sealing it";
    return Status::Corruption("ExternalLogFileSet::AddFile", ss.str());
  }

  // Existing unsealed metadata can be replaced by a seal record for the same
  // file, which records the final logical size and checksum.
  file_iter->second = metadata;
  return Status::OK();
}

Status ExternalLogFileSet::AddFiles(const ExternalLogFileAdditions& files) {
  Status s;
  for (const auto& file : files) {
    s = AddFile(file);
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

void ExternalLogFileSet::DeleteFile(const ExternalLogFileDeletion& file) {
  const std::string& name = file.GetName();
  auto name_iter = name_to_number_.find(name);
  if (name_iter == name_to_number_.end()) {
    return;
  }
  auto file_iter = files_.find(name_iter->second);
  assert(file_iter != files_.end());
  files_.erase(file_iter);
  name_to_number_.erase(name_iter);
}

Status ExternalLogFileSet::DeleteFiles(const ExternalLogFileDeletions& files) {
  for (const auto& file : files) {
    DeleteFile(file);
  }
  return Status::OK();
}

void ExternalLogFileSet::Reset() {
  files_.clear();
  name_to_number_.clear();
}

const ExternalLogFileMetadata* ExternalLogFileSet::FindFile(
    const std::string& name, ExternalLogFileNumber* number) const {
  auto name_iter = name_to_number_.find(name);
  if (name_iter == name_to_number_.end()) {
    return nullptr;
  }
  auto file_iter = files_.find(name_iter->second);
  assert(file_iter != files_.end());
  if (number != nullptr) {
    *number = name_iter->second;
  }
  return &file_iter->second;
}

}  // namespace ROCKSDB_NAMESPACE
