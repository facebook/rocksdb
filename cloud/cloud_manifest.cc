//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.

#include "cloud/cloud_manifest.h"

#include <rocksdb/status.h>

#include <algorithm>
#include <vector>

#include "db/log_reader.h"
#include "db/log_writer.h"
#include "file/writable_file_writer.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {
struct CorruptionReporter : public log::Reader::Reporter {
  void Corruption(size_t /*bytes*/, const Status& s) override {
    if (status->ok()) {
      *status = s;
    }
  }
  Status* status;
};

enum class RecordTags : uint32_t {
  kPastEpoch = 1,
  kCurrentEpoch = 2,
};

}  // namespace

// Format:
// header: format_version (varint) number of records (varint)
// record: tag (varint, 1 or 2)
// record 1: epoch (slice), file number
// record 2: current epoch
IOStatus CloudManifest::LoadFromLog(std::unique_ptr<SequentialFileReader> log,
                                    std::unique_ptr<CloudManifest>* manifest) {
  Status status;
  CorruptionReporter reporter;
  reporter.status = &status;
  log::Reader reader(nullptr, std::move(log), &reporter, true /* checksum */,
                     0 /* log_num */);
  Slice record;
  std::string scratch;
  bool headerRead = false;
  uint32_t expectedRecords = 0;
  uint32_t recordsRead = 0;
  std::string currentEpoch;
  std::vector<std::pair<uint64_t, std::string>> pastEpochs;
  while (reader.ReadRecord(&record, &scratch,
                           WALRecoveryMode::kAbsoluteConsistency) &&
         status.ok()) {
    if (!headerRead) {
      uint32_t formatVersion;
      bool ok = GetVarint32(&record, &formatVersion);
      if (ok) {
        ok = GetVarint32(&record, &expectedRecords);
      }
      if (!ok) {
        return IOStatus::Corruption("Corruption in cloud manifest header");
      }
      if (formatVersion != kCurrentFormatVersion) {
        return IOStatus::Corruption("Unknown cloud manifest format version");
      }
      headerRead = true;
    } else {
      ++recordsRead;
      uint32_t tag;
      uint64_t fileNumber;
      Slice epoch;
      bool ok = GetVarint32(&record, &tag);
      if (!ok) {
        return IOStatus::Corruption("Failed to read cloud manifest record");
      }
      switch (tag) {
        case static_cast<uint32_t>(RecordTags::kPastEpoch): {
          ok = GetLengthPrefixedSlice(&record, &epoch) &&
               GetVarint64(&record, &fileNumber);
          if (ok) {
            pastEpochs.emplace_back(fileNumber, epoch.ToString());
          }
          break;
        }
        case static_cast<uint32_t>(RecordTags::kCurrentEpoch): {
          ok = GetLengthPrefixedSlice(&record, &epoch);
          if (ok) {
            ok = currentEpoch.empty();
            currentEpoch = epoch.ToString();
          }
          break;
        }
        default:
          ok = false;
      }
      if (!ok) {
        return IOStatus::Corruption("Failed to read cloud manifest record");
      }
    }
  }
  if (recordsRead != expectedRecords) {
    return IOStatus::Corruption(
        "Records read does not match the number of expected records");
  }
  // is sorted by filenum in ascending order
  if (!std::is_sorted(pastEpochs.begin(), pastEpochs.end(),
                      [](auto& e1, auto& e2) { return e1.first < e2.first; })) {
    return IOStatus::Corruption("Cloud manifest records not sorted");
  }
  manifest->reset(
      new CloudManifest(std::move(pastEpochs), std::move(currentEpoch)));
  return status_to_io_status(std::move(status));
}

IOStatus CloudManifest::CreateForEmptyDatabase(
    std::string currentEpoch, std::unique_ptr<CloudManifest>* manifest) {
  manifest->reset(new CloudManifest({}, std::move(currentEpoch)));
  return IOStatus::OK();
}

std::unique_ptr<CloudManifest> CloudManifest::clone() const {
  ReadLock lck(&mutex_);
  return std::unique_ptr<CloudManifest>(
      new CloudManifest(pastEpochs_, currentEpoch_));
}

// Serialization format is quite simple:
//
// Header: (current_format_version: varint) (number_of_records: varint)
// We store number_of_records just for sanity checks
//
// Record:
// * (kPastEpoch tag: 1 byte) (epochId: length-prefixed-string) (file_number:
// varint)
//
// Header comes first, and is followed with number_of_records Records.
IOStatus CloudManifest::WriteToLog(
    std::unique_ptr<WritableFileWriter> log) const {
  log::Writer writer(std::move(log), 0, false);
  std::string record;

  ReadLock lck(&mutex_);

  // 1. write header
  PutVarint32(&record, kCurrentFormatVersion);
  PutVarint32(&record, static_cast<uint32_t>(pastEpochs_.size() + 1));
  auto status = writer.AddRecord({}, record);
  if (!status.ok()) {
    return status;
  }

  // 2. put past epochs
  for (auto& pe : pastEpochs_) {
    record.clear();
    PutVarint32(&record, static_cast<uint32_t>(RecordTags::kPastEpoch));
    PutLengthPrefixedSlice(&record, pe.second);
    PutVarint64(&record, pe.first);
    status = writer.AddRecord({}, record);
    if (!status.ok()) {
      return status;
    }
  }

  // 3. put current epoch
  record.clear();
  PutVarint32(&record, static_cast<uint32_t>(RecordTags::kCurrentEpoch));
  PutLengthPrefixedSlice(&record, currentEpoch_);

  status = writer.AddRecord({}, record);
  if (!status.ok()) {
    return status;
  }
  return writer.file()->Sync({}, true);
}

bool CloudManifest::AddEpoch(uint64_t startFileNumber, std::string epochId) {
  WriteLock lck(&mutex_);
  if (!pastEpochs_.empty() && startFileNumber < pastEpochs_.back().first) {
    return false;
  }

  // Check all the epochs with same file number
  // For each (filenum, epoch) pair in `pastEpochs_`, it means next epoch of `epoch`
  // starts at `filenum`. So we should compare epochId with next epoch of `epoch`.
  auto nxtEpoch = &currentEpoch_;
  for (auto rit = pastEpochs_.rbegin(); rit != pastEpochs_.rend(); rit++) {
    if (rit->first == startFileNumber && *nxtEpoch == epochId) {
      return false;
    }
    if (rit->first < startFileNumber) {
      break;
    }
    nxtEpoch = &(rit->second);
  }

  pastEpochs_.emplace_back(startFileNumber, std::move(currentEpoch_));
  currentEpoch_ = std::move(epochId);
  return true;
}

std::string CloudManifest::GetEpoch(uint64_t fileNumber) {
  ReadLock lck(&mutex_);
  // Note: We are looking for fileNumber + 1 because fileNumbers in pastEpochs_
  // are exclusive. In other words, if pastEpochs_ contains (10, "x"), it means
  // that "x" epoch ends at 9, not 10.
  auto itr =
      std::lower_bound(pastEpochs_.begin(), pastEpochs_.end(),
                       std::pair<uint64_t, std::string>(fileNumber + 1, ""));
  if (itr == pastEpochs_.end()) {
    return currentEpoch_;
  }
  return itr->second;
}

std::string CloudManifest::GetCurrentEpoch() {
  ReadLock lck(&mutex_);
  return currentEpoch_;
}

std::vector<std::pair<uint64_t, std::string>>
CloudManifest::TEST_GetPastEpochs() const {
  ReadLock lck(&mutex_);
  return pastEpochs_;
}

std::string CloudManifest::ToString(bool include_past_epochs) {
  ReadLock lck(&mutex_);
  std::ostringstream oss;
  if (include_past_epochs) {
    oss << "Past Epochs: [\n";
    for (auto& pe : pastEpochs_) {
      oss << "\t(" << pe.first << ", " << pe.second << "), \n";
    }
    oss << "]\n";
  }
  oss << "Current Epoch: " << currentEpoch_;
  return oss.str();
}

}  // namespace ROCKSDB_NAMESPACE
