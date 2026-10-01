//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_addition.h"

#include <limits>
#include <ostream>
#include <sstream>

#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

// Tags for custom fields. Note that these get persisted in the manifest,
// so existing tags should not be modified.
enum BlobFileAddition::CustomFieldTags : uint32_t {
  kEndMarker,

  // Add forward compatible fields here

  /////////////////////////////////////////////////////////////////////

  kForwardIncompatibleMask = 1 << 6,

  // Add forward incompatible fields here
  kIndirectionInfo = (1 << 6) | 1,
};

void BlobFileAddition::SetIndirectionIdentity() {
  assert(blob_file_number_ != kInvalidBlobFileNumber);
  origin_file_number_ = blob_file_number_;
  map_offset_ = 0;
  map_size_ = 0;
  map_checksum_ = 0;
}

Status BlobFileAddition::SetIndirectionCarrier(uint64_t origin_file_number,
                                               uint64_t map_offset,
                                               uint64_t map_size,
                                               uint32_t map_checksum) {
  if (origin_file_number == kInvalidBlobFileNumber ||
      origin_file_number == blob_file_number_) {
    return Status::InvalidArgument(
        "Blob indirection carrier requires a distinct valid origin file");
  }
  if (map_offset == 0 || map_size == 0 ||
      map_offset > std::numeric_limits<uint64_t>::max() - map_size) {
    return Status::InvalidArgument("Invalid embedded BlobMap extent");
  }
  origin_file_number_ = origin_file_number;
  map_offset_ = map_offset;
  map_size_ = map_size;
  map_checksum_ = map_checksum;
  return Status::OK();
}

void BlobFileAddition::EncodeTo(std::string* output) const {
  PutVarint64(output, blob_file_number_);
  PutVarint64(output, total_blob_count_);
  PutVarint64(output, total_blob_bytes_);
  PutLengthPrefixedSlice(output, checksum_method_);
  PutLengthPrefixedSlice(output, checksum_value_);

  // Encode any custom fields here. The format to use is a Varint32 tag (see
  // CustomFieldTags above) followed by a length prefixed slice. Unknown custom
  // fields will be ignored during decoding unless they're in the forward
  // incompatible range.

  if (HasIndirectionInfo()) {
    std::string value;
    PutVarint64(&value, origin_file_number_);
    PutVarint64(&value, map_offset_);
    PutVarint64(&value, map_size_);
    PutFixed32(&value, map_checksum_);
    PutVarint32(output, kIndirectionInfo);
    PutLengthPrefixedSlice(output, value);
  }

  TEST_SYNC_POINT_CALLBACK("BlobFileAddition::EncodeTo::CustomFields", output);

  PutVarint32(output, kEndMarker);
}

Status BlobFileAddition::DecodeFrom(Slice* input) {
  constexpr char class_name[] = "BlobFileAddition";

  if (!GetVarint64(input, &blob_file_number_)) {
    return Status::Corruption(class_name, "Error decoding blob file number");
  }

  if (!GetVarint64(input, &total_blob_count_)) {
    return Status::Corruption(class_name, "Error decoding total blob count");
  }

  if (!GetVarint64(input, &total_blob_bytes_)) {
    return Status::Corruption(class_name, "Error decoding total blob bytes");
  }

  Slice checksum_method;
  if (!GetLengthPrefixedSlice(input, &checksum_method)) {
    return Status::Corruption(class_name, "Error decoding checksum method");
  }
  checksum_method_ = checksum_method.ToString();

  Slice checksum_value;
  if (!GetLengthPrefixedSlice(input, &checksum_value)) {
    return Status::Corruption(class_name, "Error decoding checksum value");
  }
  checksum_value_ = checksum_value.ToString();

  while (true) {
    uint32_t custom_field_tag = 0;
    if (!GetVarint32(input, &custom_field_tag)) {
      return Status::Corruption(class_name, "Error decoding custom field tag");
    }

    if (custom_field_tag == kEndMarker) {
      break;
    }

    Slice custom_field_value;
    if (!GetLengthPrefixedSlice(input, &custom_field_value)) {
      return Status::Corruption(class_name,
                                "Error decoding custom field value");
    }

    if (custom_field_tag == kIndirectionInfo) {
      if (HasIndirectionInfo()) {
        return Status::Corruption(class_name,
                                  "Duplicate blob indirection info");
      }
      if (!GetVarint64(&custom_field_value, &origin_file_number_) ||
          !GetVarint64(&custom_field_value, &map_offset_) ||
          !GetVarint64(&custom_field_value, &map_size_) ||
          !GetFixed32(&custom_field_value, &map_checksum_) ||
          !custom_field_value.empty()) {
        return Status::Corruption(class_name,
                                  "Error decoding blob indirection info");
      }
      if (origin_file_number_ == kInvalidBlobFileNumber) {
        return Status::Corruption(class_name, "Invalid origin file number");
      }
      const bool identity = origin_file_number_ == blob_file_number_;
      if (identity !=
          (map_offset_ == 0 && map_size_ == 0 && map_checksum_ == 0)) {
        return Status::Corruption(class_name, "Invalid blob indirection state");
      }
      if (!identity &&
          (map_offset_ == 0 || map_size_ == 0 ||
           map_offset_ > std::numeric_limits<uint64_t>::max() - map_size_)) {
        return Status::Corruption(class_name,
                                  "Invalid embedded BlobMap extent");
      }
      continue;
    }

    if (custom_field_tag & kForwardIncompatibleMask) {
      return Status::Corruption(
          class_name, "Forward incompatible custom field encountered");
    }
  }

  return Status::OK();
}

std::string BlobFileAddition::DebugString() const {
  std::ostringstream oss;

  oss << *this;

  return oss.str();
}

std::string BlobFileAddition::DebugJSON() const {
  JSONWriter jw;

  jw << *this;

  jw.EndObject();

  return jw.Get();
}

bool operator==(const BlobFileAddition& lhs, const BlobFileAddition& rhs) {
  return lhs.GetBlobFileNumber() == rhs.GetBlobFileNumber() &&
         lhs.GetTotalBlobCount() == rhs.GetTotalBlobCount() &&
         lhs.GetTotalBlobBytes() == rhs.GetTotalBlobBytes() &&
         lhs.GetChecksumMethod() == rhs.GetChecksumMethod() &&
         lhs.GetChecksumValue() == rhs.GetChecksumValue() &&
         lhs.GetOriginFileNumber() == rhs.GetOriginFileNumber() &&
         lhs.GetMapOffset() == rhs.GetMapOffset() &&
         lhs.GetMapSize() == rhs.GetMapSize() &&
         lhs.GetMapChecksum() == rhs.GetMapChecksum();
}

bool operator!=(const BlobFileAddition& lhs, const BlobFileAddition& rhs) {
  return !(lhs == rhs);
}

std::ostream& operator<<(std::ostream& os,
                         const BlobFileAddition& blob_file_addition) {
  os << "blob_file_number: " << blob_file_addition.GetBlobFileNumber()
     << " total_blob_count: " << blob_file_addition.GetTotalBlobCount()
     << " total_blob_bytes: " << blob_file_addition.GetTotalBlobBytes()
     << " checksum_method: " << blob_file_addition.GetChecksumMethod()
     << " checksum_value: "
     << Slice(blob_file_addition.GetChecksumValue()).ToString(/* hex */ true);
  if (blob_file_addition.HasIndirectionInfo()) {
    os << " origin_file_number: " << blob_file_addition.GetOriginFileNumber()
       << " map_offset: " << blob_file_addition.GetMapOffset()
       << " map_size: " << blob_file_addition.GetMapSize()
       << " map_checksum: " << blob_file_addition.GetMapChecksum();
  }

  return os;
}

JSONWriter& operator<<(JSONWriter& jw,
                       const BlobFileAddition& blob_file_addition) {
  jw << "BlobFileNumber" << blob_file_addition.GetBlobFileNumber()
     << "TotalBlobCount" << blob_file_addition.GetTotalBlobCount()
     << "TotalBlobBytes" << blob_file_addition.GetTotalBlobBytes()
     << "ChecksumMethod" << blob_file_addition.GetChecksumMethod()
     << "ChecksumValue"
     << Slice(blob_file_addition.GetChecksumValue()).ToString(/* hex */ true);
  if (blob_file_addition.HasIndirectionInfo()) {
    jw << "OriginFileNumber" << blob_file_addition.GetOriginFileNumber()
       << "MapOffset" << blob_file_addition.GetMapOffset() << "MapSize"
       << blob_file_addition.GetMapSize() << "MapChecksum"
       << blob_file_addition.GetMapChecksum();
  }

  return jw;
}

}  // namespace ROCKSDB_NAMESPACE
