//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob_file_state.h"

#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/coding.h"

#include <sstream>

namespace rocksdb {

namespace {

// Tags for custom fields. Note that these get persisted in the manifest,
// so existing tags should not be modified.
enum CustomFieldTags : uint32_t {
  kEndMarker,

  // Add forward compatible fields here

  /////////////////////////////////////////////////////////////////////

  kForwardIncompatibleMask = 1 << 6,

  // Add forward incompatible fields here
};

}  // anonymous namespace

void BlobFileState::EncodeTo(std::string* output) const {
  PutVarint64(output, blob_file_number_);
  PutVarint64(output, total_blob_count_);
  PutVarint64(output, total_blob_bytes_);
  PutVarint64(output, garbage_blob_count_);
  PutVarint64(output, garbage_blob_bytes_);

  // Encode any custom fields here. The format to use is a Varint32 tag (see
  // CustomFieldTags above) followed by a length prefixed slice. Unknown custom
  // fields will be ignored during decoding unless they're in the forward
  // incompatible range.

  TEST_SYNC_POINT_CALLBACK("BlobFileState::EncodeTo::CustomFields", output);

  PutVarint32(output, kEndMarker);
}

Status BlobFileState::DecodeFrom(Slice* input) {
  constexpr char class_name[] = "BlobFileState";

  if (!GetVarint64(input, &blob_file_number_)) {
    return Status::Corruption(class_name, "Error decoding blob file number");
  }

  if (!GetVarint64(input, &total_blob_count_)) {
    return Status::Corruption(class_name, "Error decoding total blob count");
  }

  if (!GetVarint64(input, &total_blob_bytes_)) {
    return Status::Corruption(class_name, "Error decoding total blob bytes");
  }

  if (!GetVarint64(input, &garbage_blob_count_)) {
    return Status::Corruption(class_name, "Error decoding garbage blob count");
  }

  if (!GetVarint64(input, &garbage_blob_bytes_)) {
    return Status::Corruption(class_name, "Error decoding garbage blob bytes");
  }

  while (true) {
    uint32_t custom_field_tag = 0;
    if (!GetVarint32(input, &custom_field_tag)) {
      return Status::Corruption(class_name, "Error decoding custom field tag");
    }

    if (custom_field_tag == kEndMarker) {
      break;
    }

    if (custom_field_tag & kForwardIncompatibleMask) {
      return Status::Corruption(
          class_name, "Forward incompatible custom field encountered");
    }

    Slice custom_field_value;
    if (!GetLengthPrefixedSlice(input, &custom_field_value)) {
      return Status::Corruption(class_name,
                                "Error decoding custom field value");
    }
  }

  return Status::OK();
}

std::string BlobFileState::DebugString() const {
  std::ostringstream oss;

  oss << "blob_file_number: " << blob_file_number_
      << " total_blob_count: " << total_blob_count_
      << " total_blob_bytes: " << total_blob_bytes_
      << " garbage_blob_count: " << garbage_blob_count_
      << " garbage_blob_bytes: " << garbage_blob_bytes_;

  return oss.str();
}

std::string BlobFileState::DebugJSON() const {
  JSONWriter jw;

  jw << "BlobFileNumber" << blob_file_number_ << "TotalBlobCount"
     << total_blob_count_ << "TotalBlobBytes" << total_blob_bytes_
     << "GarbageBlobCount" << garbage_blob_count_ << "GarbageBlobBytes"
     << garbage_blob_bytes_;

  jw.EndObject();

  return jw.Get();
}

}  // namespace rocksdb
