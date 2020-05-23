//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_meta.h"

#include <ostream>
#include <sstream>

namespace ROCKSDB_NAMESPACE {

std::string SharedBlobFileMetaData::DebugString() const {
  std::ostringstream oss;
  oss << (*this);

  return oss.str();
}

std::ostream& operator<<(std::ostream& os,
                         const SharedBlobFileMetaData& shared_meta) {
  os << "blob_file_number: " << shared_meta.GetBlobFileNumber()
     << " total_blob_count: " << shared_meta.GetTotalBlobCount()
     << " total_blob_bytes: " << shared_meta.GetTotalBlobBytes()
     << " checksum_method: " << shared_meta.GetChecksumMethod()
     << " checksum_value: " << shared_meta.GetChecksumValue();

  return os;
}

std::string BlobFileMetaData::DebugString() const {
  std::ostringstream oss;
  oss << (*this);

  return oss.str();
}

std::ostream& operator<<(std::ostream& os, const BlobFileMetaData& meta) {
  const auto& shared_meta = meta.GetSharedMeta();
  assert(shared_meta);

  os << (*shared_meta) << " garbage_blob_count: " << meta.GetGarbageBlobCount()
     << " garbage_blob_bytes: " << meta.GetGarbageBlobBytes();

  return os;
}

}  // namespace ROCKSDB_NAMESPACE
