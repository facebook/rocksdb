#pragma once

#include <set>

#include "rocksdb/slice.h"
#include "utilities/titandb/blob_format.h"

namespace rocksdb {
namespace titandb {

class VersionEdit {
 public:
  void SetNextFileNumber(uint64_t v) {
    has_next_file_number_ = true;
    next_file_number_ = v;
  }

  void SetColumnFamilyID(uint32_t v) { column_family_id_ = v; }

  void AddBlobFile(std::shared_ptr<BlobFileMeta> file) {
    added_files_.push_back(file);
  }

  void DeleteBlobFile(uint64_t file_number, SequenceNumber obsolete_sequence = 0) {
    deleted_files_.emplace_back(std::make_pair(file_number, obsolete_sequence));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const VersionEdit& lhs, const VersionEdit& rhs);

 private:
  friend class VersionSet;

  bool has_next_file_number_{false};
  uint64_t next_file_number_{0};
  uint32_t column_family_id_{0};

  std::vector<std::shared_ptr<BlobFileMeta>> added_files_;
  std::vector<std::pair<uint64_t, SequenceNumber>> deleted_files_;
};

}  // namespace titandb
}  // namespace rocksdb
