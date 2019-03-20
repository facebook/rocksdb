#include "utilities/titandb/blob_file_size_collector.h"
#include "base_db_listener.h"

namespace rocksdb {
namespace titandb {

TablePropertiesCollector*
BlobFileSizeCollectorFactory::CreateTablePropertiesCollector(
    rocksdb::TablePropertiesCollectorFactory::Context /* context */) {
  return new BlobFileSizeCollector();
}

const std::string BlobFileSizeCollector::kPropertiesName =
    "TitanDB.blob_discardable_size";

bool BlobFileSizeCollector::Encode(
    const std::map<uint64_t, uint64_t>& blob_files_size, std::string* result) {
  PutVarint32(result, static_cast<uint32_t>(blob_files_size.size()));
  for (const auto& bfs : blob_files_size) {
    PutVarint64(result, bfs.first);
    PutVarint64(result, bfs.second);
  }
  return true;
}
bool BlobFileSizeCollector::Decode(
    Slice* slice, std::map<uint64_t, uint64_t>* blob_files_size) {
  uint32_t num = 0;
  if (!GetVarint32(slice, &num)) {
    return false;
  }
  uint64_t file_number;
  uint64_t size;
  for (uint32_t i = 0; i < num; ++i) {
    if (!GetVarint64(slice, &file_number)) {
      return false;
    }
    if (!GetVarint64(slice, &size)) {
      return false;
    }
    (*blob_files_size)[file_number] = size;
  }
  return true;
}

Status BlobFileSizeCollector::AddUserKey(const Slice& /* key */,
                                         const Slice& value, EntryType type,
                                         SequenceNumber /* seq */,
                                         uint64_t /* file_size */) {
  if (type != kEntryBlobIndex) {
    return Status::OK();
  }

  BlobIndex index;
  auto s = index.DecodeFrom(const_cast<Slice*>(&value));
  if (!s.ok()) {
    return s;
  }

  auto iter = blob_files_size_.find(index.file_number);
  if (iter == blob_files_size_.end()) {
    blob_files_size_[index.file_number] = index.blob_handle.size;
  } else {
    iter->second += index.blob_handle.size;
  }

  return Status::OK();
}

Status BlobFileSizeCollector::Finish(UserCollectedProperties* properties) {
  if (blob_files_size_.empty()) {
    return Status::OK();
  }

  std::string res;
  if (!Encode(blob_files_size_, &res) || res.empty()) {
    fprintf(stderr, "blob file size collector encode failed\n");
    abort();
  }
  properties->emplace(std::make_pair(kPropertiesName, res));
  return Status::OK();
}

}  // namespace titandb
}  // namespace rocksdb
