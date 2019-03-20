#pragma once

#include "rocksdb/listener.h"
#include "rocksdb/table_properties.h"
#include "util/coding.h"
#include "utilities/titandb/db_impl.h"
#include "utilities/titandb/version.h"
#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

class BlobFileSizeCollectorFactory final
    : public TablePropertiesCollectorFactory {
 public:
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override;

  const char* Name() const override { return "BlobFileSizeCollector"; }
};

class BlobFileSizeCollector final : public TablePropertiesCollector {
 public:
  const static std::string kPropertiesName;

  static bool Encode(const std::map<uint64_t, uint64_t>& blob_files_size,
                     std::string* result);
  static bool Decode(Slice* slice,
                     std::map<uint64_t, uint64_t>* blob_files_size);

  Status AddUserKey(const Slice& key, const Slice& value, EntryType type,
                    SequenceNumber seq, uint64_t file_size) override;
  Status Finish(UserCollectedProperties* properties) override;
  UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties();
  }
  const char* Name() const override { return "BlobFileSizeCollector"; }

 private:
  std::map<uint64_t, uint64_t> blob_files_size_;
};

}  // namespace titandb
}  // namespace rocksdb
