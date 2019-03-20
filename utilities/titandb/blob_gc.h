#pragma once

#include <memory>

#include "db/column_family.h"
#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/options.h"

namespace rocksdb {
namespace titandb {

class Version;

// A BlobGC encapsulates information about a blob gc.
class BlobGC {
 public:
  BlobGC(std::vector<BlobFileMeta*>&& blob_files,
         TitanCFOptions&& _titan_cf_options);

  // No copying allowed
  BlobGC(const BlobGC&) = delete;
  void operator=(const BlobGC&) = delete;

  ~BlobGC();

  const std::vector<BlobFileMeta*>& inputs() { return inputs_; }

  void set_sampled_inputs(std::vector<BlobFileMeta*>&& files) {
    sampled_inputs_ = std::move(files);
  }

  const std::vector<BlobFileMeta*>& sampled_inputs() { return sampled_inputs_; }

  const TitanCFOptions& titan_cf_options() { return titan_cf_options_; }

  void SetInputVersion(ColumnFamilyHandle* cfh, Version* version);

  ColumnFamilyHandle* column_family_handle() { return cfh_; }

  ColumnFamilyData* GetColumnFamilyData();

  void MarkFilesBeingGC();

  void AddOutputFile(BlobFileMeta*);

  void ReleaseGcFiles();

 private:
  std::vector<BlobFileMeta*> inputs_;
  std::vector<BlobFileMeta*> sampled_inputs_;
  std::vector<BlobFileMeta*> outputs_;
  TitanCFOptions titan_cf_options_;
  ColumnFamilyHandle* cfh_{nullptr};
  Version* current_{nullptr};
};

struct GCScore {
  uint64_t file_number;
  double score;
};

}  // namespace titandb
}  // namespace rocksdb
