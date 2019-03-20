#pragma once

#include <memory>

#include "db/column_family.h"
#include "db/write_callback.h"
#include "rocksdb/status.h"
#include "util/filename.h"
#include "utilities/titandb/blob_file_manager.h"
#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/blob_gc.h"
#include "utilities/titandb/version.h"

namespace rocksdb {
namespace titandb {

class BlobGCPicker {
 public:
  BlobGCPicker(){};
  virtual ~BlobGCPicker(){};

  // Pick candidate blob files for a new gc.
  // Returns nullptr if there is no gc to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the gc.  Caller should delete the result.
  virtual std::unique_ptr<BlobGC> PickBlobGC(BlobStorage* blob_storage) = 0;
};

class BasicBlobGCPicker final : public BlobGCPicker {
 public:
  BasicBlobGCPicker(TitanDBOptions, TitanCFOptions);
  ~BasicBlobGCPicker();

  std::unique_ptr<BlobGC> PickBlobGC(BlobStorage* blob_storage) override;

 private:
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;

  // Check if blob_file needs to gc, return true means we need pick this
  // file for gc
  bool CheckBlobFile(BlobFileMeta* blob_file) const;
};

}  // namespace titandb
}  // namespace rocksdb
