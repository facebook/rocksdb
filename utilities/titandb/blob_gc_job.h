#pragma once

#include "db/db_impl.h"
#include "rocksdb/status.h"
#include "utilities/titandb/blob_file_builder.h"
#include "utilities/titandb/blob_file_iterator.h"
#include "utilities/titandb/blob_file_manager.h"
#include "utilities/titandb/blob_gc.h"
#include "utilities/titandb/options.h"
#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

class BlobGCJob {
 public:
  BlobGCJob(BlobGC* blob_gc, DB* db, port::Mutex* mutex,
            const TitanDBOptions& titan_db_options, Env* env,
            const EnvOptions& env_options, BlobFileManager* blob_file_manager,
            VersionSet* version_set, LogBuffer* log_buffer,
            std::atomic_bool* shuting_down);

  // No copying allowed
  BlobGCJob(const BlobGCJob&) = delete;
  void operator=(const BlobGCJob&) = delete;

  ~BlobGCJob();

  // REQUIRE: mutex held
  Status Prepare();
  // REQUIRE: mutex not held
  Status Run();
  // REQUIRE: mutex held
  Status Finish();

 private:
  class GarbageCollectionWriteCallback;
  friend class BlobGCJobTest;

  BlobGC* blob_gc_;
  DB* base_db_;
  DBImpl* base_db_impl_;
  port::Mutex* mutex_;
  TitanDBOptions db_options_;
  Env* env_;
  EnvOptions env_options_;
  BlobFileManager* blob_file_manager_;
  titandb::VersionSet* version_set_;
  LogBuffer* log_buffer_{nullptr};

  std::vector<std::pair<std::unique_ptr<BlobFileHandle>,
                        std::unique_ptr<BlobFileBuilder>>>
      blob_file_builders_;
  std::vector<std::pair<WriteBatch, GarbageCollectionWriteCallback>>
      rewrite_batches_;
  InternalKeyComparator* cmp_{nullptr};

  std::atomic_bool* shuting_down_{nullptr};

  Status SampleCandidateFiles();
  bool DoSample(const BlobFileMeta* file);
  Status DoRunGC();
  Status BuildIterator(std::unique_ptr<BlobFileMergeIterator>* result);
  bool DiscardEntry(const Slice& key, const BlobIndex& blob_index);
  Status InstallOutputBlobFiles();
  Status RewriteValidKeyToLSM();
  Status DeleteInputBlobFiles() const;

  bool IsShutingDown();
};

}  // namespace titandb
}  // namespace rocksdb
