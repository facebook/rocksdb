#pragma once

#include "rocksdb/options.h"
#include "utilities/titandb/blob_file_cache.h"
#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/blob_gc.h"

namespace rocksdb {
namespace titandb {

// Provides methods to access the blob storage for a specific
// column family. The version must be valid when this storage is used.
class BlobStorage {
 public:
  BlobStorage(const BlobStorage& bs) : mutex_() {
    this->files_ = bs.files_;
    this->file_cache_ = bs.file_cache_;
    this->titan_cf_options_ = bs.titan_cf_options_;
  }

  BlobStorage(const TitanCFOptions& _options,
              std::shared_ptr<BlobFileCache> _file_cache)
      : titan_cf_options_(_options), mutex_(), file_cache_(_file_cache) {}

  // Gets the blob record pointed by the blob index. The provided
  // buffer is used to store the record data, so the buffer must be
  // valid when the record is used.
  Status Get(const ReadOptions& options, const BlobIndex& index,
             BlobRecord* record, PinnableSlice* buffer);

  // Creates a prefetcher for the specified file number.
  Status NewPrefetcher(uint64_t file_number,
                       std::unique_ptr<BlobFilePrefetcher>* result);

  // Finds the blob file meta for the specified file number. It is a
  // corruption if the file doesn't exist in the specific version.
  std::weak_ptr<BlobFileMeta> FindFile(uint64_t file_number) const;

  std::size_t NumBlobFiles() const { 
    ReadLock rl(&mutex_);
    return files_.size(); 
  }
  
  void ExportBlobFiles(
      std::map<uint64_t, std::weak_ptr<BlobFileMeta>>& ret) const;

  void MarkAllFilesForGC() {
    WriteLock wl(&mutex_);
    for (auto& file : files_) {
      file.second->FileStateTransit(BlobFileMeta::FileEvent::kDbRestart);
      //      file.second->marked_for_gc_ = true;
    }
  }

  const std::vector<GCScore> gc_score() { return gc_score_; }

  void ComputeGCScore();

  const TitanCFOptions& titan_cf_options() { return titan_cf_options_; }

  void AddBlobFile(std::shared_ptr<BlobFileMeta>& file);

  void DeleteBlobFile(uint64_t file);

 private:
  friend class VersionSet;
  friend class VersionTest;
  friend class BlobGCPickerTest;
  friend class BlobGCJobTest;
  friend class BlobFileSizeCollectorTest;

  TitanCFOptions titan_cf_options_;

  // Read Write Mutex, which protects the `files_` structures 
  mutable port::RWMutex mutex_;

  // Only BlobStorage OWNS BlobFileMeta
  std::unordered_map<uint64_t, std::shared_ptr<BlobFileMeta>> files_;
  std::shared_ptr<BlobFileCache> file_cache_;

  std::vector<GCScore> gc_score_;
};

}  // namespace titandb
}  // namespace rocksdb
