#pragma once

#include "rocksdb/options.h"
#include "utilities/titandb/blob_file_cache.h"
#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/blob_gc.h"

namespace rocksdb {
namespace titandb {

class VersionSet;

// Provides methods to access the blob storage for a specific
// version. The version must be valid when this storage is used.
class BlobStorage {
 public:
  BlobStorage(const BlobStorage& bs) {
    this->files_ = bs.files_;
    this->file_cache_ = bs.file_cache_;
    this->titan_cf_options_ = bs.titan_cf_options_;
  }

  BlobStorage(const TitanCFOptions& _options,
              std::shared_ptr<BlobFileCache> _file_cache)
      : titan_cf_options_(_options), file_cache_(_file_cache) {}

  // Gets the blob record pointed by the blob index. The provided
  // buffer is used to store the record data, so the buffer must be
  // valid when the record is used.
  Status Get(const ReadOptions& options, const BlobIndex& index,
             BlobRecord* record, PinnableSlice* buffer);

  // Creates a prefetcher for the specified file number.
  // REQUIRES: mutex is held
  Status NewPrefetcher(uint64_t file_number,
                       std::unique_ptr<BlobFilePrefetcher>* result);

  // Finds the blob file meta for the specified file number. It is a
  // corruption if the file doesn't exist in the specific version.
  std::weak_ptr<BlobFileMeta> FindFile(uint64_t file_number);

  std::size_t NumBlobFiles() { return files_.size(); }

  void MarkAllFilesForGC() {
    for (auto& file : files_) {
      file.second->FileStateTransit(BlobFileMeta::FileEvent::kDbRestart);
      //      file.second->marked_for_gc_ = true;
    }
  }

  const std::vector<GCScore> gc_score() { return gc_score_; }

  void ComputeGCScore();

  const TitanCFOptions& titan_cf_options() { return titan_cf_options_; }

  void AddBlobFiles(
      const std::map<uint64_t, std::shared_ptr<BlobFileMeta>>& files);
  void DeleteBlobFiles(const std::set<uint64_t>& files);

 private:
  friend class Version;
  friend class VersionSet;
  friend class VersionBuilder;
  friend class VersionTest;
  friend class BlobGCPickerTest;
  friend class BlobGCJobTest;
  friend class BlobFileSizeCollectorTest;

  TitanCFOptions titan_cf_options_;
  // Only BlobStorage OWNS BlobFileMeta
  std::map<uint64_t, std::shared_ptr<BlobFileMeta>> files_;
  std::shared_ptr<BlobFileCache> file_cache_;

  std::vector<GCScore> gc_score_;
};

class Version {
 public:
  Version(VersionSet* vset) : vset_(vset), prev_(this), next_(this) {}

  // Reference count management.
  void Ref();
  void Unref();

  // Returns the blob storage for the specific column family.
  // The version must be valid when the blob storage is used.
  // Except Version, Nobody else can extend the life time of
  // BlobStorage. Otherwise, It's a wrong design. Because
  // BlobStorage only belongs to Version, Others only have
  // the right to USE it.
  std::weak_ptr<BlobStorage> GetBlobStorage(uint32_t cf_id);

  void MarkAllFilesForGC() {
    for (auto& cf : column_families_) {
      cf.second->MarkAllFilesForGC();
    }
  }

 private:
  friend class VersionList;
  friend class VersionBuilder;
  friend class VersionSet;
  friend class VersionTest;
  friend class BlobFileSizeCollectorTest;

  ~Version();

  VersionSet* vset_;
  std::atomic_int refs_{0};
  Version* prev_;
  Version* next_;
  std::map<uint32_t, std::shared_ptr<BlobStorage>> column_families_;
};

class VersionList {
 public:
  VersionList();

  ~VersionList();

  Version* current() { return current_; }

  void Append(Version* v);

 private:
  Version list_{nullptr};
  Version* current_{nullptr};
};

}  // namespace titandb
}  // namespace rocksdb
