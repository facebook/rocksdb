#pragma once

#include "rocksdb/options.h"
#include "utilities/titandb/blob_file_reader.h"
#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/options.h"

namespace rocksdb {
namespace titandb {

class BlobFileCache {
 public:
  // Constructs a blob file cache to cache opened files.
  BlobFileCache(const TitanDBOptions& db_options,
                const TitanCFOptions& cf_options, std::shared_ptr<Cache> cache);

  // Gets the blob record pointed by the handle in the specified file
  // number. The corresponding file size must be exactly "file_size"
  // bytes. The provided buffer is used to store the record data, so
  // the buffer must be valid when the record is used.
  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const BlobHandle& handle, BlobRecord* record,
             PinnableSlice* buffer);

  // Creates a prefetcher for the specified file number.
  Status NewPrefetcher(uint64_t file_number, uint64_t file_size,
                       std::unique_ptr<BlobFilePrefetcher>* result);

  // Evicts the file cache for the specified file number.
  void Evict(uint64_t file_number);

 private:
  // Finds the file for the specified file number. Opens the file if
  // the file is not found in the cache and caches it.
  // If successful, sets "*handle" to the cached file.
  Status FindFile(uint64_t file_number, uint64_t file_size,
                  Cache::Handle** handle);

  Env* env_;
  EnvOptions env_options_;
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  std::shared_ptr<Cache> cache_;
};

}  // namespace titandb
}  // namespace rocksdb
