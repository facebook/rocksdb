#pragma once

#include "util/file_reader_writer.h"
#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/options.h"

namespace rocksdb {
namespace titandb {

Status NewBlobFileReader(uint64_t file_number, uint64_t readahead_size,
                         const TitanDBOptions& db_options,
                         const EnvOptions& env_options, Env* env,
                         std::unique_ptr<RandomAccessFileReader>* result);

class BlobFileReader {
 public:
  // Opens a blob file and read the necessary metadata from it.
  // If successful, sets "*result" to the newly opened file reader.
  static Status Open(const TitanCFOptions& options,
                     std::unique_ptr<RandomAccessFileReader> file,
                     uint64_t file_size,
                     std::unique_ptr<BlobFileReader>* result);

  // Gets the blob record pointed by the handle in this file. The data
  // of the record is stored in the provided buffer, so the buffer
  // must be valid when the record is used.
  Status Get(const ReadOptions& options, const BlobHandle& handle,
             BlobRecord* record, PinnableSlice* buffer);

 private:
  friend class BlobFilePrefetcher;

  BlobFileReader(const TitanCFOptions& options,
                 std::unique_ptr<RandomAccessFileReader> file);

  Status ReadRecord(const BlobHandle& handle, BlobRecord* record,
                    OwnedSlice* buffer);

  TitanCFOptions options_;
  std::unique_ptr<RandomAccessFileReader> file_;

  std::shared_ptr<Cache> cache_;
  std::string cache_prefix_;

  // Information read from the file.
  BlobFileFooter footer_;
};

// Performs readahead on continuous reads.
class BlobFilePrefetcher : public Cleanable {
 public:
  // Constructs a prefetcher with the blob file reader.
  // "*reader" must be valid when the prefetcher is used.
  BlobFilePrefetcher(BlobFileReader* reader) : reader_(reader) {}

  Status Get(const ReadOptions& options, const BlobHandle& handle,
             BlobRecord* record, PinnableSlice* buffer);

 private:
  BlobFileReader* reader_;
  uint64_t last_offset_{0};
  uint64_t readahead_size_{0};
  uint64_t readahead_limit_{0};
};

}  // namespace titandb
}  // namespace rocksdb
