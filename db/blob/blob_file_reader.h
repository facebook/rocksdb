//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>

#include "file/random_access_file_reader.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/rocksdb_namespace.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class Status;
struct ImmutableOptions;
struct FileOptions;
class HistogramImpl;
struct ReadOptions;
class Slice;
class FilePrefetchBuffer;
class PinnableSlice;
class Statistics;

class BlobFileReader {
 public:
  static Status Create(const ImmutableOptions& immutable_options,
                       const FileOptions& file_options,
                       uint32_t column_family_id,
                       HistogramImpl* blob_file_read_hist,
                       uint64_t blob_file_number,
                       const std::shared_ptr<IOTracer>& io_tracer,
                       std::unique_ptr<BlobFileReader>* reader);

  BlobFileReader(const BlobFileReader&) = delete;
  BlobFileReader& operator=(const BlobFileReader&) = delete;

  ~BlobFileReader();

  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 uint64_t offset, uint64_t value_size,
                 CompressionType compression_type,
                 FilePrefetchBuffer* prefetch_buffer, PinnableSlice* value,
                 uint64_t* bytes_read) const;

  // offsets must be sorted in ascending order by caller.
  void MultiGetBlob(
      const ReadOptions& read_options,
      const autovector<std::reference_wrapper<const Slice>>& user_keys,
      const autovector<uint64_t>& offsets,
      const autovector<uint64_t>& value_sizes, autovector<Status*>& statuses,
      autovector<PinnableSlice*>& values, uint64_t* bytes_read) const;

  CompressionType GetCompressionType() const { return compression_type_; }

  uint64_t GetFileSize() const { return file_size_; }

 private:
  BlobFileReader(std::unique_ptr<RandomAccessFileReader>&& file_reader,
                 uint64_t file_size, CompressionType compression_type,
                 SystemClock* clock, Statistics* statistics);

  static Status OpenFile(const ImmutableOptions& immutable_options,
                         const FileOptions& file_opts,
                         HistogramImpl* blob_file_read_hist,
                         uint64_t blob_file_number,
                         const std::shared_ptr<IOTracer>& io_tracer,
                         uint64_t* file_size,
                         std::unique_ptr<RandomAccessFileReader>* file_reader);

  static Status ReadHeader(const RandomAccessFileReader* file_reader,
                           uint32_t column_family_id, Statistics* statistics,
                           CompressionType* compression_type);

  static Status ReadFooter(const RandomAccessFileReader* file_reader,
                           uint64_t file_size, Statistics* statistics);

  using Buffer = std::unique_ptr<char[]>;

  static Status ReadFromFile(const RandomAccessFileReader* file_reader,
                             uint64_t read_offset, size_t read_size,
                             Statistics* statistics, Slice* slice, Buffer* buf,
                             AlignedBuf* aligned_buf,
                             Env::IOPriority rate_limiter_priority);

  static Status VerifyBlob(const Slice& record_slice, const Slice& user_key,
                           uint64_t value_size);

  static Status UncompressBlobIfNeeded(const Slice& value_slice,
                                       CompressionType compression_type,
                                       SystemClock* clock,
                                       Statistics* statistics,
                                       PinnableSlice* value);

  static void SaveValue(const Slice& src, PinnableSlice* dst);

  std::unique_ptr<RandomAccessFileReader> file_reader_;
  uint64_t file_size_;
  CompressionType compression_type_;
  SystemClock* clock_;
  Statistics* statistics_;
};

}  // namespace ROCKSDB_NAMESPACE
