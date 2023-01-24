//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_reader.h"

#include <cassert>
#include <string>

#include "db/blob/blob_contents.h"
#include "db/blob/blob_log_format.h"
#include "file/file_prefetch_buffer.h"
#include "file/filename.h"
#include "monitoring/statistics.h"
#include "options/cf_options.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/multiget_context.h"
#include "test_util/sync_point.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

Status BlobFileReader::Create(
    const ImmutableOptions& immutable_options, const FileOptions& file_options,
    uint32_t column_family_id, HistogramImpl* blob_file_read_hist,
    uint64_t blob_file_number, const std::shared_ptr<IOTracer>& io_tracer,
    std::unique_ptr<BlobFileReader>* blob_file_reader) {
  assert(blob_file_reader);
  assert(!*blob_file_reader);

  uint64_t file_size = 0;
  std::unique_ptr<RandomAccessFileReader> file_reader;

  {
    const Status s =
        OpenFile(immutable_options, file_options, blob_file_read_hist,
                 blob_file_number, io_tracer, &file_size, &file_reader);
    if (!s.ok()) {
      return s;
    }
  }

  assert(file_reader);

  Statistics* const statistics = immutable_options.stats;

  CompressionType compression_type = kNoCompression;

  {
    const Status s = ReadHeader(file_reader.get(), column_family_id, statistics,
                                &compression_type);
    if (!s.ok()) {
      return s;
    }
  }

  {
    const Status s = ReadFooter(file_reader.get(), file_size, statistics);
    if (!s.ok()) {
      return s;
    }
  }

  blob_file_reader->reset(
      new BlobFileReader(std::move(file_reader), file_size, compression_type,
                         immutable_options.clock, statistics));

  return Status::OK();
}

Status BlobFileReader::OpenFile(
    const ImmutableOptions& immutable_options, const FileOptions& file_opts,
    HistogramImpl* blob_file_read_hist, uint64_t blob_file_number,
    const std::shared_ptr<IOTracer>& io_tracer, uint64_t* file_size,
    std::unique_ptr<RandomAccessFileReader>* file_reader) {
  assert(file_size);
  assert(file_reader);

  const auto& cf_paths = immutable_options.cf_paths;
  assert(!cf_paths.empty());

  const std::string blob_file_path =
      BlobFileName(cf_paths.front().path, blob_file_number);

  FileSystem* const fs = immutable_options.fs.get();
  assert(fs);

  constexpr IODebugContext* dbg = nullptr;

  {
    TEST_SYNC_POINT("BlobFileReader::OpenFile:GetFileSize");

    const Status s =
        fs->GetFileSize(blob_file_path, IOOptions(), file_size, dbg);
    if (!s.ok()) {
      return s;
    }
  }

  if (*file_size < BlobLogHeader::kSize + BlobLogFooter::kSize) {
    return Status::Corruption("Malformed blob file");
  }

  std::unique_ptr<FSRandomAccessFile> file;

  {
    TEST_SYNC_POINT("BlobFileReader::OpenFile:NewRandomAccessFile");

    const Status s =
        fs->NewRandomAccessFile(blob_file_path, file_opts, &file, dbg);
    if (!s.ok()) {
      return s;
    }
  }

  assert(file);

  if (immutable_options.advise_random_on_open) {
    file->Hint(FSRandomAccessFile::kRandom);
  }

  file_reader->reset(new RandomAccessFileReader(
      std::move(file), blob_file_path, immutable_options.clock, io_tracer,
      immutable_options.stats, BLOB_DB_BLOB_FILE_READ_MICROS,
      blob_file_read_hist, immutable_options.rate_limiter.get(),
      immutable_options.listeners));

  return Status::OK();
}

Status BlobFileReader::ReadHeader(const RandomAccessFileReader* file_reader,
                                  uint32_t column_family_id,
                                  Statistics* statistics,
                                  CompressionType* compression_type) {
  assert(file_reader);
  assert(compression_type);

  Slice header_slice;
  Buffer buf;
  AlignedBuf aligned_buf;

  {
    TEST_SYNC_POINT("BlobFileReader::ReadHeader:ReadFromFile");

    constexpr uint64_t read_offset = 0;
    constexpr size_t read_size = BlobLogHeader::kSize;

    // TODO: rate limit reading headers from blob files.
    const Status s = ReadFromFile(file_reader, read_offset, read_size,
                                  statistics, &header_slice, &buf, &aligned_buf,
                                  Env::IO_TOTAL /* rate_limiter_priority */);
    if (!s.ok()) {
      return s;
    }

    TEST_SYNC_POINT_CALLBACK("BlobFileReader::ReadHeader:TamperWithResult",
                             &header_slice);
  }

  BlobLogHeader header;

  {
    const Status s = header.DecodeFrom(header_slice);
    if (!s.ok()) {
      return s;
    }
  }

  constexpr ExpirationRange no_expiration_range;

  if (header.has_ttl || header.expiration_range != no_expiration_range) {
    return Status::Corruption("Unexpected TTL blob file");
  }

  if (header.column_family_id != column_family_id) {
    return Status::Corruption("Column family ID mismatch");
  }

  *compression_type = header.compression;

  return Status::OK();
}

Status BlobFileReader::ReadFooter(const RandomAccessFileReader* file_reader,
                                  uint64_t file_size, Statistics* statistics) {
  assert(file_size >= BlobLogHeader::kSize + BlobLogFooter::kSize);
  assert(file_reader);

  Slice footer_slice;
  Buffer buf;
  AlignedBuf aligned_buf;

  {
    TEST_SYNC_POINT("BlobFileReader::ReadFooter:ReadFromFile");

    const uint64_t read_offset = file_size - BlobLogFooter::kSize;
    constexpr size_t read_size = BlobLogFooter::kSize;

    // TODO: rate limit reading footers from blob files.
    const Status s = ReadFromFile(file_reader, read_offset, read_size,
                                  statistics, &footer_slice, &buf, &aligned_buf,
                                  Env::IO_TOTAL /* rate_limiter_priority */);
    if (!s.ok()) {
      return s;
    }

    TEST_SYNC_POINT_CALLBACK("BlobFileReader::ReadFooter:TamperWithResult",
                             &footer_slice);
  }

  BlobLogFooter footer;

  {
    const Status s = footer.DecodeFrom(footer_slice);
    if (!s.ok()) {
      return s;
    }
  }

  constexpr ExpirationRange no_expiration_range;

  if (footer.expiration_range != no_expiration_range) {
    return Status::Corruption("Unexpected TTL blob file");
  }

  return Status::OK();
}

Status BlobFileReader::ReadFromFile(const RandomAccessFileReader* file_reader,
                                    uint64_t read_offset, size_t read_size,
                                    Statistics* statistics, Slice* slice,
                                    Buffer* buf, AlignedBuf* aligned_buf,
                                    Env::IOPriority rate_limiter_priority) {
  assert(slice);
  assert(buf);
  assert(aligned_buf);

  assert(file_reader);

  RecordTick(statistics, BLOB_DB_BLOB_FILE_BYTES_READ, read_size);

  Status s;

  if (file_reader->use_direct_io()) {
    constexpr char* scratch = nullptr;

    s = file_reader->Read(IOOptions(), read_offset, read_size, slice, scratch,
                          aligned_buf, rate_limiter_priority);
  } else {
    buf->reset(new char[read_size]);
    constexpr AlignedBuf* aligned_scratch = nullptr;

    s = file_reader->Read(IOOptions(), read_offset, read_size, slice,
                          buf->get(), aligned_scratch, rate_limiter_priority);
  }

  if (!s.ok()) {
    return s;
  }

  if (slice->size() != read_size) {
    return Status::Corruption("Failed to read data from blob file");
  }

  return Status::OK();
}

BlobFileReader::BlobFileReader(
    std::unique_ptr<RandomAccessFileReader>&& file_reader, uint64_t file_size,
    CompressionType compression_type, SystemClock* clock,
    Statistics* statistics)
    : file_reader_(std::move(file_reader)),
      file_size_(file_size),
      compression_type_(compression_type),
      clock_(clock),
      statistics_(statistics) {
  assert(file_reader_);
}

BlobFileReader::~BlobFileReader() = default;

Status BlobFileReader::GetBlob(
    const ReadOptions& read_options, const Slice& user_key, uint64_t offset,
    uint64_t value_size, CompressionType compression_type,
    FilePrefetchBuffer* prefetch_buffer, MemoryAllocator* allocator,
    std::unique_ptr<BlobContents>* result, uint64_t* bytes_read) const {
  assert(result);

  const uint64_t key_size = user_key.size();

  if (!IsValidBlobOffset(offset, key_size, value_size, file_size_)) {
    return Status::Corruption("Invalid blob offset");
  }

  if (compression_type != compression_type_) {
    return Status::Corruption("Compression type mismatch when reading blob");
  }

  // Note: if verify_checksum is set, we read the entire blob record to be able
  // to perform the verification; otherwise, we just read the blob itself. Since
  // the offset in BlobIndex actually points to the blob value, we need to make
  // an adjustment in the former case.
  const uint64_t adjustment =
      read_options.verify_checksums
          ? BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size)
          : 0;
  assert(offset >= adjustment);

  const uint64_t record_offset = offset - adjustment;
  const uint64_t record_size = value_size + adjustment;

  Slice record_slice;
  Buffer buf;
  AlignedBuf aligned_buf;

  bool prefetched = false;

  if (prefetch_buffer) {
    Status s;
    constexpr bool for_compaction = true;

    prefetched = prefetch_buffer->TryReadFromCache(
        IOOptions(), file_reader_.get(), record_offset,
        static_cast<size_t>(record_size), &record_slice, &s,
        read_options.rate_limiter_priority, for_compaction);
    if (!s.ok()) {
      return s;
    }
  }

  if (!prefetched) {
    TEST_SYNC_POINT("BlobFileReader::GetBlob:ReadFromFile");
    PERF_COUNTER_ADD(blob_read_count, 1);
    PERF_COUNTER_ADD(blob_read_byte, record_size);
    PERF_TIMER_GUARD(blob_read_time);
    const Status s = ReadFromFile(file_reader_.get(), record_offset,
                                  static_cast<size_t>(record_size), statistics_,
                                  &record_slice, &buf, &aligned_buf,
                                  read_options.rate_limiter_priority);
    if (!s.ok()) {
      return s;
    }
  }

  TEST_SYNC_POINT_CALLBACK("BlobFileReader::GetBlob:TamperWithResult",
                           &record_slice);

  if (read_options.verify_checksums) {
    const Status s = VerifyBlob(record_slice, user_key, value_size);
    if (!s.ok()) {
      return s;
    }
  }

  const Slice value_slice(record_slice.data() + adjustment, value_size);

  {
    const Status s = UncompressBlobIfNeeded(
        value_slice, compression_type, allocator, clock_, statistics_, result);
    if (!s.ok()) {
      return s;
    }
  }

  if (bytes_read) {
    *bytes_read = record_size;
  }

  return Status::OK();
}

void BlobFileReader::MultiGetBlob(
    const ReadOptions& read_options, MemoryAllocator* allocator,
    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>&
        blob_reqs,
    uint64_t* bytes_read) const {
  const size_t num_blobs = blob_reqs.size();
  assert(num_blobs > 0);
  assert(num_blobs <= MultiGetContext::MAX_BATCH_SIZE);

#ifndef NDEBUG
  for (size_t i = 0; i < num_blobs - 1; ++i) {
    assert(blob_reqs[i].first->offset <= blob_reqs[i + 1].first->offset);
  }
#endif  // !NDEBUG

  std::vector<FSReadRequest> read_reqs;
  autovector<uint64_t> adjustments;
  uint64_t total_len = 0;
  read_reqs.reserve(num_blobs);
  for (size_t i = 0; i < num_blobs; ++i) {
    BlobReadRequest* const req = blob_reqs[i].first;
    assert(req);
    assert(req->user_key);
    assert(req->status);

    const size_t key_size = req->user_key->size();
    const uint64_t offset = req->offset;
    const uint64_t value_size = req->len;

    if (!IsValidBlobOffset(offset, key_size, value_size, file_size_)) {
      *req->status = Status::Corruption("Invalid blob offset");
      continue;
    }
    if (req->compression != compression_type_) {
      *req->status =
          Status::Corruption("Compression type mismatch when reading a blob");
      continue;
    }

    const uint64_t adjustment =
        read_options.verify_checksums
            ? BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size)
            : 0;
    assert(req->offset >= adjustment);
    adjustments.push_back(adjustment);

    FSReadRequest read_req = {};
    read_req.offset = req->offset - adjustment;
    read_req.len = req->len + adjustment;
    read_reqs.emplace_back(read_req);
    total_len += read_req.len;
  }

  RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, total_len);

  Buffer buf;
  AlignedBuf aligned_buf;

  Status s;
  bool direct_io = file_reader_->use_direct_io();
  if (direct_io) {
    for (size_t i = 0; i < read_reqs.size(); ++i) {
      read_reqs[i].scratch = nullptr;
    }
  } else {
    buf.reset(new char[total_len]);
    std::ptrdiff_t pos = 0;
    for (size_t i = 0; i < read_reqs.size(); ++i) {
      read_reqs[i].scratch = buf.get() + pos;
      pos += read_reqs[i].len;
    }
  }
  TEST_SYNC_POINT("BlobFileReader::MultiGetBlob:ReadFromFile");
  PERF_COUNTER_ADD(blob_read_count, num_blobs);
  PERF_COUNTER_ADD(blob_read_byte, total_len);
  s = file_reader_->MultiRead(IOOptions(), read_reqs.data(), read_reqs.size(),
                              direct_io ? &aligned_buf : nullptr,
                              read_options.rate_limiter_priority);
  if (!s.ok()) {
    for (auto& req : read_reqs) {
      req.status.PermitUncheckedError();
    }
    for (auto& blob_req : blob_reqs) {
      BlobReadRequest* const req = blob_req.first;
      assert(req);
      assert(req->status);

      if (!req->status->IsCorruption()) {
        // Avoid overwriting corruption status.
        *req->status = s;
      }
    }
    return;
  }

  assert(s.ok());

  uint64_t total_bytes = 0;
  for (size_t i = 0, j = 0; i < num_blobs; ++i) {
    BlobReadRequest* const req = blob_reqs[i].first;
    assert(req);
    assert(req->user_key);
    assert(req->status);

    if (!req->status->ok()) {
      continue;
    }

    assert(j < read_reqs.size());
    auto& read_req = read_reqs[j++];
    const auto& record_slice = read_req.result;
    if (read_req.status.ok() && record_slice.size() != read_req.len) {
      read_req.status =
          IOStatus::Corruption("Failed to read data from blob file");
    }

    *req->status = read_req.status;
    if (!req->status->ok()) {
      continue;
    }

    // Verify checksums if enabled
    if (read_options.verify_checksums) {
      *req->status = VerifyBlob(record_slice, *req->user_key, req->len);
      if (!req->status->ok()) {
        continue;
      }
    }

    // Uncompress blob if needed
    Slice value_slice(record_slice.data() + adjustments[i], req->len);
    *req->status =
        UncompressBlobIfNeeded(value_slice, compression_type_, allocator,
                               clock_, statistics_, &blob_reqs[i].second);
    if (req->status->ok()) {
      total_bytes += record_slice.size();
    }
  }

  if (bytes_read) {
    *bytes_read = total_bytes;
  }
}

Status BlobFileReader::VerifyBlob(const Slice& record_slice,
                                  const Slice& user_key, uint64_t value_size) {
  PERF_TIMER_GUARD(blob_checksum_time);

  BlobLogRecord record;

  const Slice header_slice(record_slice.data(), BlobLogRecord::kHeaderSize);

  {
    const Status s = record.DecodeHeaderFrom(header_slice);
    if (!s.ok()) {
      return s;
    }
  }

  if (record.key_size != user_key.size()) {
    return Status::Corruption("Key size mismatch when reading blob");
  }

  if (record.value_size != value_size) {
    return Status::Corruption("Value size mismatch when reading blob");
  }

  record.key =
      Slice(record_slice.data() + BlobLogRecord::kHeaderSize, record.key_size);
  if (record.key != user_key) {
    return Status::Corruption("Key mismatch when reading blob");
  }

  record.value = Slice(record.key.data() + record.key_size, value_size);

  {
    TEST_SYNC_POINT_CALLBACK("BlobFileReader::VerifyBlob:CheckBlobCRC",
                             &record);

    const Status s = record.CheckBlobCRC();
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status BlobFileReader::UncompressBlobIfNeeded(
    const Slice& value_slice, CompressionType compression_type,
    MemoryAllocator* allocator, SystemClock* clock, Statistics* statistics,
    std::unique_ptr<BlobContents>* result) {
  assert(result);

  if (compression_type == kNoCompression) {
    BlobContentsCreator::Create(result, nullptr, value_slice, allocator);
    return Status::OK();
  }

  UncompressionContext context(compression_type);
  UncompressionInfo info(context, UncompressionDict::GetEmptyDict(),
                         compression_type);

  size_t uncompressed_size = 0;
  constexpr uint32_t compression_format_version = 2;

  CacheAllocationPtr output;

  {
    PERF_TIMER_GUARD(blob_decompress_time);
    StopWatch stop_watch(clock, statistics, BLOB_DB_DECOMPRESSION_MICROS);
    output = UncompressData(info, value_slice.data(), value_slice.size(),
                            &uncompressed_size, compression_format_version,
                            allocator);
  }

  TEST_SYNC_POINT_CALLBACK(
      "BlobFileReader::UncompressBlobIfNeeded:TamperWithResult", &output);

  if (!output) {
    return Status::Corruption("Unable to uncompress blob");
  }

  result->reset(new BlobContents(std::move(output), uncompressed_size));

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
