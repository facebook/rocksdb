//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_reader.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <string>

#include "db/blob/blob_contents.h"
#include "db/blob/blob_log_format.h"
#include "db/blog/blog_format.h"
#include "file/file_prefetch_buffer.h"
#include "file/filename.h"
#include "monitoring/statistics_impl.h"
#include "options/cf_options.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/format.h"
#include "table/multiget_context.h"
#include "test_util/sync_point.h"
#include "util/compression.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

Status BlobFileReader::Create(
    const ImmutableOptions& immutable_options, const ReadOptions& read_options,
    const FileOptions& file_options, uint32_t column_family_id,
    HistogramImpl* blob_file_read_hist, uint64_t blob_file_number,
    const std::shared_ptr<IOTracer>& io_tracer, bool skip_footer_validation,
    std::unique_ptr<BlobFileReader>* blob_file_reader) {
  assert(blob_file_reader);
  assert(!*blob_file_reader);

  uint64_t file_size = 0;
  std::unique_ptr<RandomAccessFileReader> file_reader;

  {
    const Status s =
        OpenFile(immutable_options, file_options, blob_file_read_hist,
                 blob_file_number, io_tracer, &file_size, &file_reader,
                 /*skip_footer_size_check=*/skip_footer_validation);
    if (!s.ok()) {
      return s;
    }
  }

  assert(file_reader);

  Statistics* const statistics = immutable_options.stats;

  // Construct with defaults; ReadHeader will detect blog vs legacy format
  // and populate the appropriate fields.
  blob_file_reader->reset(
      new BlobFileReader(std::move(file_reader), file_size, kNoCompression,
                         /*decompressor=*/nullptr, immutable_options.clock,
                         statistics, /*has_footer=*/false));

  {
    const Status s =
        (*blob_file_reader)->ReadHeader(read_options, column_family_id);
    if (!s.ok()) {
      blob_file_reader->reset();
      return s;
    }
  }

  if (!skip_footer_validation) {
    const Status s = (*blob_file_reader)->ReadFooter(read_options);
    if (!s.ok()) {
      blob_file_reader->reset();
      return s;
    }
  }

  return Status::OK();
}

Status BlobFileReader::OpenFile(
    const ImmutableOptions& immutable_options, const FileOptions& file_opts,
    HistogramImpl* blob_file_read_hist, uint64_t blob_file_number,
    const std::shared_ptr<IOTracer>& io_tracer, uint64_t* file_size,
    std::unique_ptr<RandomAccessFileReader>* file_reader,
    bool skip_footer_size_check) {
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

  if (!skip_footer_size_check &&
      *file_size < BlobLogHeader::kSize + BlobLogFooter::kSize) {
    return Status::Corruption("Malformed blob file");
  }
  if (skip_footer_size_check && *file_size < BlobLogHeader::kSize) {
    return Status::Corruption("Malformed blob file");
  }

  std::unique_ptr<FSRandomAccessFile> file;
  FileOptions reader_file_opts = file_opts;

  if (skip_footer_size_check && reader_file_opts.use_direct_reads) {
    reader_file_opts.use_direct_reads = false;
  }

  {
    TEST_SYNC_POINT("BlobFileReader::OpenFile:NewRandomAccessFile");

    const Status s =
        fs->NewRandomAccessFile(blob_file_path, reader_file_opts, &file, dbg);
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

Status BlobFileReader::ReadHeader(const ReadOptions& read_options,
                                  uint32_t column_family_id) {
  assert(file_reader_);

  // Read ~4 KiB from the start to cover both legacy (30B) and blog (40B
  // fixed + property section) headers in a single I/O. This is enough for
  // typical blog headers; only if the property section exceeds ~4 KiB do
  // we need a second read.
  constexpr size_t kInitialReadSize = 4096;
  const size_t read_size =
      std::min(static_cast<uint64_t>(kInitialReadSize), file_size_);

  Slice header_slice;
  Buffer buf;
  AlignedBuf aligned_buf;

  {
    TEST_SYNC_POINT("BlobFileReader::ReadHeader:ReadFromFile");

    const Status s =
        ReadFromFile(file_reader_.get(), read_options, 0, read_size,
                     statistics_, &header_slice, &buf, &aligned_buf);
    if (!s.ok()) {
      return s;
    }

    TEST_SYNC_POINT_CALLBACK("BlobFileReader::ReadHeader:TamperWithResult",
                             &header_slice);
  }

  // Detect blog format by checking the 12-byte magic.
  if (BlogFileHeader::IsBlogFormat(header_slice.data(), header_slice.size())) {
    // Try to decode the blog header from the buffer we already read.
    // DecodeFromBuffer handles the case where the property section
    // extends beyond the buffer.
    BlogFileHeader blog_header;
    Slice buf_slice = header_slice;
    size_t additional_needed = 0;
    Status s = blog_header.DecodeFromBuffer(&buf_slice, &additional_needed);

    if (s.IsIncomplete()) {
      // Need more data. Read the rest and retry.
      Slice extra_slice;
      Buffer extra_buf;
      AlignedBuf extra_aligned;
      s = ReadFromFile(file_reader_.get(), read_options, header_slice.size(),
                       additional_needed, statistics_, &extra_slice, &extra_buf,
                       &extra_aligned);
      if (!s.ok()) {
        return s;
      }
      std::string extended;
      extended.append(header_slice.data(), header_slice.size());
      extended.append(extra_slice.data(), extra_slice.size());
      Slice extended_slice(extended);
      s = blog_header.DecodeFromBuffer(&extended_slice, nullptr);
    }

    if (!s.ok()) {
      return s;
    }

    is_blog_format_ = true;
    blog_checksum_type_ = blog_header.checksum_type;
    blog_incarnation_id_ = blog_header.incarnation_id();
    memcpy(blog_escape_sequence_, blog_header.escape_sequence,
           kBlogEscapeSequenceSize);
    compression_type_ = kNoCompression;
    decompressor_ = GetBuiltinV2CompressionManager()->GetDecompressor();
    return Status::OK();
  }

  // Legacy blob format. The initial read already covers the 30-byte header.
  if (header_slice.size() < BlobLogHeader::kSize) {
    return Status::Corruption("Blob file header too small");
  }

  Slice legacy_slice(header_slice.data(), BlobLogHeader::kSize);
  BlobLogHeader header;

  {
    const Status s = header.DecodeFrom(legacy_slice);
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

  compression_type_ = header.compression;
  if (compression_type_ != kNoCompression) {
    decompressor_ =
        GetBuiltinV2CompressionManager()->GetDecompressorOptimizeFor(
            compression_type_);
  }

  return Status::OK();
}

Status BlobFileReader::ReadFooter(const ReadOptions& read_options) {
  assert(file_reader_);

  if (is_blog_format_) {
    // Blog format: verify the file was cleanly sealed by scanning backward
    // from EOF for the last record's escape sequence. Read the trailing
    // ~4 KiB in a single I/O and scan within the buffer.
    if (file_size_ < kBlogFileFixedHeaderSize + kBlogEscapeSequenceSize) {
      return Status::Corruption("Blog blob file too small for footer");
    }

    constexpr size_t kTrailingReadSize = 4096;
    const uint64_t min_offset = kBlogFileFixedHeaderSize;
    // Round trailing_offset up to 4-byte alignment so buffer positions
    // and file positions share the same alignment grid.
    const uint64_t trailing_offset =
        (file_size_ -
         std::min(static_cast<uint64_t>(kTrailingReadSize),
                  file_size_ - min_offset) +
         3) &
        ~uint64_t{3};
    const size_t trailing_size =
        static_cast<size_t>(file_size_ - trailing_offset);

    Slice trailing_slice;
    Buffer trailing_buf;
    AlignedBuf trailing_aligned;
    const Status s = ReadFromFile(
        file_reader_.get(), read_options, trailing_offset, trailing_size,
        statistics_, &trailing_slice, &trailing_buf, &trailing_aligned);
    if (!s.ok()) {
      return s;
    }

    if (VerifyBlogFooterLocator(trailing_slice.data(), trailing_size,
                                trailing_offset, blog_escape_sequence_,
                                blog_checksum_type_, blog_incarnation_id_)) {
      has_footer_ = true;
      return Status::OK();
    }

    return Status::Corruption("Blog blob file: no footer locator record found");
  }

  // Legacy blob format.
  assert(file_size_ >= BlobLogHeader::kSize + BlobLogFooter::kSize);

  Slice footer_slice;
  Buffer buf;
  AlignedBuf aligned_buf;

  {
    TEST_SYNC_POINT("BlobFileReader::ReadFooter:ReadFromFile");

    const uint64_t read_offset = file_size_ - BlobLogFooter::kSize;
    constexpr size_t read_size = BlobLogFooter::kSize;

    const Status s =
        ReadFromFile(file_reader_.get(), read_options, read_offset, read_size,
                     statistics_, &footer_slice, &buf, &aligned_buf);
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

  has_footer_ = true;
  return Status::OK();
}

Status BlobFileReader::ReadFromFile(const RandomAccessFileReader* file_reader,
                                    const ReadOptions& read_options,
                                    uint64_t read_offset, size_t read_size,
                                    Statistics* statistics, Slice* slice,
                                    Buffer* buf, AlignedBuf* aligned_buf) {
  assert(slice);
  assert(buf);
  assert(aligned_buf);

  assert(file_reader);

  RecordTick(statistics, BLOB_DB_BLOB_FILE_BYTES_READ, read_size);

  Status s;

  IOOptions io_options;
  IODebugContext dbg;
  s = file_reader->PrepareIOOptions(read_options, io_options, &dbg);
  if (!s.ok()) {
    return s;
  }

  if (file_reader->use_direct_io()) {
    constexpr char* scratch = nullptr;

    s = file_reader->Read(io_options, read_offset, read_size, slice, scratch,
                          aligned_buf, &dbg);
  } else {
    buf->reset(new char[read_size]);
    constexpr AlignedBuf* aligned_scratch = nullptr;

    s = file_reader->Read(io_options, read_offset, read_size, slice, buf->get(),
                          aligned_scratch, &dbg);
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
    CompressionType compression_type,
    std::shared_ptr<Decompressor> decompressor, SystemClock* clock,
    Statistics* statistics, bool has_footer)
    : file_reader_(std::move(file_reader)),
      file_size_(file_size),
      compression_type_(compression_type),
      decompressor_(std::move(decompressor)),
      clock_(clock),
      statistics_(statistics),
      has_footer_(has_footer) {
  assert(file_reader_);
}

BlobFileReader::~BlobFileReader() = default;

Status BlobFileReader::GetBlob(
    const ReadOptions& read_options, const Slice& user_key, uint64_t offset,
    uint64_t value_size, CompressionType compression_type,
    FilePrefetchBuffer* prefetch_buffer, MemoryAllocator* allocator,
    std::unique_ptr<BlobContents>* result, uint64_t* bytes_read) const {
  assert(result);

  // Compute the read region. Both formats store the blob value at `offset`
  // with size `value_size`. The difference:
  // - Legacy: if verifying checksums, also read the record header (before
  //   the value) which contains key + CRC.
  // - Blog: if verifying checksums, also read the 5-byte trailer (after
  //   the value) which contains compression_type + checksum.
  uint64_t adjustment_before = 0;  // bytes before the value to include
  uint64_t adjustment_after = 0;   // bytes after the value to include

  if (is_blog_format_) {
    if (read_options.verify_checksums) {
      adjustment_after = kBlogBlockTrailerSize;
    }
  } else {
    const uint64_t key_size = user_key.size();
    if (!IsValidBlobOffset(offset, key_size, value_size, file_size_,
                           has_footer_)) {
      return Status::Corruption("Invalid blob offset");
    }
    if (compression_type != compression_type_) {
      return Status::Corruption("Compression type mismatch when reading blob");
    }
    if (read_options.verify_checksums) {
      adjustment_before =
          BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size);
    }
  }

  assert(offset >= adjustment_before);
  const uint64_t record_offset = offset - adjustment_before;
  const uint64_t record_size =
      adjustment_before + value_size + adjustment_after;

  // Read the data: try prefetch buffer first, then fall back to file read.
  Slice record_slice;
  Buffer buf;
  AlignedBuf aligned_buf;

  bool prefetched = false;

  if (prefetch_buffer) {
    Status s;
    constexpr bool for_compaction = true;

    IOOptions io_options;
    IODebugContext dbg;
    s = file_reader_->PrepareIOOptions(read_options, io_options, &dbg);
    if (!s.ok()) {
      return s;
    }
    prefetched = prefetch_buffer->TryReadFromCache(
        io_options, file_reader_.get(), record_offset,
        static_cast<size_t>(record_size), &record_slice, &s, for_compaction);
    if (!s.ok()) {
      return s;
    }
  }

  if (!prefetched) {
    TEST_SYNC_POINT("BlobFileReader::GetBlob:ReadFromFile");
    PERF_COUNTER_ADD(blob_read_count, 1);
    PERF_COUNTER_ADD(blob_read_byte, record_size);
    PERF_TIMER_GUARD(blob_read_time);
    const Status s =
        ReadFromFile(file_reader_.get(), read_options, record_offset,
                     static_cast<size_t>(record_size), statistics_,
                     &record_slice, &buf, &aligned_buf);
    if (!s.ok()) {
      return s;
    }
  }

  TEST_SYNC_POINT_CALLBACK("BlobFileReader::GetBlob:TamperWithResult",
                           &record_slice);

  // Verify checksum (format-specific).
  CompressionType actual_comp_type = compression_type;
  if (read_options.verify_checksums) {
    if (is_blog_format_) {
      const Status s = VerifyBlogRecordTrailer(
          blog_checksum_type_, record_slice.data(),
          static_cast<size_t>(value_size), blog_incarnation_id_, offset,
          &actual_comp_type);
      if (!s.ok()) {
        return s;
      }
    } else {
      const Status s = VerifyBlob(record_slice, user_key, value_size);
      if (!s.ok()) {
        return s;
      }
    }
  }

  // Extract the value slice (format-specific offset within the read region).
  const Slice value_slice(record_slice.data() + adjustment_before, value_size);

  {
    const Status s = UncompressBlobIfNeeded(value_slice, actual_comp_type,
                                            decompressor_.get(), allocator,
                                            clock_, statistics_, result);
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

    if (!IsValidBlobOffset(offset, key_size, value_size, file_size_,
                           has_footer_)) {
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

    FSReadRequest read_req;
    read_req.offset = req->offset - adjustment;
    read_req.len = req->len + adjustment;
    total_len += read_req.len;
    read_reqs.emplace_back(std::move(read_req));
  }

  RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, total_len);

  if (read_reqs.empty()) {
    if (bytes_read) {
      *bytes_read = 0;
    }
    return;
  }

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
  IOOptions opts;
  IODebugContext dbg;
  s = file_reader_->PrepareIOOptions(read_options, opts, &dbg);
  if (s.ok()) {
    s = file_reader_->MultiRead(opts, read_reqs.data(), read_reqs.size(),
                                direct_io ? &aligned_buf : nullptr, &dbg);
  }
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
    Slice value_slice(record_slice.data() + adjustments[j - 1], req->len);
    *req->status = UncompressBlobIfNeeded(
        value_slice, compression_type_, decompressor_.get(), allocator, clock_,
        statistics_, &blob_reqs[i].second);
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
    Decompressor* decompressor, MemoryAllocator* allocator, SystemClock* clock,
    Statistics* statistics, std::unique_ptr<BlobContents>* result) {
  assert(result);

  if (compression_type == kNoCompression) {
    BlobContentsCreator::Create(result, nullptr, value_slice, kNoCompression,
                                allocator);
    return Status::OK();
  }

  assert(decompressor);

  Decompressor::Args args;
  args.compression_type = compression_type;
  args.compressed_data = value_slice;

  Status s = decompressor->ExtractUncompressedSize(args);
  if (!s.ok()) {
    return Status::Corruption(s.ToString());
  }

  CacheAllocationPtr output = AllocateBlock(args.uncompressed_size, allocator);

  {
    PERF_TIMER_GUARD(blob_decompress_time);
    StopWatch stop_watch(clock, statistics, BLOB_DB_DECOMPRESSION_MICROS);
    s = decompressor->DecompressBlock(args, output.get());
  }

  TEST_SYNC_POINT_CALLBACK(
      "BlobFileReader::UncompressBlobIfNeeded:TamperWithResult", &s);

  if (!s.ok()) {
    return Status::Corruption(s.ToString());
  }

  result->reset(new BlobContents(std::move(output), args.uncompressed_size));

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
