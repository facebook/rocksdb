//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/orphan_blob_file_resolver.h"

#include <cinttypes>

#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "logging/logging.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/advanced_compression.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {

OrphanBlobFileResolver::OrphanBlobFileResolver(SystemClock* clock,
                                               Statistics* statistics,
                                               Logger* info_log)
    : fs_(nullptr),
      clock_(clock),
      statistics_(statistics),
      info_log_(info_log) {}

OrphanBlobFileResolver::~OrphanBlobFileResolver() = default;

Status OrphanBlobFileResolver::Create(
    FileSystem* fs, const std::string& dbname, SystemClock* clock,
    Statistics* statistics, Logger* info_log, VersionSet* versions,
    std::unique_ptr<OrphanBlobFileResolver>* resolver) {
  assert(fs);
  assert(versions);
  assert(resolver);

  // All I/O in this method runs during DB::Open, so set io_activity
  // accordingly for proper histogram tracking and ThreadStatusUtil.
  IOOptions io_opts;
  io_opts.io_activity = Env::IOActivity::kDBOpen;

  auto r = std::unique_ptr<OrphanBlobFileResolver>(
      new OrphanBlobFileResolver(clock, statistics, info_log));
  r->fs_ = fs;

  // Collect all registered blob file numbers across all CFs.
  for (auto* cfd : *versions->GetColumnFamilySet()) {
    if (cfd->current()) {
      const auto& blob_files = cfd->current()->storage_info()->GetBlobFiles();
      for (const auto& meta : blob_files) {
        r->registered_files_.insert(meta->GetBlobFileNumber());
      }
    }
  }

  // List all files in the DB directory.
  std::vector<std::string> filenames;
  IOStatus io_s = fs->GetChildren(dbname, io_opts, &filenames, nullptr);
  if (!io_s.ok()) {
    // Non-fatal: if we can't list the directory, just create an empty resolver.
    ROCKS_LOG_WARN(info_log,
                   "OrphanBlobFileResolver: failed to list DB directory: %s",
                   io_s.ToString().c_str());
    *resolver = std::move(r);
    return Status::OK();
  }

  for (const auto& fname : filenames) {
    uint64_t file_number;
    FileType file_type;
    if (!ParseFileName(fname, &file_number, &file_type) ||
        file_type != kBlobFile) {
      continue;
    }

    // Check if this blob file is registered in any CF's VersionStorageInfo.
    if (r->registered_files_.count(file_number) > 0) {
      continue;
    }

    std::string blob_path = BlobFileName(dbname, file_number);

    // Get file size.
    uint64_t file_size = 0;
    io_s = fs->GetFileSize(blob_path, io_opts, &file_size, nullptr);
    if (!io_s.ok()) {
      continue;
    }

    // Empty or headerless blob files: these can appear when a crash happens
    // after RotateAllPartitions creates new blob files on disk but before the
    // BG flush thread writes the header+data (deferred flush mode). The WAL
    // may already contain PutBlobIndex entries referencing these files. Treat
    // them as empty orphans so the batch validator can detect them and
    // atomically discard the entire batch (the blob data was never durable).
    if (file_size < BlobLogHeader::kSize) {
      OrphanFile orphan;
      orphan.reader = nullptr;
      orphan.file_size = 0;
      orphan.compression = kNoCompression;
      orphan.column_family_id = 0;
      orphan.has_footer = false;
      orphan.blob_count = 0;
      orphan.total_blob_bytes = 0;

      ROCKS_LOG_INFO(info_log,
                     "OrphanBlobFileResolver: empty orphan blob file %" PRIu64
                     " (%" PRIu64 " bytes, no header)",
                     file_number, file_size);

      r->orphan_files_.emplace(file_number, std::move(orphan));
      continue;
    }

    // Open the file.
    std::unique_ptr<FSRandomAccessFile> file;
    FileOptions file_opts;
    file_opts.io_options.io_activity = Env::IOActivity::kDBOpen;
    io_s = fs->NewRandomAccessFile(blob_path, file_opts, &file, nullptr);
    if (!io_s.ok()) {
      continue;
    }
    auto file_reader = std::make_unique<RandomAccessFileReader>(
        std::move(file), blob_path, clock);

    // Read and validate the blob file header.
    char header_buf[BlobLogHeader::kSize];
    Slice header_slice;
    io_s = file_reader->Read(io_opts, 0, BlobLogHeader::kSize, &header_slice,
                             header_buf, nullptr, nullptr);
    if (!io_s.ok() || header_slice.size() != BlobLogHeader::kSize) {
      ROCKS_LOG_WARN(info_log,
                     "OrphanBlobFileResolver: skipping blob file %" PRIu64
                     " with unreadable header",
                     file_number);
      continue;
    }

    BlobLogHeader header;
    Status s = header.DecodeFrom(header_slice);
    if (!s.ok()) {
      ROCKS_LOG_WARN(info_log,
                     "OrphanBlobFileResolver: skipping blob file %" PRIu64
                     " with corrupt header",
                     file_number);
      continue;
    }

    // Skip files belonging to dropped column families.
    auto* cfd = versions->GetColumnFamilySet()->GetColumnFamily(
        header.column_family_id);
    if (cfd == nullptr) {
      ROCKS_LOG_INFO(info_log,
                     "OrphanBlobFileResolver: skipping blob file %" PRIu64
                     " for dropped CF %" PRIu32,
                     file_number, header.column_family_id);
      continue;
    }

    OrphanFile orphan;
    orphan.reader = std::move(file_reader);
    orphan.file_size = file_size;
    orphan.compression = header.compression;
    orphan.column_family_id = header.column_family_id;
    orphan.has_footer = false;

    // Check if the file already has a valid footer (e.g., sealed during a
    // previous DB::Close that didn't call LogAndApply). This avoids
    // appending a duplicate footer during orphan recovery.
    if (file_size >= BlobLogHeader::kSize + BlobLogFooter::kSize) {
      char footer_buf[BlobLogFooter::kSize];
      Slice footer_slice;
      io_s = orphan.reader->Read(io_opts, file_size - BlobLogFooter::kSize,
                                 BlobLogFooter::kSize, &footer_slice,
                                 footer_buf, nullptr, nullptr);
      if (io_s.ok() && footer_slice.size() == BlobLogFooter::kSize) {
        BlobLogFooter existing_footer;
        if (existing_footer.DecodeFrom(footer_slice).ok()) {
          orphan.has_footer = true;
        }
      }
    }

    // Scan records to compute blob_count and total_blob_bytes.
    // These are needed for the BlobFileAddition when registering in MANIFEST.
    // For files with a footer, stop before the footer to avoid misreading it.
    //
    // Truncate-to-last-valid: if the file has a partial record at the end
    // (e.g., SIGKILL during a write), we stop at the last fully intact
    // record. This mirrors how WAL recovery truncates to the last valid
    // record. The file will be truncated to valid_data_end before sealing.
    uint64_t blob_count = 0;
    uint64_t total_blob_bytes = 0;
    const uint64_t scan_limit =
        orphan.has_footer ? (file_size - BlobLogFooter::kSize) : file_size;
    uint64_t pos = BlobLogHeader::kSize;
    while (pos + BlobLogRecord::kHeaderSize <= scan_limit) {
      char rec_header_buf[BlobLogRecord::kHeaderSize];
      Slice rec_header_slice;
      io_s = orphan.reader->Read(io_opts, pos, BlobLogRecord::kHeaderSize,
                                 &rec_header_slice, rec_header_buf, nullptr,
                                 nullptr);
      if (!io_s.ok() || rec_header_slice.size() != BlobLogRecord::kHeaderSize) {
        break;
      }
      BlobLogRecord record;
      Status rec_s = record.DecodeHeaderFrom(rec_header_slice);
      if (!rec_s.ok()) {
        break;
      }
      const uint64_t record_size =
          BlobLogRecord::kHeaderSize + record.key_size + record.value_size;
      // Check that the full record (header + key + value) fits within the
      // file. A partial write could produce a valid header but truncated
      // key/value data. Without this check, we would count the partial
      // record, and TryResolveBlob would later fail with a CRC mismatch.
      if (pos + record_size > scan_limit) {
        ROCKS_LOG_INFO(info_log,
                       "OrphanBlobFileResolver: truncating blob file %" PRIu64
                       " at offset %" PRIu64 " (partial record: need %" PRIu64
                       " bytes, only %" PRIu64 " available)",
                       file_number, pos, record_size, scan_limit - pos);
        break;
      }
      blob_count++;
      total_blob_bytes += record_size;
      pos += record_size;
    }
    orphan.blob_count = blob_count;
    orphan.total_blob_bytes = total_blob_bytes;
    // valid_data_end is the position after the last complete, validated
    // record. For files without a footer, set file_size to this value so
    // that TryResolveBlob rejects offsets in any corrupt/partial trailing
    // data. For files with a footer, the original file_size is correct.
    const uint64_t valid_data_end = BlobLogHeader::kSize + total_blob_bytes;
    if (!orphan.has_footer) {
      orphan.file_size = valid_data_end;
    }

    ROCKS_LOG_INFO(info_log,
                   "OrphanBlobFileResolver: orphan blob file %" PRIu64
                   " CF %" PRIu32 " has %" PRIu64 " blobs, %" PRIu64 " bytes",
                   file_number, header.column_family_id, blob_count,
                   total_blob_bytes);

    r->orphan_files_.emplace(file_number, std::move(orphan));
  }

  if (!r->orphan_files_.empty()) {
    ROCKS_LOG_INFO(info_log,
                   "OrphanBlobFileResolver: found %zu orphan blob files",
                   r->orphan_files_.size());
  }

  *resolver = std::move(r);
  return Status::OK();
}

bool OrphanBlobFileResolver::IsOrphan(uint64_t file_number) const {
  return orphan_files_.count(file_number) > 0;
}

bool OrphanBlobFileResolver::IsRegistered(uint64_t file_number) const {
  return registered_files_.count(file_number) > 0;
}

Status OrphanBlobFileResolver::TryResolveBlob(
    uint64_t file_number, uint64_t offset, uint64_t value_size,
    CompressionType compression, const Slice& user_key, std::string* value) {
  assert(value);

  auto it = orphan_files_.find(file_number);
  if (it == orphan_files_.end()) {
    return Status::NotFound("Not an orphan blob file");
  }

  const OrphanFile& orphan = it->second;
  const uint64_t key_size = user_key.size();

  // Validate the offset.
  if (!IsValidBlobOffset(offset, key_size, value_size, orphan.file_size,
                         orphan.has_footer)) {
    ++discarded_count_;
    return Status::Corruption("Invalid blob offset in orphan file");
  }

  // Read the full record: header + key + value.
  // BlobIndex offset points to the blob value, not the record start.
  // This runs during WAL replay (DB::Open), so use kDBOpen io_activity.
  IOOptions io_opts;
  io_opts.io_activity = Env::IOActivity::kDBOpen;

  const uint64_t adjustment =
      BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size);
  assert(offset >= adjustment);
  const uint64_t record_offset = offset - adjustment;
  const uint64_t record_size = adjustment + value_size;

  std::unique_ptr<char[]> buf(new char[static_cast<size_t>(record_size)]);
  Slice record_slice;

  IOStatus io_s = orphan.reader->Read(
      io_opts, record_offset, static_cast<size_t>(record_size), &record_slice,
      buf.get(), nullptr, nullptr);
  if (!io_s.ok()) {
    ++discarded_count_;
    return Status::Corruption("Failed to read blob record from orphan file: " +
                              io_s.ToString());
  }

  if (record_slice.size() != record_size) {
    ++discarded_count_;
    return Status::Corruption("Short read from orphan blob file");
  }

  // Verify the record: decode header (checks header CRC), verify key/value
  // sizes, verify key matches, check blob CRC.
  BlobLogRecord record;
  {
    const Slice header_slice(record_slice.data(), BlobLogRecord::kHeaderSize);
    Status s = record.DecodeHeaderFrom(header_slice);
    if (!s.ok()) {
      ++discarded_count_;
      return s;
    }
  }

  if (record.key_size != user_key.size()) {
    ++discarded_count_;
    return Status::Corruption("Key size mismatch in orphan blob record");
  }
  if (record.value_size != value_size) {
    ++discarded_count_;
    return Status::Corruption("Value size mismatch in orphan blob record");
  }

  record.key =
      Slice(record_slice.data() + BlobLogRecord::kHeaderSize, record.key_size);
  if (record.key != user_key) {
    ++discarded_count_;
    return Status::Corruption("Key mismatch in orphan blob record");
  }

  record.value = Slice(record.key.data() + record.key_size, value_size);
  {
    Status s = record.CheckBlobCRC();
    if (!s.ok()) {
      ++discarded_count_;
      return s;
    }
  }

  // Extract the value slice (after header + key).
  const Slice value_slice(record_slice.data() + adjustment, value_size);

  // Decompress if needed.
  if (compression != kNoCompression) {
    auto decompressor =
        GetBuiltinV2CompressionManager()->GetDecompressorOptimizeFor(
            compression);

    Decompressor::Args args;
    args.compression_type = compression;
    args.compressed_data = value_slice;

    Status s = decompressor->ExtractUncompressedSize(args);
    if (!s.ok()) {
      ++discarded_count_;
      return Status::Corruption("Decompression size extraction failed: " +
                                s.ToString());
    }

    std::string decompressed(args.uncompressed_size, '\0');
    s = decompressor->DecompressBlock(args, decompressed.data());
    if (!s.ok()) {
      ++discarded_count_;
      return Status::Corruption("Decompression failed: " + s.ToString());
    }
    *value = std::move(decompressed);
  } else {
    value->assign(value_slice.data(), value_slice.size());
  }

  ++resolved_count_;
  RecordTick(statistics_, BLOB_DB_ORPHAN_RECOVERY_RESOLVED);
  return Status::OK();
}

std::vector<OrphanBlobFileResolver::OrphanFileInfo>
OrphanBlobFileResolver::GetOrphanFileInfo() const {
  std::vector<OrphanFileInfo> result;
  result.reserve(orphan_files_.size());
  for (const auto& [file_number, orphan] : orphan_files_) {
    const uint64_t valid_data_size =
        BlobLogHeader::kSize + orphan.total_blob_bytes;
    result.push_back({file_number, orphan.column_family_id, orphan.file_size,
                      orphan.blob_count, orphan.total_blob_bytes,
                      orphan.has_footer, valid_data_size});
  }
  return result;
}

}  // namespace ROCKSDB_NAMESPACE
