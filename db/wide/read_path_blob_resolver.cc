//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/read_path_blob_resolver.h"

#include <cassert>

#include "db/blob/blob_fetcher.h"
#include "db/blob/same_file_blob_reader.h"
#include "db/version_set.h"
#include "db/wide/blob_column_resolver_util.h"

namespace ROCKSDB_NAMESPACE {

ReadPathBlobResolver::ReadPathBlobResolver(const Version* version,
                                           const ReadOptions& read_options,
                                           BlobFileCache* blob_file_cache,
                                           bool allow_write_path_fallback)
    : blob_fetcher_(version, ReadOptions(read_options), blob_file_cache,
                    allow_write_path_fallback) {}

void ReadPathBlobResolver::Reset(
    const Slice& user_key, const std::vector<WideColumn>* columns,
    const std::vector<std::pair<size_t, BlobIndex>>* blob_columns,
    const SameFileBlobReader* same_file_reader) {
  user_key_ = user_key;
  columns_ = columns;
  blob_columns_ = blob_columns;
  same_file_reader_ = same_file_reader;
  resolved_cache_.clear();
}

Status ReadPathBlobResolver::ResolveColumn(size_t column_index,
                                           Slice* resolved_value) {
  return ResolveColumnInternal(column_index, /*force_verify=*/false,
                               resolved_value);
}

Status ReadPathBlobResolver::ResolveColumnInternal(size_t column_index,
                                                   bool force_verify,
                                                   Slice* resolved_value) {
  assert(columns_);
  assert(resolved_value);

  Status status = Status::OK();
  if (column_index >= columns_->size()) {
    status = Status::InvalidArgument("Column index out of bounds");
  } else {
    const BlobIndex* blob_index_ptr =
        blob_resolver_util::FindBlobColumn(blob_columns_, column_index);

    if (blob_index_ptr == nullptr) {
      // Inline column -- return the value directly
      *resolved_value = (*columns_)[column_index].value();
    } else {
      // Check if already resolved
      PinnableSlice* cached =
          blob_resolver_util::FindInCache(resolved_cache_, column_index);
      if (cached != nullptr) {
        *resolved_value = *cached;
      } else {
        const BlobIndex& blob_index = *blob_index_ptr;

        // Handle inlined blobs
        if (blob_index.IsInlined()) {
          *resolved_value = blob_resolver_util::CacheInlinedBlob(
              resolved_cache_, column_index, blob_index);
        } else {
          resolved_cache_.emplace_back(column_index,
                                       std::make_unique<PinnableSlice>());
          auto& new_entry = resolved_cache_.back();

          constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
          constexpr uint64_t* bytes_read = nullptr;

          // TODO(lazy-blob-resolution-cleanup): the force_verify branching here
          // (and the whole-vs-range split in ResolveColumnRange) is really a
          // single 3-valued verify policy -- must-verify-if-present (today's
          // force_verify), verify-if-no-amplification (verify_checksums on: a
          // whole read verifies, a partial read skips), and ignore. Map
          // (verify_checksums, force_verify) to that internal enum once at this
          // boundary and thread it down, collapsing these branches. Deferred
          // for feature velocity; see the lazy blob resolution plan.
          if (force_verify && !blob_index.IsSameFile()) {
            // Honor force_verify for separate-file references: read and verify
            // the whole record even if ReadOptions::verify_checksums is off.
            status = blob_fetcher_.FetchBlobForceVerify(
                user_key_, blob_index, prefetch_buffer, new_entry.second.get(),
                bytes_read);
          } else if (force_verify && blob_index.IsSameFile()) {
            // Honor force_verify for same-file (embedded) references the same
            // way: force checksum verification on via a verify-enabled copy of
            // the read options. A same-file reference is only produced when the
            // entity was read from an SST that supplied its SameFileBlobReader,
            // so it is expected to be set; surface a clean error rather than
            // silently dropping force_verify (and taking the unverified path
            // below) if it somehow is not.
            assert(same_file_reader_ != nullptr);
            if (same_file_reader_ == nullptr) {
              status = Status::Corruption(
                  "Cannot force-verify same-file blob: no same-file reader");
            } else {
              ReadOptions verify_read_options = blob_fetcher_.read_options();
              verify_read_options.verify_checksums = true;
              status = same_file_reader_->GetSameFileBlob(
                  verify_read_options, blob_index, new_entry.second.get());
            }
          } else {
            // Route same-file ("embedded") references through the current SST's
            // SameFileBlobReader when one is set; separate-file references (and
            // the no-same-file-reader case) go straight to the Version-backed
            // fetcher. EffectiveFetcher() returns the plain base fetcher when
            // there is no same-file reader, adding no indirection.
            EmbeddedAwareBlobFetcher embedded_fetcher(&blob_fetcher_,
                                                      same_file_reader_);
            status = embedded_fetcher.EffectiveFetcher()->FetchBlob(
                user_key_, blob_index, prefetch_buffer, new_entry.second.get(),
                bytes_read);
          }
          if (!status.ok()) {
            resolved_cache_.pop_back();
          } else {
            *resolved_value = *new_entry.second;
          }
        }
      }
    }
  }
  return status;
}

namespace {

// Pin [range_offset, range_offset + range_length) of `whole` into *result,
// clamped to `whole`'s size (offset at/past end -> empty). The bytes are not
// copied or owned here: *result points into `whole`, whose storage must outlive
// *result (the resolver's whole-column cache / entity buffer, which lives as
// long as this resolver).
void PinClampedSubRange(const Slice& whole, uint64_t range_offset,
                        size_t range_length, PinnableSlice* result) {
  result->Reset();
  if (range_offset >= whole.size()) {
    result->PinSlice(Slice(), nullptr);
    return;
  }
  const size_t off = static_cast<size_t>(range_offset);
  const size_t avail = whole.size() - off;
  const size_t len = range_length > avail ? avail : range_length;
  result->PinSlice(Slice(whole.data() + off, len), nullptr);
}

}  // namespace

Status ReadPathBlobResolver::ResolveColumnRange(size_t column_index,
                                                uint64_t range_offset,
                                                size_t range_length,
                                                bool force_verify,
                                                PinnableSlice* result) {
  assert(columns_);

  if (column_index >= columns_->size()) {
    return Status::InvalidArgument("Column index out of bounds");
  }

  // No output buffer: the caller only wants to surface any I/O / integrity
  // error (and honor force_verify). Resolve the whole column and return; there
  // is nothing to slice into.
  if (result == nullptr) {
    Slice ignored;
    return ResolveColumnInternal(column_index, force_verify, &ignored);
  }

  const BlobIndex* blob_index_ptr =
      blob_resolver_util::FindBlobColumn(blob_columns_, column_index);

  // Decide whether the I/O-saving partial path applies. It requires a blob
  // reference that is: not already resolved (else we slice the cached whole
  // value), uncompressed (a strict sub-range of a compressed record can't be
  // decompressed in isolation), a strict sub-range (a whole-column read takes
  // the verifying + cache-filling path), and not force_verify. The read then
  // goes to either the separate-file range fetcher (needs a Version) or, for a
  // same-file / embedded reference, the current SST's SameFileBlobReader.
  if (blob_index_ptr != nullptr && !force_verify &&
      blob_resolver_util::FindInCache(resolved_cache_, column_index) ==
          nullptr) {
    const BlobIndex& blob_index = *blob_index_ptr;
    const bool separate_file_ok =
        !blob_index.IsSameFile() && blob_fetcher_.SupportsRangeRead();
    const bool same_file_ok =
        blob_index.IsSameFile() && same_file_reader_ != nullptr;
    if (!blob_index.IsInlined() && blob_index.compression() == kNoCompression &&
        (separate_file_ok || same_file_ok)) {
      const uint64_t value_size = blob_index.size();
      const bool strict_subrange =
          range_offset > 0 || range_length < value_size;
      if (strict_subrange) {
        result->Reset();
        if (range_offset >= value_size || range_length == 0) {
          // Nothing to read; empty (not an error), no I/O.
          result->PinSlice(Slice(), nullptr);
          return Status::OK();
        }
        const size_t avail = static_cast<size_t>(value_size - range_offset);
        const size_t actual_len = range_length > avail ? avail : range_length;
        if (blob_index.IsSameFile()) {
          return same_file_reader_->GetSameFileBlobRange(
              blob_fetcher_.read_options(), blob_index, range_offset,
              actual_len, result);
        }
        return blob_fetcher_.FetchBlobRange(user_key_, blob_index, range_offset,
                                            actual_len, result,
                                            /*bytes_read=*/nullptr);
      }
    }
  }

  // Full path: resolve the whole column (inline value directly, blob reference
  // via the resolver's cache, filling it on a miss and verifying under
  // ReadOptions::verify_checksums or force_verify), then slice out the
  // requested range.
  Slice whole;
  Status s = ResolveColumnInternal(column_index, force_verify, &whole);
  if (!s.ok()) {
    return s;
  }
  PinClampedSubRange(whole, range_offset, range_length, result);
  return Status::OK();
}

Status ReadPathBlobResolver::ResolveColumns(
    const std::vector<size_t>& column_indices,
    std::vector<Slice>* resolved_values) {
  assert(resolved_values != nullptr);

  resolved_values->clear();
  resolved_values->reserve(column_indices.size());

  for (size_t column_index : column_indices) {
    Slice resolved_value;
    Status s = ResolveColumn(column_index, &resolved_value);
    if (!s.ok()) {
      resolved_values->clear();
      return s;
    }
    resolved_values->push_back(resolved_value);
  }

  return Status::OK();
}

Status ReadPathBlobResolver::ResolveAllColumns() {
  assert(columns_);

  if (!blob_columns_) {
    return Status::OK();
  }

  for (const auto& blob_col : *blob_columns_) {
    // ResolveColumn internally checks the cache, so we can just call it
    // directly; it will no-op for already resolved columns.
    Slice resolved_value;
    Status s = ResolveColumn(blob_col.first, &resolved_value);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

bool ReadPathBlobResolver::IsUnresolvedColumn(size_t column_index) const {
  if (!columns_ || column_index >= columns_->size()) {
    return false;
  }

  if (!blob_resolver_util::IsBlobColumnIndex(blob_columns_, column_index)) {
    return false;
  }

  return blob_resolver_util::FindInCache(resolved_cache_, column_index) ==
         nullptr;
}

bool ReadPathBlobResolver::HasUnresolvedColumns() const {
  if (!blob_columns_ || blob_columns_->empty()) {
    return false;
  }

  for (const auto& blob_col : *blob_columns_) {
    if (blob_resolver_util::FindInCache(resolved_cache_, blob_col.first) ==
        nullptr) {
      return true;
    }
  }

  return false;
}

size_t ReadPathBlobResolver::NumColumns() const {
  if (!columns_) {
    return 0;
  }
  return columns_->size();
}

}  // namespace ROCKSDB_NAMESPACE
