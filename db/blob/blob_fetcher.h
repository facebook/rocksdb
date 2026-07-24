//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileCache;
class Version;
class Slice;
class FilePrefetchBuffer;
class PinnableSlice;
class BlobIndex;
class SameFileBlobReader;

// Abstract interface for blob retrieval on the read path. An implementation
// resolves a BlobIndex to its value, pinning it into a PinnableSlice. Callers
// hold a plain const BlobFetcher* so resolution can be intercepted by a
// decorator (e.g. EmbeddedAwareBlobFetcher) without their knowledge.
class BlobFetcher {
 public:
  virtual ~BlobFetcher() = default;

  // Convenience overload: decodes `blob_index_slice` into a BlobIndex and
  // dispatches to the virtual overload below. Non-virtual; shared by all
  // implementations.
  Status FetchBlob(const Slice& user_key, const Slice& blob_index_slice,
                   FilePrefetchBuffer* prefetch_buffer,
                   PinnableSlice* blob_value, uint64_t* bytes_read) const;

  virtual Status FetchBlob(const Slice& user_key, const BlobIndex& blob_index,
                           FilePrefetchBuffer* prefetch_buffer,
                           PinnableSlice* blob_value,
                           uint64_t* bytes_read) const = 0;

  // The read options this fetcher operates under. Used (via an
  // EmbeddedAwareBlobFetcher decorator) to resolve same-file blob references,
  // which need the current read_tier.
  virtual const ReadOptions& read_options() const = 0;
};

// The default BlobFetcher: reads through a Version, optionally falling back to
// direct-write blob files that are not yet manifest-visible. Usable by value.
class VersionBlobFetcher : public BlobFetcher {
 public:
  VersionBlobFetcher(const Version* version, const ReadOptions& read_options,
                     BlobFileCache* blob_file_cache = nullptr,
                     bool allow_write_path_fallback = false)
      : version_(version),
        read_options_(read_options),
        blob_file_cache_(blob_file_cache),
        allow_write_path_fallback_(allow_write_path_fallback) {}

  // Un-hide the base's Slice-based convenience overload.
  using BlobFetcher::FetchBlob;

  Status FetchBlob(const Slice& user_key, const BlobIndex& blob_index,
                   FilePrefetchBuffer* prefetch_buffer,
                   PinnableSlice* blob_value,
                   uint64_t* bytes_read) const override;

  const ReadOptions& read_options() const override { return read_options_; }

 private:
  const Version* version_;
  ReadOptions read_options_;
  BlobFileCache* blob_file_cache_;
  bool allow_write_path_fallback_;
};

// A BlobFetcher decorator that resolves same-file ("embedded") blob references
// against a SameFileBlobReader (the current SST) and delegates all other
// references to a base BlobFetcher. It is cheap to construct as a stack local
// per resolution: the same-file target is per-SST while the base fetcher is one
// per-Get object shared across SSTs, so an immutable per-SST decorator avoids
// shared-mutable-state hazards under async MultiGet.
//
// A decorator is "enabled" when it has a same-file reader and "disabled"
// (unusable) when constructed with a null `same_file_reader` (e.g. via the
// default constructor). A disabled decorator must not be routed through
// directly; callers construct one unconditionally and use EffectiveFetcher(),
// which returns this decorator when enabled or the undecorated base fetcher
// otherwise. This avoids std::optional / branching at call sites while adding
// no indirection on the common (non-embedded) path.
//
// The base fetcher may be null for an entity whose blob columns are all
// same-file (no separate-file blob support wired, e.g. SstFileReader); a
// non-same-file reference then surfaces as a Corruption rather than silently.
class EmbeddedAwareBlobFetcher : public BlobFetcher {
 public:
  explicit EmbeddedAwareBlobFetcher(
      const BlobFetcher* base = nullptr,
      const SameFileBlobReader* same_file_reader = nullptr)
      : base_(base), same_file_reader_(same_file_reader) {}

  // The fetcher to route resolution through: this decorator when it has a
  // same-file reader, otherwise the (undecorated) base fetcher. Only an enabled
  // decorator's FetchBlob is ever invoked.
  const BlobFetcher* EffectiveFetcher() const {
    return same_file_reader_ != nullptr ? this : base_;
  }

  // Un-hide the base's Slice-based convenience overload.
  using BlobFetcher::FetchBlob;

  Status FetchBlob(const Slice& user_key, const BlobIndex& blob_index,
                   FilePrefetchBuffer* prefetch_buffer,
                   PinnableSlice* blob_value,
                   uint64_t* bytes_read) const override;

  const ReadOptions& read_options() const override {
    // Same-file resolution uses the base fetcher's read options; when there is
    // no base (e.g. SstFileReader) a default (kReadAllTier) suffices.
    static const ReadOptions kDefaultReadOptions;
    return base_ != nullptr ? base_->read_options() : kDefaultReadOptions;
  }

 private:
  const BlobFetcher* base_;
  // Null iff this decorator is disabled (see class comment); otherwise the
  // same-file blob reader for the current SST.
  const SameFileBlobReader* same_file_reader_;
};

}  // namespace ROCKSDB_NAMESPACE
