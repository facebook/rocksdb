//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Supporting structs and definitions for lazy blob resolution and wide column
// projection / partial reads.

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

#include "rocksdb/compression_type.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class LazyWideColumnsHelper;

// Sentinel `length` for a column read: read from `offset` to the end of the
// column's logical (post-decompression) value.
inline constexpr size_t kLazyWholeColumn = std::numeric_limits<size_t>::max();

// EXPERIMENTAL and subject to change
//
// A single byte-range read against one column of a LazyWideColumns result, used
// by LazyWideColumns::MultiResolve (and its GetColumn/GetColumnRange sugar).
//
// `result` and `status` are effectively out-params, filled by the resolve call,
// so both pointed-to objects must outlive that call. Zero-copy: on success
// `*result` is a view into a stable backing buffer owned by the
// LazyWideColumns.
struct LazyColumnReadRequest {
  // Which column of the target LazyWideColumns to read (index into the in-order
  // column set; see LazyWideColumns::num_columns()).
  size_t column_index = 0;

  // Starting byte offset within the column's logical value. An offset at or
  // past the end of the value yields an empty result (not an error). A nonzero
  // offset makes this a partial read; see `force_verify` for checksums.
  //
  // Not yet optimized: a partial read currently resolves the whole column and
  // slices it, so for now it saves no I/O and skips no checksum.
  uint64_t offset = 0;

  // Number of bytes to read starting at `offset`, clamped to the end of the
  // value. kLazyWholeColumn reads the entire remainder from `offset`. Anything
  // less (or a nonzero `offset`) is a partial read: it returns only the
  // requested bytes and by default skips whole-record checksum verification
  // (see `force_verify`).
  //
  // Not yet optimized: a partial read currently resolves the whole column and
  // slices it, so for now it saves no I/O and skips no checksum.
  size_t length = kLazyWholeColumn;

  // Extra lever to prioritize checksum verification over I/O efficiency, mainly
  // for partial reads. Partial reads normally skip checksum verification (they
  // read less than a checksum covers); set this to verify anyway, reading and
  // checking as much as the checksum requires (today the whole record) even
  // when that exceeds the requested range or ReadOptions::verify_checksums is
  // off. No effect where verification already happens (e.g. a whole-column read
  // under verify_checksums) or where the format has no usable checksum.
  //
  // Not yet honored: because reads currently resolve the whole column, this
  // flag has no effect for now.
  bool force_verify = false;

  // Output: on OK status, a zero-copy view of the requested bytes.
  PinnableSlice* result = nullptr;

  // Output: per-request status. InvalidArgument for an out-of-range
  // column_index; Incomplete for a cache miss when the originating
  // GetEntityLazy()/MultiGetEntityLazy() call used ReadTier::kBlockCacheTier;
  // otherwise the I/O status of the (possibly partial) read.
  Status* status = nullptr;
};

// EXPERIMENTAL and subject to change
//
// A self-contained result of a *lazy* wide-column query (see
// DB::GetEntityLazy). Its inline columns are materialized zero-copy up front,
// but its blob-backed columns are left as *unresolved references*: the blob
// bytes are read from storage only when explicitly pulled (by byte range),
// driving only the I/O the caller actually needs. Columns that are never pulled
// are never read.
//
// Unlike PinnableWideColumns (the eager result type), a LazyWideColumns can
// outlive the DB call that produced it: it holds a pin (as an iterator does)
// that keeps the referenced blob files / SST readers valid so deferred reads
// stay resolvable. A standalone result from GetEntityLazy() holds its own pin;
// a result from MultiGetEntityLazy() is kept alive by its enclosing
// LazyWideColumnsBatch, so it is valid only as long as that batch is.
//
// Zero-copy: every enumerated inline value and every resolved range is a Slice
// into a stable backing buffer owned or pinned by this object, so results stay
// valid across move. Like PinnableSlice / PinnableWideColumns, this type is
// move-only.
//
// Thread safety: not thread-safe; use a single result from one thread at a
// time. Distinct results may be resolved concurrently.
class LazyWideColumns {
 public:
  LazyWideColumns();
  ~LazyWideColumns();

  LazyWideColumns(const LazyWideColumns&) = delete;
  LazyWideColumns& operator=(const LazyWideColumns&) = delete;

  LazyWideColumns(LazyWideColumns&&) noexcept;
  LazyWideColumns& operator=(LazyWideColumns&&) noexcept;

  // ---- Enumeration (no I/O) ----
  // Columns are presented in sorted (by name) order, matching GetEntity. All of
  // the accessors below require column_index < num_columns().

  // Number of columns in the entity.
  size_t num_columns() const;

  // Name of the column at `column_index`.
  const Slice& name(size_t column_index) const;

  // True if the column is stored as a blob reference, false if it is an inline
  // column whose bytes are already available via inline_value(). This reflects
  // how the column is stored and does NOT change when the column is later
  // resolved: resolved bytes come from the read APIs below (which cache them),
  // never from inline_value().
  bool is_reference(size_t column_index) const;

  // Whether the column's exact logical size is known without any I/O. True for
  // inline columns and for uncompressed blob references; false for compressed
  // blob references (BlobIndex records on-disk size, not logical size).
  bool logical_size_known(size_t column_index) const;

  // The column's logical (post-decompression) size in bytes. Only meaningful
  // when logical_size_known(column_index) is true.
  uint64_t logical_size(size_t column_index) const;

  // The compression used for a blob-referenced column (kNoCompression for
  // inline columns).
  CompressionType compression(size_t column_index) const;

  // Zero-copy view of an inline column's value. Valid only when
  // !is_reference(column_index); resolve references via the read APIs below.
  const Slice& inline_value(size_t column_index) const;

  // ---- Resolution: batch-first, async-ready ----
  //
  // Resolution uses the ReadOptions from the originating
  // GetEntityLazy()/MultiGetEntityLazy() call (e.g. read_tier,
  // verify_checksums); these methods intentionally take no ReadOptions of their
  // own (only a handful of ReadOptions fields are meaningful once the entity's
  // references are fixed).

  // Primary single-entity API: resolve a batch of byte-range reads against this
  // entity's columns in one call. Each entry's `result`/`status` out-params are
  // filled independently; multiple ranges from one column and reads spanning
  // multiple columns are all just more entries. (To resolve across many keys,
  // use LazyWideColumnsBatch.)
  //
  // A column's blob is fetched from storage at most once over this result's
  // lifetime: its resolved bytes are cached in the result (which is why these
  // methods are non-const), so repeated reads of the same column -- several
  // entries in one call or across separate calls -- reuse that one fetch and do
  // no further I/O.
  //
  // Returns an overall Status (OK if every read was dispatched; per-read
  // outcomes are in each request's `status`).
  Status MultiResolve(size_t num_reads, LazyColumnReadRequest* reads);

  // ---- Per-column convenience (sugar over a one-entry batch) ----

  // Resolve a single byte range [offset, offset+length) of one column.
  // Equivalent to a one-entry MultiResolve; prefer MultiResolve when pulling
  // several ranges so they can be coalesced.
  Status GetColumnRange(size_t column_index, uint64_t offset, size_t length,
                        PinnableSlice* result);

  // Resolve a whole column (offset 0, kLazyWholeColumn).
  Status GetColumn(size_t column_index, PinnableSlice* result);

  // Release all buffers and (for a standalone result) the pin.
  void Reset();

  // Opaque internal representation (pimpl), so this header stays free of
  // internal dependencies.
  class Rep;

 private:
  friend class LazyWideColumnsBatch;
  friend class LazyWideColumnsHelper;

  std::unique_ptr<Rep> rep_;
};

// EXPERIMENTAL and subject to change
//
// A batch of lazy wide-column results produced by one MultiGetEntityLazy call.
//
// The batch owns the pin(s) that keep its entities resolvable and holds the N
// per-key results. Cross-key resolution is a method on this batch, and each of
// its reads names its target entity by index into the batch, so a read always
// resolves through its owning entity and cannot mix results from a different
// batch.
//
// Move-only, like LazyWideColumns. The contained entities are valid only while
// the batch is alive; destroy the batch promptly to release the pin(s).
class LazyWideColumnsBatch {
 public:
  LazyWideColumnsBatch();
  ~LazyWideColumnsBatch();

  LazyWideColumnsBatch(const LazyWideColumnsBatch&) = delete;
  LazyWideColumnsBatch& operator=(const LazyWideColumnsBatch&) = delete;

  LazyWideColumnsBatch(LazyWideColumnsBatch&&) noexcept;
  LazyWideColumnsBatch& operator=(LazyWideColumnsBatch&&) noexcept;

  // Number of per-key results (matches the num_keys passed to
  // MultiGetEntityLazy). Entities whose corresponding status was not OK are
  // present but empty.
  size_t num_entities() const;

  // The per-key result at `entity_index` (< num_entities()). The returned
  // reference is valid until the batch is moved, Reset(), or destroyed.
  const LazyWideColumns& entity(size_t entity_index) const;
  LazyWideColumns& entity(size_t entity_index);

  // Cross-key batch resolution: resolve a set of byte-range reads that may span
  // any entities in this batch, in one call. Each read is a
  // std::pair<size_t, LazyColumnReadRequest>: `first` is the target entity's
  // index (< num_entities()) and `second` is the read. This is the cross-key
  // analogue of LazyWideColumns::MultiResolve. Per-read outcomes are reported
  // in each request's `status`; the returned Status is OK if the batch was
  // dispatched.
  Status MultiResolve(size_t num_reads,
                      std::pair<size_t, LazyColumnReadRequest>* reads);

  // Release all entities and the pin(s).
  void Reset();

  // Opaque internal representation (pimpl).
  class Rep;

 private:
  friend class LazyWideColumnsHelper;

  std::unique_ptr<Rep> rep_;
};

}  // namespace ROCKSDB_NAMESPACE
