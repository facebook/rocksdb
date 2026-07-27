//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>

#include "rocksdb/compression_type.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

// NOTE: This is scaffolding for the "lazy blob resolution + partial
// (byte-range) column reads" work. The types below define the intended public
// surface; the implementation is stubbed (see db/wide/lazy_wide_columns.cc) and
// the DB-side entry points (DB::GetEntityLazy et al.) currently return
// NotSupported. Names (LazyWideColumns / GetEntityLazy / ...) are working names
// and may change.

namespace ROCKSDB_NAMESPACE {

struct ReadOptions;
class LazyWideColumnsHelper;

// Sentinel `length` for a column read: read from `offset` to the end of the
// column's logical (post-decompression) value.
inline constexpr size_t kLazyWholeColumn = std::numeric_limits<size_t>::max();

// EXPERIMENTAL and subject to change
//
// A single byte-range read against one column of a LazyWideColumns result.
//
// This intentionally mirrors the internal BlobReadRequest so a batch of these
// maps directly onto the engine's coalescing/async blob-read machinery. The
// result and status are *out-params* (filled by the resolve call) rather than
// return-by-value, so an asynchronous batch can complete them after the call
// that submitted them returns -- without changing this request type.
//
// The referenced `result` and `status` must outlive the resolve call (and any
// async completion of it). Zero-copy: on success, `*result` is a view into a
// stable backing buffer owned by the LazyWideColumns (see class comment).
struct LazyColumnReadRequest {
  // Which column of the owning LazyWideColumns to read (index into the
  // in-order column set; see LazyWideColumns::num_columns()).
  size_t column_index = 0;

  // Starting byte offset within the column's logical value. An offset at or
  // past the end of the value yields an empty result (not an error).
  uint64_t offset = 0;

  // Number of bytes to read starting at `offset`, clamped to the end of the
  // value. kLazyWholeColumn reads the entire remainder from `offset`.
  size_t length = kLazyWholeColumn;

  // Force a full, CRC-checked read (and, for a cache hit, use the cached full
  // value) even when a partial read would otherwise suffice. When false, an
  // uncompressed blob on a cache miss may be read as a bare byte range,
  // skipping CRC verification and cache population (the documented tradeoff).
  bool verify = false;

  // Output: on OK status, a zero-copy view of the requested bytes.
  PinnableSlice* result = nullptr;

  // Output: per-request status. InvalidArgument for an out-of-range
  // column_index; Incomplete for a cache miss under ReadTier::kBlockCacheTier;
  // otherwise the I/O status of the (possibly partial) read.
  Status* status = nullptr;
};

// EXPERIMENTAL and subject to change
//
// The batch (cross-key) form used by LazyWideColumnsBatch::MultiResolve: the
// same read as above, plus which entity within the batch it targets, referenced
// *by index* rather than by pointer. The index form is deliberate: every entity
// in a batch shares one SuperVersion (see LazyWideColumnsBatch), so an
// index-based read cannot smuggle in an entity from a different batch /
// Version, and there is no dangling-pointer hazard if results are moved around.
struct LazyBatchColumnReadRequest : public LazyColumnReadRequest {
  // Which entity in the batch to read (index into
  // LazyWideColumnsBatch::num_entities()).
  size_t entity_index = 0;
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
// outlive the DB call that produced it: the referenced SuperVersion is pinned
// for its whole lifetime (as an iterator does), which keeps the referenced blob
// files / SST readers valid so deferred reads remain resolvable. For a
// standalone result from GetEntityLazy() the pin is held by the result itself;
// for a result obtained via MultiGetEntityLazy() the pin is held by the
// enclosing LazyWideColumnsBatch (which pins one SuperVersion per column family
// it spans), so the individual entities stay valid only as long as that batch
// does.
//
// Zero-copy & lifetime: every enumerated inline value and every resolved range
// is a Slice into a stable backing buffer owned or pinned by this object, so
// results stay valid across move without the caller retaining other state. Like
// PinnableSlice / PinnableWideColumns, this type is move-only.
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

  // True if the column is a (not-yet-resolved) blob reference; false if it is
  // an inline column whose bytes are already available via inline_value().
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

  // Primary single-entity API: resolve a batch of byte-range reads against this
  // entity's columns in one call. Reads are grouped and issued so this maps
  // onto the engine's coalesced (and, later, asynchronous) blob-read path --
  // this is what lets a single call resolve many blobs/fragments efficiently.
  // Each entry's `result`/`status` out-params are filled independently;
  // multiple ranges from one column and reads spanning multiple columns are all
  // just more entries. (To resolve across many keys, use LazyWideColumnsBatch.)
  //
  // Returns an overall Status (OK if every read was dispatched; per-read
  // outcomes are in each request's `status`).
  Status MultiResolve(const ReadOptions& read_options, size_t num_reads,
                      LazyColumnReadRequest* reads);

  // ---- Per-column convenience (sugar over a one-entry batch) ----

  // Resolve a single byte range [offset, offset+length) of one column.
  // Equivalent to a one-entry MultiResolve; prefer MultiResolve when pulling
  // several ranges so they can be coalesced.
  Status GetColumnRange(const ReadOptions& read_options, size_t column_index,
                        uint64_t offset, size_t length, PinnableSlice* result);

  // Resolve a whole column (offset 0, kLazyWholeColumn).
  Status GetColumn(const ReadOptions& read_options, size_t column_index,
                   PinnableSlice* result);

  // Release all buffers and (for a standalone result) the SuperVersion pin.
  void Reset();

  // Internal representation (owns backing buffers, the blob resolver, and, for
  // a standalone result, the SuperVersion pin). Defined in
  // db/wide/lazy_wide_columns.cc; opaque here so the public header stays free
  // of internal (db/wide, db/blob) dependencies.
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
// The batch owns the SuperVersion pin(s) for all of its entities, rather than
// one pin per key: it pins one SuperVersion per distinct column family it spans
// (a single pin for the common single-CF call; the API is shaped so a future
// cross-CF MultiGetEntityLazy pins one per CF). Cross-key resolution is a
// method on this batch, and its reads reference entities by index
// (LazyBatchColumnReadRequest::entity_index), so every read resolves through
// the Version its owning entity was read against. That matters because the
// coalescing/async blob-read machinery (Version::MultiGetBlob, per-CF
// BlobSource) is only valid within a single (column family, Version): making
// the batch the unit of resolution -- and routing each read by index to its
// owner -- turns that invariant into something structural rather than a caller
// responsibility, and removes the hazard of mixing results from different
// MultiGetEntityLazy calls (or, later, different column families).
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
  // any entities in this batch, in one call. Each read names its target entity
  // by index (LazyBatchColumnReadRequest::entity_index) plus a column index and
  // byte range. Reads are grouped per (column family, Version, blob file) and
  // each group is issued together (and, later, asynchronously) -- for a
  // single-CF batch that is one Version; a future cross-CF batch simply forms
  // one group per CF. This is the cross-key analogue of
  // LazyWideColumns::MultiResolve. Per-read outcomes are reported in each
  // request's `status`; the returned Status is OK if the batch was dispatched.
  Status MultiResolve(const ReadOptions& read_options, size_t num_reads,
                      LazyBatchColumnReadRequest* reads);

  // Release all entities and the SuperVersion pin(s).
  void Reset();

  // Internal representation (owns one SuperVersion pin per column family
  // spanned and the per-key LazyWideColumns). Defined in
  // db/wide/lazy_wide_columns.cc.
  class Rep;

 private:
  friend class LazyWideColumnsHelper;

  std::unique_ptr<Rep> rep_;
};

}  // namespace ROCKSDB_NAMESPACE
