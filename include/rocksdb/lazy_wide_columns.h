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
#include <optional>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class LazyWideColumn;
class LazyWideColumnsHelper;

// Sentinel `length` for a column read: read from `offset` to the end of the
// column's logical (post-decompression) value.
inline constexpr size_t kLazyWholeColumn = std::numeric_limits<size_t>::max();

// EXPERIMENTAL and subject to change
//
// A single byte-range read against one column of a LazyWideColumns result, used
// by both LazyWideColumns::MultiResolve and LazyWideColumnsBatch::MultiResolve
// (and the ResolveColumn/ResolveColumnRange sugar).
//
// `column` names the target column, obtained from the result via operator[] or
// iteration. Because a column identifies its owning result, a batch resolve
// routes each read to the right entity and both resolves reject a column that
// does not belong to the target (InvalidArgument) -- there are no untyped
// indices to mix up. `result` and `status` are out-params filled by the resolve
// call, so both pointed-to objects must outlive it. Zero-copy: on success
// `*result` is a view into a stable backing buffer owned by the owning
// LazyWideColumns.
struct LazyColumnReadRequest {
  // The column to read (from the target result's operator[] / iteration). Must
  // belong to the result being resolved.
  const LazyWideColumn* column = nullptr;

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

  // Output: per-request status. InvalidArgument for a null `column` or one that
  // does not belong to the target result; Incomplete for a cache miss when the
  // originating GetEntityLazy()/MultiGetEntityLazy() call used
  // ReadTier::kBlockCacheTier; otherwise the I/O status of the (possibly
  // partial) read.
  Status* status = nullptr;
};

// EXPERIMENTAL and subject to change
//
// One column of a LazyWideColumns result (the lazy analogue of WideColumn): its
// name, its inline bytes when materialized, and its logical size when known
// without I/O. A blob-backed column that has not been resolved is a
// "reference": it has no inline_value(); resolve it by naming it in a
// LazyColumnReadRequest passed to the owning result's read APIs. Accessors
// return references to this base; the implementation extends it internally.
class LazyWideColumn {
 public:
  const Slice& name() const { return name_; }

  // Position of this column within its owning LazyWideColumns.
  size_t index() const { return index_; }

  // The column's inline bytes, or no value if this is an unresolved blob
  // reference. Equivalent: is_reference() == !inline_value().has_value().
  OptSlice inline_value() const {
    return inline_data_ == nullptr ? OptSlice()
                                   : OptSlice(Slice(inline_data_, *size_));
  }
  bool is_reference() const { return inline_data_ == nullptr; }

  // The column's logical (post-decompression) size in bytes when known without
  // I/O: always for inline columns and uncompressed references, empty for
  // compressed references.
  std::optional<uint64_t> logical_size() const { return size_; }

 protected:
  LazyWideColumn() = default;

  Slice name_;
  // Points at the inline bytes (length is `*size_`), or null when this column
  // is an unresolved blob reference. Non-null even for an empty inline value
  // (Slice guarantees a non-null data()).
  const char* inline_data_ = nullptr;
  // Logical size when known (see logical_size()); also the inline value length
  // when inline_data_ is non-null.
  std::optional<uint64_t> size_;
  // Position within the owning result.
  size_t index_ = 0;
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

  // ---- Columns (enumeration; no I/O) ----
  //
  // Indexable and iterable like a vector of LazyWideColumn, in sorted (by name)
  // order (matching GetEntity). A column's position is its index(); pass the
  // column itself (not an index) to the resolution APIs below.
  size_t size() const;
  bool empty() const { return size() == 0; }

  // The column at `i` (requires i < size()). The reference is valid until this
  // result is moved, Reset(), or destroyed.
  const LazyWideColumn& operator[](size_t i) const;

  // Read-only forward iteration over the columns.
  class Iterator {
   public:
    Iterator(const LazyWideColumns* owner, size_t pos)
        : owner_(owner), pos_(pos) {}
    const LazyWideColumn& operator*() const { return (*owner_)[pos_]; }
    const LazyWideColumn* operator->() const { return &(*owner_)[pos_]; }
    Iterator& operator++() {
      ++pos_;
      return *this;
    }
    bool operator==(const Iterator& other) const { return pos_ == other.pos_; }
    bool operator!=(const Iterator& other) const { return pos_ != other.pos_; }

   private:
    const LazyWideColumns* owner_;
    size_t pos_;
  };
  Iterator begin() const { return Iterator(this, 0); }
  Iterator end() const { return Iterator(this, size()); }

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

  // Sugar over the pointer+count form for a std::vector of reads.
  Status MultiResolve(std::vector<LazyColumnReadRequest>& reads) {
    return MultiResolve(reads.size(), reads.data());
  }

  // ---- Per-column convenience (sugar over a one-entry batch) ----

  // Resolve a single byte range [offset, offset+length) of one column.
  // Equivalent to a one-entry MultiResolve; prefer MultiResolve when pulling
  // several ranges so they can be coalesced.
  Status ResolveColumnRange(const LazyWideColumn& column, uint64_t offset,
                            size_t length, PinnableSlice* result);

  // Resolve a whole column (offset 0, kLazyWholeColumn).
  Status ResolveColumn(const LazyWideColumn& column, PinnableSlice* result);

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
// per-key results (indexable/iterable like a vector of LazyWideColumns).
// Cross-key resolution is a method on this batch; each read names its target
// column (which identifies its owning entity), so a read always resolves
// through its owning entity and cannot mix results from a different batch.
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

  // Vector-like access to the per-key results (in the key order passed to
  // MultiGetEntityLazy). Entities whose corresponding status was not OK are
  // present but empty. References are valid until the batch is moved, Reset(),
  // or destroyed.
  size_t size() const;
  bool empty() const { return size() == 0; }
  const LazyWideColumns& operator[](size_t i) const;
  LazyWideColumns& operator[](size_t i);

  // Read-only forward iteration over the per-key results.
  class Iterator {
   public:
    Iterator(const LazyWideColumnsBatch* owner, size_t pos)
        : owner_(owner), pos_(pos) {}
    const LazyWideColumns& operator*() const { return (*owner_)[pos_]; }
    const LazyWideColumns* operator->() const { return &(*owner_)[pos_]; }
    Iterator& operator++() {
      ++pos_;
      return *this;
    }
    bool operator==(const Iterator& other) const { return pos_ == other.pos_; }
    bool operator!=(const Iterator& other) const { return pos_ != other.pos_; }

   private:
    const LazyWideColumnsBatch* owner_;
    size_t pos_;
  };
  Iterator begin() const { return Iterator(this, 0); }
  Iterator end() const { return Iterator(this, size()); }

  // Cross-key batch resolution: resolve a set of byte-range reads that may span
  // any entities in this batch, in one call, using the same
  // LazyColumnReadRequest as LazyWideColumns::MultiResolve. Each read's
  // `column` names its target entity (which must belong to this batch); a
  // foreign or null column yields InvalidArgument on that read. Per-read
  // outcomes are reported in each request's `status`; the returned Status is OK
  // if the batch was dispatched.
  Status MultiResolve(size_t num_reads, LazyColumnReadRequest* reads);

  // Sugar over the pointer+count form for a std::vector of reads.
  Status MultiResolve(std::vector<LazyColumnReadRequest>& reads) {
    return MultiResolve(reads.size(), reads.data());
  }

  // Release all entities and the pin(s).
  void Reset();

  // Opaque internal representation (pimpl).
  class Rep;

 private:
  friend class LazyWideColumnsHelper;

  std::unique_ptr<Rep> rep_;
};

}  // namespace ROCKSDB_NAMESPACE
