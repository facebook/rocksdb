//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/lazy_wide_columns.h"

#include <cassert>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/wide/blob_column_resolver_util.h"
#include "db/wide/lazy_wide_columns_helper.h"
#include "db/wide/read_path_blob_resolver.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "rocksdb/cleanable.h"
#include "rocksdb/options.h"
#include "rocksdb/wide_columns.h"

// Current implementation of the lazy blob resolution API. Enumeration and
// whole-column resolution are functional; a byte range is served by resolving
// the whole column and slicing it. A future phase reads only the requested
// bytes from storage (skipping checksum verification and cache-fill for
// uncompressed blobs) instead of the whole column. Cross-key coalescing and
// async execution are also future work; here LazyWideColumnsBatch::MultiResolve
// simply routes each read to its owning per-key result, and MultiGetEntityLazy
// fills the batch key-by-key.

namespace ROCKSDB_NAMESPACE {
// Internal representation. Owns the serialized-entity backing buffer + inline
// columns (via `entity_`), the decoded blob references, the per-column views,
// the on-demand blob resolver, and the SuperVersion pin that keeps all of it
// (blob files, immortal table readers) valid after the producing DB call
// returns. The Rep is heap-allocated and never relocated (it lives behind a
// unique_ptr that only ever transfers ownership), so pointers to it and to its
// `columns_` elements stay stable across LazyWideColumns/Batch moves -- which
// is what lets a LazyColumnReadRequest identify its column (and owning result)
// by pointer without any move-time fix-ups.
class LazyWideColumns::Rep {
 public:
  // Holds the serialized entity as a single backing buffer and, after
  // SetWideColumnValue (without resolution), the full sorted column set --
  // inline columns' value() are zero-copy Slices into that buffer; blob
  // columns' value() are the raw serialized BlobIndex bytes (the resolver reads
  // decoded indices from blob_columns_, not from here).
  PinnableWideColumns entity_;

  // Decoded blob references (column_index -> BlobIndex), for the columns whose
  // value is a blob reference. Empty for a fully inline entity.
  std::vector<std::pair<size_t, BlobIndex>> blob_columns_;

  // Per-column view returned by LazyWideColumns::operator[]. Derives from the
  // public LazyWideColumn (its ctor sets the protected base fields) and adds a
  // back-pointer to this Rep, used to validate that a read's column belongs to
  // this result (and, via owning_batch_rep_, to this batch). Nested (rather
  // than in an anonymous namespace) so it shares the enclosing class's linkage
  // -- an internal-linkage subobject type in this external-linkage class trips
  // -Wsubobject-linkage in unity builds.
  struct ColumnImpl : public LazyWideColumn {
    ColumnImpl(Rep* parent, size_t index, Slice name, const char* inline_data,
               std::optional<uint64_t> size)
        : parent_rep_(parent) {
      name_ = name;
      inline_data_ = inline_data;
      size_ = size;
      index_ = index;
    }
    Rep* parent_rep_ = nullptr;
  };

  // One entry per column in `entity_.columns()`, in sorted order.
  std::vector<ColumnImpl> columns_;

  // Identity of the owning batch's Rep (stable across batch move), or null for
  // a standalone result. Used to validate that a batch read targets an entity
  // of that batch. Stored as void* to avoid naming LazyWideColumnsBatch::Rep
  // here (it is only an identity token, never dereferenced).
  const void* owning_batch_rep_ = nullptr;

  // Stable copy of the user key, referenced by the resolver for blob
  // verification (the originating key argument does not outlive the call).
  std::string user_key_;

  // Pins the SuperVersion for this result's lifetime. Declared before
  // `resolver_` so the resolver (which references the Version) is destroyed
  // first.
  Cleanable pin_;

  // On-demand blob resolver, bound to entity_.columns() + blob_columns_.
  // Emplaced at finalize time (needs the Version). Absent for an unpopulated
  // (e.g. NotFound) result.
  std::optional<ReadPathBlobResolver> resolver_;

  // Resolve one read against column `index` of this result (index < size()),
  // writing the per-read outcome to read.status / read.result. Shared by the
  // single-entity and batch MultiResolve paths.
  void ResolveOneRead(size_t index, LazyColumnReadRequest& read) {
    if (read.status) {
      *read.status = Status::OK();
    }

    // Resolve the whole column (inline value directly, blob reference via the
    // resolver, which caches so repeated reads of one column read the blob at
    // most once), then slice out the requested byte range.
    //
    // TODO(lazy-blob-resolution-phase1): for a strict sub-range of an
    // uncompressed separate-file blob on a cache miss, read only the requested
    // bytes from storage (skipping checksum and cache-fill) instead of the
    // whole column; a further phase (TODO(lazy-blob-resolution-phase2)) does
    // the same for embedded (same-file) blobs.
    const LazyWideColumn& info = columns_[index];
    Slice whole;
    Status s;
    if (!info.is_reference()) {
      whole = *info.inline_value();
    } else {
      assert(resolver_);
      s = resolver_->ResolveColumn(index, &whole);
    }

    if (!s.ok()) {
      if (read.status) {
        *read.status = s;
      }
      return;
    }

    if (read.result != nullptr) {
      read.result->Reset();
      if (read.offset >= whole.size()) {
        // Offset at/past the end clamps to empty (not an error).
        read.result->PinSlice(Slice(), nullptr);
      } else {
        const size_t offset = static_cast<size_t>(read.offset);
        const size_t avail = whole.size() - offset;
        const size_t len =
            (read.length == kLazyWholeColumn || read.length > avail)
                ? avail
                : read.length;
        read.result->PinSlice(Slice(whole.data() + offset, len), nullptr);
      }
    }
    // read.status already OK.
  }
};

LazyWideColumns::LazyWideColumns() = default;
LazyWideColumns::~LazyWideColumns() = default;
LazyWideColumns::LazyWideColumns(LazyWideColumns&&) noexcept = default;
LazyWideColumns& LazyWideColumns::operator=(LazyWideColumns&&) noexcept =
    default;

size_t LazyWideColumns::size() const {
  return rep_ ? rep_->columns_.size() : 0;
}

const LazyWideColumn& LazyWideColumns::operator[](size_t i) const {
  assert(rep_);
  assert(i < rep_->columns_.size());
  return rep_->columns_[i];
}

Status LazyWideColumns::MultiResolve(size_t num_reads,
                                     LazyColumnReadRequest* reads) {
  for (size_t i = 0; i < num_reads; ++i) {
    LazyColumnReadRequest& read = reads[i];
    if (read.status) {
      *read.status = Status::OK();
    }
    // The column must belong to this result (it carries a back-pointer to the
    // Rep that owns it).
    if (read.column == nullptr ||
        static_cast<const Rep::ColumnImpl*>(read.column)->parent_rep_ !=
            rep_.get()) {
      if (read.status) {
        *read.status = Status::InvalidArgument(
            "Column does not belong to this LazyWideColumns");
      }
      continue;
    }
    rep_->ResolveOneRead(read.column->index(), read);
  }
  // Every read was dispatched synchronously; per-read outcomes are in
  // read.status.
  return Status::OK();
}

Status LazyWideColumns::ResolveColumnRange(const LazyWideColumn& column,
                                           uint64_t offset, size_t length,
                                           PinnableSlice* result) {
  // Sugar over a one-entry batch, so the batch path is the single real,
  // optimizable (coalescing/async) code path.
  Status status;
  LazyColumnReadRequest read;
  read.column = &column;
  read.offset = offset;
  read.length = length;
  read.result = result;
  read.status = &status;

  const Status batch_status = MultiResolve(/*num_reads=*/1, &read);
  return batch_status.ok() ? status : batch_status;
}

Status LazyWideColumns::ResolveColumn(const LazyWideColumn& column,
                                      PinnableSlice* result) {
  return ResolveColumnRange(column, /*offset=*/0, kLazyWholeColumn, result);
}

void LazyWideColumns::Reset() { rep_.reset(); }

// ---- LazyWideColumnsBatch ----

// Internal representation. Owns the per-key LazyWideColumns. (In the current
// phase each entity holds its own SuperVersion pin; cf_pins is reserved for the
// future shared-pin, cross-CF design and is currently unused.)
class LazyWideColumnsBatch::Rep {
 public:
  // One result per key of the MultiGetEntityLazy call, in key order.
  std::vector<LazyWideColumns> entities;

  // TODO(lazy-blob-resolution-phase3): switch to one shared SuperVersion pin
  // per column family here (instead of one self-pin per entity), populated by a
  // batched MultiGetEntityLazy.
  std::map<uint32_t /* column_family_id */, Cleanable> cf_pins;
};

LazyWideColumnsBatch::LazyWideColumnsBatch() = default;
LazyWideColumnsBatch::~LazyWideColumnsBatch() = default;
LazyWideColumnsBatch::LazyWideColumnsBatch(LazyWideColumnsBatch&&) noexcept =
    default;
LazyWideColumnsBatch& LazyWideColumnsBatch::operator=(
    LazyWideColumnsBatch&&) noexcept = default;

size_t LazyWideColumnsBatch::size() const {
  return rep_ ? rep_->entities.size() : 0;
}

const LazyWideColumns& LazyWideColumnsBatch::operator[](size_t i) const {
  assert(rep_);
  assert(i < rep_->entities.size());
  return rep_->entities[i];
}

LazyWideColumns& LazyWideColumnsBatch::operator[](size_t i) {
  assert(rep_);
  assert(i < rep_->entities.size());
  return rep_->entities[i];
}

Status LazyWideColumnsBatch::MultiResolve(size_t num_reads,
                                          LazyColumnReadRequest* reads) {
  const void* this_rep = rep_.get();
  for (size_t i = 0; i < num_reads; ++i) {
    LazyColumnReadRequest& read = reads[i];
    if (read.status) {
      *read.status = Status::OK();
    }
    if (read.column == nullptr) {
      if (read.status) {
        *read.status = Status::InvalidArgument("Null column in batch read");
      }
      continue;
    }
    // Route the read to the entity that owns its column, and require that
    // entity to belong to this batch.
    // TODO(lazy-blob-resolution-phase3): group reads per (CF, Version, blob
    // file) across entities and coalesce them; the request shape is unchanged.
    LazyWideColumns::Rep* entity_rep =
        static_cast<const LazyWideColumns::Rep::ColumnImpl*>(read.column)
            ->parent_rep_;
    if (entity_rep == nullptr || entity_rep->owning_batch_rep_ != this_rep) {
      if (read.status) {
        *read.status =
            Status::InvalidArgument("Column does not belong to this batch");
      }
      continue;
    }
    entity_rep->ResolveOneRead(read.column->index(), read);
  }
  return Status::OK();
}

void LazyWideColumnsBatch::Reset() { rep_.reset(); }

// ---- LazyWideColumnsHelper (DB-side construction) ----

PinnableWideColumns* LazyWideColumnsHelper::EntityBuffer(
    LazyWideColumns* result) {
  assert(result);
  if (!result->rep_) {
    result->rep_ = std::make_unique<LazyWideColumns::Rep>();
  }
  return &result->rep_->entity_;
}

Status LazyWideColumnsHelper::Finalize(
    LazyWideColumns* result, const Slice& user_key, const Version* version,
    const ReadOptions& read_options, BlobFileCache* blob_file_cache,
    bool allow_write_path_fallback, const SameFileBlobReader* same_file_reader,
    Cleanable&& pin) {
  assert(result);
  assert(result->rep_);
  LazyWideColumns::Rep& rep = *result->rep_;

  // Decode the blob references (if the entity still has unresolved ones -- it
  // does not for a fully inline entity, or one resolved eagerly on the memtable
  // path). When unresolved indices are present the entity occupies a single
  // backing buffer, so GetSerializedEntity() is valid.
  if (!PinnableWideColumnsHelper::GetUnresolvedBlobColumnIndices(rep.entity_)
           .empty()) {
    WideColumns unused_columns;
    const Status s = WideColumnSerialization::Deserialize(
        PinnableWideColumnsHelper::GetSerializedEntity(rep.entity_),
        unused_columns, &rep.blob_columns_);
    if (!s.ok()) {
      return s;
    }
  }

  // Build the per-column views from the (sorted) column set + blob references.
  const WideColumns& columns = rep.entity_.columns();
  rep.columns_.clear();
  rep.columns_.reserve(columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    const BlobIndex* blob_index =
        blob_resolver_util::FindBlobColumn(&rep.blob_columns_, i);
    const char* inline_data = nullptr;
    std::optional<uint64_t> size;
    if (blob_index == nullptr) {
      // Inline column: bytes available with no I/O. Slice guarantees a non-null
      // data(), so a non-null inline_data marks this as inline (not a
      // reference) even for an empty value.
      inline_data = columns[i].value().data();
      size = columns[i].value().size();
    } else if (blob_index->IsInlined()) {
      // TTL-inlined blob: an unresolved reference whose logical size is known
      // (the value lives in the index).
      size = blob_index->value().size();
    } else if (blob_index->compression() == kNoCompression) {
      // Uncompressed reference: on-disk size equals logical size.
      size = blob_index->size();
    }
    // else: compressed reference -- logical size unknown until resolved.
    rep.columns_.emplace_back(&rep, i, columns[i].name(), inline_data, size);
  }

  // Take ownership of the SuperVersion pin and stand up the resolver bound to
  // the (address-stable, since Rep is heap-allocated) entity columns + blob
  // references.
  rep.pin_ = std::move(pin);
  rep.user_key_.assign(user_key.data(), user_key.size());
  rep.resolver_.emplace(version, read_options, blob_file_cache,
                        allow_write_path_fallback);
  rep.resolver_->Reset(Slice(rep.user_key_), &rep.entity_.columns(),
                       &rep.blob_columns_, same_file_reader);
  return Status::OK();
}

void LazyWideColumnsHelper::InitBatch(LazyWideColumnsBatch* batch,
                                      size_t num_entities) {
  assert(batch);
  if (!batch->rep_) {
    batch->rep_ = std::make_unique<LazyWideColumnsBatch::Rep>();
  }
  batch->rep_->entities.clear();
  batch->rep_->entities.resize(num_entities);
}

void LazyWideColumnsHelper::FinalizeBatch(LazyWideColumnsBatch* batch) {
  assert(batch);
  if (!batch->rep_) {
    return;
  }
  // Link each populated entity to this batch (by the batch Rep's stable
  // identity) so batch reads can validate that a column belongs to this batch.
  const void* batch_rep = batch->rep_.get();
  for (LazyWideColumns& entity : batch->rep_->entities) {
    if (entity.rep_) {
      entity.rep_->owning_batch_rep_ = batch_rep;
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE
