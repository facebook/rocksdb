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
// bytes from storage (skipping CRC/cache-fill for uncompressed blobs) instead
// of the whole column. Cross-key coalescing and async execution are also future
// work; here LazyWideColumnsBatch::MultiResolve simply routes each read to its
// owning per-key result, and MultiGetEntityLazy fills the batch key-by-key.

namespace ROCKSDB_NAMESPACE {

// Per-column enumeration metadata, precomputed at finalize time so the
// (no-I/O) accessors are trivial. Slices (name/inline_value) point into the
// entity's backing buffer, which this result owns.
namespace {
struct ColumnInfo {
  Slice name;
  bool is_reference = false;
  bool logical_size_known = false;
  uint64_t logical_size = 0;
  CompressionType compression = kNoCompression;
  // Valid only when !is_reference.
  Slice inline_value;
};
}  // namespace

// Internal representation. Owns the serialized-entity backing buffer + inline
// columns (via `entity_`), the decoded blob references, the enumeration
// metadata, the on-demand blob resolver, and the SuperVersion pin that keeps
// all of it (blob files, immortal table readers) valid after the producing DB
// call returns.
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

  // Enumeration metadata, one entry per column in `entity_.columns()`.
  std::vector<ColumnInfo> column_infos_;

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
};

LazyWideColumns::LazyWideColumns() = default;
LazyWideColumns::~LazyWideColumns() = default;
LazyWideColumns::LazyWideColumns(LazyWideColumns&&) noexcept = default;
LazyWideColumns& LazyWideColumns::operator=(LazyWideColumns&&) noexcept =
    default;

size_t LazyWideColumns::num_columns() const {
  return rep_ ? rep_->column_infos_.size() : 0;
}

const Slice& LazyWideColumns::name(size_t column_index) const {
  assert(rep_);
  assert(column_index < rep_->column_infos_.size());
  return rep_->column_infos_[column_index].name;
}

bool LazyWideColumns::is_reference(size_t column_index) const {
  assert(rep_);
  assert(column_index < rep_->column_infos_.size());
  return rep_->column_infos_[column_index].is_reference;
}

bool LazyWideColumns::logical_size_known(size_t column_index) const {
  assert(rep_);
  assert(column_index < rep_->column_infos_.size());
  return rep_->column_infos_[column_index].logical_size_known;
}

uint64_t LazyWideColumns::logical_size(size_t column_index) const {
  assert(rep_);
  assert(column_index < rep_->column_infos_.size());
  return rep_->column_infos_[column_index].logical_size;
}

CompressionType LazyWideColumns::compression(size_t column_index) const {
  assert(rep_);
  assert(column_index < rep_->column_infos_.size());
  return rep_->column_infos_[column_index].compression;
}

const Slice& LazyWideColumns::inline_value(size_t column_index) const {
  assert(rep_);
  assert(column_index < rep_->column_infos_.size());
  assert(!rep_->column_infos_[column_index].is_reference);
  return rep_->column_infos_[column_index].inline_value;
}

Status LazyWideColumns::MultiResolve(const ReadOptions& /* read_options */,
                                     size_t num_reads,
                                     LazyColumnReadRequest* reads) {
  const size_t num_cols = num_columns();
  for (size_t i = 0; i < num_reads; ++i) {
    LazyColumnReadRequest& read = reads[i];
    if (read.status) {
      *read.status = Status::OK();
    }

    if (read.column_index >= num_cols) {
      if (read.status) {
        *read.status =
            Status::InvalidArgument("Column index out of range for lazy read");
      }
      continue;
    }

    // Resolve the whole column (inline value directly, blob reference via the
    // resolver, which caches so repeated reads of one column read the blob at
    // most once), then slice out the requested byte range.
    //
    // TODO(lazy-blob-resolution-phase1): for a strict sub-range of an
    // uncompressed separate-file blob on a cache miss, read only the requested
    // bytes from storage (skipping CRC and cache-fill) instead of the whole
    // column; a further phase (TODO(lazy-blob-resolution-phase2)) does the same
    // for embedded (same-file) blobs.
    const ColumnInfo& info = rep_->column_infos_[read.column_index];
    Slice whole;
    Status s;
    if (!info.is_reference) {
      whole = info.inline_value;
    } else {
      assert(rep_->resolver_);
      s = rep_->resolver_->ResolveColumn(read.column_index, &whole);
    }

    if (!s.ok()) {
      if (read.status) {
        *read.status = s;
      }
      continue;
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
  // Every read was dispatched synchronously; per-read outcomes are in
  // read.status.
  return Status::OK();
}

Status LazyWideColumns::GetColumnRange(const ReadOptions& read_options,
                                       size_t column_index, uint64_t offset,
                                       size_t length, PinnableSlice* result) {
  // Sugar over a one-entry batch, so the batch path is the single real,
  // optimizable (coalescing/async) code path.
  Status status;
  LazyColumnReadRequest read;
  read.column_index = column_index;
  read.offset = offset;
  read.length = length;
  read.result = result;
  read.status = &status;

  const Status batch_status =
      MultiResolve(read_options, /*num_reads=*/1, &read);
  return batch_status.ok() ? status : batch_status;
}

Status LazyWideColumns::GetColumn(const ReadOptions& read_options,
                                  size_t column_index, PinnableSlice* result) {
  return GetColumnRange(read_options, column_index, /*offset=*/0,
                        kLazyWholeColumn, result);
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

size_t LazyWideColumnsBatch::num_entities() const {
  return rep_ ? rep_->entities.size() : 0;
}

const LazyWideColumns& LazyWideColumnsBatch::entity(size_t entity_index) const {
  assert(rep_);
  assert(entity_index < rep_->entities.size());
  return rep_->entities[entity_index];
}

LazyWideColumns& LazyWideColumnsBatch::entity(size_t entity_index) {
  assert(rep_);
  assert(entity_index < rep_->entities.size());
  return rep_->entities[entity_index];
}

Status LazyWideColumnsBatch::MultiResolve(const ReadOptions& read_options,
                                          size_t num_reads,
                                          LazyBatchColumnReadRequest* reads) {
  const size_t num_entities = rep_ ? rep_->entities.size() : 0;
  for (size_t i = 0; i < num_reads; ++i) {
    LazyBatchColumnReadRequest& read = reads[i];
    if (read.entity_index >= num_entities) {
      if (read.status) {
        *read.status =
            Status::InvalidArgument("Entity index out of range for batch read");
      }
      continue;
    }
    // Route to the owning entity.
    // TODO(lazy-blob-resolution-phase3): group reads per (CF, Version, blob
    // file) across entities and coalesce them; the request shape is unchanged.
    LazyColumnReadRequest single;
    single.column_index = read.column_index;
    single.offset = read.offset;
    single.length = read.length;
    single.verify = read.verify;
    single.result = read.result;
    single.status = read.status;
    rep_->entities[read.entity_index].MultiResolve(read_options,
                                                   /*num_reads=*/1, &single);
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

  // Build enumeration metadata from the (sorted) column set + blob references.
  const WideColumns& columns = rep.entity_.columns();
  rep.column_infos_.clear();
  rep.column_infos_.reserve(columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    ColumnInfo info;
    info.name = columns[i].name();
    const BlobIndex* blob_index =
        blob_resolver_util::FindBlobColumn(&rep.blob_columns_, i);
    if (blob_index == nullptr) {
      // Inline column: bytes available with no I/O.
      info.is_reference = false;
      info.inline_value = columns[i].value();
      info.logical_size_known = true;
      info.logical_size = columns[i].value().size();
      info.compression = kNoCompression;
    } else {
      info.is_reference = true;
      if (blob_index->IsInlined()) {
        // TTL-inlined blob: value lives in the index; no I/O and size known.
        info.logical_size_known = true;
        info.logical_size = blob_index->value().size();
        info.compression = kNoCompression;
      } else {
        info.compression = blob_index->compression();
        if (blob_index->compression() == kNoCompression) {
          // Uncompressed: on-disk size equals logical size.
          info.logical_size_known = true;
          info.logical_size = blob_index->size();
        } else {
          // Compressed: BlobIndex records on-disk (compressed) size only.
          info.logical_size_known = false;
          info.logical_size = 0;
        }
      }
    }
    rep.column_infos_.push_back(info);
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

}  // namespace ROCKSDB_NAMESPACE
