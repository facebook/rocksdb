//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>

#include "rocksdb/cleanable.h"
#include "rocksdb/lazy_wide_columns.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileCache;
class PinnableWideColumns;
class SameFileBlobReader;
class Version;
struct ReadOptions;

// Internal (DB-side) helper for populating LazyWideColumns /
// LazyWideColumnsBatch results without exposing their construction machinery on
// the public API (the same pattern as PinnableWideColumnsHelper). Only the DB
// read path uses this.
class LazyWideColumnsHelper {
 public:
  // Creates (if needed) the internal representation of `result` and returns the
  // PinnableWideColumns to use as the columns output of a lazy point lookup.
  // The returned buffer is owned by `result`; after the lookup fills it, call
  // Finalize().
  static PinnableWideColumns* EntityBuffer(LazyWideColumns* result);

  // Finalizes a lazy result after its EntityBuffer() was populated by a point
  // lookup: decodes any remaining blob references, builds the enumeration
  // metadata, constructs the on-demand resolver bound to the entity, and takes
  // ownership of the SuperVersion `pin`. `version` is the Version blob
  // references resolve against; `same_file_reader` (may be null) resolves
  // same-file/embedded references. Returns non-OK only on a decode error.
  static Status Finalize(LazyWideColumns* result, const Slice& user_key,
                         const Version* version,
                         const ReadOptions& read_options,
                         BlobFileCache* blob_file_cache,
                         bool allow_write_path_fallback,
                         const SameFileBlobReader* same_file_reader,
                         Cleanable&& pin);

  // Prepares `batch` to hold `num_entities` empty per-key results (creating the
  // batch's representation). Use (*batch)[i] to access each for filling, then
  // call FinalizeBatch() once all entities are populated.
  static void InitBatch(LazyWideColumnsBatch* batch, size_t num_entities);

  // Links every populated entity of `batch` back to the batch, so a batch read
  // can validate that a column belongs to this batch. Call after all entities
  // have been filled (and individually Finalize()d).
  static void FinalizeBatch(LazyWideColumnsBatch* batch);
};

}  // namespace ROCKSDB_NAMESPACE
