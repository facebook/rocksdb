//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/secondary_index.h"

namespace faiss {
struct IndexIVF;
}

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL
//
// Creates a new FAISS inverted file based secondary index that indexes the
// embedding in the specified primary column using the given pre-trained
// faiss::IndexIVF object (which the secondary index takes ownership of).
// The secondary index iterator returned by the index can be used to perform
// K-nearest-neighbors queries (see also SecondaryIndex::NewIterator and
// SecondaryIndexReadOptions).
std::unique_ptr<SecondaryIndex> NewFaissIVFIndex(
    std::unique_ptr<faiss::IndexIVF>&& index, std::string primary_column_name);

// Helper methods to convert embeddings from a span of floats to Slice or vice
// versa

// Convert the given span of floats of size dim to a Slice.
// PRE: embedding points to a contiguous span of floats of size dim
inline Slice ConvertFloatsToSlice(const float* embedding, size_t dim) {
  return Slice(reinterpret_cast<const char*>(embedding), dim * sizeof(float));
}

// Convert the given Slice to a span of floats of size dim.
// PRE: embedding.size() == dim * sizeof(float)
// Returns nullptr if the precondition is violated.
inline const float* ConvertSliceToFloats(const Slice& embedding, size_t dim) {
  if (embedding.size() != dim * sizeof(float)) {
    return nullptr;
  }

  return reinterpret_cast<const float*>(embedding.data());
}

}  // namespace ROCKSDB_NAMESPACE
