//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <optional>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL
//
// Read options for secondary index iterators (see also
// SecondaryIndex::NewIterator)

struct SecondaryIndexReadOptions {
  // The maximum number of neighbors K to return when performing a
  // K-nearest-neighbors vector similarity search. The number of neighbors
  // returned can be smaller if there are not enough vectors in the inverted
  // lists probed. Only applicable to FAISS IVF secondary indices, where it must
  // be specified and positive. See also SecondaryIndex::NewIterator and
  // similarity_search_probes below.
  //
  // Default: none
  std::optional<size_t> similarity_search_neighbors;

  // The number of inverted lists to probe when performing a K-nearest-neighbors
  // vector similarity search. Only applicable to FAISS IVF secondary indices,
  // where it must be specified and positive. See also
  // SecondaryIndex::NewIterator and similarity_search_neighbors above.
  //
  // Default: none
  std::optional<size_t> similarity_search_probes;
};

}  // namespace ROCKSDB_NAMESPACE
