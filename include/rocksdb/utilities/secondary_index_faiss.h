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
// A SecondaryIndex implementation that wraps a FAISS inverted file based index.
// Indexes the embedding in the specified primary column using the given
// pre-trained faiss::IndexIVF object (which the secondary index takes ownership
// of). Can be used to perform K-nearest-neighbors queries.
class FaissIVFIndex : public SecondaryIndex {
 public:
  explicit FaissIVFIndex(std::unique_ptr<faiss::IndexIVF>&& index,
                         std::string primary_column_name);
  ~FaissIVFIndex() override;

  void SetPrimaryColumnFamily(ColumnFamilyHandle* column_family) override;
  void SetSecondaryColumnFamily(ColumnFamilyHandle* column_family) override;

  ColumnFamilyHandle* GetPrimaryColumnFamily() const override;
  ColumnFamilyHandle* GetSecondaryColumnFamily() const override;

  Slice GetPrimaryColumnName() const override;

  Status UpdatePrimaryColumnValue(
      const Slice& primary_key, const Slice& primary_column_value,
      std::optional<std::variant<Slice, std::string>>* updated_column_value)
      const override;

  Status GetSecondaryKeyPrefix(
      const Slice& primary_key, const Slice& primary_column_value,
      std::variant<Slice, std::string>* secondary_key_prefix) const override;

  Status FinalizeSecondaryKeyPrefix(
      std::variant<Slice, std::string>* secondary_key_prefix) const override;

  Status GetSecondaryValue(const Slice& primary_key,
                           const Slice& primary_column_value,
                           const Slice& original_column_value,
                           std::optional<std::variant<Slice, std::string>>*
                               secondary_value) const override;

  std::unique_ptr<Iterator> NewIterator(
      const SecondaryIndexReadOptions& read_options,
      std::unique_ptr<Iterator>&& underlying_it) const override;

 private:
  class KNNIterator;
  class Adapter;

  std::unique_ptr<Adapter> adapter_;
  std::unique_ptr<faiss::IndexIVF> index_;
  std::string primary_column_name_;
  ColumnFamilyHandle* primary_column_family_{};
  ColumnFamilyHandle* secondary_column_family_{};
};

std::unique_ptr<FaissIVFIndex> NewFaissIVFIndex(
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
