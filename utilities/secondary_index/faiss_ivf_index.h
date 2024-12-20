//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>

#include "faiss/IndexIVF.h"
#include "rocksdb/utilities/secondary_index.h"

namespace ROCKSDB_NAMESPACE {

// A SecondaryIndex implementation that wraps a FAISS inverted file index.
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

  Status GetSecondaryValue(const Slice& primary_key,
                           const Slice& primary_column_value,
                           const Slice& original_column_value,
                           std::optional<std::variant<Slice, std::string>>*
                               secondary_value) const override;

 private:
  class Adapter;

  static std::string SerializeLabel(faiss::idx_t label);
  static faiss::idx_t DeserializeLabel(Slice label_slice);

  std::unique_ptr<Adapter> adapter_;
  std::unique_ptr<faiss::IndexIVF> index_;
  std::string primary_column_name_;
  ColumnFamilyHandle* primary_column_family_{};
  ColumnFamilyHandle* secondary_column_family_{};
};

}  // namespace ROCKSDB_NAMESPACE
