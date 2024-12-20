//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "db/wide/wide_columns_helper.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/secondary_index.h"
#include "rocksdb/wide_columns.h"
#include "util/autovector.h"
#include "util/overload.h"

namespace ROCKSDB_NAMESPACE {

template <typename Txn>
class SecondaryIndexMixin : public Txn {
 public:
  template <typename... Args>
  explicit SecondaryIndexMixin(
      const std::vector<std::shared_ptr<SecondaryIndex>>* secondary_indices,
      Args&&... args)
      : Txn(std::forward<Args>(args)...),
        secondary_indices_(secondary_indices) {
    assert(secondary_indices_);
    assert(!secondary_indices_->empty());
  }

  using Txn::Put;
  Status Put(ColumnFamilyHandle* /* column_family */, const Slice& /* key */,
             const Slice& /* value */,
             const bool /* assume_tracked */ = false) override {
    return Status::NotSupported("Put with secondary indices not yet supported");
  }
  Status Put(ColumnFamilyHandle* /* column_family */,
             const SliceParts& /* key */, const SliceParts& /* value */,
             const bool /* assume_tracked */ = false) override {
    return Status::NotSupported("Put with secondary indices not yet supported");
  }

  Status PutEntity(ColumnFamilyHandle* column_family, const Slice& key,
                   const WideColumns& columns,
                   bool assume_tracked = false) override {
    return PerformWithSavePoint([&]() {
      const bool do_validate = !assume_tracked;
      return PutEntityWithSecondaryIndices(column_family, key, columns,
                                           do_validate);
    });
  }

  using Txn::Merge;
  Status Merge(ColumnFamilyHandle* /* column_family */, const Slice& /* key */,
               const Slice& /* value */,
               const bool /* assume_tracked */ = false) override {
    return Status::NotSupported(
        "Merge with secondary indices not yet supported");
  }

  using Txn::Delete;
  Status Delete(ColumnFamilyHandle* /* column_family */, const Slice& /* key */,
                const bool /* assume_tracked */ = false) override {
    return Status::NotSupported(
        "Delete with secondary indices not yet supported");
  }
  Status Delete(ColumnFamilyHandle* /* column_family */,
                const SliceParts& /* key */,
                const bool /* assume_tracked */ = false) override {
    return Status::NotSupported(
        "Delete with secondary indices not yet supported");
  }

  using Txn::SingleDelete;
  Status SingleDelete(ColumnFamilyHandle* /* column_family */,
                      const Slice& /* key */,
                      const bool /* assume_tracked */ = false) override {
    return Status::NotSupported(
        "SingleDelete with secondary indices not yet supported");
  }
  Status SingleDelete(ColumnFamilyHandle* /* column_family */,
                      const SliceParts& /* key */,
                      const bool /* assume_tracked */ = false) override {
    return Status::NotSupported(
        "SingleDelete with secondary indices not yet supported");
  }

  using Txn::PutUntracked;
  Status PutUntracked(ColumnFamilyHandle* /* column_family */,
                      const Slice& /* key */,
                      const Slice& /* value */) override {
    return Status::NotSupported(
        "PutUntracked with secondary indices not yet supported");
  }
  Status PutUntracked(ColumnFamilyHandle* /* column_family */,
                      const SliceParts& /* key */,
                      const SliceParts& /* value */) override {
    return Status::NotSupported(
        "PutUntracked with secondary indices not yet supported");
  }

  Status PutEntityUntracked(ColumnFamilyHandle* column_family, const Slice& key,
                            const WideColumns& columns) override {
    return PerformWithSavePoint([&]() {
      constexpr bool do_validate = false;
      return PutEntityWithSecondaryIndices(column_family, key, columns,
                                           do_validate);
    });
  }

  using Txn::MergeUntracked;
  Status MergeUntracked(ColumnFamilyHandle* /* column_family */,
                        const Slice& /* key */,
                        const Slice& /* value */) override {
    return Status::NotSupported(
        "MergeUntracked with secondary indices not yet supported");
  }

  using Txn::DeleteUntracked;
  Status DeleteUntracked(ColumnFamilyHandle* /* column_family */,
                         const Slice& /* key */) override {
    return Status::NotSupported(
        "DeleteUntracked with secondary indices not yet supported");
  }
  Status DeleteUntracked(ColumnFamilyHandle* /* column_family */,
                         const SliceParts& /* key */) override {
    return Status::NotSupported(
        "DeleteUntracked with secondary indices not yet supported");
  }

  using Txn::SingleDeleteUntracked;
  Status SingleDeleteUntracked(ColumnFamilyHandle* /* column_family */,
                               const Slice& /* key */) override {
    return Status::NotSupported(
        "SingleDeleteUntracked with secondary indices not yet supported");
  }

 private:
  class IndexData {
   public:
    IndexData(const SecondaryIndex* index, const Slice& previous_column_value)
        : index_(index), previous_column_value_(previous_column_value) {
      assert(index_);
    }

    const SecondaryIndex* index() const { return index_; }
    const Slice& previous_column_value() const {
      return previous_column_value_;
    }
    std::optional<std::variant<Slice, std::string>>& updated_column_value() {
      return updated_column_value_;
    }
    Slice primary_column_value() const {
      return updated_column_value_.has_value() ? AsSlice(*updated_column_value_)
                                               : previous_column_value_;
    }

   private:
    const SecondaryIndex* index_;
    Slice previous_column_value_;
    std::optional<std::variant<Slice, std::string>> updated_column_value_;
  };

  static Slice AsSlice(const std::variant<Slice, std::string>& var) {
    return std::visit([](const auto& value) -> Slice { return value; }, var);
  }

  static std::string AsString(const std::variant<Slice, std::string>& var) {
    return std::visit(
        overload{
            [](const Slice& value) -> std::string { return value.ToString(); },
            [](const std::string& value) -> std::string { return value; }},
        var);
  }

  template <typename Iterator>
  static Iterator FindPrimaryColumn(const SecondaryIndex* secondary_index,
                                    ColumnFamilyHandle* column_family,
                                    Iterator begin, Iterator end) {
    assert(secondary_index);
    assert(column_family);

    if (column_family != secondary_index->GetPrimaryColumnFamily()) {
      return end;
    }

    return WideColumnsHelper::Find(begin, end,
                                   secondary_index->GetPrimaryColumnName());
  }

  template <typename Operation>
  Status PerformWithSavePoint(Operation&& operation) {
    Txn::SetSavePoint();

    const Status s = operation();

    if (!s.ok()) {
      [[maybe_unused]] const Status st = Txn::RollbackToSavePoint();
      assert(st.ok());

      return s;
    }

    [[maybe_unused]] const Status st = Txn::PopSavePoint();
    assert(st.ok());

    return Status::OK();
  }

  Status GetPrimaryEntryForUpdate(ColumnFamilyHandle* column_family,
                                  const Slice& primary_key,
                                  PinnableWideColumns* existing_primary_columns,
                                  bool do_validate) {
    assert(column_family);
    assert(existing_primary_columns);

    constexpr bool exclusive = true;

    return Txn::GetEntityForUpdate(ReadOptions(), column_family, primary_key,
                                   existing_primary_columns, exclusive,
                                   do_validate);
  }

  Status RemoveSecondaryEntry(const SecondaryIndex* secondary_index,
                              const Slice& primary_key,
                              const Slice& existing_primary_column_value) {
    assert(secondary_index);

    std::variant<Slice, std::string> secondary_key_prefix;

    const Status s = secondary_index->GetSecondaryKeyPrefix(
        primary_key, existing_primary_column_value, &secondary_key_prefix);
    if (!s.ok()) {
      return s;
    }

    const std::string secondary_key =
        AsString(secondary_key_prefix) + primary_key.ToString();

    return Txn::SingleDelete(secondary_index->GetSecondaryColumnFamily(),
                             secondary_key);
  }

  Status AddPrimaryEntry(ColumnFamilyHandle* column_family,
                         const Slice& primary_key,
                         const WideColumns& primary_columns) {
    assert(column_family);

    constexpr bool assume_tracked = true;

    return Txn::PutEntity(column_family, primary_key, primary_columns,
                          assume_tracked);
  }

  Status AddSecondaryEntry(const SecondaryIndex* secondary_index,
                           const Slice& primary_key,
                           const Slice& primary_column_value,
                           const Slice& previous_column_value) {
    assert(secondary_index);

    std::variant<Slice, std::string> secondary_key_prefix;

    {
      const Status s = secondary_index->GetSecondaryKeyPrefix(
          primary_key, primary_column_value, &secondary_key_prefix);
      if (!s.ok()) {
        return s;
      }
    }

    std::optional<std::variant<Slice, std::string>> secondary_value;

    {
      const Status s = secondary_index->GetSecondaryValue(
          primary_key, primary_column_value, previous_column_value,
          &secondary_value);
      if (!s.ok()) {
        return s;
      }
    }

    {
      const std::string secondary_key =
          AsString(secondary_key_prefix) + primary_key.ToString();

      const Status s = Txn::Put(
          secondary_index->GetSecondaryColumnFamily(), secondary_key,
          secondary_value.has_value() ? AsSlice(*secondary_value) : Slice());
      if (!s.ok()) {
        return s;
      }
    }

    return Status::OK();
  }

  Status RemoveSecondaryEntries(ColumnFamilyHandle* column_family,
                                const Slice& primary_key, bool do_validate) {
    assert(column_family);

    PinnableWideColumns existing_primary_columns;

    const Status s = GetPrimaryEntryForUpdate(
        column_family, primary_key, &existing_primary_columns, do_validate);
    if (s.IsNotFound()) {
      return Status::OK();
    }
    if (!s.ok()) {
      return s;
    }

    const auto& existing_columns = existing_primary_columns.columns();

    for (const auto& secondary_index : *secondary_indices_) {
      const auto it =
          FindPrimaryColumn(secondary_index.get(), column_family,
                            existing_columns.cbegin(), existing_columns.cend());
      if (it == existing_columns.cend()) {
        continue;
      }

      const Status st =
          RemoveSecondaryEntry(secondary_index.get(), primary_key, it->value());
      if (!st.ok()) {
        return st;
      }
    }

    return Status::OK();
  }

  Status UpdatePrimaryColumnValues(ColumnFamilyHandle* column_family,
                                   const Slice& primary_key,
                                   WideColumns& primary_columns,
                                   autovector<IndexData>& applicable_indices) {
    assert(applicable_indices.empty());

    applicable_indices.reserve(secondary_indices_->size());

    for (const auto& secondary_index : *secondary_indices_) {
      const auto it =
          FindPrimaryColumn(secondary_index.get(), column_family,
                            primary_columns.begin(), primary_columns.end());
      if (it == primary_columns.end()) {
        continue;
      }

      applicable_indices.emplace_back(
          IndexData(secondary_index.get(), it->value()));

      auto& index_data = applicable_indices.back();

      const Status s = secondary_index->UpdatePrimaryColumnValue(
          primary_key, index_data.previous_column_value(),
          &index_data.updated_column_value());
      if (!s.ok()) {
        return s;
      }

      it->value() = index_data.primary_column_value();
    }

    return Status::OK();
  }

  Status AddSecondaryEntries(const Slice& primary_key,
                             const autovector<IndexData>& applicable_indices) {
    for (const auto& index_data : applicable_indices) {
      const Status s = AddSecondaryEntry(index_data.index(), primary_key,
                                         index_data.primary_column_value(),
                                         index_data.previous_column_value());
      if (!s.ok()) {
        return s;
      }
    }

    return Status::OK();
  }

  Status PutEntityWithSecondaryIndices(ColumnFamilyHandle* column_family,
                                       const Slice& key,
                                       const WideColumns& columns,
                                       bool do_validate) {
    // TODO: we could avoid removing and recreating secondary entries for
    // which neither the secondary key prefix nor the value has changed

    if (!column_family) {
      column_family = Txn::DefaultColumnFamily();
    }

    const Slice& primary_key = key;

    {
      const Status s =
          RemoveSecondaryEntries(column_family, primary_key, do_validate);
      if (!s.ok()) {
        return s;
      }
    }

    autovector<IndexData> applicable_indices;

    WideColumns primary_columns(columns);
    WideColumnsHelper::SortColumns(primary_columns);

    {
      const Status s = UpdatePrimaryColumnValues(
          column_family, primary_key, primary_columns, applicable_indices);
      if (!s.ok()) {
        return s;
      }
    }

    {
      const Status s =
          AddPrimaryEntry(column_family, primary_key, primary_columns);
      if (!s.ok()) {
        return s;
      }
    }

    {
      const Status s = AddSecondaryEntries(primary_key, applicable_indices);
      if (!s.ok()) {
        return s;
      }
    }

    return Status::OK();
  }

  const std::vector<std::shared_ptr<SecondaryIndex>>* secondary_indices_;
};

}  // namespace ROCKSDB_NAMESPACE
