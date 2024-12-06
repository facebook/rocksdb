//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <optional>
#include <string>
#include <variant>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;

//       / \     UNDER CONSTRUCTION
//      / ! \    UNDER CONSTRUCTION
//     /-----\   UNDER CONSTRUCTION

// A secondary index is an additional data structure built over a set of primary
// key-values that enables efficiently querying key-values by value instead of
// key. Both plain and wide-column key-values can be indexed, the latter on a
// per-column basis. The secondary index then maintains a mapping from (column)
// value to the list of primary keys that have the corresponding value (in the
// given column).
//
// The primary and secondary key-value pairs can be stored in either the same
// column family or different ones. It is the application's responsibility to
// avoid conflicts and ambiguities (for example, by using prefixes to create
// separate key spaces or using a dedicated column family for each secondary
// index). Also, note that applications are not expected to manipulate secondary
// index entries directly.
//
// In the general case where there are concurrent writers, maintaining a
// secondary index requires transactional semantics and concurrency control.
// Because of this, secondary indices are only supported via the transaction
// layer. With secondary indices, whenever a (primary) key-value is inserted,
// updated, or deleted via a transaction (regardless of whether it is an
// explicit or implicit one), RocksDB will invoke any applicable SecondaryIndex
// objects based on primary column family and column name, and it will
// automatically add or remove any secondary index entries as needed (using
// the same transaction).
//
// Note: the methods of SecondaryIndex implementations are expected to be
// thread-safe with the exception of Set{Primary,Secondary}ColumnFamily (which
// are not expected to be called after initialization).
class SecondaryIndex {
 public:
  virtual ~SecondaryIndex() = default;

  virtual void SetPrimaryColumnFamily(ColumnFamilyHandle* column_family) = 0;
  virtual void SetSecondaryColumnFamily(ColumnFamilyHandle* column_family) = 0;

  virtual ColumnFamilyHandle* GetPrimaryColumnFamily() const = 0;
  virtual ColumnFamilyHandle* GetSecondaryColumnFamily() const = 0;

  // The name of the primary column to index. Plain key-values can be indexed by
  // specifying kDefaultWideColumnName.
  virtual Slice GetPrimaryColumnName() const = 0;

  // Optionally update the primary column value during an insert or update of a
  // primary key-value. Called by the transaction layer before the primary
  // key-value write is added to the transaction. Returning a non-OK status
  // rolls back all operations in the transaction related to this primary
  // key-value.
  virtual Status UpdatePrimaryColumnValue(
      const Slice& primary_key, const Slice& primary_column_value,
      std::optional<std::variant<Slice, std::string>>* updated_column_value)
      const = 0;

  // Get the secondary key prefix for a given primary key-value. This method is
  // called by the transaction layer when adding or removing secondary index
  // entries (which have the form <secondary_key_prefix><primary_key> ->
  // <secondary_value>) and should be deterministic. The output parameter
  // secondary_key_prefix is expected to be based on primary_column_value,
  // potentially with some additional metadata to prevent ambiguities (e.g.
  // index id or length indicator). Returning a non-OK status rolls back all
  // operations in the transaction related to this primary key-value.
  virtual Status GetSecondaryKeyPrefix(
      const Slice& primary_key, const Slice& primary_column_value,
      std::variant<Slice, std::string>* secondary_key_prefix) const = 0;

  // Get the optional secondary value for a given primary key-value. This method
  // is called by the transaction layer when adding secondary index
  // entries (which have the form <secondary_key_prefix><primary_key> ->
  // <secondary_value>). previous_column_value contains the previous value of
  // the primary column in case it was changed by UpdatePrimaryColumnValue.
  // Returning a non-OK status rolls back all operations in the transaction
  // related to this primary key-value.
  virtual Status GetSecondaryValue(
      const Slice& primary_key, const Slice& primary_column_value,
      const Slice& previous_column_value,
      std::optional<std::variant<Slice, std::string>>* secondary_value)
      const = 0;
};

}  // namespace ROCKSDB_NAMESPACE
