//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <variant>

#include "rocksdb/iterator.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/secondary_index_options.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;

// EXPERIMENTAL
//
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
// the same transaction). Secondary indices can also be queried using an
// iterator API, which has implementation-specific semantics.
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
  // secondary_key_prefix is expected to be based on primary_key and/or
  // primary_column_value. Returning a non-OK status rolls back all operations
  // in the transaction related to this primary key-value.
  virtual Status GetSecondaryKeyPrefix(
      const Slice& primary_key, const Slice& primary_column_value,
      std::variant<Slice, std::string>* secondary_key_prefix) const = 0;

  // Finalize the secondary key prefix, for instance by adding some metadata to
  // prevent ambiguities (e.g. index id or length indicator). This method is
  // called by the transaction layer when adding or removing secondary index
  // entries (which have the form <secondary_key_prefix><primary_key> ->
  // <secondary_value>) and also when querying the index (in which case it is
  // called with the search target). The method should be deterministic.
  // Returning a non-OK status rolls back all operations in the transaction
  // related to this primary key-value.
  virtual Status FinalizeSecondaryKeyPrefix(
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

  // Create an iterator that can be used by applications to query the index.
  // This method takes a SecondaryIndexReadOptions structure, which can be used
  // by applications to provide (implementation-specific) query parameters to
  // the index as well as an underlying iterator over the index's secondary
  // column family, which the returned iterator is expected to take ownership of
  // and use to read the actual secondary index entries. (Providing the
  // underlying iterator this way enables querying the index as of a specific
  // point in time for example.)
  //
  // Querying the index can be performed by calling the returned iterator's
  // Seek API with a search target, and then using Next (and potentially
  // Prev) to iterate through the matching index entries. SeekToFirst,
  // SeekToLast, and SeekForPrev are not expected to be supported by the
  // iterator. The iterator should expose primary keys, that is, the secondary
  // key prefix should be stripped from the index entries.
  //
  // The exact semantics of the returned iterator depend on the index and are
  // implementation-specific. For simple indices, the search target might be a
  // primary column value, and the iterator might return all primary keys that
  // have the given column value; however, other semantics are also possible.
  // For vector indices, the search target might be a vector, and the iterator
  // might return similar vectors from the index.
  virtual std::unique_ptr<Iterator> NewIterator(
      const SecondaryIndexReadOptions& read_options,
      std::unique_ptr<Iterator>&& underlying_it) const = 0;
};

// Create a simple secondary index iterator that can be used to query an index,
// i.e. find the primary keys for a given search target. Can be used as-is or as
// a building block for more complex iterators. The returned iterator supports
// Seek/Next/Prev but not SeekToFirst/SeekToLast/SeekForPrev, and exposes
// primary keys. See also SecondaryIndex::NewIterator above.
std::unique_ptr<Iterator> NewSecondaryIndexIterator(
    const SecondaryIndex* index, std::unique_ptr<Iterator>&& underlying_it);

}  // namespace ROCKSDB_NAMESPACE
