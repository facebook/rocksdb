//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <ostream>
#include <tuple>
#include <utility>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Class representing a wide column, which is defined as a pair of column name
// and column value.
class WideColumn {
 public:
  WideColumn() = default;

  // Initializes a WideColumn object by forwarding the name and value
  // arguments to the corresponding member Slices. This makes it possible to
  // construct a WideColumn using combinations of const char*, const
  // std::string&, const Slice& etc., for example:
  //
  // constexpr char foo[] = "foo";
  // const std::string bar("bar");
  // WideColumn column(foo, bar);
  template <typename N, typename V>
  WideColumn(N&& name, V&& value)
      : name_(std::forward<N>(name)), value_(std::forward<V>(value)) {}

  // Initializes a WideColumn object by forwarding the elements of
  // name_tuple and value_tuple to the constructors of the corresponding member
  // Slices. This makes it possible to initialize the Slices using the Slice
  // constructors that take more than one argument, for example:
  //
  // constexpr char foo_name[] = "foo_name";
  // constexpr char bar_value[] = "bar_value";
  // WideColumn column(std::piecewise_construct,
  //                   std::forward_as_tuple(foo_name, 3),
  //                   std::forward_as_tuple(bar_value, 3));
  template <typename NTuple, typename VTuple>
  WideColumn(std::piecewise_construct_t, NTuple&& name_tuple,
             VTuple&& value_tuple)
      : name_(std::make_from_tuple<Slice>(std::forward<NTuple>(name_tuple))),
        value_(std::make_from_tuple<Slice>(std::forward<VTuple>(value_tuple))) {
  }

  const Slice& name() const { return name_; }
  const Slice& value() const { return value_; }

  Slice& name() { return name_; }
  Slice& value() { return value_; }

 private:
  Slice name_;
  Slice value_;
};

// Note: column names and values are compared bytewise.
inline bool operator==(const WideColumn& lhs, const WideColumn& rhs) {
  return lhs.name() == rhs.name() && lhs.value() == rhs.value();
}

inline bool operator!=(const WideColumn& lhs, const WideColumn& rhs) {
  return !(lhs == rhs);
}

inline std::ostream& operator<<(std::ostream& os, const WideColumn& column) {
  const bool hex =
      (os.flags() & std::ios_base::basefield) == std::ios_base::hex;
  if (!column.name().empty()) {
    if (hex) {
      os << "0x";
    }
    os << column.name().ToString(hex);
  }
  os << ':';
  if (!column.value().empty()) {
    if (hex) {
      os << "0x";
    }
    os << column.value().ToString(hex);
  }
  return os;
}

// A collection of wide columns.
using WideColumns = std::vector<WideColumn>;

// The anonymous default wide column (an empty Slice).
extern const Slice kDefaultWideColumnName;

// An empty set of wide columns.
extern const WideColumns kNoWideColumns;

// A self-contained collection of wide columns. Used for the results of
// wide-column queries.
class PinnableWideColumns {
 public:
  const WideColumns& columns() const { return columns_; }
  size_t serialized_size() const { return value_.size(); }

  void SetPlainValue(const Slice& value);
  void SetPlainValue(const Slice& value, Cleanable* cleanable);
  void SetPlainValue(PinnableSlice&& value);
  void SetPlainValue(std::string&& value);

  Status SetWideColumnValue(const Slice& value);
  Status SetWideColumnValue(const Slice& value, Cleanable* cleanable);
  Status SetWideColumnValue(PinnableSlice&& value);
  Status SetWideColumnValue(std::string&& value);

  void Reset();

  PinnableWideColumns() = default;
  PinnableWideColumns(const PinnableWideColumns& p);

 private:
  void CopyValue(const Slice& value);
  void PinOrCopyValue(const Slice& value, Cleanable* cleanable);
  void MoveValue(PinnableSlice&& value);
  void MoveValue(std::string&& value);

  void CreateIndexForPlainValue();
  Status CreateIndexForWideColumns();

  PinnableSlice value_;
  WideColumns columns_;
};

inline void PinnableWideColumns::CopyValue(const Slice& value) {
  value_.PinSelf(value);
}

inline void PinnableWideColumns::PinOrCopyValue(const Slice& value,
                                                Cleanable* cleanable) {
  if (!cleanable) {
    CopyValue(value);
    return;
  }

  value_.PinSlice(value, cleanable);
}

inline void PinnableWideColumns::MoveValue(PinnableSlice&& value) {
  value_ = std::move(value);
}

inline void PinnableWideColumns::MoveValue(std::string&& value) {
  std::string* const buf = value_.GetSelf();
  assert(buf);

  *buf = std::move(value);
  value_.PinSelf();
}

inline void PinnableWideColumns::CreateIndexForPlainValue() {
  columns_ = WideColumns{{kDefaultWideColumnName, value_}};
}

inline void PinnableWideColumns::SetPlainValue(const Slice& value) {
  CopyValue(value);
  CreateIndexForPlainValue();
}

inline void PinnableWideColumns::SetPlainValue(const Slice& value,
                                               Cleanable* cleanable) {
  PinOrCopyValue(value, cleanable);
  CreateIndexForPlainValue();
}

inline void PinnableWideColumns::SetPlainValue(PinnableSlice&& value) {
  MoveValue(std::move(value));
  CreateIndexForPlainValue();
}

inline void PinnableWideColumns::SetPlainValue(std::string&& value) {
  MoveValue(std::move(value));
  CreateIndexForPlainValue();
}

inline Status PinnableWideColumns::SetWideColumnValue(const Slice& value) {
  CopyValue(value);
  return CreateIndexForWideColumns();
}

inline Status PinnableWideColumns::SetWideColumnValue(const Slice& value,
                                                      Cleanable* cleanable) {
  PinOrCopyValue(value, cleanable);
  return CreateIndexForWideColumns();
}

inline Status PinnableWideColumns::SetWideColumnValue(PinnableSlice&& value) {
  MoveValue(std::move(value));
  return CreateIndexForWideColumns();
}

inline Status PinnableWideColumns::SetWideColumnValue(std::string&& value) {
  MoveValue(std::move(value));
  return CreateIndexForWideColumns();
}

inline void PinnableWideColumns::Reset() {
  value_.Reset();
  columns_.clear();
}

inline bool operator==(const PinnableWideColumns& lhs,
                       const PinnableWideColumns& rhs) {
  return lhs.columns() == rhs.columns();
}

inline bool operator!=(const PinnableWideColumns& lhs,
                       const PinnableWideColumns& rhs) {
  return !(lhs == rhs);
}

inline PinnableWideColumns::PinnableWideColumns(const PinnableWideColumns& p)
    : columns_(p.columns_) {
  CopyValue(p.value_);
}

// List of PinnableWideColumns grouped by column families. statuses[i] should
// match the status of getting columns_array[i] when GetEntity() or
// MultiGetEntity() was called (where 0 <= i < num_column_families_)
class GroupedPinnableWideColumns {
 public:
  const size_t& num_column_families() const { return num_column_families_; }
  const std::vector<Status>& statuses() const { return statuses_; }
  const std::vector<PinnableWideColumns>& columns_array() const {
    return columns_array_;
  }
  PinnableWideColumns* columns_ref(const size_t& column_family_index) {
    assert(column_family_index < num_column_families_);
    return &columns_array_[column_family_index];
  }

  explicit GroupedPinnableWideColumns(const size_t& num_column_families)
      : num_column_families_(num_column_families),
        statuses_(num_column_families),
        columns_array_(num_column_families) {}

  void SetStatus(const size_t& column_family_index, Status&& status);
  void SetStatusForAllColumnFamilies(const Status& status);

  void Reset();

 private:
  const size_t num_column_families_;
  std::vector<Status> statuses_;
  std::vector<PinnableWideColumns> columns_array_;
};

inline void GroupedPinnableWideColumns::SetStatus(
    const size_t& column_family_index, Status&& status) {
  statuses_[column_family_index] = status;
}
inline void GroupedPinnableWideColumns::SetStatusForAllColumnFamilies(
    const Status& status) {
  statuses_.assign(num_column_families_, status);
}

inline void GroupedPinnableWideColumns::Reset() {
  std::for_each(columns_array_.begin(), columns_array_.end(),
                [](PinnableWideColumns& columns) { columns.Reset(); });
}

}  // namespace ROCKSDB_NAMESPACE
