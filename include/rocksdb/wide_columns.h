//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

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
  os << column.name().ToString(hex) << ':' << column.value().ToString(hex);

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

  Status SetWideColumnValue(const Slice& value);
  Status SetWideColumnValue(const Slice& value, Cleanable* cleanable);

  void Reset();

 private:
  void CopyValue(const Slice& value);
  void PinOrCopyValue(const Slice& value, Cleanable* cleanable);
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

inline Status PinnableWideColumns::SetWideColumnValue(const Slice& value) {
  CopyValue(value);
  return CreateIndexForWideColumns();
}

inline Status PinnableWideColumns::SetWideColumnValue(const Slice& value,
                                                      Cleanable* cleanable) {
  PinOrCopyValue(value, cleanable);
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

}  // namespace ROCKSDB_NAMESPACE
