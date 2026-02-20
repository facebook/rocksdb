//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/wide_columns.h"

#include "db/wide/read_path_blob_resolver.h"
#include "db/wide/wide_column_serialization.h"

namespace ROCKSDB_NAMESPACE {

const Slice kDefaultWideColumnName;

const WideColumns kNoWideColumns;

// Out-of-line constructor and destructor needed because
// ReadPathBlobResolver is incomplete in the header.
PinnableWideColumns::PinnableWideColumns() = default;
PinnableWideColumns::~PinnableWideColumns() = default;

Status PinnableWideColumns::CreateIndexForWideColumns() {
  columns_.clear();

  Slice value_copy = value_;
  return WideColumnSerialization::Deserialize(value_copy, columns_);
}

bool PinnableWideColumns::IsUnresolvedColumn(size_t column_index) const {
  if (!resolver_) {
    return false;
  }
  return resolver_->IsUnresolvedColumn(column_index);
}

bool PinnableWideColumns::HasUnresolvedColumns() const {
  if (!resolver_) {
    return false;
  }
  return resolver_->HasUnresolvedColumns();
}

Status PinnableWideColumns::ResolveColumn(size_t column_index) {
  if (!resolver_) {
    return Status::OK();
  }

  if (column_index >= columns_.size()) {
    return Status::InvalidArgument("Column index out of bounds");
  }

  if (!resolver_->IsUnresolvedColumn(column_index)) {
    return Status::OK();
  }

  Slice resolved_value;
  Status s = resolver_->ResolveColumn(column_index, &resolved_value);
  if (!s.ok()) {
    return s;
  }

  // Update the column value to point to the resolved data
  columns_[column_index].value() = resolved_value;

  return Status::OK();
}

Status PinnableWideColumns::ResolveAllColumns() {
  if (!resolver_) {
    return Status::OK();
  }

  // Resolve each column individually so we can update the column values
  for (size_t i = 0; i < columns_.size(); ++i) {
    Status s = ResolveColumn(i);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

void PinnableWideColumns::SetWideColumnValueLazy(
    std::string&& serialized_entity, WideColumns&& columns,
    std::unique_ptr<ReadPathBlobResolver>&& resolver) {
  MoveValue(std::move(serialized_entity));
  columns_ = std::move(columns);
  resolver_ = std::move(resolver);
}

void PinnableWideColumns::Reset() {
  value_.Reset();
  columns_.clear();
  resolver_.reset();
}

void PinnableWideColumns::Move(PinnableWideColumns&& other) {
  assert(columns_.empty());

  if (other.columns_.empty()) {
    resolver_ = std::move(other.resolver_);
    return;
  }

  const char* const data = other.value_.data();
  const bool is_plain_value =
      other.columns_.size() == 1 &&
      other.columns_.front().name() == kDefaultWideColumnName &&
      other.columns_.front().value() == other.value_;

  MoveValue(std::move(other.value_));

  if (value_.data() == data) {
    columns_ = std::move(other.columns_);
  } else {
    if (is_plain_value) {
      CreateIndexForPlainValue();
    } else {
      const Status s = CreateIndexForWideColumns();
      assert(s.ok());

      s.PermitUncheckedError();
    }
  }

  resolver_ = std::move(other.resolver_);

  other.Reset();
}

PinnableWideColumns::PinnableWideColumns(PinnableWideColumns&& other) {
  Move(std::move(other));
}

PinnableWideColumns& PinnableWideColumns::operator=(
    PinnableWideColumns&& other) {
  if (this != &other) {
    Reset();
    Move(std::move(other));
  }

  return *this;
}

Status PinnableWideColumns::SetWideColumnValue(const Slice& value) {
  CopyValue(value);

  const Status s = CreateIndexForWideColumns();
  if (!s.ok()) {
    Reset();
  }

  return s;
}

Status PinnableWideColumns::SetWideColumnValue(const Slice& value,
                                               Cleanable* cleanable) {
  PinOrCopyValue(value, cleanable);

  const Status s = CreateIndexForWideColumns();
  if (!s.ok()) {
    Reset();
  }

  return s;
}

Status PinnableWideColumns::SetWideColumnValue(PinnableSlice&& value) {
  MoveValue(std::move(value));

  const Status s = CreateIndexForWideColumns();
  if (!s.ok()) {
    Reset();
  }

  return s;
}

Status PinnableWideColumns::SetWideColumnValue(std::string&& value) {
  MoveValue(std::move(value));

  const Status s = CreateIndexForWideColumns();
  if (!s.ok()) {
    Reset();
  }

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
