//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/column_db.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <limits>
#include <utility>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

namespace {

const WideColumn* FindColumn(const WideColumns& columns, const Slice& name) {
  for (const WideColumn& column : columns) {
    if (column.name() == name) {
      return &column;
    }
  }
  return nullptr;
}

Status ValidateRange(const ColumnDBBlobRange& range, size_t num_columns) {
  if (range.column_index >= num_columns) {
    return Status::InvalidArgument("ColumnDB range column index out of range");
  }
  if (range.size >
      std::numeric_limits<uint64_t>::max() - range.offset) {
    return Status::InvalidArgument("ColumnDB range overflows");
  }
  if (range.size > std::numeric_limits<size_t>::max()) {
    return Status::InvalidArgument("ColumnDB range is too large");
  }
  return Status::OK();
}

class ColumnDBImpl : public ColumnDB {
 public:
  using ColumnDB::Get;

  ColumnDBImpl(const ColumnDBOptions& options, std::unique_ptr<DB>&& db)
      : ColumnDB(std::move(db)), options_(options) {}

  Status Get(const ReadOptions& read_options, ColumnFamilyHandle* column_family,
             const Slice& key, const std::vector<Slice>& columns,
             std::vector<PinnableSlice>* values) override {
    if (column_family == nullptr) {
      return Status::InvalidArgument(
          "Cannot call ColumnDB::Get without a column family handle");
    }
    if (values == nullptr) {
      return Status::InvalidArgument(
          "Cannot call ColumnDB::Get without a values output vector");
    }
    if (!options_.translate) {
      return Status::InvalidArgument("ColumnDB translate callback is not set");
    }

    values->resize(columns.size());
    for (PinnableSlice& value : *values) {
      value.Reset();
    }

    DB* const root = GetRootDB();
    DBImpl* const db_impl = dynamic_cast<DBImpl*>(root);
    if (db_impl == nullptr) {
      return Status::NotSupported(
          "ColumnDB partial blob reads require a DBImpl root DB");
    }

    ReadOptions entity_read_options(read_options);
    if (entity_read_options.io_activity == Env::IOActivity::kUnknown) {
      entity_read_options.io_activity = Env::IOActivity::kGetEntity;
    }

    PinnableWideColumns entity;
    bool is_blob_index = false;
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = column_family;
    get_impl_options.columns = &entity;
    get_impl_options.is_blob_index = &is_blob_index;
    Status s = db_impl->GetImpl(entity_read_options, key, get_impl_options);
    if (!s.ok()) {
      return s;
    }

    const Slice schema_name(options_.schema_column_name);
    const Slice blob_name(options_.blob_column_name);
    const WideColumn* const schema_column =
        FindColumn(entity.columns(), schema_name);
    if (schema_column == nullptr) {
      return Status::Corruption("ColumnDB schema column not found");
    }

    const WideColumn* const blob_column =
        FindColumn(entity.columns(), blob_name);
    if (blob_column == nullptr) {
      return Status::Corruption("ColumnDB blob column not found");
    }

    BlobIndex blob_index;
    Slice blob_index_value = blob_column->value();
    s = blob_index.DecodeFrom(blob_index_value);
    if (!s.ok()) {
      return s;
    }
    if (blob_index.IsInlined() || blob_index.HasTTL()) {
      return Status::NotSupported(
          "ColumnDB only supports non-TTL blob column references");
    }

    std::vector<ColumnDBBlobRange> ranges;
    s = options_.translate(schema_column->value(), columns, &ranges);
    if (!s.ok()) {
      return s;
    }

    struct CoalescedRange {
      uint64_t offset = 0;
      uint64_t size = 0;
    };

    struct PendingRange {
      ColumnDBBlobRange range;
      size_t coalesced_index = 0;
    };

    std::vector<PendingRange> pending;
    pending.reserve(ranges.size());
    std::vector<bool> projected(columns.size(), false);
    for (const ColumnDBBlobRange& range : ranges) {
      s = ValidateRange(range, columns.size());
      if (!s.ok()) {
        return s;
      }
      if (projected[range.column_index]) {
        return Status::InvalidArgument(
            "ColumnDB translate callback returned duplicate column ranges");
      }
      projected[range.column_index] = true;
      if (range.size == 0) {
        (*values)[range.column_index].PinSelf(Slice());
        continue;
      }
      pending.push_back({range, 0});
    }
    for (bool was_projected : projected) {
      if (!was_projected) {
        return Status::InvalidArgument(
            "ColumnDB translate callback did not project every column");
      }
    }

    std::sort(pending.begin(), pending.end(),
              [](const PendingRange& lhs, const PendingRange& rhs) {
                if (lhs.range.offset != rhs.range.offset) {
                  return lhs.range.offset < rhs.range.offset;
                }
                return lhs.range.size < rhs.range.size;
              });

    std::vector<CoalescedRange> coalesced;
    for (PendingRange& range : pending) {
      if (coalesced.empty()) {
        range.coalesced_index = 0;
        coalesced.push_back({range.range.offset, range.range.size});
        continue;
      }

      CoalescedRange& current = coalesced.back();
      const uint64_t current_end = current.offset + current.size;
      if (range.range.offset <= current_end) {
        range.coalesced_index = coalesced.size() - 1;
        const uint64_t range_end = range.range.offset + range.range.size;
        if (range_end > current_end) {
          current.size = range_end - current.offset;
        }
      } else {
        range.coalesced_index = coalesced.size();
        coalesced.push_back({range.range.offset, range.range.size});
      }
    }

    if (coalesced.empty()) {
      return Status::OK();
    }

    std::vector<std::pair<uint64_t, uint64_t>> blob_ranges;
    blob_ranges.reserve(coalesced.size());
    for (const CoalescedRange& range : coalesced) {
      blob_ranges.emplace_back(range.offset, range.size);
    }

    std::vector<PinnableSlice> coalesced_values;
    s = db_impl->MultiGetBlobRanges(read_options, column_family, key,
                                    blob_index, blob_ranges,
                                    &coalesced_values,
                                    /*bytes_read=*/nullptr);
    if (!s.ok()) {
      return s;
    }
    assert(coalesced_values.size() == coalesced.size());

    for (const PendingRange& pending_range : pending) {
      const CoalescedRange& whole = coalesced[pending_range.coalesced_index];
      const PinnableSlice& whole_value =
          coalesced_values[pending_range.coalesced_index];
      assert(pending_range.range.offset >= whole.offset);

      const uint64_t delta = pending_range.range.offset - whole.offset;
      if (delta > whole_value.size() ||
          pending_range.range.size > whole_value.size() - delta) {
        return Status::Corruption(
            "Blob range result is shorter than requested column");
      }

      (*values)[pending_range.range.column_index].PinSelf(
          Slice(whole_value.data() + delta,
                static_cast<size_t>(pending_range.range.size)));
    }

    return Status::OK();
  }

 private:
  ColumnDBOptions options_;
};

}  // namespace

Status ColumnDB::Open(const ColumnDBOptions& options, std::unique_ptr<DB>&& db,
                      ColumnDB** column_db) {
  if (column_db == nullptr) {
    return Status::InvalidArgument(
        "Cannot open ColumnDB without an output pointer");
  }
  *column_db = nullptr;
  if (!db) {
    return Status::InvalidArgument("Cannot open ColumnDB with a null DB");
  }
  if (!options.translate) {
    return Status::InvalidArgument("ColumnDB translate callback is not set");
  }

  *column_db = new ColumnDBImpl(options, std::move(db));
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
