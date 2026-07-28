//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <forward_list>
#include <iterator>
#include <ostream>
#include <utility>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class WideColumnsHelper {
 public:
  static void DumpWideColumns(const WideColumns& columns, std::ostream& os,
                              bool hex);

  static Status DumpSliceAsWideColumns(const Slice& value, std::ostream& os,
                                       bool hex);

  static bool HasDefaultColumn(const WideColumns& columns) {
    return !columns.empty() && columns.front().name() == kDefaultWideColumnName;
  }

  static bool HasDefaultColumnOnly(const WideColumns& columns) {
    return columns.size() == 1 &&
           columns.front().name() == kDefaultWideColumnName;
  }

  static const Slice& GetDefaultColumn(const WideColumns& columns) {
    assert(HasDefaultColumn(columns));
    return columns.front().value();
  }

  static void SortColumns(WideColumns& columns) {
    std::sort(columns.begin(), columns.end(),
              [](const WideColumn& lhs, const WideColumn& rhs) {
                return lhs.name().compare(rhs.name()) < 0;
              });
  }

  template <typename Iterator>
  static Iterator Find(Iterator begin, Iterator end, const Slice& column_name) {
    assert(std::is_sorted(begin, end,
                          [](const WideColumn& lhs, const WideColumn& rhs) {
                            return lhs.name().compare(rhs.name()) < 0;
                          }));

    auto it = std::lower_bound(begin, end, column_name,
                               [](const WideColumn& lhs, const Slice& rhs) {
                                 return lhs.name().compare(rhs) < 0;
                               });

    if (it == end || it->name() != column_name) {
      return end;
    }

    return it;
  }
};

// Internal helper for mutating the private backing storage of a
// PinnableWideColumns without exposing these operations on its public API.
class PinnableWideColumnsHelper {
 public:
  // Replaces `columns`'s current columns with an already-resolved set. Every
  // name/value Slice in `resolved_columns` must point into stable memory:
  // either a buffer already held by `columns` (e.g. the original entity, for
  // inline columns) or one of the buffers in `extra_buffers` (e.g. fetched blob
  // values). Ownership of `extra_buffers` is transferred via splice; its nodes
  // are individually heap-allocated and never relocate, so Slices into them
  // stay valid. Clears any unresolved-blob metadata. No serialization occurs.
  static void ResolveColumns(PinnableWideColumns& columns,
                             WideColumns&& resolved_columns,
                             std::forward_list<PinnableSlice>&& extra_buffers) {
    columns.backing_.splice_after(columns.backing_.before_begin(),
                                  std::move(extra_buffers));
    columns.columns_ = std::move(resolved_columns);
    columns.unresolved_blob_column_indices_.clear();
  }

  // Sorted positions of the columns whose values still hold encoded BlobIndex
  // payloads (V2 entities that have not been blob-resolved yet). Empty once
  // resolved or for entities without blob references.
  static const std::vector<size_t>& GetUnresolvedBlobColumnIndices(
      const PinnableWideColumns& columns) {
    return columns.unresolved_blob_column_indices_;
  }

  // The backing buffer holding the serialized entity. Valid only before
  // resolution, when the entity occupies a single backing buffer (the state
  // right after SetWideColumnValue()); used as the input for blob resolution.
  static Slice GetSerializedEntity(const PinnableWideColumns& columns) {
    assert(!columns.backing_.empty());
    // Exactly one backing buffer: there is a single serialized entity and it
    // has not been split across resolved blob buffers yet.
    assert(std::next(columns.backing_.begin()) == columns.backing_.end());
    return columns.backing_.front();
  }
};

}  // namespace ROCKSDB_NAMESPACE
