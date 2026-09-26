// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL and subject to change.
//
// StreamAggregation replaces contiguous groups of compaction output records
// with aggregate records anchored at a subset of the input user keys. It is a
// physical-schema transformation: applications must use a logical reader that
// understands both the original row representation and aggregate records.
// Ordinary point Get(), iteration, snapshots, and deletes on absorbed user
// keys are not preserved by this interface.
//
// The application must ensure ordinary writes to retained anchor keys cannot
// replace an entire transformed group. Subsequent updates must use the
// application's logical row/chunk write path.
//
// A factory is configured through ColumnFamilyOptions. RocksDB invokes it for
// full compactions, where the inputs include every SST in the column family.
// Non-full compactions proceed without stream aggregation.
//
// Range tombstones are processed by RocksDB and are not included in the input
// passed to StreamAggregation. When integrated blob garbage collection is
// enabled, full compactions proceed without stream aggregation so blob
// references are relocated by the ordinary compaction path.
//
// When a CompactionFilter is configured, it runs on individual records before
// they reach StreamAggregation. Records removed by the filter are absent from
// the aggregation input, and changed values are visible to the aggregator.
// Values emitted by StreamAggregation are not filtered again.
class StreamAggregation {
 public:
  static constexpr size_t kInvalidInputIndex =
      std::numeric_limits<size_t>::max();
  static constexpr size_t kMaxBufferedInputRecords = 100000;
  static constexpr uint64_t kMaxBufferedInputBytes = 10 * 1024 * 1024;

  enum class ValueType {
    kValue,
    kWideColumnEntity,
  };

  // One logical record produced by CompactionIterator after same-user-key
  // history, merges, deletes, and sequence-number elision have been processed,
  // but before output blob extraction.
  //
  // All pointers and Slices remain valid only for the duration of the current
  // Aggregate() or Finish() call. For a wide-column entity, columns contains
  // every column. When blob_resolver is non-null, blob-backed columns contain
  // their serialized BlobIndex in columns and must be read through
  // blob_resolver. Inline columns can be inspected directly without blob I/O.
  struct Input {
    Slice user_key;
    SequenceNumber sequence = 0;
    ValueType value_type = ValueType::kValue;
    const Slice* value = nullptr;
    const WideColumns* columns = nullptr;
    WideColumnBlobResolver* blob_resolver = nullptr;
  };

  enum class OutputAction {
    // Emit every input in this segment unchanged.
    kKeep,

    // Emit one replacement at an input user key selected by
    // OutputSegment::anchor_input_index. RocksDB uses the anchor input's
    // sequence number after normal compaction elision. Applications with
    // timestamp-ordered keys can select the newest surviving input as the
    // anchor. RocksDB does not interpret the user key. RocksDB also
    // materializes output blob columns.
    kEmit,

    // Emit nothing for this segment. This is intended for retention trimming
    // in a bottommost compaction where older versions cannot resurface.
    kDrop,
  };

  // Describes one contiguous segment in the consumed input prefix. Segments
  // returned from Aggregate() must be ordered, non-overlapping, and exactly
  // partition [0, *num_consumed).
  //
  // For kEmit, anchor_input_index must be in [input_begin, input_end). For
  // kValue, `value` is used and `columns` must be empty. For
  // kWideColumnEntity, `columns` is used and `value` must be empty. kKeep and
  // kDrop require both output value fields to be empty and ignore
  // anchor_input_index.
  struct OutputSegment {
    size_t input_begin = 0;
    size_t input_end = 0;
    size_t anchor_input_index = kInvalidInputIndex;
    OutputAction action = OutputAction::kKeep;
    ValueType value_type = ValueType::kValue;
    std::string value;
    std::vector<std::pair<std::string, std::string>> columns;
  };

  virtual ~StreamAggregation() = default;

  // Aggregates a non-empty, comparator-ordered batch. The implementation sets
  // *num_consumed to a prefix length in [0, inputs.size()]. Returning zero
  // with no output requests more lookahead. RocksDB retains the entire batch,
  // appends more input, and calls Aggregate() again until the hard input
  // buffer limits above are reached.
  //
  // When *num_consumed is positive, output segments must exactly partition
  // [0, *num_consumed). They are structurally validated before RocksDB writes
  // output for the consumed prefix.
  //
  // Implementations must be deterministic, bounded, side-effect free, and
  // must not retain pointers or Slices after this call returns. Exceptions
  // must not propagate into RocksDB.
  virtual Status Aggregate(const std::vector<Input>& inputs,
                           size_t* num_consumed,
                           std::vector<OutputSegment>* outputs) = 0;

  // Finalizes a non-empty batch after RocksDB reaches the end of one compaction
  // job's input range. A compaction request can execute more than one job. On
  // success, outputs must exactly partition every input. The default
  // implementation calls Aggregate() once and rejects a partial or deferred
  // result.
  virtual Status Finish(const std::vector<Input>& inputs,
                        std::vector<OutputSegment>* outputs) {
    size_t num_consumed = 0;
    Status status = Aggregate(inputs, &num_consumed, outputs);
    if (!status.ok()) {
      return status;
    }
    if (num_consumed != inputs.size()) {
      return Status::InvalidArgument(
          "StreamAggregation did not consume all input at EOF");
    }
    return Status::OK();
  }

  // Limits transformed output memory. This limit must be non-zero.
  virtual uint64_t MaxBatchOutputBytes() const { return 8 * 1024 * 1024; }
};

// EXPERIMENTAL and subject to change.
class StreamAggregationFactory {
 public:
  struct ValidationContext {
    bool blob_files_enabled = false;
    size_t user_timestamp_size = 0;
    size_t max_buffered_input_records =
        StreamAggregation::kMaxBufferedInputRecords;
    uint64_t max_buffered_input_bytes =
        StreamAggregation::kMaxBufferedInputBytes;
  };

  struct Context {
    bool is_full_compaction = false;
    bool is_manual_compaction = false;
    bool is_bottommost_level = false;
    int input_start_level = -1;
    int output_level = -1;
    uint32_t column_family_id = 0;
  };

  virtual ~StreamAggregationFactory() = default;

  virtual const char* Name() const = 0;

  // Called while validating column-family options. Implementations can
  // validate schema/codec configuration and run application-provided contract
  // evaluation cases. RocksDB still validates every runtime result.
  virtual Status Validate(const ValidationContext& /*context*/) const {
    return Status::OK();
  }

  // A separate aggregation is created for each full compaction. The
  // experimental implementation disables subcompactions for those jobs.
  virtual std::unique_ptr<StreamAggregation> Create(
      const Context& context) const = 0;
};

}  // namespace ROCKSDB_NAMESPACE
