//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//  @author Adam Retter

#pragma once
#include <cstdint>
#include <deque>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace rocksdb {

/**
 * A constraint on the uniqueness of items within a Collection.
 */
enum UniqueConstraint : uint8_t {
  kMakeUnique = 0x0,         // Duplicate values in the Collection will be removed
  kEnforceUnique = 0x1,      // If duplicate values are detected, the merge aborts
  kNone = 0x2                // No uniqueness constraint on values in the Collection
};

/**
 * The Operation to apply to a Collection.
 */
enum CollectionOperation : uint8_t {
  kAdd = 0x0,          // Add one or more records to the Collection
  kRemove = 0x1,       // Remove one or more records from the Collection
  kClear = 0x2,        // Clear all records from the Collection

  // (internal use only) Multiple operations as packed by PartialMergeMulti
  _kMulti = 0x3
};

/**
 * A Merge Operator where the value of a key/value pair in the database is
 * actually a Collection of records (byte strings).
 * 
 * Provides support for both Set and Vector like collections.
 * 
 * The value sent to db.merge(key, value) should be a byte string with the
 * following format:
 *     [RecordType, Record+]
 * 
 * RecordType is a single byte and must be one of CollectionOperation.
 * Records is a byte string of one or more record(s), each record must be
 * of length fixed_record_len.
 * 
 * For example (for a fixed_record_len of 4):
 *     db.merge(key1, [kAdd, 100020003000])
 *     db.merge(key1, [kRemove, 2000])
 * 
 *     db.get(key1) == [10003000]
 */
class CollectionMergeOperator : public MergeOperator {
 public:

  /**
   * Creates a Collection Merge Operator.
   * 
   * @param fixed_record_len the fixed size of each record.
   * @param comparator if records should be ordered, a comparator to order them.
   * @param unique_constraint A constraint on whether records should be unique
   *     or not, controls Set vs Vector behaviour.
   * @param trace true if input and output to the CollectionMergeOperator should
   *     be written to the trace file /tmp/CollectionMergeOperator.trace.
   * @param trace_record_serializer a function which knows how to serialize
   *     your records into some format that is readable for you. The default
   *     is just a simple hex serialization.
   */
  CollectionMergeOperator(const uint16_t fixed_record_len,
      const Comparator* comparator = nullptr,
      const UniqueConstraint unique_constraint = UniqueConstraint::kNone,
      const bool trace = false,
      const std::function<std::string(const char* const, size_t, const size_t)> trace_record_serializer = [](const char* const str, size_t offset, const size_t len){ return CollectionMergeOperator::toHex(str+offset, len); });
  virtual const char* Name() const override;
  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
      MergeOperationOutput* merge_out) const override;
  virtual bool PartialMergeMulti(const Slice& key,
      const std::deque<Slice>& operand_list, std::string* new_value,
      Logger* logger) const override;
 
 private:
  const uint16_t fixed_record_len_;
  const Comparator* const comparator_;
  const UniqueConstraint unique_constraint_;
  const bool trace_;
  const std::function<std::string(const char* const, size_t, const size_t)> trace_record_serializer_;

  static constexpr const char* const TRACE_FILE = "/tmp/CollectionMergeOperator.trace";

  /**
   * A comparator for Slices.
   */
  struct SliceComparator {
    bool operator()(const Slice& lhs, const Slice& rhs) const { 
      return lhs.compare(rhs) == 0;
    }
  };

  /**
   * A hasher for Slices.
   */
  struct SliceHasher {
    size_t operator()(const Slice& slice) const {
      size_t result = 0;
      const size_t prime = 31;
      for (size_t i = 0; i < slice.size(); ++i) {
          result = slice[i] + (result * prime);
      }
      return result;
    }
  };

  /**
   * Represents a PartialMerge Operation on one or more records.
   */
  struct Operation {
      Operation(CollectionOperation operation, std::initializer_list<Slice> records) : operation_(operation), records_(records) {}
      CollectionOperation operation_;
      std::vector<Slice> records_;
  };

  // NOTE: the std::vector in Operations and RecordsOffsets is used as a stack with elements pushed to/or popped from the end
  using Operations = std::vector<Operation>;
  using OperationOffset = size_t;
  using RecordOffset = size_t;
  using RecordOffsets = std::vector<RecordOffset>;
  using OperationRecordOffsets = std::map<OperationOffset, RecordOffsets>;
  using OperationsIndex = std::unordered_map<Slice, OperationRecordOffsets, SliceHasher, SliceComparator>;

  /* FullMergeV2 implementation */
  void fm_clear(std::string& existing, Logger* logger, const bool debug) const;
  bool fm_add(const char* value, const size_t value_len, std::string& existing,
      Logger* logger, const bool debug) const;
  bool insert(const char* value, const size_t value_len, std::string& existing,
      Logger* logger, const bool debug) const;
  bool insert_record(Slice& value_record, std::string& existing,
      Logger* logger, const bool debug) const;
  bool append(const char* value, const size_t value_len, std::string& existing,
      Logger* logger, const bool debug) const;
  bool exists(Slice& value_record, std::string& existing) const;
  bool fm_remove(const char* value, const size_t value_len, std::string& existing,
      Logger* logger, const bool debug) const;
  void fm_trace_start(const MergeOperationInput& merge_in,
      MergeOperationOutput* merge_out) const;
  void fm_trace_start_process_op(Slice *slice, std::ofstream& trace_file) const;
  void fm_trace_start_records(Slice *slice, const size_t len,
      std::ofstream& trace_file) const;
  void fm_trace_end(MergeOperationOutput* merge_out) const;

  /* PartialMergeMulti implementation */
  void pm_clear(Operations& operations) const;
  bool pm_add(Operations& operations,
      OperationsIndex& operations_index,
      const char * const records_ptr, const size_t records_len,
      Logger* logger, const bool debug) const;
  bool pm_remove(Operations& operations,
      OperationsIndex& operations_index,
      const char * const records_ptr, const size_t records_len,
      Logger* logger, const bool debug) const;
  void push_or_mergeprev_operation_record(Operations& operations,
    OperationsIndex& operations_index,
    const OperationsIndex::iterator& it_operation_idx,
    CollectionOperation collection_op,
    Slice& record) const;
  void remove_operation_record(Operations& operations,
    OperationsIndex& operations_index,
    const OperationsIndex::iterator& it_operation_idx,
    const OperationRecordOffsets::reverse_iterator& it_operation_record_offsets,
    Operations::iterator& it_prev_operation) const;
  void pm_contiguous_operations(Operations& operations) const;
  void pm_serialize(Operations& operations, std::string* new_value) const;
  void pm_trace_start(const Slice& key,
      const std::deque<Slice>& operand_list, std::string* new_value) const;
  void pm_trace_end(std::string* new_value) const;
  void pm_trace_end_process_op(std::string* new_value, size_t& i,
      std::ofstream& trace_file) const;
  void pm_trace_end_records(std::string* new_value, size_t& i,
      std::ofstream& trace_file) const;
  void trace_exit(const char* const msg, const bool success) const;

/**
 * Produces a hex string representation of a byte string.
 * 
 * @param s the byte string
 * @param len the length of the byte string to read
 * 
 * @return the hex string
 */
static std::string toHex(const char* s, const size_t len) {
  std::string result;
  char buf[10];
  for (size_t i = 0; i < len; i++) {
    snprintf(buf, 10, "%02X", (unsigned char)s[i]);
    result += buf;
  }
  return result;
}

};
}  // end rocksdb namespace
