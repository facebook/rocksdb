// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class Logger;

// The Merge Operator
//
// Essentially, a MergeOperator specifies the SEMANTICS of a merge, which only
// client knows. It could be numeric addition, list append, string
// concatenation, edit data structure, ... , anything.
// The library, on the other hand, is concerned with the exercise of this
// interface, at the right time (during get, iteration, compaction...)
//
// To use merge, the client needs to provide an object implementing one of
// the following interfaces:
//  a) SimpleAssociativeMergeOperator - simplest interface for implementing
//    many but not all kinds of merge semantics. The implementation always
//    takes two inputs (values or merge operands) and merges them to one
//    value.
//    Requires: operator is associative, and no distinction between values
//    and merge operands.
//    Examples: numeric addition, and string concatenation.
//    Non-examples: numeric subtraction (not associative), editable document
//    (values are documents, merge operands are edits)
//
//  b - rarely useful) AssociativeMergeOperator - not as simple as
//    SimpleAssociativeMergeOperator because left side input is optional.
//    Not as general as MergeOperator.
//
//  c) MergeOperator - the generic class for all the more abstract / complex
//    operations; one method (FullMergeV2) to merge a Put/Delete value with a
//    merge operand; and another method (PartialMerge) that merges multiple
//    operands together. This is especially useful if your key values have
//    complex structures but you would still like to support client-specific
//    incremental updates, including if the byte interpretation of merge
//    operands is different from values.
//
// Refer to rocksdb-merge wiki for more details and example implementations.
//
class MergeOperator {
 public:
  virtual ~MergeOperator() {}
  static const char* Type() { return "MergeOperator"; }

  // Gives the client a way to express the read -> modify -> write semantics
  // key:      (IN)    The key that's associated with this merge operation.
  //                   Client could multiplex the merge operator based on it
  //                   if the key space is partitioned and different subspaces
  //                   refer to different types of data which have different
  //                   merge operation semantics
  // existing: (IN)    null indicates that the key does not exist before this op
  // operand_list:(IN) the sequence of merge operations to apply, front() first.
  // new_value:(OUT)   Client is responsible for filling the merge result here.
  // The string that new_value is pointing to will be empty.
  // logger:   (IN)    Client could use this to log errors during merge.
  //
  // Return true on success.
  // All values passed in will be client-specific values. So if this method
  // returns false, it is because client specified bad data or there was
  // internal corruption. This will be treated as an error by the library.
  //
  // Also make use of the *logger for error messages.
  virtual bool FullMerge(const Slice& /*key*/, const Slice* /*existing_value*/,
                         const std::deque<std::string>& /*operand_list*/,
                         std::string* /*new_value*/, Logger* /*logger*/) const {
    // deprecated, please use FullMergeV2()
    assert(false);
    return false;
  }

  struct MergeOperationInput {
    explicit MergeOperationInput(const Slice& _key,
                                 const Slice* _existing_value,
                                 const std::vector<Slice>& _operand_list,
                                 Logger* _logger)
        : key(_key),
          existing_value(_existing_value),
          operand_list(_operand_list),
          logger(_logger) {}

    // The key associated with the merge operation.
    const Slice& key;
    // The existing value of the current key, nullptr means that the
    // value doesn't exist.
    const Slice* existing_value;
    // A list of operands to apply.
    const std::vector<Slice>& operand_list;
    // Logger could be used by client to log any errors that happen during
    // the merge operation.
    Logger* logger;
  };

  struct MergeOperationOutput {
    explicit MergeOperationOutput(std::string& _new_value,
                                  Slice& _existing_operand)
        : new_value(_new_value), existing_operand(_existing_operand) {}

    // Client is responsible for filling the merge result here.
    std::string& new_value;
    // If the merge result is one of the existing operands (or existing_value),
    // client can set this field to the operand (or existing_value) instead of
    // using new_value.
    Slice& existing_operand;
  };

  // This function applies a stack of merge operands in chrionological order
  // on top of an existing value. There are two ways in which this method is
  // being used:
  // a) During Get() operation, it used to calculate the final value of a key
  // b) During compaction, in order to collapse some operands with the based
  //    value.
  //
  // Note: The name of the method is somewhat misleading, as both in the cases
  // of Get() or compaction it may be called on a subset of operands:
  // K:    0    +1    +2    +7    +4     +5      2     +1     +2
  //                              ^
  //                              |
  //                          snapshot
  // In the example above, Get(K) operation will call FullMerge with a base
  // value of 2 and operands [+1, +2]. Compaction process might decide to
  // collapse the beginning of the history up to the snapshot by performing
  // full Merge with base value of 0 and operands [+1, +2, +7, +4].
  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const;

  // This function performs merge(left_op, right_op)
  // when both the operands are themselves merge operation types
  // that you would have passed to a DB::Merge() call in the same order
  // (i.e.: DB::Merge(key,left_op), followed by DB::Merge(key,right_op)).
  //
  // PartialMerge should combine them into a single merge operation that is
  // saved into *new_value, and then it should return true.
  // *new_value should be constructed such that a call to
  // DB::Merge(key, *new_value) would yield the same result as a call
  // to DB::Merge(key, left_op) followed by DB::Merge(key, right_op).
  //
  // The string that new_value is pointing to should be empty initially.
  //
  // The default implementation of PartialMergeMulti will use this function
  // as a helper, for backward compatibility.  Any successor class of
  // MergeOperator should either implement PartialMerge or PartialMergeMulti,
  // although implementing PartialMergeMulti is suggested as it is in general
  // more effective to merge multiple operands at a time instead of two
  // operands at a time.
  //
  // If it is impossible or infeasible to combine the two operations,
  // leave new_value unchanged and return false. The library will
  // internally keep track of the operations, and apply them in the
  // correct order once a base-value (a Put/Delete/End-of-Database) is seen.
  //
  // TODO: Presently there is no way to differentiate between error/corruption
  // and simply "return false". For now, the client should simply return
  // false in any case it cannot perform partial-merge, regardless of reason.
  // If there is corruption in the data, handle it in the FullMergeV2() function
  // and return false there.  The default implementation of PartialMerge will
  // always return false.
  virtual bool PartialMerge(const Slice& /*key*/, const Slice& /*left_operand*/,
                            const Slice& /*right_operand*/,
                            std::string* /*new_value*/,
                            Logger* /*logger*/) const {
    return false;
  }

  // This function performs merge when all the operands are themselves merge
  // operation types that you would have passed to a DB::Merge() call in the
  // same order (front() first)
  // (i.e. DB::Merge(key, operand_list[0]), followed by
  //  DB::Merge(key, operand_list[1]), ...)
  //
  // PartialMergeMulti should combine them into a single merge operation that is
  // saved into *new_value, and then it should return true.  *new_value should
  // be constructed such that a call to DB::Merge(key, *new_value) would yield
  // the same result as subquential individual calls to DB::Merge(key, operand)
  // for each operand in operand_list from front() to back().
  //
  // The string that new_value is pointing to should be empty initially.
  //
  // The PartialMergeMulti function will be called when there are at least two
  // operands.
  //
  // In the default implementation, PartialMergeMulti will invoke PartialMerge
  // multiple times, where each time it only merges two operands.  Developers
  // should either implement PartialMergeMulti, or implement PartialMerge which
  // is served as the helper function of the default PartialMergeMulti.
  virtual bool PartialMergeMulti(const Slice& key,
                                 const std::deque<Slice>& operand_list,
                                 std::string* new_value, Logger* logger) const;

  // The name of the MergeOperator. Used to check for MergeOperator
  // mismatches (i.e., a DB created with one MergeOperator is
  // accessed using a different MergeOperator)
  // TODO: the name is currently not stored persistently and thus
  //       no checking is enforced. Client is responsible for providing
  //       consistent MergeOperator between DB opens.
  virtual const char* Name() const = 0;

  // Determines whether the PartialMerge can be called with just a single
  // merge operand.
  // Override and return true for allowing a single operand. PartialMerge
  // and PartialMergeMulti should be overridden and implemented
  // correctly to properly handle a single operand.
  virtual bool AllowSingleOperand() const { return false; }

  // Allows to control when to invoke a full merge during Get.
  // This could be used to limit the number of merge operands that are looked at
  // during a point lookup, thereby helping in limiting the number of levels to
  // read from.
  // Doesn't help with iterators.
  //
  // Note: the merge operands are passed to this function in the reversed order
  // relative to how they were merged (passed to FullMerge or FullMergeV2)
  // for performance reasons, see also:
  // https://github.com/facebook/rocksdb/issues/3865
  virtual bool ShouldMerge(const std::vector<Slice>& /*operands*/) const {
    return false;
  }
};

// Base of SimpleAssociativeMergeOperator that allows customizing how a merge
// operand is combined with a non-existant value association, such as
// canonicalizing operands in some way before promoting them to values.
// Since this is rarely needed, SimpleAssociativeMergeOperator is generally
// recommended instead.
//
// In mathematical terms: implementations of this class must satisfy
// associativity, like SimpleAssociativeMergeOperator, but null might not
// be an identity element of the operator, unlike
// SimpleAssociativeMergeOperator.
class AssociativeMergeOperator : public MergeOperator {
 public:
  ~AssociativeMergeOperator() override {}

  // Merges two values/operands (left side optional) to express the
  // read -> modify -> write semantics
  // key:           (IN) The key associated with this merge (often unused)
  // left:          (IN) a value or operand sequenced before the right input,
  // or null indicating a merge with a non-existant or deleted value binding.
  // right:         (IN) an operand or merge result to merge with.
  // result:       (OUT) Client is responsible for filling the merge result
  // here. The string that result is pointing to should be empty initially.
  // logger:        (IN) Client can use this to log errors during merge.
  //
  // Return true on success.
  // All values passed in will be client-specific values. So if this method
  // returns false, it is because client specified bad data or there was
  // internal corruption. The client should assume that this will be treated
  // as an error by the library.
  virtual bool Merge(const Slice& key, const Slice* left, const Slice& right,
                     std::string* result, Logger* logger) const = 0;

 private:
  // Default implementations of the MergeOperator functions
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override;

  bool PartialMerge(const Slice& key, const Slice& left_operand,
                    const Slice& right_operand, std::string* new_value,
                    Logger* logger) const override;
};

// A simple merge operator based on an associative binary operator on
// values/operands. Implementations must satisfy associativity,
//      merge(merge(x, y), z) == merge(x, merge(y, z))
// so that a single implementation can be used for both
// 1) combining an existing associated value with a merge operand, and
// 2) combining two merge operands (for partial merge)
//
// When extending this class, DB::Merge(opts, k, v) will be equivalent to
// DB::Put(opts, k, v) when k has no existing value binding. Under this
// simplified construction, there needs to be a unified interpretation of
// values (what keys map to) and merge operands (passed to DB::Merge).
// Justification: the overrider of AssociativeMergeOperator::Merge is not
// told which it is operating on and (for SimpleAssociativeMergeOperator
// only) an operand is simply promoted to a value when there's no existing
// value to merge it with.
//
// In mathematical terms: null (no binding) is treated as an identity
// element of the operator, so does not need to be handled by the
// implementation, unlike AssociativeMergeOperator.
class SimpleAssociativeMergeOperator : public AssociativeMergeOperator {
 public:
  ~SimpleAssociativeMergeOperator() override {}

  // Merges two values/operands to express the read -> modify -> write
  // semantics
  // key:           (IN) The key associated with this merge (often unused)
  // left:          (IN) a value/operand sequenced before the right input.
  // right:         (IN) a value/operand to merge with.
  // result:       (OUT) Client is responsible for filling the merge result
  // here. The string that result is pointing to should be empty initially.
  // logger:        (IN) Client can use this to log errors during merge.
  //
  // Return true on success.
  // All values passed in will be client-specific values. So if this method
  // returns false, it is because client specified bad data or there was
  // internal corruption. The client should assume that this will be treated
  // as an error by the library.
  virtual bool BinaryMerge(const Slice& key, const Slice& left,
                           const Slice& right, std::string* result,
                           Logger* logger) const = 0;

 private:
  bool Merge(const Slice& key, const Slice* left, const Slice& right,
             std::string* result, Logger* logger) const final;
};

}  // namespace ROCKSDB_NAMESPACE
