//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//  @author Adam Retter

#define __STDC_FORMAT_MACROS 1
#include <inttypes.h>

#include "collection_merge_operator.h"
#include "multi_operand_list.h"

#include "util/coding.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

// TODO(AR) consider optimising memory use in PartialMergeMulti, replace Operations and OperationsIndex with a pointers approach
// TODO(AR) consider implementing Collection Operations for kRemoveAll, kReplace, and kReplaceAll
// TODO(AR) implement support for variable sized records per key
// TODO(AR) implement support for variable sized records within a key using variable bit encoding

CollectionMergeOperator::CollectionMergeOperator(
    const uint16_t fixed_record_len, const Comparator* comparator,
    const UniqueConstraint unique_constraint, const bool trace,
    const std::function<std::string(const char* const, size_t, const size_t)> trace_record_serializer)
        : fixed_record_len_(fixed_record_len),
        comparator_(comparator),
        unique_constraint_(unique_constraint),
        trace_(trace),
        trace_record_serializer_(trace_record_serializer) {
}

const char* CollectionMergeOperator::Name() const {
  return "CollectionMergeOperator";
}

bool CollectionMergeOperator::FullMergeV2(const MergeOperationInput& merge_in,
    MergeOperationOutput* merge_out) const {

  if (trace_) {
    fm_trace_start(merge_in, merge_out);
  }

  if (merge_in.operand_list.empty()) {
    merge_out->existing_operand = *merge_in.existing_value;
    if (trace_) {
      trace_exit("Empty Operand List", true);
    }
    return true; 
  }

  // is debug level logging enabled?
  Logger* logger = merge_in.logger;
  const bool debug = logger->GetInfoLogLevel() == InfoLogLevel::DEBUG_LEVEL;

  if (debug) {
    Log(InfoLogLevel::DEBUG_LEVEL, logger, "Starting CollectionMergeOperator::FullMergeV2");
    Log(InfoLogLevel::DEBUG_LEVEL, logger, "key=[%s]", merge_in.key.ToString(true).c_str());
    Log(InfoLogLevel::DEBUG_LEVEL, logger, "existing_value: %p", merge_in.existing_value);
    if (merge_in.existing_value) {
      Log(InfoLogLevel::DEBUG_LEVEL, logger, "existing_value: [%s]", merge_in.existing_value->ToString(true).c_str());
    }
    Log(InfoLogLevel::DEBUG_LEVEL, logger, "operand_list.size: %zu", merge_in.operand_list.size());
  }

  // we wrap operand list in MultiOperandList so that _kMulti is transparently expanded for us
  MultiOperandList<const std::vector<Slice>> multi_operand_list(merge_in.operand_list);

  // optimisation: we can ignore any operands before the last kClear
  bool found_clear_operation = false;
  auto last_clear_operation = multi_operand_list.begin();

  auto it = multi_operand_list.end();
  while (it != multi_operand_list.begin()) {
    --it;
    Slice operand = *it;
    if (operand[0] == CollectionOperation::kClear) {
      last_clear_operation = it;
      found_clear_operation = true;
      break;
    }
  }

  // do we need to merge with the existing_value?
  if (merge_in.existing_value != nullptr
      && !found_clear_operation) {
    /* no clear operation, so copy the existing_value to the new_value,
       all subsequent operations will be on new_value */
    merge_out->new_value.assign(merge_in.existing_value->data(), merge_in.existing_value->size());
  }

  // iterate all relevant operands
  for (auto it_operand = last_clear_operation; it_operand != multi_operand_list.end(); ++it_operand) {
    const Slice* const operand = &(*it_operand);

    // TODO(AR) temp - see https://github.com/facebook/rocksdb/issues/3655
    // handles the case where PartialMergeMulti returns a no-op
    if (operand->size() == 0) {
      continue;
    }

    const char* value = operand->data();
    const char collection_op = *value++;

    switch (collection_op) {
      case CollectionOperation::kClear:
        fm_clear(merge_out->new_value, logger, debug);
        break;
      
      case CollectionOperation::kAdd:
        if (!fm_add(value, operand->size() - sizeof(CollectionOperation), merge_out->new_value, logger, debug)) {
          if (trace_) {
            trace_exit("fm_add failed", false);
          }
          return false;
        }
        break;
      
      case CollectionOperation::kRemove:
        if (!fm_remove(value, operand->size() - sizeof(CollectionOperation), merge_out->new_value, logger, debug)) {
          if (trace_) {
            trace_exit("fm_remove failed", false);
          }
          return false;
        }
        break;

      default:
        Log(InfoLogLevel::ERROR_LEVEL, logger, "Unknown Collection operation: %x", collection_op);
        if (trace_) {
          trace_exit("unknown operation", false);
        }
        return false;
    }
  }

  if (debug) {
    Log(InfoLogLevel::DEBUG_LEVEL, logger, "Updated new_value=[%s]", toHex(merge_out->new_value.c_str(), merge_out->new_value.length()).c_str());
    Log(InfoLogLevel::DEBUG_LEVEL, logger, "CollectionMergeOperator::FullMergeV2 OK");
  }

  if (trace_) {
    fm_trace_end(merge_out);
  }

  return true;
}

bool CollectionMergeOperator::PartialMergeMulti(const Slice& key,
    const std::deque<Slice>& operand_list, std::string* new_value,
    Logger* logger) const {

  if (trace_) {
    pm_trace_start(key, operand_list, new_value);
  }

  // is debug level logging enabled?
  const bool debug = logger->GetInfoLogLevel() == InfoLogLevel::DEBUG_LEVEL;

  if (debug) {
      Log(InfoLogLevel::DEBUG_LEVEL, logger, "Starting CollectionMergeOperator::PartialMergeMulti");
      Log(InfoLogLevel::DEBUG_LEVEL, logger, "key=[%s]", key.ToString(true).c_str());
      Log(InfoLogLevel::DEBUG_LEVEL, logger, "operand_list.size()=[%zu]", operand_list.size());
  }

  if (operand_list.empty()) {
    if (trace_) {
      trace_exit("operand_list is empty", true);
    }
    return true;
  }

  // state and index of partial_merge results
  Operations operations;
  OperationsIndex operations_index;

  // we wrap operand list in MultiOperandList so that _kMulti is transparently expanded for us
  MultiOperandList<const std::deque<Slice>> multi_operand_list(operand_list);
  
  // iterate in reverse through operands
  auto it = multi_operand_list.end();
  while (it != multi_operand_list.begin()) {
    --it;
    const Slice* const operand = &(*it);

    // TODO(AR) temp - see https://github.com/facebook/rocksdb/issues/3655
    // handles the case where previous PartialMergeMulti returns a no-op
    if (operand->size() == 0) {
      continue;
    }

    const char* record_ptr = operand->data();
    const char collection_op = *record_ptr++;

    if (collection_op == CollectionOperation::kClear) {
      /* this kClear will be the last kClear in the operand_list
          any operands before that can be ignored, so we prepend
          it to the new_value and stop processing */
      pm_clear(operations);
      break;  // NOTE: we can stop processing, don't need to examine anything before this kClear

    } else if(collection_op == CollectionOperation::kAdd) {
      if (!pm_add(operations, operations_index, record_ptr, operand->size() - sizeof(CollectionOperation), logger, debug)) {
        new_value->clear();  // we should leave new_value unchanged, so cleanup any progress so far
        if (trace_) {
          trace_exit("pm_add failed", false);
        }
        return false;
      }

    } else if (collection_op == CollectionOperation::kRemove) {
      if (!pm_remove(operations, operations_index, record_ptr, operand->size() - sizeof(CollectionOperation), logger, debug)) {
        new_value->clear();  // we should leave new_value unchanged, so cleanup any progress so far
        if (trace_) {
          trace_exit("pm_remove failed", false);
        }
        return false;
      }

    } else {
      Log(InfoLogLevel::ERROR_LEVEL, logger, "Unknown Collection operation: %x", collection_op);
      new_value->clear();  // we should leave new_value unchanged, so cleanup any progress so far
      if (trace_) {
        trace_exit("unknown operation", false);
      }
      return false;
    }
  }

  pm_contiguous_operations(operations);
  pm_serialize(operations, new_value);

  if (debug) {
    Log(InfoLogLevel::DEBUG_LEVEL, logger, "Updated new_value=[%s]", toHex(new_value->c_str(), new_value->length()).c_str());
    Log(InfoLogLevel::DEBUG_LEVEL, logger, "CollectionMergeOperator::PartialMergeMulti OK");
  }

  if (trace_) {
    pm_trace_end(new_value);
  }

  return true;
}

void CollectionMergeOperator::pm_contiguous_operations(Operations& operations) const {
  for (auto it_a = operations.begin(); it_a != operations.end(); it_a++) {
    for (auto it_b = it_a + 1; it_b != operations.end(); it_b++) {
      if (it_a->operation_ == it_b->operation_) {
        // copy it_b records to it_a
        it_a->records_.insert(it_a->records_.end(), it_b->records_.begin(), it_b->records_.end());
        
        // remove it_b
        operations.erase(it_b);

        // re-position it_b
        it_b = it_a;
      } else {
        break;  // non-contiguous operation types
      }
    }
  }
}

void CollectionMergeOperator::pm_serialize(Operations& operations, std::string* new_value) const {
  const bool is_multi_op = operations.size() > 1;
  // did we merge to a single operation?
  if (is_multi_op) {
    // nope, so add _kMulti prefix
    new_value->push_back(CollectionOperation::_kMulti);
  }

  // serialize each operation
  while (!operations.empty()) {
    const auto operation = operations.back();

    const size_t start_operation_offset = new_value->size();
    new_value->push_back(operation.operation_);

    // serialize records
    size_t records_len = 0;
    if (operation.operation_ != CollectionOperation::kClear) {
      for(auto it = operation.records_.rbegin(); it != operation.records_.rend(); ++it) {
        new_value->append(it->data(), it->size());
        records_len += it->size();
      }
    }

    // add records size indicator
    if (is_multi_op) {
      char str_records_len[4];
      EncodeFixed32(str_records_len, records_len & 0xFFFFFFFF);
      new_value->insert(start_operation_offset, str_records_len, 4);
    }

    operations.pop_back();
  }
}

void CollectionMergeOperator::pm_clear(Operations& operations) const {
  operations.push_back(Operation(CollectionOperation::kClear, {}));
}

bool CollectionMergeOperator::pm_add(Operations& operations,
    OperationsIndex& operations_index,
    const char * const records_ptr, const size_t records_len,
    Logger* logger, const bool debug) const {

  // loop through records in reverse
  for (const char* record_ptr = records_ptr + records_len - fixed_record_len_; record_ptr >= records_ptr; record_ptr -= fixed_record_len_) {
    Slice record(record_ptr, fixed_record_len_);

    // have we previously processed an operation on this record?
    auto operation_idx = operations_index.find(record);
    if (operation_idx == operations_index.end()) {
      // no, first time we have seen this record

      // record an operation for the record
      push_or_mergeprev_operation_record(operations, operations_index, operation_idx, CollectionOperation::kAdd, record);
    } else {
      // yes, previously processed an operation for this record
      
      const auto it_operation_record_offsets = operation_idx->second.rbegin();
      auto it_prev_operation = operations.begin() + it_operation_record_offsets->first;

      // previously processed (following) operation was same operation?
      if (it_prev_operation->operation_ == CollectionOperation::kAdd) {
        // yes
        switch (unique_constraint_) {
          case UniqueConstraint::kMakeUnique:
              /* this is a kAdd operation and a following
               instruction for this record was also a kAdd
               to ensure the uniqueness constraint, we just ignore it */
              if (debug) {
                Log(InfoLogLevel::DEBUG_LEVEL, logger, "encountered new record for kAdd which is a duplicate of another new record, skipping as UniqueConstraint::kMakeUnique is set. (value=%s)", record.ToString(true).c_str());
              }
            break;
            
          case UniqueConstraint::kEnforceUnique:
              /* this is a kAdd operation and a following
               instruction for this record was also a kAdd
               this violates the uniqueness constraint */
              Log(InfoLogLevel::ERROR_LEVEL, logger, "encountered new record kAdd which is a duplicate of another new record, but UniqueConstraint::kEnforceUnique is set. (value=%s)", record.ToString(true).c_str());
              return false;  // PartialMergeMulti failed!
            break;
            
          case UniqueConstraint::kNone:
            /* we don't know how many kAdd are before
             the final kAdd or kRemove for a record
             so we always emit this record */
            
            // output the record for this op
            push_or_mergeprev_operation_record(operations, operations_index, operation_idx, CollectionOperation::kAdd, record);
            break;
        }
      } else if (it_prev_operation->operation_ == CollectionOperation::kRemove) {
        // no, the previously processed (following) operation was kRemove, so we IGNORE this kAdd and drop the following kRemove
        remove_operation_record(operations, operations_index, operation_idx, it_operation_record_offsets, it_prev_operation);
        
      } else {
        Log(InfoLogLevel::ERROR_LEVEL, logger, "Processing of kAdd before op=%" PRIu8 " is not implemented.", it_prev_operation->operation_);
        return false;
      }
    }
  }

  return true;
}

void CollectionMergeOperator::push_or_mergeprev_operation_record(Operations& operations,
    OperationsIndex& operations_index, const OperationsIndex::iterator& it_operation_idx,
    CollectionOperation collection_op, Slice& record) const {
  // was the previous operation (on a different record) of the same kind?
  if (operations.empty() == false && operations.back().operation_ == collection_op) {
    // yes, so merge with previous op
    operations.back().records_.push_back(record);
  } else {
    // no, so output a new op
    operations.push_back(Operation(collection_op, {record}));
  }

  // add an entry to the index for the record
  if (it_operation_idx == operations_index.end()) {
    OperationRecordOffsets operation_record_offsets { {operations.size() - 1, {operations.back().records_.size() - 1}} };
    operations_index.emplace(record, operation_record_offsets);
  } else {
    auto it_existing_operation_record_offsets = it_operation_idx->second.find(operations.size() - 1);
    if (it_existing_operation_record_offsets == it_operation_idx->second.end()) {
      it_operation_idx->second.emplace(operations.size() - 1, RecordOffsets { operations.back().records_.size() - 1 });
    } else {
      it_existing_operation_record_offsets->second.push_back(operations.back().records_.size() - 1);
    }
  }
}

void CollectionMergeOperator::remove_operation_record(Operations& operations,
  OperationsIndex& operations_index,
  const OperationsIndex::iterator& it_operation_idx,
  const OperationRecordOffsets::reverse_iterator& it_operation_record_offsets,
  Operations::iterator& it_prev_operation) const {
  
  // offset of the last matching record in the prev_operation
  const size_t operation_offset = it_operation_record_offsets->first;
  const size_t record_offset = it_operation_record_offsets->second.back();
  
  // we remove the record from the prev_operation
  it_prev_operation->records_.erase(it_prev_operation->records_.begin() + record_offset);
  
  // we need to renumber any records_offset_(s) after operation_record_offset.record_offset_ in operation_idx
  const bool drop_operation = it_prev_operation->records_.empty();
  if (drop_operation) {
    // drop the entire prev_operation
    operations.erase(it_prev_operation);
    
    // update the index - drop the only operation_offset from the index (for this record)
    it_operation_idx->second.erase(operation_offset);
    // are there any other OperationRecordOffsets for the record in the index?
    if (it_operation_idx->second.empty()) {
      // no, so remove the record from the index
      operations_index.erase(it_operation_idx);
    }
    
    // update the index - renumber anything in the index greater than operation_offset
    for (auto it_idx = operations_index.begin(); it_idx != operations_index.end(); ++it_idx) {
      const OperationRecordOffsets* const cur_op_recs = &(it_idx->second);
      OperationRecordOffsets updated_op_recs;
      for (auto it_op_recs = cur_op_recs->begin(); it_op_recs != cur_op_recs->end(); ++ it_op_recs) {
        const size_t cur_operation_offset = it_op_recs->first;
        size_t new_operation_offset = cur_operation_offset;
        if(cur_operation_offset > operation_offset) {
          new_operation_offset--;
        }
        
        updated_op_recs.emplace(new_operation_offset, it_op_recs->second);
      }
      
      it_idx->second = updated_op_recs;
    }
    
  } else {
    // update the index - for the record, at operation_offset, drop the record_offset
    if (it_operation_record_offsets->second.size() > 1) {
      it_operation_record_offsets->second.pop_back();
    } else {
      // there is only one record_offset, so drop the entry
      it_operation_idx->second.erase(--(it_operation_record_offsets.base()));
      if (it_operation_idx->second.empty()) {
        // there are no records left for the offset in the index, so drop the entry
        operations_index.erase(it_operation_idx);
      }
    }
    
    // update the index - for any records at operation_offset whoose record_offset > record_offset, subtract 1
    for (auto it_idx = operations_index.begin(); it_idx != operations_index.end(); ++it_idx) {
      auto it_op_rec = it_idx->second.find(operation_offset);
      if (it_op_rec != it_idx->second.end()) {
        for(auto it_rec = it_op_rec->second.begin(); it_rec != it_op_rec->second.end(); ++it_rec) {
          const size_t cur_record_offset = *it_rec;
          if (cur_record_offset > record_offset) {
            const size_t new_record_offset = cur_record_offset - 1;
            *it_rec = new_record_offset;
          }
        }
      }
    }
  }
}

bool CollectionMergeOperator::pm_remove(Operations& operations,
      OperationsIndex& operations_index,
      const char * const records_ptr, const size_t records_len,
      Logger* logger, const bool debug) const {

  // loop through records in reverse
  for (const char* record_ptr = records_ptr + records_len - fixed_record_len_; record_ptr >= records_ptr; record_ptr -= fixed_record_len_) {
    Slice record(record_ptr, fixed_record_len_);

    // have we previously processed an operation on this record?
    auto operation_idx = operations_index.find(record);
    if (operation_idx == operations_index.end()) {
      // no, first time we have seen this record

      // record an operation for the record
      push_or_mergeprev_operation_record(operations, operations_index, operation_idx, CollectionOperation::kRemove, record);
    } else {
      // yes, previously processed an operation for this record

      const auto it_operation_record_offsets = operation_idx->second.rbegin();
      auto prev_operation = operations[it_operation_record_offsets->first];

      // we need to consider what the prev_operation is!!!
      if (prev_operation.operation_ == CollectionOperation::kRemove) { 
        switch (unique_constraint_) {
          case UniqueConstraint::kMakeUnique:
            /* this is a kRemove operation and a following
              instruction for this record was also a kRemove
              to ensure the uniqueness constraint, we just ignore it */
              if (debug) {
                Log(InfoLogLevel::DEBUG_LEVEL, logger, "encountered new record for kRemove which is a duplicate of another new record, skipping as UniqueConstraint::kMakeUnique is set. (value=%s)", record.ToString(true).c_str());
              }
            break;

          case UniqueConstraint::kEnforceUnique:
            /* this is a kRemove operation and a following
              instruction for this record was also a kRemove
              this violates the uniqueness constraint */
            Log(InfoLogLevel::ERROR_LEVEL, logger, "encountered new record for kRemove which is a duplicate of another new record, but UniqueConstraint::kEnforceUnique is set. (value=%s)", record.ToString(true).c_str());
            return false;  // PartialMergeMulti failed!
            break;

          case UniqueConstraint::kNone:
            /* we don't know how many kRemove are before
                the final kAdd or kRemove for a record
                so we always emit this record */

            // record an operation for the record
            push_or_mergeprev_operation_record(operations, operations_index, operation_idx, CollectionOperation::kRemove, record);
            break;
        }
     } else {
        /* we don't know how many kRemove are before
                the final kAdd or kRemove for a record
                so we always emit this record */

        // record an operation for the record
        push_or_mergeprev_operation_record(operations, operations_index, operation_idx, CollectionOperation::kRemove, record);
     }
    }
  }

  return true;
}

void CollectionMergeOperator::fm_clear(std::string& existing, Logger* logger, const bool debug) const {
  if (debug) {
    Log(InfoLogLevel::DEBUG_LEVEL, logger, "Processing kClear");
  }
  existing.clear();
}

bool CollectionMergeOperator::fm_add(const char* value, const size_t value_len, std::string& existing, Logger* logger, const bool debug) const {
  if (debug) {
    Log(InfoLogLevel::DEBUG_LEVEL, logger, "Processing kAdd(existing=[%s], value=[%s])", toHex(existing.c_str(), existing.length()).c_str(), toHex(value, value_len).c_str());
  }

  // check we have a whole number of records in the new value
  if (value_len % fixed_record_len_ > 0) {
    Log(InfoLogLevel::ERROR_LEVEL, logger, "add encountered incomplete records in the new value (value_len=%zu, fixed_record_len_=%" PRIu16 ")", value_len, fixed_record_len_);
    return false;
  }

  // check we have a whole number of records in the existing Collection
  if (existing.size() % fixed_record_len_ > 0) {
    Log(InfoLogLevel::ERROR_LEVEL, logger, "add encountered incomplete records in the existing Collection (existing.size=%zu, fixed_record_len_=%" PRIu16 ")", existing.size(), fixed_record_len_);
    return false;
  }

  /* if there is no comparator then we
         append, otherwise we insert */
  if (comparator_ == nullptr) {
    return append(value, value_len, existing, logger, debug);
  } else {
    return insert(value, value_len, existing, logger, debug);
  }
}

bool CollectionMergeOperator::insert(const char* value, const size_t value_len, std::string& existing, Logger* logger, const bool debug) const {
  // count of records to insert
  const size_t value_records_len = value_len / fixed_record_len_;

  // iterate the records to insert
  for (size_t i = 0; i < value_records_len; i++) {
    Slice value_record(value, fixed_record_len_);
    if (!insert_record(value_record, existing, logger, debug)) {
      return false;
    }

    // TODO(AR) future optimisation: inform next insert_record based on where the previous insert_record did insertion (optimisation for value that has records pre-ordered by comparator)

    // move to the next value record
    value += fixed_record_len_;
  }

  return true;
}

bool CollectionMergeOperator::insert_record(Slice& value_record, std::string& existing, Logger* logger, const bool debug) const {
  // count of existing records
  const size_t existing_records_len = existing.size() / fixed_record_len_;

  // TODO(AR) future optimisation: rather than iterating existing records from start to finish, we could do a binary search!

  // iterate the existing records
  const char* existing_ptr = existing.data();
  for (size_t i = 0; i < existing_records_len; i++) {
    Slice existing_record(existing_ptr, fixed_record_len_);
    const int64_t comparison = comparator_->Compare(existing_record, value_record);

    if (comparison > 0) {
      // existing_record > value_record
      
      // insert before existing_record
      const size_t insert_offset = i * fixed_record_len_;
      existing.insert(insert_offset, value_record.data(), fixed_record_len_);
      return true;
    
    } else if (comparison == 0) {
      // existing record == value_record

      // apply uniqueness constraint
      switch (unique_constraint_) {
        case kMakeUnique:
          // new record is a duplicate of an existing record, so we don't insert
          if (debug) {
            Log(InfoLogLevel::DEBUG_LEVEL, logger, "insert_record encountered existing record which is the same as the new record, skipping insert_record as UniqueConstraint::kMakeUnique is set. (value=%s)", toHex(value_record.data(), fixed_record_len_).c_str());
          }
          return true;  // insert_record effectively succeeded!

        case kEnforceUnique:
          // new record is a duplicate of an existing record, this is not allowed!
          Log(InfoLogLevel::ERROR_LEVEL, logger, "insert_record encountered existing record which is the same as the new record, but UniqueConstraint::kEnforceUnique is set. (value=%s)", toHex(value_record.data(), fixed_record_len_).c_str());
          return false;  // insert_record failed!
        
        case kNone:
        default:
          // insert before existing_record
          const size_t insert_offset = i * fixed_record_len_;
          existing.insert(insert_offset, value_record.data(), fixed_record_len_);
          return true;
      }
    }
    // else, keep searching. i.e. existing_record < value_record

    // move to the next existing record
    existing_ptr += fixed_record_len_;
  }

  // we have reached the end of the existing records, and not yet inserted, so we append the new record
  existing.append(value_record.data(), fixed_record_len_);
  return true;
}

bool CollectionMergeOperator::append(const char* value, const size_t value_len, std::string& existing, Logger* logger, const bool debug) const {
  // count of records to append
  const size_t value_records_len = value_len / fixed_record_len_;

  // append new records one by one, checking the uniqueness constraint
  for (size_t i = 0; i < value_records_len; i++) {
    Slice value_record(value, fixed_record_len_);
    switch (unique_constraint_) {
      case kMakeUnique:
          if (exists(value_record, existing)) {
            // new record is a duplicate of an existing record, so we don't append
            if (debug) {
              Log(InfoLogLevel::DEBUG_LEVEL, logger, "append encountered existing record which is the same as the new record, skipping insert_record as UniqueConstraint::kMakeUnique is set. (value=%s)", toHex(value_record.data(), fixed_record_len_).c_str());
            }
          } else {
            // append new record to existing records
            existing.append(value_record.data(), fixed_record_len_);
          }
        break;

      case kEnforceUnique:
          if (exists(value_record, existing)) {
            // new record is a duplicate of an existing record, this is not allowed!
            Log(InfoLogLevel::ERROR_LEVEL, logger, "apppend encountered existing record which is the same as the new record, but UniqueConstraint::kEnforceUnique is set. (value=%s)", toHex(value_record.data(), fixed_record_len_).c_str());
            return false;  // append failed!
          } else {
            // append new record to existing records
            existing.append(value_record.data(), fixed_record_len_);
          }
        break;

      case kNone:
      default:
        // append new record to existing records
        existing.append(value_record.data(), fixed_record_len_);
        break;
    }

    // move to the next value record
    value += fixed_record_len_;
  }

  return true;
}

bool CollectionMergeOperator::exists(Slice& value_record, std::string& existing) const {
  const char* existing_ptr = existing.data();
  for (size_t i = 0; i < existing.size(); i += fixed_record_len_) {
    Slice existing_record(existing_ptr, fixed_record_len_);
    if (existing_record.compare(value_record) == 0) {
      return true;
    }

    existing_ptr += fixed_record_len_;
  }

  return false;
}

bool CollectionMergeOperator::fm_remove(const char* value, const size_t value_len,
    std::string& existing, Logger* logger, const bool debug) const {
  if (debug) {
    Log(InfoLogLevel::DEBUG_LEVEL, logger, "Processing kRemove(existing=[%s], value=[%s])", toHex(existing.c_str(), existing.length()).c_str(), toHex(value, value_len).c_str());
  }

  if (existing.empty()) {
    return true;  // nothing to do
  }

  // check we have a whole number of records in the new value
  if (value_len % fixed_record_len_ > 0) {
    Log(InfoLogLevel::ERROR_LEVEL, logger, "remove encountered incomplete records in the new value (value_len=%zu, fixed_record_len_=%" PRIu16 ")", value_len, fixed_record_len_);
    return false;
  }

  // check we have a whole number of records in the existing Collection
  if (existing.size() % fixed_record_len_ > 0) {
    Log(InfoLogLevel::ERROR_LEVEL, logger, "remove encountered incomplete records in the existing Collection (existing.size=%zu, fixed_record_len_=%" PRIu16 ")", existing.size(), fixed_record_len_);
    return false;
  }

  // count of records to remove
  const size_t value_records_len = value_len / fixed_record_len_;

  // iterate records to remove one by one
  for (size_t i = 0; i < value_records_len; i++) {
    Slice value_record(value, fixed_record_len_);

    // count of existing records
    const size_t existing_records_len = existing.size() / fixed_record_len_;

    // loop through existing records, remove the first that matches the record to remove
    const char* existing_ptr = existing.data();
    for (size_t j = 0; j < existing_records_len; j++) {
      Slice existing_record(existing_ptr, fixed_record_len_);
      if (existing_record.compare(value_record) == 0) {
        // match, so remove existing_record
        const size_t erase_offset = j * fixed_record_len_;
        existing.erase(erase_offset, fixed_record_len_);
        break;  // exit inner-loop
      }

      existing_ptr += fixed_record_len_;
    }
    
    value += fixed_record_len_;
  }

  return true;
}

void CollectionMergeOperator::fm_trace_start(const MergeOperationInput& merge_in,
    MergeOperationOutput* merge_out) const {
  std::ofstream trace_file;
  trace_file.open (TRACE_FILE, std::ofstream::out | std::ofstream::app);
  trace_file << "# FullMergeV2" << std::endl;
  trace_file << "* key: [" << merge_in.key.ToString(true) << "]" << std::endl;
  trace_file << "* existing_value: ";
  if (merge_in.existing_value == nullptr) {
    trace_file << "nullptr" << std::endl;
  } else {
    trace_file << "[" << merge_in.existing_value->ToString(true) << "]" << std::endl;
  }
  trace_file << "* new_value: [" << toHex(merge_out->new_value.c_str(), merge_out->new_value.size()) << "]" << std::endl;
  trace_file << "* existing_operand: [";
  if (merge_out->existing_operand.size() > 0) {
    trace_file << merge_out->existing_operand.ToString(true);
  }
  trace_file << "]" << std::endl;
  size_t i= 0;
  trace_file << "* operand_list(size=" << merge_in.operand_list.size() << "): {" << std::endl;
  for (auto it = merge_in.operand_list.begin(); it != merge_in.operand_list.end(); ++it) {
    Slice slice(*it);
    trace_file << "\toperand(" << i++ << ", size=" << slice.size() << ")=[";
    if (slice.size() > 0) {
      fm_trace_start_process_op(&slice, trace_file);
    }
    trace_file << "]" << std::endl;
  }
  trace_file << "}" << std::endl;
  trace_file.close();
}

void CollectionMergeOperator::fm_trace_start_process_op(Slice *slice, std::ofstream& trace_file) const {
  const char collection_op = (*slice)[0];
  slice->remove_prefix(1);

  switch (collection_op) {

    case CollectionOperation::kClear:
      trace_file << "Clear:";
      break;

    case CollectionOperation::kAdd:
      trace_file << "Add:";
      fm_trace_start_records(slice, slice->size(), trace_file);
      break;

    case CollectionOperation::kRemove:
      trace_file << "Remove:";
      fm_trace_start_records(slice, slice->size(), trace_file);
      break;

    case CollectionOperation::_kMulti:
      trace_file << "Multi:" << std::endl;
      while (slice->size() > 0) {
        const size_t op_records_len = DecodeFixed32(slice->data());
        slice->remove_prefix(4);
        const char sub_collection_op = (*slice)[0];
        slice->remove_prefix(1);

        switch (sub_collection_op) {
          case CollectionOperation::kClear:
            trace_file << "\t\tClear" << std::endl;
            break;

          case CollectionOperation::kAdd:
            trace_file << "\t\tAdd:";
            fm_trace_start_records(slice, op_records_len, trace_file);
            slice->remove_prefix(op_records_len);
            trace_file << std::endl;
            break;

          case CollectionOperation::kRemove:
            trace_file << "\t\tRemove:";
            fm_trace_start_records(slice, op_records_len, trace_file);
            slice->remove_prefix(op_records_len);
            trace_file << std::endl;
            break;

          default:
          trace_file << "\t\tUNKNOWN" << std::endl;
          break;
        }
      }
      trace_file << "]";
      break;

    default:
      trace_file << "UNKNOWN";
      break;
  }
}

void CollectionMergeOperator::fm_trace_start_records(Slice *slice, const size_t len, std::ofstream& trace_file) const {
  for (size_t j = 0; j < len; j += fixed_record_len_) {
    trace_file << trace_record_serializer_(slice->data(), j, fixed_record_len_);
    if (j + fixed_record_len_ < len) {
      trace_file << ",";
    }
  }
}

void CollectionMergeOperator::fm_trace_end(MergeOperationOutput* merge_out) const {
  std::ofstream trace_file;
  trace_file.open (TRACE_FILE, std::ofstream::out | std::ofstream::app);
  trace_file << "## IMPLEMENTATION..." << std::endl;
  trace_file << "* new_value: [";
  for (size_t i = 0; i < merge_out->new_value.size(); i+= fixed_record_len_) {
    trace_file << trace_record_serializer_(merge_out->new_value.data(), i, fixed_record_len_);
    if (i + fixed_record_len_ < merge_out->new_value.size()) {
      trace_file << ",";
    }
  }
  trace_file << "]" << std::endl;
  trace_file << std::endl;
  trace_file.close();
}

void CollectionMergeOperator::pm_trace_start(const Slice& key,
    const std::deque<Slice>& operand_list, std::string* new_value) const {
  std::ofstream trace_file;
  trace_file.open (TRACE_FILE, std::ofstream::out | std::ofstream::app);
  trace_file << "# PartialMergeMulti" << std::endl;
  trace_file << "* key: [" << key.ToString(true) << "]" << std::endl;
  trace_file << "* new_value: [" << toHex(new_value->c_str(), new_value->size()) << "]" << std::endl;
  size_t i= 0;
  trace_file << "* operand_list(size=" << operand_list.size() << "): {" << std::endl;
  for (auto it = operand_list.begin(); it != operand_list.end(); ++it) {
    Slice slice = *it;
    trace_file << "\t operand(" << i++ << ", size=" << slice.size() << ")=[";
    if (slice.size() > 0) {
      const char collection_op = slice[0];
      switch (collection_op) {
        case CollectionOperation::kClear:
          trace_file << "Clear:";
          break;
        case CollectionOperation::kAdd:
          trace_file << "Add:";
          break;
        case CollectionOperation::kRemove:
          trace_file << "Remove:";
          break;
        case CollectionOperation::_kMulti:
          trace_file << "Multi:";
          break;
        default:
          trace_file << "UNKNOWN:";
          break;
      }
      slice.remove_prefix(1);

      for (size_t j = 0; j < slice.size(); j += fixed_record_len_) {
        trace_file << trace_record_serializer_(slice.data(), j, fixed_record_len_);
        if (j + fixed_record_len_ < slice.size()) {
          trace_file << ",";
        }
      }
    }
    trace_file << "]" << std::endl;
  }
  trace_file << "}" << std::endl;
  trace_file.close();
}

void CollectionMergeOperator::pm_trace_end(std::string* new_value) const {
  std::ofstream trace_file;
  trace_file.open (TRACE_FILE, std::ofstream::out | std::ofstream::app);
  trace_file << "## IMPLEMENTATION..." << std::endl;
  if(new_value == nullptr) {
    trace_file << "* new_value: nullptr" << std::endl;
  } else if(new_value->size() == 0) {
    trace_file << "* new_value: []" << std::endl;
  } else {
    trace_file << "* new_value(size=" << new_value->size() << ")=[" << std::endl;

    size_t i = 0;
    while (i < new_value->size()) {
      pm_trace_end_process_op(new_value, i, trace_file);
    }

    trace_file << "]" << std::endl;
  }
  trace_file << std::endl;
  trace_file.close();
}

void CollectionMergeOperator::pm_trace_end_process_op(std::string* new_value, size_t& i, std::ofstream& trace_file) const {
  const char collection_op = new_value->at(i++);
  switch (collection_op) {
    case CollectionOperation::kClear:
      trace_file << "\tClear" << std::endl;
      break;
    case CollectionOperation::kAdd:
      trace_file << "\tAdd(";
      pm_trace_end_records(new_value, i, trace_file);
      trace_file << ")" << std::endl;
      break;
    case CollectionOperation::kRemove:
      trace_file << "\tRemove(";
      pm_trace_end_records(new_value, i, trace_file);
      trace_file << ")" << std::endl;
      break;
    case CollectionOperation::_kMulti:
      trace_file << "\tMulti: TODO";
      i = new_value->size();  // TODO(AR) implement
      break;
    default:
      assert(false);
      i = new_value->size();
      break;
  }
}

void CollectionMergeOperator::pm_trace_end_records(std::string* new_value, size_t& i, std::ofstream& trace_file) const {
  while(i < new_value->size()) {
    trace_file << trace_record_serializer_(new_value->data(), i, fixed_record_len_);
    if (i + fixed_record_len_ < new_value->size()) {
      trace_file << ",";
    }
    i += fixed_record_len_;
  }
}

void CollectionMergeOperator::trace_exit(const char* const msg, const bool success) const {
  std::ofstream trace_file;
  trace_file.open (TRACE_FILE, std::ofstream::out | std::ofstream::app);
  trace_file << "## EXIT(success=" << std::boolalpha << success << "): " << msg << std::endl;
  trace_file << std::endl;
  trace_file.close();
}

}  // end ROCKSDB_NAMESPACE namespace
