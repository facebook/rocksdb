// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_MERGE_OPERATOR_H_
#define STORAGE_LEVELDB_INCLUDE_MERGE_OPERATOR_H_

#include <string>

namespace leveldb {

class Slice;
class Logger;

// The Merge Operator interface.
// Client needs to provide an object implementing this interface if Merge
// operation is accessed.
// Essentially, MergeOperator specifies the SEMANTICS of a merge, which only
// client knows. It could be numeric addition, list append, string
// concatenation, ... , anything.
// The library, on the other hand, is concerned with the exercise of this
// interface, at the right time (during get, iteration, compaction...)
// Note that, even though in principle we don't require any special property
// of the merge operator, the current rocksdb compaction order does imply that
// an associative operator could be exercised more naturally (and more
// efficiently).
//
// Refer to my_test.cc for an example of implementation
//
class MergeOperator {
 public:
  virtual ~MergeOperator() {}

  // Gives the client a way to express the read -> modify -> write semantics
  // key:      (IN)    The key that's associated with this merge operation.
  //                   Client could multiplex the merge operator based on it
  //                   if the key space is partitioned and different subspaces
  //                   refer to different types of data which have different
  //                   merge operation semantics
  // existing: (IN)    null indicates that the key does not exist before this op
  // value:    (IN)    The passed-in merge operand value (when Merge is issued)
  // new_value:(OUT)   Client is responsible for filling the merge result here
  // logger:   (IN)    Client could use this to log errors during merge.
  //
  // Note: Merge does not return anything to indicate if a merge is successful
  //       or not.
  // Rationale: If a merge failed due to, say de-serialization error, we still
  //            need to define a consistent merge result. Should we throw away
  //            the existing value? the merge operand? Or reset the merged value
  //            to sth? The rocksdb library is not in a position to make the
  //            right choice. On the other hand, client knows exactly what
  //            happened during Merge, thus is able to make the best decision.
  //            Just save the final decision in new_value. logger is passed in,
  //            in case client wants to leave a trace of what went wrong.
  virtual void Merge(const Slice& key,
                     const Slice* existing_value,
                     const Slice& value,
                     std::string* new_value,
                     Logger* logger) const = 0;


  // The name of the MergeOperator.  Used to check for MergeOperator
  // mismatches (i.e., a DB created with one MergeOperator is
  // accessed using a different MergeOperator)
  // TODO: the name is currently not stored persistently and thus
  //       no checking is enforced. Client is responsible for providing
  //       consistent MergeOperator between DB opens.
  virtual const char* Name() const = 0;

};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_MERGE_OPERATOR_H_
