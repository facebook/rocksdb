#ifndef MERGE_HELPER_H
#define MERGE_HELPER_H

#include "db/dbformat.h"
#include "leveldb/slice.h"
#include <string>

namespace leveldb {

class Comparator;
class Iterator;
class Logger;
class MergeOperator;

class MergeHelper {
 public:
  MergeHelper(const Comparator* user_comparator,
              const MergeOperator* user_merge_operator,
              Logger* logger,
              bool assert_valid_internal_key)
      : user_comparator_(user_comparator),
        user_merge_operator_(user_merge_operator),
        logger_(logger),
        assert_valid_internal_key_(assert_valid_internal_key) {}

  // Merge entries until we hit
  //     - a corrupted key
  //     - a Put/Delete,
  //     - a different user key,
  //     - a specific sequence number (snapshot boundary),
  //  or - the end of iteration
  // iter: (IN)  points to the first merge type entry
  //       (OUT) points to the first entry not included in the merge process
  // stop_before: (IN) a sequence number that merge should not cross.
  //                   0 means no restriction
  // at_bottom:   (IN) true if the iterator covers the bottem level, which means
  //                   we could reach the start of the history of this user key.
  void MergeUntil(Iterator* iter, SequenceNumber stop_before = 0,
                  bool at_bottom = false);

  // Query the merge result
  // These are valid until the next MergeUtil call
  // IMPORTANT: the key type could change after the MergeUntil call.
  //            Put/Delete + Merge + ... + Merge => Put
  //            Merge + ... + Merge => Merge
  Slice key() { return Slice(key_); }
  Slice value() { return Slice(value_); }

 private:
  const Comparator* user_comparator_;
  const MergeOperator* user_merge_operator_;
  Logger* logger_;
  Iterator* iter_; // in: the internal iterator, positioned at the first merge entry
  bool assert_valid_internal_key_; // enforce no internal key corruption?

  // the scratch area that holds the result of MergeUntil
  // valid up to the next MergeUntil call
  std::string key_;
  std::string value_;
};

} // namespace leveldb

#endif
