#include "merge_helper.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/merge_operator.h"
#include <string>
#include <stdio.h>

namespace leveldb {

// PRE:  iter points to the first merge type entry
// POST: iter points to the first entry beyond the merge process (or the end)
//       key_, value_ are updated to reflect the merge result
void MergeHelper::MergeUntil(Iterator* iter, SequenceNumber stop_before,
                             bool at_bottom) {
  // get a copy of the internal key, before it's invalidated by iter->Next()
  key_.assign(iter->key().data(), iter->key().size());
  // we need to parse the internal key again as the parsed key is
  // backed by the internal key!
  ParsedInternalKey orig_ikey;
  // Assume no internal key corruption as it has been successfully parsed
  // by the caller.
  // TODO: determine a good alternative of assert (exception?)
  ParseInternalKey(key_, &orig_ikey);
  std::string operand(iter->value().data(), iter->value().size());

  bool hit_the_next_user_key = false;
  ParsedInternalKey ikey;
  for (iter->Next(); iter->Valid(); iter->Next()) {
    if (!ParseInternalKey(iter->key(), &ikey)) {
      // stop at corrupted key
      if (assert_valid_internal_key_) {
        assert(!"corrupted internal key is not expected");
      }
      break;
    }

    if (user_comparator_->Compare(ikey.user_key, orig_ikey.user_key) != 0) {
      // hit a different user key, stop right here
      hit_the_next_user_key = true;
      break;
    }

    if (stop_before && ikey.sequence <= stop_before) {
      // hit an entry that's visible by the previous snapshot, can't touch that
      break;
    }

    if (kTypeDeletion == ikey.type) {
      // hit a delete
      //   => merge nullptr with operand
      //   => change the entry type to kTypeValue
      // We are done!
      user_merge_operator_->Merge(ikey.user_key, nullptr, operand,
                                  &value_, logger_);
      orig_ikey.type = kTypeValue;
      UpdateInternalKey(&key_[0], key_.size(),
                        orig_ikey.sequence, orig_ikey.type);
      // move iter to the next entry
      iter->Next();
      return;
    }

    if (kTypeValue == ikey.type) {
      // hit a put
      //   => merge the put value with operand
      //   => change the entry type to kTypeValue
      // We are done!
      const Slice value = iter->value();
      user_merge_operator_->Merge(ikey.user_key, &value, Slice(operand),
                                  &value_, logger_);
      orig_ikey.type = kTypeValue;
      UpdateInternalKey(&key_[0], key_.size(),
                        orig_ikey.sequence, orig_ikey.type);
      // move iter to the next entry
      iter->Next();
      return;
    }

    if (kTypeMerge == ikey.type) {
      // hit a merge
      //   => merge the value with operand.
      //   => put the result back to operand and continue
      const Slice value = iter->value();
      user_merge_operator_->Merge(ikey.user_key, &value, operand,
                                  &value_, logger_);
      swap(value_, operand);
      continue;
    }
  }

  // We have seen the root history of this key if we are at the
  // bottem level and exhausted all internal keys of this user key
  // NOTE: !iter->Valid() does not necessarily mean we hit the
  // beginning of a user key, as versions of a user key might be
  // split into multiple files and some files might not be included
  // in the merge.
  bool seen_the_beginning = hit_the_next_user_key && at_bottom;

  if (seen_the_beginning) {
    // do a final merge with nullptr as the existing value and say
    // bye to the merge type (it's now converted to a Put)
    assert(kTypeMerge == orig_ikey.type);
    user_merge_operator_->Merge(orig_ikey.user_key, nullptr, operand,
                                &value_, logger_);
    orig_ikey.type = kTypeValue;
    UpdateInternalKey(&key_[0], key_.size(),
                      orig_ikey.sequence, orig_ikey.type);
  } else {
    swap(value_, operand);
  }
}

} // namespace leveldb
