//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <string>

#include "rocksdb/comparator.h"
#include "rocksdb/perf_context.h"

namespace rocksdb {

class InternalKey;
class ParsedInternalKey;

// Returns the user key portion of an internal key.
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
class InternalKeyComparator : public Comparator {
 private:
  const Comparator* user_comparator_;
  std::string name_;
 public:
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c),
    name_("rocksdb.InternalKeyComparator:" +
          std::string(user_comparator_->Name())) {
  }
  virtual ~InternalKeyComparator() {}

  virtual const char* Name() const override { return name_.c_str(); }

  // Have the method definition in public header in order to make it
  // able to be inlinded by compiler.
  virtual int Compare(const Slice& akey, const Slice& bkey) const override {
    // Order by:
    //    increasing user key (according to user-supplied comparator)
    //    decreasing sequence number
    //    decreasing type (though sequence# should be enough to disambiguate)
    int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
    PERF_COUNTER_ADD(user_key_comparison_count, 1);
    if (r == 0) {
      const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
      const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
      if (anum > bnum) {
        r = -1;
      } else if (anum < bnum) {
        r = +1;
      }
    }
    return r;
  }

  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const override;
  virtual void FindShortSuccessor(std::string* key) const override;

  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const InternalKey& a, const InternalKey& b) const;
  int Compare(const ParsedInternalKey& a, const ParsedInternalKey& b) const;
};

}  // namespace rocksdb
