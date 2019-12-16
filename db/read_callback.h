// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/types.h"

namespace rocksdb {

class ReadCallback {
 public:
  ReadCallback(SequenceNumber last_visible_seq)
      : max_visible_seq_(last_visible_seq) {}
  ReadCallback(SequenceNumber last_visible_seq, SequenceNumber min_uncommitted)
      : max_visible_seq_(last_visible_seq), min_uncommitted_(min_uncommitted) {}

  virtual ~ReadCallback() {}

  // Will be called to see if the seq number visible; if not it moves on to
  // the next seq number.
  virtual bool IsVisibleFullCheck(SequenceNumber seq) = 0;

  inline bool IsVisible(SequenceNumber seq) {
    assert(min_uncommitted_ > 0);
    assert(min_uncommitted_ >= kMinUnCommittedSeq);
    if (seq < min_uncommitted_) {  // handles seq == 0 as well
      assert(seq <= max_visible_seq_);
      return true;
    } else if (max_visible_seq_ < seq) {
      assert(seq != 0);
      return false;
    } else {
      assert(seq != 0);  // already handled in the first if-then clause
      return IsVisibleFullCheck(seq);
    }
  }

  inline SequenceNumber max_visible_seq() { return max_visible_seq_; }

  // Refresh to a more recent visible seq
  virtual void Refresh(SequenceNumber seq) { max_visible_seq_ = seq; }

 protected:
  // The max visible seq, it is usually the snapshot but could be larger if
  // transaction has its own writes written to db.
  SequenceNumber max_visible_seq_ = kMaxSequenceNumber;
  // Any seq less than min_uncommitted_ is committed.
  const SequenceNumber min_uncommitted_ = kMinUnCommittedSeq;
};

}  //  namespace rocksdb
