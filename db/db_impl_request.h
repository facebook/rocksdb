//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#pragma once

#include "async/async_status_capture.h"
#include "db/merge_context.h"
#include "db/range_del_aggregator.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/async/callables.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

#include "util/stop_watch.h"


#include <string>
#include <type_traits>

namespace rocksdb {

class ColumnFamilyData;
class DB;
class DBImpl;
class InternalKeyComparator;
struct ReadOptions;
struct SuperVersion;

namespace async {

// DB::Get() async implementation for DBImpl
class DBImplGetContext : private AsyncStatusCapture {
public:
  using
  Callback = Callable<Status, const Status&>;

  DBImplGetContext(const DBImplGetContext&) = delete;
  DBImplGetContext& operator=(const DBImplGetContext&) = delete;

  static Status Get(DB* db, const ReadOptions& read_options,
    ColumnFamilyHandle* column_family, const Slice& key,
    PinnableSlice* pinnable_input, std::string* value,
    bool* value_found = nullptr) {
    assert(!pinnable_input || !value);
    const Callback empty_cb;
    DBImplGetContext context(empty_cb, db, read_options, key, value,
      pinnable_input, column_family, value_found);
    return context.GetImpl();
  }

  static Status RequestGet(const Callback& cb, DB* db,
    const ReadOptions& read_options,
    ColumnFamilyHandle* column_family, const Slice& key,
    PinnableSlice* pinnable_input, std::string* value, 
    bool* value_found = nullptr) {
    assert(!pinnable_input || !value);
    std::unique_ptr<DBImplGetContext> context(new DBImplGetContext(cb, db,
      read_options, key, value, pinnable_input, column_family, value_found));
    Status s = context->GetImpl();
    if (s.IsIOPending()) {
      context.release();
    }
    return s;
  }

  ~DBImplGetContext() {
    GetRangeDel().~RangeDelAggregator();
    DestroyPinnableSlice();
  }

private:

  DBImplGetContext(const Callback& cb, DB* db, const ReadOptions& ro,
        const Slice& key, std::string* value, PinnableSlice* pinnable_input,
        ColumnFamilyHandle* cfd,
        bool* value_found);

  void InitRangeDelAggreagator(const InternalKeyComparator& icomp,
                               SequenceNumber snapshot) {
    new (&range_del_agg_) RangeDelAggregator(icomp, snapshot);
  }

  RangeDelAggregator& GetRangeDel() {
    return *reinterpret_cast<RangeDelAggregator*>(&range_del_agg_);
  }

  void InitPinnableSlice(PinnableSlice* pinnable_input, std::string* value) {
    if (pinnable_input) {
      pinnable_val_input_ = pinnable_input;
    } else {
      assert(value);
      new (&pinnable_val_) PinnableSlice(value);
    }
  }

  void DestroyPinnableSlice() {
    if (!pinnable_val_input_) {
      reinterpret_cast<PinnableSlice*>(&pinnable_val_)->~PinnableSlice();
    }
  }

  PinnableSlice& GetPinnable() {
    if (pinnable_val_input_) {
      return *pinnable_val_input_;
    }
    return *reinterpret_cast<PinnableSlice*>(&pinnable_val_);
  }

  Status GetImpl();

  Status OnGetComplete(const Status&);

  Status OnComplete(const Status&);

  Callback            cb_;
  DBImpl*             db_impl_;
  ReadOptions         read_options_;
  Slice               key_;
  std::string*        value_;
  bool*               value_found_;

  ColumnFamilyData*   cfd_;
  SuperVersion*       sv_;
  StopWatch           sw_;
  PERF_METER_DECL(get_from_output_files_time);
  MergeContext        merge_context_;
  PinnableSlice*      pinnable_val_input_; // External for sync
  std::aligned_storage<sizeof(PinnableSlice)>::type pinnable_val_;
  std::aligned_storage<sizeof(RangeDelAggregator)>::type range_del_agg_;
};

}
}

