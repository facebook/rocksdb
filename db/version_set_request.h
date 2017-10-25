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

#include "db/table_cache_request.h"
#include "db/pinned_iterators_manager.h"
#include "db/version_set.h"
#include "rocksdb/async/callables.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "table/get_context.h"

#include <type_traits>

namespace rocksdb {

class LookupKey;
class MergeContext;
class RangeDelAggregator;
struct ReadOptions;
class Version;

namespace async {

class VersionSetGetContext : private AsyncStatusCapture {
 public:

  // The callback will accept
  //   Status, bool value_found, bool key_exists SequenceNumber
  using
  Callback = async::Callable<Status, const Status&>;

  VersionSetGetContext(const VersionSetGetContext&) = delete;
  VersionSetGetContext& operator=(const VersionSetGetContext&) = delete;

  // The return status is the same as output parameter
  static Status Get(
    Version* version,
    const ReadOptions& read_options, const LookupKey& k,
    PinnableSlice* value, Status* status,
    MergeContext* merge_context,
    RangeDelAggregator* range_del_agg, bool* value_found = nullptr,
    bool* key_exists = nullptr, SequenceNumber* seq = nullptr) {

    Slice ikey = k.internal_key();
    Slice user_key = k.user_key();
    assert(status->ok() || status->IsMergeInProgress());

    const Callback empty_cb;
    VersionSetGetContext context(empty_cb, version, read_options, ikey, user_key,
                                 value, merge_context, key_exists);
    context.InitGetState(status->ok() ? GetContext::kNotFound : GetContext::kMerge,
                         range_del_agg, value_found, seq);
    context.InitFilePicker();
    *status = context.StartGetIteratation();
    return *status;
  }

  // In async version value of status is not returned in the
  // output parameter, only via a return value
  static Status RequestGet(
    const Callback& cb, Version* version,
    const ReadOptions& read_options, const LookupKey& k,
    PinnableSlice* value, Status* status,
    MergeContext* merge_context,
    RangeDelAggregator* range_del_agg, bool* value_found = nullptr,
    bool* key_exists = nullptr, SequenceNumber* seq = nullptr) {

    Slice ikey = k.internal_key();
    Slice user_key = k.user_key();

    assert(status->ok() || status->IsMergeInProgress());

    std::unique_ptr<VersionSetGetContext> context(new VersionSetGetContext(cb, version,
      read_options, ikey, user_key, value, merge_context, key_exists));

    context->InitGetState(status->ok() ? GetContext::kNotFound :
                          GetContext::kMerge,
                          range_del_agg, value_found, seq);
    context->InitFilePicker();
    Status s = context->StartGetIteratation();
    if (s.IsIOPending()) {
      context.release();
    }
    return s;
  }

  VersionSetGetContext(const Callback& cb, Version* version,
                       const ReadOptions& read_options,
                       const Slice& ikey, const Slice& user_key,
                       PinnableSlice* value,
                       MergeContext* merge_context,
                       bool* key_exists) :
    cb_(cb), version_(version), read_options_(&read_options),
    ikey_(ikey), user_key_(user_key),
    value_(value),
    merge_context_(merge_context),
    key_exists_(key_exists),
    pinned_iters_mgr_() {

    SetKeyExists(true);
  }

  ~VersionSetGetContext() {
    file_picker()->~FilePicker();
    get_context()->~GetContext();
  }

 private:

  using
  FilePicker = versionset_detail::FilePicker;

  void SetKeyExists(bool v) {
    if (key_exists_) {
      *key_exists_ = v;
    }
  }

  void InitGetState(GetContext::GetState init_state,
                    RangeDelAggregator* range_del_agg, bool* value_found,
                    SequenceNumber* seq) {

    PinnedIteratorsManager* pinned_mgr(version_->merge_operator_ ?
                                       &pinned_iters_mgr_ : nullptr);

    new (&get_context_) GetContext(version_->user_comparator(),
                                   version_->merge_operator(),
                                   version_->info_log_, version_->db_statistics_,
                                   init_state, user_key_, value_, value_found, merge_context_, range_del_agg,
                                   version_->env_, seq,
                                   pinned_mgr);

    // Pin blocks that we read to hold merge operands
    if (version_->merge_operator_) {
      pinned_iters_mgr_.StartPinning();
    }
  }

  void InitFilePicker() {
    VersionStorageInfo& storage_info = version_->storage_info_;
    new (&fp_) FilePicker(storage_info.files_, user_key_, ikey_,
                          &storage_info.level_files_brief_,
                          storage_info.num_non_empty_levels_, &storage_info.file_indexer_,
                          version_->user_comparator(), version_->internal_comparator());
  }

  GetContext* get_context() {
    return reinterpret_cast<GetContext*>(&get_context_);
  }

  FilePicker* file_picker() {
    return reinterpret_cast<FilePicker*>(&fp_);
  }

  Status StartGetIteratation() {
    Status s;
    FdWithKeyRange* f = file_picker()->GetNextFile();
    if (f != nullptr) {
      s = CacheGet(f);
      if (!s.IsIOPending()) {
        s = IterateFilePicker(s);
      }
    } else {
      s = HandleMerge();
      s = OnComplete(s);
    }
    return s;
  }


  Status CacheGet(FdWithKeyRange* f) {
    assert(f != nullptr);
    Status s;
    FilePicker* fp = file_picker();
    if (cb_) {
      CallableFactory<VersionSetGetContext, Status, const Status&> fac(this);
      auto on_table_cache_get =
        fac.GetCallable<&VersionSetGetContext::IterateFilePicker>();
      s = TableCacheGetContext::RequestGet(on_table_cache_get,
                                           version_->table_cache_,
                                           *read_options_, *version_->internal_comparator(),
                                           f->fd, ikey_, get_context(),
                                           version_->cfd_->internal_stats()->GetFileReadHist(fp->GetHitFileLevel()),
                                           version_->IsFilterSkipped(static_cast<int>(fp->GetHitFileLevel()),
                                               fp->IsHitFileLastInLevel()));
      if (s.IsIOPending()) {
        return s;
      }
    } else {
      s = TableCacheGetContext::Get(version_->table_cache_,
                                    *read_options_, *version_->internal_comparator(), f->fd, ikey_, get_context(),
                                    version_->cfd_->internal_stats()->GetFileReadHist(fp->GetHitFileLevel()),
                                    version_->IsFilterSkipped(static_cast<int>(fp->GetHitFileLevel()),
                                        fp->IsHitFileLastInLevel()));
    }
    return s;
  }

  Status IterateFilePicker(const Status&);

  Status HandleMerge() {
    Status s;
    // Check if the last status was merge
    if (GetContext::kMerge == get_context()->State()) {
      if (!version_->merge_operator_) {
        s = Status::InvalidArgument(
              "merge_operator is not properly initialized.");
      } else {
        // merge_operands are in saver and we hit the beginning of the key history
        // do a final merge of nullptr and operands;
        std::string* str_value = (value_ != nullptr) ? value_->GetSelf() : nullptr;
        s = MergeHelper::TimedFullMerge(
              version_->merge_operator_, user_key_, nullptr, merge_context_->GetOperands(),
              str_value, version_->info_log_, version_->db_statistics_, version_->env_,
              nullptr /* result_operand */, true);
        if (LIKELY(value_ != nullptr)) {
          value_->PinSelf();
        }
      }
    } else {
      SetKeyExists(false);
      s = Status::NotFound(); // Use an empty error message for speed
    }
    return s;
  }

  Status OnComplete(const Status& status) {
    if (cb_ && async()) {
      Status s(status);
      s.async(true);
      cb_.Invoke(s);
      delete this;
      return status;
    }
    return status;
  }

  Callback               cb_;
  Version*               version_;
  const ReadOptions*     read_options_;
  Slice                  ikey_;
  Slice                  user_key_;
  PinnableSlice*         value_;
  MergeContext*          merge_context_;
  bool*                  key_exists_;

  PinnedIteratorsManager pinned_iters_mgr_;
  std::aligned_storage<sizeof(GetContext)>::type get_context_;
  std::aligned_storage<sizeof(FilePicker)>::type fp_;
};

}
}

