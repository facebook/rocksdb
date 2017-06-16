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

#include "db/column_family.h"
#include "db/internal_stats.h"
#include "db/merge_helper.h"
#include "db/table_cache_request.h"
#include "db/version_set_request.h"
#include "db/version_edit.h"

namespace rocksdb {
namespace async {

Status VersionSetGetContext::Get(Version* version,
    const ReadOptions & read_options, const LookupKey & k, PinnableSlice* value,
    Status * status, MergeContext * merge_context,
    RangeDelAggregator * range_del_agg, bool* value_found, bool * key_exists,
    SequenceNumber * seq) {

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

Status VersionSetGetContext::RequestGet(const Callback& cb, Version* version,
    const ReadOptions & read_options, const LookupKey & k, PinnableSlice* value,
    Status* status, MergeContext* merge_context,
    RangeDelAggregator* range_del_agg, bool* value_found, bool* key_exists,
    SequenceNumber* seq) {

  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();

  assert(status->ok() || status->IsMergeInProgress());

  std::unique_ptr<VersionSetGetContext> context(new VersionSetGetContext(cb,
    version, read_options, ikey, user_key, value, merge_context, key_exists));

  context->InitGetState(status->ok() ? GetContext::kNotFound : GetContext::kMerge,
    range_del_agg, value_found, seq);

  context->InitFilePicker();

  Status s = context->StartGetIteratation();

  if (s.IsIOPending()) {
    context.release();
  }
  return s;
}


Status VersionSetGetContext::CacheGet(FdWithKeyRange* f) {

  assert(f != nullptr);

  Status s;
  FilePicker* fp = file_picker();

  if (cb_) {
    CallableFactory<VersionSetGetContext, Status, const Status&> fac(this);
    auto on_table_cache_get = fac.GetCallable<&VersionSetGetContext::IterateFilePicker>();

    s = TableCacheGetContext::RequestGet(on_table_cache_get, version_->table_cache_,
      *read_options_, *version_->internal_comparator(), f->fd, ikey_, get_context(),
      version_->cfd_->internal_stats()->GetFileReadHist(fp->GetHitFileLevel()),
      version_->IsFilterSkipped(static_cast<int>(fp->GetHitFileLevel()),
        fp->IsHitFileLastInLevel()));

    if (s.IsIOPending()) {
      return s;
    }

  } else {
    s = TableCacheGetContext::Get( version_->table_cache_,
      *read_options_, *version_->internal_comparator(), f->fd, ikey_, get_context(),
      version_->cfd_->internal_stats()->GetFileReadHist(fp->GetHitFileLevel()),
      version_->IsFilterSkipped(static_cast<int>(fp->GetHitFileLevel()),
        fp->IsHitFileLastInLevel()));
  }

  return s;
}

Status VersionSetGetContext::IterateFilePicker(const Status& status) {
  async(status);

  Status s(status);
  bool done = false;
  while (s.ok()) {

    switch (get_context()->State()) {
    case GetContext::kNotFound:
      // Keep searching in other files
      break;
    case GetContext::kFound: {
      unsigned int hit_level = file_picker()->GetHitFileLevel();
      if (hit_level == 0) {
        RecordTick(version_->db_statistics_, GET_HIT_L0);
      } else if (hit_level == 1) {
        RecordTick(version_->db_statistics_, GET_HIT_L1);
      } else if (hit_level >= 2) {
        RecordTick(version_->db_statistics_, GET_HIT_L2_AND_UP);
      }
      done = true;
    }
      break;
    case GetContext::kDeleted:
      // Use empty error message for speed
      s = Status::NotFound();
      done = true;
      break;
    case GetContext::kCorrupt:
      s = Status::Corruption("corrupted key for ", user_key_);
      done = true;
      break;
    case GetContext::kMerge:
      break;
    }

    if (done) {
      break;
    }

    FdWithKeyRange* f = file_picker()->GetNextFile();

    if (f == nullptr) {
      s = HandleMerge();
      break;
    }

    s = CacheGet(f);
    if (s.IsIOPending()) {
      return s;
    }
  }

  return OnComplete(s);
}

Status VersionSetGetContext::HandleMerge() {

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
        str_value, version_->info_log_, version_->db_statistics_, version_->env_);
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

Status VersionSetGetContext::OnComplete(const Status& status) {

  if (cb_ && async()) {

    ROCKS_LOG_DEBUG(
      version_->info_log_,
      "TableCacheNewIteratorContext async completion: %s",
      status.ToString().c_str());

    Status s(status);
    s.async(true);
    cb_.Invoke(s);
    delete this;
    return s;
  }

  ROCKS_LOG_DEBUG(
    version_->info_log_,
    "TableCacheNewIteratorContext sync completion: %s",
    status.ToString().c_str());

  return status;
}

}
}
