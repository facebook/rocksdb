//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifdef XFUNC
#include <string>
#include "db/db_impl.h"
#include "db/managed_iterator.h"
#include "rocksdb/options.h"
#include "util/xfunc.h"


namespace rocksdb {

std::string XFuncPoint::xfunc_test_;
bool XFuncPoint::initialized_ = false;
bool XFuncPoint::enabled_ = false;
int XFuncPoint::skip_policy_ = 0;

void GetXFTestOptions(Options* options, int skip_policy) {
  if (XFuncPoint::Check("inplace_lock_test") &&
      (!(skip_policy & kSkipNoSnapshot))) {
    options->inplace_update_support = true;
  }
}

void xf_manage_release(ManagedIterator* iter) {
  if (!(XFuncPoint::GetSkip() & kSkipNoPrefix)) {
    iter->ReleaseIter(false);
  }
}

void xf_manage_options(ReadOptions* read_options) {
  if (!XFuncPoint::Check("managed_xftest_dropold") &&
      (!XFuncPoint::Check("managed_xftest_release"))) {
    return;
  }
  read_options->managed = true;
}

void xf_manage_new(DBImpl* db, ReadOptions* read_options,
                   bool is_snapshot_supported) {
  if ((!XFuncPoint::Check("managed_xftest_dropold") &&
       (!XFuncPoint::Check("managed_xftest_release"))) ||
      (!read_options->managed)) {
    return;
  }
  if ((!read_options->tailing) && (read_options->snapshot == nullptr) &&
      (!is_snapshot_supported)) {
    read_options->managed = false;
    return;
  }
  if (db->GetOptions().prefix_extractor != nullptr) {
    if (strcmp(db->GetOptions().table_factory.get()->Name(), "PlainTable")) {
      if (!(XFuncPoint::GetSkip() & kSkipNoPrefix)) {
        read_options->total_order_seek = true;
      }
    } else {
      read_options->managed = false;
    }
  }
}

void xf_manage_create(ManagedIterator* iter) { iter->SetDropOld(false); }

}  // namespace rocksdb

#endif  // XFUNC
