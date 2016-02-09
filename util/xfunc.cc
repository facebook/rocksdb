//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifdef XFUNC
#include "util/xfunc.h"

#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/write_batch.h"

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

void xf_manage_options(ReadOptions* read_options) {
  if (!XFuncPoint::Check("managed_xftest_dropold") &&
      (!XFuncPoint::Check("managed_xftest_release"))) {
    return;
  }
  read_options->managed = true;
}

void xf_transaction_set_memtable_history(
    int32_t* max_write_buffer_number_to_maintain) {
  *max_write_buffer_number_to_maintain = 10;
}

void xf_transaction_clear_memtable_history(
    int32_t* max_write_buffer_number_to_maintain) {
  *max_write_buffer_number_to_maintain = 0;
}

}  // namespace rocksdb

#endif  // XFUNC
