//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include "rocksdb/options.h"
#include "util/xfunc.h"

#ifdef XFUNC

namespace rocksdb {

std::string XFuncPoint::xfunc_test_;
bool XFuncPoint::initialized_ = false;
bool XFuncPoint::enabled_ = false;

void GetXFTestOptions(Options* options, int skip_policy) {
  if (XFuncPoint::Check("inplace_lock_test") &&
      (!(skip_policy & kSkipNoSnapshot))) {
    options->inplace_update_support = true;
  }
}

}  // namespace rocksdb

#endif  // XFUNC
