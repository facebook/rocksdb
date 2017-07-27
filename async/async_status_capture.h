//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once

#include "rocksdb/status.h"

namespace rocksdb {
namespace async {

// This is a helper class to
// hold the async state of the async request
// We want to capture and hold to make sure
// we know if any of the async requests was really
// invoked async at least once (if it implements more
// than one async operation). It is possible, though
// unlikely that all of the operations for a given instance
// of the request complete asynchronously either because
// IO completes sync or all of the data requests are actually
// satisfied from cache in which case there is not a need
// for IO at all. In such a case, we do not want to call
// a callback and self-destroy. The results of the particular
// operation should be retrieved from the Request Context itself
// The context must be destroyed by the caller when no longer needed
// and the next code being executed must continue sync.
class AsyncStatusCapture {
  bool async_;
 public:

  AsyncStatusCapture() : async_(false) {}
  explicit
  AsyncStatusCapture(bool a) : async_(a) {}
  explicit
  AsyncStatusCapture(const Status& s) : async_(false) {
    if (s.async()) {
      async_ = true;
    }
  }

  AsyncStatusCapture(const AsyncStatusCapture&) = default;
  AsyncStatusCapture& operator=(const AsyncStatusCapture&) = default;
  AsyncStatusCapture(AsyncStatusCapture&&) = default;
  AsyncStatusCapture& operator=(AsyncStatusCapture&&) = default;

  bool async() const { return async_; }
  void async(bool a) { async_ = a; }
  void async(const Status& s) { 
    if (s.async()) {
      async_ = true;
    }
  }
  void reset_async(const Status& s) {
    async_ = s.async();
  }
};

}
}
