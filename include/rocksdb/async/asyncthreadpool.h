//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

namespace rocksdb {
namespace async {

// Env depended implementation
class AsyncThreadPool {
protected:
  AsyncThreadPool() {}
public:
  virtual ~AsyncThreadPool() {}
  // Ensures that all outstanding items
  // are closed. Any async callbacks in progress
  // are waited on
  virtual void CloseThreadPool() = 0;
};

}
}
