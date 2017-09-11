// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/types.h"

namespace rocksdb {

class ReadCallback {
 public:
  virtual ~ReadCallback() {}

  // Will be called to see if the seq number accepted; if not it moves on to the
  // next seq number.
  virtual bool IsCommitted(SequenceNumber seq) = 0;
};

}  //  namespace rocksdb
