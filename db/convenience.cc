//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef ROCKSDB_LITE

#include "rocksdb/convenience.h"

#include "db/db_impl.h"

namespace rocksdb {

void CancelAllBackgroundWork(DB* db, bool wait) {
  (dynamic_cast<DBImpl*>(db->GetRootDB()))->CancelAllBackgroundWork(wait);
}

Status DeleteFilesInRange(DB* db, ColumnFamilyHandle* column_family,
                          const Slice* begin, const Slice* end) {
  return (dynamic_cast<DBImpl*>(db->GetRootDB()))
      ->DeleteFilesInRange(column_family, begin, end);
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
