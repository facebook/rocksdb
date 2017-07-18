//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

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
