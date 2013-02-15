// Copyright (c) 2013 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_METRICS_INFO_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_METRICS_INFO_H_

#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include <stdint.h>

namespace leveldb {

class BlockMetrics;

// Returns true if the record pointed too by iter is hot according to the
// value stored in metrics_db.
//
// *block_metrics_store temporarily stores a BlockMetrics instance.  At the
// end, when the application no longer plans to call IsRecordHot(), if
// *block_metrics_store is not NULL then this value must be deleted.
//
// REQUIRES: neither iter, metrics_db nor block_metrics_store is NULL.
bool IsRecordHot(const Iterator* iter, DB* metrics_db,
                 const ReadOptions& metrics_read_opts,
                 BlockMetrics** block_metrics_store);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_METRICS_INFO_H_
