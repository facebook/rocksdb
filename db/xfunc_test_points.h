//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "db/db_impl.h"
#include "db/managed_iterator.h"
#include "db/write_callback.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"
#include "util/xfunc.h"

namespace rocksdb {

#ifdef XFUNC

// DB-specific test points for the cross-functional test framework (see
// util/xfunc.h).
void xf_manage_release(ManagedIterator* iter);
void xf_manage_create(ManagedIterator* iter);
void xf_manage_new(DBImpl* db, ReadOptions* readoptions,
                   bool is_snapshot_supported);
void xf_transaction_write(const WriteOptions& write_options,
                          const DBOptions& db_options,
                          class WriteBatch* my_batch,
                          class WriteCallback* callback, DBImpl* db_impl,
                          Status* success, bool* write_attempted);

#endif  // XFUNC

}  // namespace rocksdb
