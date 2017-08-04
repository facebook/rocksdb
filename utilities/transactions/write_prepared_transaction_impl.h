// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <algorithm>
#include <atomic>
#include <mutex>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/write_callback.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/autovector.h"
#include "utilities/transactions/transaction_base.h"
#include "utilities/transactions/transaction_impl.h"
#include "utilities/transactions/transaction_util.h"

namespace rocksdb {

class TransactionDBImpl;

// This impl could write to DB also uncomitted data and then later tell apart
// committed data from uncomitted data. Uncommitted data could be after the
// Prepare phase in 2PC (WritePreparedTxnImpl) or before that
// (WriteUnpreparedTxnImpl).
class WritePreparedTxnImpl : public PessimisticTxn {
 public:
  WritePreparedTxnImpl(TransactionDB* db, const WriteOptions& write_options,
                       const TransactionOptions& txn_options);

  virtual ~WritePreparedTxnImpl() {}

  Status Prepare() override;

  Status Commit() override;

  Status CommitBatch(WriteBatch* batch) override;

  Status Rollback() override;

 private:
  // TODO(myabandeh): verify that the current impl work with values being
  // written with prepare sequence number too.
  // Status ValidateSnapshot(ColumnFamilyHandle* column_family, const Slice&
  // key,
  //                        SequenceNumber prev_seqno, SequenceNumber*
  //                        new_seqno);

  // No copying allowed
  WritePreparedTxnImpl(const WritePreparedTxnImpl&);
  void operator=(const WritePreparedTxnImpl&);
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
