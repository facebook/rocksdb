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
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_base.h"
#include "utilities/transactions/transaction_util.h"

namespace rocksdb {

class WritePreparedTxnDB;

// This impl could write to DB also uncomitted data and then later tell apart
// committed data from uncomitted data. Uncommitted data could be after the
// Prepare phase in 2PC (WritePreparedTxn) or before that
// (WriteUnpreparedTxnImpl).
class WritePreparedTxn : public PessimisticTransaction {
 public:
  WritePreparedTxn(WritePreparedTxnDB* db, const WriteOptions& write_options,
                   const TransactionOptions& txn_options);

  virtual ~WritePreparedTxn() {}

  Status CommitBatch(WriteBatch* batch) override;

  Status Rollback() override;

 private:
  Status PrepareInternal() override;

  Status CommitWithoutPrepareInternal() override;

  Status CommitInternal() override;

  // TODO(myabandeh): verify that the current impl work with values being
  // written with prepare sequence number too.
  // Status ValidateSnapshot(ColumnFamilyHandle* column_family, const Slice&
  // key,
  //                        SequenceNumber prev_seqno, SequenceNumber*
  //                        new_seqno);

  // No copying allowed
  WritePreparedTxn(const WritePreparedTxn&);
  void operator=(const WritePreparedTxn&);

  WritePreparedTxnDB* wpt_db_;
  uint64_t prepare_seq_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
