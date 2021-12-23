#include <string>
#include <vector>

#include "rocksdb/transaction_log.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE

class WalDump {
 public:
  WalDumper() {}
  ~WalDumper() {}

  static Status CreateWalIter(std::string dbname,
                              SequenceNumber start_seq,
                              SequenceNumber end_seq,
                              std::unique_ptr<TransactionLogIterator>* iter,
                              const TransactionLogIterator::ReadOptions& read_options =
                              TransactionLogIterator::ReadOptions());

};


#endif  // ROCKSDB_LITE
}  // namespace rocksdb
