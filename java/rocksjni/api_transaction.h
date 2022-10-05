#include <iostream>

#include "api_base.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"

template <class TDatabase>
class APITransaction : APIBase {
 public:
  std::shared_ptr<TDatabase> db;
  std::shared_ptr<ROCKSDB_NAMESPACE::Transaction> txn;

  APITransaction(std::shared_ptr<TDatabase> db,
                 std::shared_ptr<ROCKSDB_NAMESPACE::Transaction> txn)
      : db(db), txn(txn){};

  ROCKSDB_NAMESPACE::Transaction* operator->() const { return txn.get(); }

  std::shared_ptr<ROCKSDB_NAMESPACE::Transaction>& operator*() { return txn; }

  ROCKSDB_NAMESPACE::Transaction* get() const { return txn.get(); }

  std::vector<long> use_counts() {
    std::vector<long> vec;

    vec.push_back(db.use_count());
    vec.push_back(txn.use_count());

    return vec;
  }
};
