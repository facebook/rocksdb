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

  void check(std::string message) {
    std::cout << " APITransaction::check(); " << message << " ";
    std::cout << " db.use_count() " << db.use_count() << "; ";
    std::cout << " cfh.use_count() " << txn.use_count();
    std::cout << std::endl;
  }
};
