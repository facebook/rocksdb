// Copyright 2008-present Facebook. All Rights Reserved.
#ifndef STORAGE_LEVELDB_INCLUDE_TRANSACTION_LOG_ITERATOR_H_
#define STORAGE_LEVELDB_INCLUDE_TRANSACTION_LOG_ITERATOR_H_

#include "leveldb/status.h"
#include "leveldb/write_batch.h"

namespace leveldb {


//  A TransactionLogIterator is used to iterate over the Transaction's in a db.
class TransactionLogIterator {
 public:
  TransactionLogIterator() {}
  virtual ~TransactionLogIterator() {}

  // An iterator is either positioned at a WriteBatch or not valid.
  // This method returns true if the iterator is valid.
  virtual bool Valid() = 0;

  // Moves the iterator to the next WriteBatch.
  // REQUIRES: Valid() to be true.
  virtual void Next() = 0;

  // Return's ok if the iterator is in a valid stated.
  // Return the Error Status when the iterator is not Valid.
  virtual Status status() = 0;

  // If valid return's the current write_batch.
  virtual void GetBatch(WriteBatch* batch) = 0;
};
} //  namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TRANSACTION_LOG_ITERATOR_H_
