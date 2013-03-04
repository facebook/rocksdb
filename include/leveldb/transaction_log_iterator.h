// Copyright 2008-present Facebook. All Rights Reserved.
#ifndef STORAGE_LEVELDB_INCLUDE_TRANSACTION_LOG_ITERATOR_H_
#define STORAGE_LEVELDB_INCLUDE_TRANSACTION_LOG_ITERATOR_H_

#include "leveldb/status.h"
#include "leveldb/write_batch.h"

namespace leveldb {


struct BatchResult {
  SequenceNumber sequence;
  std::unique_ptr<WriteBatch> writeBatchPtr;
};

//  A TransactionLogIterator is used to iterate over the Transaction's in a db.
class TransactionLogIterator {
 public:
  TransactionLogIterator() {}
  virtual ~TransactionLogIterator() {}

  // An iterator is either positioned at a WriteBatch or not valid.
  // This method returns true if the iterator is valid.
  // Can read data from a valid iterator.
  virtual bool Valid() = 0;

  // Moves the iterator to the next WriteBatch.
  // REQUIRES: Valid() to be true.
  virtual void Next() = 0;

  // Return's ok if the iterator is valid.
  // Return the Error when something has gone wrong.
  virtual Status status() = 0;

  // If valid return's the current write_batch and the sequence number of the
  // latest transaction contained in the batch.
  // ONLY use if Valid() is true and status() is OK.
  virtual BatchResult GetBatch() = 0;
};
} //  namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TRANSACTION_LOG_ITERATOR_H_
