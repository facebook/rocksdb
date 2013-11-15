// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.

#ifndef STORAGE_ROCKSDB_INCLUDE_WRITE_BATCH_H_
#define STORAGE_ROCKSDB_INCLUDE_WRITE_BATCH_H_

#include <string>
#include "rocksdb/status.h"

namespace rocksdb {

class Slice;
struct SliceParts;

class WriteBatch {
 public:
  WriteBatch();
  ~WriteBatch();

  // Store the mapping "key->value" in the database.
  void Put(const Slice& key, const Slice& value);

  // Variant of Put() that gathers output like writev(2).  The key and value
  // that will be written to the database are concatentations of arrays of
  // slices.
  void Put(const SliceParts& key, const SliceParts& value);

  // Merge "value" with the existing value of "key" in the database.
  // "key->merge(existing, value)"
  void Merge(const Slice& key, const Slice& value);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  void Delete(const Slice& key);

  // Append a blob of arbitrary size to the records in this batch. The blob will
  // be stored in the transaction log but not in any other file. In particular,
  // it will not be persisted to the SST files. When iterating over this
  // WriteBatch, WriteBatch::Handler::LogData will be called with the contents
  // of the blob as it is encountered. Blobs, puts, deletes, and merges will be
  // encountered in the same order in thich they were inserted. The blob will
  // NOT consume sequence number(s) and will NOT increase the count of the batch
  //
  // Example application: add timestamps to the transaction log for use in
  // replication.
  void PutLogData(const Slice& blob);

  // Clear all updates buffered in this batch.
  void Clear();

  // Support for iterating over the contents of a batch.
  class Handler {
   public:
    virtual ~Handler();
    virtual void Put(const Slice& key, const Slice& value) = 0;
    // Merge and LogData are not pure virtual. Otherwise, we would break
    // existing clients of Handler on a source code level. The default
    // implementation of Merge simply throws a runtime exception.
    virtual void Merge(const Slice& key, const Slice& value);
    // The default implementation of LogData does nothing.
    virtual void LogData(const Slice& blob);
    virtual void Delete(const Slice& key) = 0;
    // Continue is called by WriteBatch::Iterate. If it returns false,
    // iteration is halted. Otherwise, it continues iterating. The default
    // implementation always returns true.
    virtual bool Continue();
  };
  Status Iterate(Handler* handler) const;

  // Retrieve the serialized version of this batch.
  std::string Data() { return rep_; }

  // Returns the number of updates in the batch
  int Count() const;

  // Constructor with a serialized string object
  explicit WriteBatch(std::string rep): rep_(rep) {}

 private:
  friend class WriteBatchInternal;

  std::string rep_;  // See comment in write_batch.cc for the format of rep_

  // Intentionally copyable
};

}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_INCLUDE_WRITE_BATCH_H_
