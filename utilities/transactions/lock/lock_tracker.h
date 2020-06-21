// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// Request for locking a single key.
struct PointLockRequest {
  // The id of the key's column family.
  uint32_t column_family_id;
  // The key to lock.
  std::string key;
  // The sequence number from which there is no concurrent update to key.
  SequenceNumber seq;
  // Whether the lock is acquired only for read.
  bool read_only;
  // Whether the lock is in exclusive mode.
  bool exclusive;
};

// Request for locking a range of keys.
struct RangeLockRequest {
  // TODO
};

struct PointLockInfo {
  // Whether the key is locked.
  bool locked;
  // Whether the key is locked in exclusive mode.
  bool exclusive;
  // The sequence number in the tracked PointLockRequest.
  // If not locked, it is kMaxSequenceNumber.
  SequenceNumber seq;
};

// Tracks the lock requests.
// In PessimisticTransaction, it tracks the locks acquired through LockMgr;
// In OptimisticTransaction, since there is no LockMgr, it tracks the lock intention.
// Not thread-safe.
class LockTracker {
 public:
  virtual ~LockTracker() {}

  // Whether tracking the lock of a single key is supported.
  virtual bool IsPointLockSupported() const = 0;

  // Whether tracking the lock of a key range is supported.
  virtual bool IsRangeLockSupported() const = 0;

  // Tracks the acquirement of a lock on key.
  //
  // If IsPointLockSupported returns false, returns Status::NotSupported.
  virtual Status Track(const PointLockRequest& lock_request) = 0;

  // Tracks the acquirement of a lock on a range of keys.
  //
  // If IsRangeLockSupported returns false, returns Status::NotSupported.
  virtual Status Track(const RangeLockRequest& lock_request) = 0;
  
  // Merges lock requests tracked in the specified tracker into the current
  // tracker.
  //
  // E.g. for point lock, if a key in tracker is not yet tracked,
  // track this new key; otherwise, merge the tracked information of the key
  // such as lock's exclusiveness, read/write statistics.
  //
  // If Merge is not supported, returns Status::NotSupported.
  //
  // REQUIRED: the specified tracker must be of the same concrete class type as
  // the current tracker.
  virtual Status Merge(const LockTracker& tracker) = 0;

  // This is a reverse operation of Merge.
  //
  // E.g. for point lock, if a key exists in both current and the sepcified
  // tracker, then subtract the information (such as read/write statistics) of
  // the key in the specified tracker from the current tracker.
  //
  // If Subtract is not supported, returns Status::NotSupported.
  //
  // REQUIRED: the specified tracker must be of the same concrete class type as
  // the current tracker.
  virtual Status Subtract(const LockTracker& tracker) = 0;

  // Gets the new locks tracked since the specified save point and add to
  // result, the locks that have been tracked before the save point will not be
  // added to result.
  //
  // save_point_tracker is the tracker used by a SavePoint to track locks
  // tracked after creating the SavePoint.
  //
  // The implementation should document whether point lock, or range lock, or
  // both are considered in this method. If this method is not supported,
  // returns Status::NotSupported.
  //
  // REQUIRED: the trackers in the parameters must be of the same concrete
  // class type as the current tracker.
  virtual Status GetTrackedLocksSinceSavePoint(
      const LockTracker& save_point_tracker, LockTracker* result);

  using ColumnFamilyId = uint32_t;
  // Gets lock related information of the key.
  //
  // If IsPointLockSupported returns false, returns Status::NotSupported.
  virtual Status GetInfo(ColumnFamilyId column_family_id,
                         const std::string& key, PointLockInfo* info) const = 0;

  // Gets number of tracked point locks.
  //
  // If IsPointLockSupported returns false, returns 0.
  virtual uint64_t GetNumPointLocks() const = 0;

  class ColumnFamilyIterator {
   public:
    virtual ~ColumnFamilyIterator() {}

    // Whether there are remaining column families.
    virtual bool HasNext() = 0;

    // Gets next column family id.
    //
    // If HasNext is false, calling this method has undefined behavior.
    virtual ColumnFamilyId Next() = 0;
  };

  // Gets an iterator for column families.
  virtual ColumnFamilyIterator GetColumnFamilyIterator() const = 0;

  class KeyIterator {
   public:
    virtual ~KeyIterator() {}

    // Whether there are remaining keys.
    virtual bool HasNext() = 0;

    // Gets the next key.
    //
    // If HasNext is false, calling this method has undefined behavior.
    virtual std::string Next() = 0;
  };

  // Gets an iterator for keys with tracked point locks in the column family.
  //
  // If IsPointLockSupported returns false, calling HasNext on the returned
  // KeyIterator always returns false.
  virtual KeyIterator GetKeyIterator(ColumnFamilyId column_family_id) const = 0;
};

} // namespace ROCKSDB_NAMESPACE
