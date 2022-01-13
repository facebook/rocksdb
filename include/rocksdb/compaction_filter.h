// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class SliceTransform;

// CompactionFilter allows an application to modify/delete a key-value during
// table file creation.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class CompactionFilter : public Customizable {
 public:
  enum ValueType {
    kValue,
    kMergeOperand,
    kBlobIndex,  // used internally by BlobDB.
  };

  enum class Decision {
    kKeep,
    kRemove,
    kChangeValue,
    kRemoveAndSkipUntil,
    kChangeBlobIndex,  // used internally by BlobDB.
    kIOError,          // used internally by BlobDB.
    kUndetermined,
  };

  enum class BlobDecision { kKeep, kChangeValue, kCorruption, kIOError };

  // Context information for a table file creation.
  struct Context {
    // Whether this table file is created as part of a compaction including all
    // table files.
    bool is_full_compaction;
    // Whether this table file is created as part of a compaction requested by
    // the client.
    bool is_manual_compaction;
    // The column family that will contain the created table file.
    uint32_t column_family_id;
    // Reason this table file is being created.
    TableFileCreationReason reason;
  };

  virtual ~CompactionFilter() {}
  static const char* Type() { return "CompactionFilter"; }
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& name,
                                 const CompactionFilter** result);

  // The table file creation process invokes this method before adding a kv to
  // the table file. A return value of false indicates that the kv should be
  // preserved in the new table file and a return value of true indicates
  // that this key-value should be removed from the new table file. The
  // application can inspect the existing value of the key and make decision
  // based on it.
  //
  // Key-Values that are results of merge operation during table file creation
  // are not passed into this function. Currently, when you have a mix of Put()s
  // and Merge()s on a same key, we only guarantee to process the merge operands
  // through the `CompactionFilter`s. Put()s might be processed, or might not.
  //
  // When the value is to be preserved, the application has the option
  // to modify the existing_value and pass it back through new_value.
  // value_changed needs to be set to true in this case.
  //
  // Note that RocksDB snapshots (i.e. call GetSnapshot() API on a
  // DB* object) will not guarantee to preserve the state of the DB with
  // CompactionFilter. Data seen from a snapshot might disappear after a
  // table file created with a `CompactionFilter` is installed. If you use
  // snapshots, think twice about whether you want to use `CompactionFilter` and
  // whether you are using it in a safe way.
  //
  // If multithreaded compaction is being used *and* a single CompactionFilter
  // instance was supplied via Options::compaction_filter, this method may be
  // called from different threads concurrently.  The application must ensure
  // that the call is thread-safe.
  //
  // If the CompactionFilter was created by a factory, then it will only ever
  // be used by a single thread that is doing the table file creation, and this
  // call does not need to be thread-safe.  However, multiple filters may be
  // in existence and operating concurrently.
  virtual bool Filter(int /*level*/, const Slice& /*key*/,
                      const Slice& /*existing_value*/,
                      std::string* /*new_value*/,
                      bool* /*value_changed*/) const {
    return false;
  }

  // The table file creation process invokes this method on every merge operand.
  // If this method returns true, the merge operand will be ignored and not
  // written out in the new table file.
  //
  // Note: If you are using a TransactionDB, it is not recommended to implement
  // FilterMergeOperand().  If a Merge operation is filtered out, TransactionDB
  // may not realize there is a write conflict and may allow a Transaction to
  // Commit that should have failed.  Instead, it is better to implement any
  // Merge filtering inside the MergeOperator.
  virtual bool FilterMergeOperand(int /*level*/, const Slice& /*key*/,
                                  const Slice& /*operand*/) const {
    return false;
  }

  // An extended API. Called for both values and merge operands.
  // Allows changing value and skipping ranges of keys.
  // The default implementation uses Filter() and FilterMergeOperand().
  // If you're overriding this method, no need to override the other two.
  // `value_type` indicates whether this key-value corresponds to a normal
  // value (e.g. written with Put())  or a merge operand (written with Merge()).
  //
  // Possible return values:
  //  * kKeep - keep the key-value pair.
  //  * kRemove - remove the key-value pair or merge operand.
  //  * kChangeValue - keep the key and change the value/operand to *new_value.
  //  * kRemoveAndSkipUntil - remove this key-value pair, and also remove
  //      all key-value pairs with key in [key, *skip_until). This range
  //      of keys will be skipped without reading, potentially saving some
  //      IO operations compared to removing the keys one by one.
  //
  //      *skip_until <= key is treated the same as Decision::kKeep
  //      (since the range [key, *skip_until) is empty).
  //
  //      Caveats:
  //       - The keys are skipped even if there are snapshots containing them,
  //         i.e. values removed by kRemoveAndSkipUntil can disappear from a
  //         snapshot - beware if you're using TransactionDB or
  //         DB::GetSnapshot().
  //       - If value for a key was overwritten or merged into (multiple Put()s
  //         or Merge()s), and `CompactionFilter` skips this key with
  //         kRemoveAndSkipUntil, it's possible that it will remove only
  //         the new value, exposing the old value that was supposed to be
  //         overwritten.
  //       - Doesn't work with PlainTableFactory in prefix mode.
  //       - If you use kRemoveAndSkipUntil for table files created by
  //         compaction, consider also reducing compaction_readahead_size
  //         option.
  //
  // Should never return kUndetermined.
  // Note: If you are using a TransactionDB, it is not recommended to filter
  // out or modify merge operands (ValueType::kMergeOperand).
  // If a merge operation is filtered out, TransactionDB may not realize there
  // is a write conflict and may allow a Transaction to Commit that should have
  // failed. Instead, it is better to implement any Merge filtering inside the
  // MergeOperator.
  virtual Decision FilterV2(int level, const Slice& key, ValueType value_type,
                            const Slice& existing_value, std::string* new_value,
                            std::string* /*skip_until*/) const {
    switch (value_type) {
      case ValueType::kValue: {
        bool value_changed = false;
        bool rv = Filter(level, key, existing_value, new_value, &value_changed);
        if (rv) {
          return Decision::kRemove;
        }
        return value_changed ? Decision::kChangeValue : Decision::kKeep;
      }
      case ValueType::kMergeOperand: {
        bool rv = FilterMergeOperand(level, key, existing_value);
        return rv ? Decision::kRemove : Decision::kKeep;
      }
      case ValueType::kBlobIndex:
        return Decision::kKeep;
    }
    assert(false);
    return Decision::kKeep;
  }

  // Internal (BlobDB) use only. Do not override in application code.
  virtual BlobDecision PrepareBlobOutput(const Slice& /* key */,
                                         const Slice& /* existing_value */,
                                         std::string* /* new_value */) const {
    return BlobDecision::kKeep;
  }

  // This function is deprecated. Snapshots will always be ignored for
  // `CompactionFilter`s, because we realized that not ignoring snapshots
  // doesn't provide the guarantee we initially thought it would provide.
  // Repeatable reads will not be guaranteed anyway. If you override the
  // function and returns false, we will fail the table file creation.
  virtual bool IgnoreSnapshots() const { return true; }

  // Returns a name that identifies this `CompactionFilter`.
  // The name will be printed to LOG file on start up for diagnosis.
  const char* Name() const override = 0;

  // Internal (BlobDB) use only. Do not override in application code.
  virtual bool IsStackedBlobDbInternalCompactionFilter() const { return false; }

  // In the case of BlobDB, it may be possible to reach a decision with only
  // the key without reading the actual value. Keys whose value_type is
  // kBlobIndex will be checked by this method.
  // Returning kUndetermined will cause FilterV2() to be called to make a
  // decision as usual.
  virtual Decision FilterBlobByKey(int /*level*/, const Slice& /*key*/,
                                   std::string* /*new_value*/,
                                   std::string* /*skip_until*/) const {
    return Decision::kUndetermined;
  }
};

// Each thread of work involving creating table files will create a new
// `CompactionFilter` according to `ShouldFilterTableFileCreation()`. This
// allows the application to know about the different ongoing threads of work
// and makes it unnecessary for `CompactionFilter` to provide thread-safety.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class CompactionFilterFactory : public Customizable {
 public:
  virtual ~CompactionFilterFactory() {}
  static const char* Type() { return "CompactionFilterFactory"; }
  static Status CreateFromString(
      const ConfigOptions& config_options, const std::string& name,
      std::shared_ptr<CompactionFilterFactory>* result);

  // Returns whether a thread creating table files for the specified `reason`
  // should invoke `CreateCompactionFilter()` and pass KVs through the returned
  // filter.
  virtual bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const {
    // For backward compatibility, default implementation only applies
    // `CompactionFilter` to files generated by compaction.
    return reason == TableFileCreationReason::kCompaction;
  }

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) = 0;

  // Returns a name that identifies this `CompactionFilter` factory.
  virtual const char* Name() const override = 0;
};

}  // namespace ROCKSDB_NAMESPACE
