//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct ConfigOptions;

// Allows applications to make periodic compactions eligible before their
// normal trigger time. The policy does not select files or schedule jobs;
// RocksDB continues to apply its normal compaction picker, conflict checks,
// and background-work limits.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe.
class PeriodicCompactionPolicy : public Customizable {
 public:
  struct Context {
    uint64_t current_time_seconds = 0;
    uint64_t periodic_compaction_seconds = 0;
    // Identify the DB and column family being evaluated, so that one policy
    // instance can be shared across DBs and column families. The slices are
    // valid only for the duration of the GetDecision() call. db_id is the
    // value returned by DB::GetDbIdentity() and may be empty while the DB is
    // being opened.
    Slice db_id;
    uint32_t column_family_id = 0;
    Slice column_family_name;
  };

  struct Decision {
    // Files whose periodic-compaction trigger time is within this many
    // seconds become eligible now. RocksDB clamps this value to
    // periodic_compaction_seconds. Zero preserves normal behavior.
    uint64_t deadline_lookahead_seconds = 0;
  };

  ~PeriodicCompactionPolicy() override = default;
  static const char* Type() { return "PeriodicCompactionPolicy"; }
  static Status CreateFromString(
      const ConfigOptions& options, const std::string& value,
      std::shared_ptr<PeriodicCompactionPolicy>* result);

  // May be called from internal DB threads and while RocksDB holds internal
  // synchronization. Implementations must be thread-safe, non-blocking, and
  // must not call back into the same DB.
  virtual Decision GetDecision(const Context& context) const = 0;

  const char* Name() const override = 0;
};

// Always reports the same deadline lookahead, configurable as
// "lookahead_seconds".
class FixedLookaheadPeriodicCompactionPolicy : public PeriodicCompactionPolicy {
 public:
  explicit FixedLookaheadPeriodicCompactionPolicy(uint64_t lookahead_seconds);

  static const char* kClassName() {
    return "FixedLookaheadPeriodicCompactionPolicy";
  }
  const char* Name() const override { return kClassName(); }

  Decision GetDecision(const Context& context) const override;

 private:
  uint64_t lookahead_seconds_;
};

}  // namespace ROCKSDB_NAMESPACE
