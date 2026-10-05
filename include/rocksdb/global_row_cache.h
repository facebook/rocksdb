//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "rocksdb/customizable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL - subject to change while under development.

// The result of a GlobalRowCache::Lookup().
struct GlobalRowCacheLookupResult {
  enum class State : uint8_t {
    // The cache cannot answer this lookup. RocksDB must use the normal read
    // path. fill_token can be passed to InsertReadResult() afterward.
    kMiss,

    // The cache contains the value visible at the requested read sequence.
    kValue,

    // The cache contains a deletion visible at the requested read sequence.
    kNotFound,
  };

  State state = State::kMiss;
  SequenceNumber sequence = 0;
  uint64_t generation = 0;
  uint64_t fill_token = 0;
};

// The logical effect of a point mutation on a global row cache.
enum class GlobalRowCacheMutationType : uint8_t {
  // Install the supplied value at the supplied sequence number.
  kValue,

  // Install a deletion marker at the supplied sequence number.
  kDeletion,

  // Prevent an older cached value from being returned. This is used when
  // RocksDB cannot provide the final value, for example for an in-place
  // update.
  kInvalidate,
};

// An optional cache of logical rows, keyed by DB instance, column family, and
// user key. Unlike DBOptions::row_cache, entries are independent of SST files
// and remain usable across flush and compaction. FIFO deletion compactions can
// make an entry invisible when its sequence or generation predates the
// sequence floor of the read's storage view.
//
// Implementations must be thread-safe, including when one shared instance is
// used by multiple DBs. Exceptions must not propagate from any method. RocksDB
// calls mutation methods after the corresponding memtable operation succeeds
// and before its sequence number is published to readers.
// Input Slice objects and their contents are valid only for the duration of
// each call, so implementations must copy anything they retain.
//
// The cache is a correctness participant, not just an advisory cache. For
// every key, it must contain the newest mutation it has observed or no usable
// entry. It must never return an older value after observing a newer mutation.
// Eviction and declined insertion are allowed as long as they leave a miss and
// cannot allow an older in-flight read to fill the key afterward. A non-OK
// Status disables this cache for the affected DB instance; a cache hook failure
// never changes the Status of the RocksDB read or write.
class GlobalRowCache : public Customizable {
 public:
  ~GlobalRowCache() override = default;

  static const char* Type() { return "GlobalRowCache"; }

  // Returns a new cache namespace. RocksDB calls this once for each DB open,
  // so an implementation can be shared by multiple DB instances without key
  // collisions or stale entries from a previous open.
  virtual uint64_t NewId() = 0;

  // Computes the generation stored with a cache entry. RocksDB calls this for
  // values and deletion markers added by a read fill or point mutation. The
  // default generation is the entry's sequence number. Implementations can
  // override this to attach application-defined visibility metadata derived
  // from the key or value.
  virtual Status ComputeEntryGeneration(const Slice& /*key*/,
                                        SequenceNumber sequence,
                                        GlobalRowCacheMutationType /*type*/,
                                        const Slice& /*value*/,
                                        uint64_t* generation) {
    *generation = sequence;
    return Status::OK();
  }

  // Applies an implementation-defined visibility check to a cache hit. The
  // default requires the entry generation to be at least the cache sequence
  // floor of the read's storage view. RocksDB separately enforces the entry's
  // sequence against both the read sequence and this floor, so an override can
  // reject additional entries but cannot make an older entry visible.
  virtual Status IsEntryVisible(
      const Slice& /*key*/, SequenceNumber /*sequence*/, uint64_t generation,
      GlobalRowCacheMutationType /*type*/, const Slice& /*value*/,
      SequenceNumber cache_sequence_floor, bool* visible) {
    *visible = generation >= cache_sequence_floor;
    return Status::OK();
  }

  // Looks up key at read_sequence. On kValue, value must contain the cached
  // value and result->sequence must be no newer than read_sequence. The value
  // must remain valid after Lookup() returns, using PinnableSlice's ownership
  // mechanisms as needed. On kNotFound, result->sequence must identify the
  // visible deletion. An entry newer than read_sequence must be reported as
  // kMiss.
  //
  // On kValue and kNotFound, result->generation must be the generation stored
  // with the entry. On every state, result->fill_token identifies the cache
  // generation observed by this lookup. InsertReadResult() uses it to replace
  // an entry rejected by IsEntryVisible() and to reject a stale fill racing
  // with a later range deletion. Implementations can also invalidate this
  // token for point mutations or eviction when they do not retain a per-key
  // sequence watermark.
  virtual Status Lookup(uint64_t db_id, ColumnFamilyId column_family_id,
                        const Slice& key, SequenceNumber read_sequence,
                        PinnableSlice* value,
                        GlobalRowCacheLookupResult* result) = 0;

  // Offers the result of a normal RocksDB read after a cache miss. type must be
  // kValue or kDeletion. Implementations must reject an entry older than a
  // point mutation already observed for the key and must reject a fill_token
  // invalidated by ApplyRangeDeletion() or another ordering event.
  virtual Status InsertReadResult(uint64_t db_id,
                                  ColumnFamilyId column_family_id,
                                  const Slice& key, SequenceNumber sequence,
                                  uint64_t generation,
                                  GlobalRowCacheMutationType type,
                                  const Slice& value, uint64_t fill_token) = 0;

  // Applies a successful point mutation. Calls can arrive out of sequence, so
  // an older mutation must not replace a newer one. generation is ignored for
  // kInvalidate. For kInvalidate, the implementation must retain enough
  // ordering information to reject a later read fill with an older sequence
  // number; simply erasing the key is not sufficient.
  virtual Status ApplyPointMutation(uint64_t db_id,
                                    ColumnFamilyId column_family_id,
                                    const Slice& key, SequenceNumber sequence,
                                    uint64_t generation,
                                    GlobalRowCacheMutationType type,
                                    const Slice& value) = 0;

  // Applies the range deletion [begin_key, end_key). Besides invalidating
  // entries in the range, this must prevent a read that began before this call
  // from inserting an older result in the range afterward. A range deletion
  // must not hide a point mutation with a newer sequence number.
  virtual Status ApplyRangeDeletion(uint64_t db_id,
                                    ColumnFamilyId column_family_id,
                                    const Slice& begin_key,
                                    const Slice& end_key,
                                    SequenceNumber sequence) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
