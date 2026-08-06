//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Tests for the lazy blob resolution + partial (byte-range) column read API
// (DB::GetEntityLazy / LazyWideColumns / DB::MultiGetEntityLazy /
// LazyWideColumnsBatch).

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/db_test_util.h"
#include "db/wide/wide_column_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/comparator.h"
#include "rocksdb/lazy_wide_columns.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/testutil.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

class DBLazyEntityTest : public DBTestBase {
 protected:
  DBLazyEntityTest()
      : DBTestBase("db_lazy_entity_test", /*env_do_fsync=*/false) {}

  // Options for the lazy API: blob files enabled with a small threshold so
  // large columns become blob references, statistics on so tests can assert
  // exactly which blobs were read, and max_open_files == -1 (required by the
  // lazy API so table readers are immortal and same-file refs stay resolvable).
  Options GetLazyTestOptions() {
    Options options =
        wide_column_test_util::GetOptionsForBlobTest(GetDefaultOptions());
    options.max_open_files = -1;
    options.statistics = CreateDBStatistics();
    return options;
  }

  uint64_t BlobBytesRead(const Options& options) {
    return options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);
  }
};

// Enumerate a lazily-read entity without triggering any blob I/O: inline
// columns expose their bytes immediately, blob columns show up as unresolved
// references with known (uncompressed) logical sizes, and no blob bytes are
// read until something is pulled.
TEST_F(DBLazyEntityTest, EnumerateColumnsWithoutResolvingBlobs) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string small = "inline";  // < min_blob_size: stays inline
  const std::string big1(200, 'a');    // >= min_blob_size: blob reference
  const std::string big2(300, 'b');    // >= min_blob_size: blob reference
  const WideColumns columns{
      {kDefaultWideColumnName, small}, {"attr1", big1}, {"attr2", big2}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  ASSERT_EQ(lazy.size(), 3U);

  // Column 0: the inline default column -- bytes available with no I/O.
  ASSERT_EQ(lazy[0].name(), kDefaultWideColumnName);
  ASSERT_FALSE(lazy[0].is_reference());
  ASSERT_EQ(*lazy[0].inline_value(), small);

  // Columns 1 and 2: unresolved blob references with known logical sizes
  // (uncompressed), reported without reading the blob payloads.
  ASSERT_EQ(lazy[1].name(), "attr1");
  ASSERT_TRUE(lazy[1].is_reference());
  ASSERT_TRUE(lazy[1].logical_size().has_value());
  ASSERT_EQ(*lazy[1].logical_size(), big1.size());

  ASSERT_EQ(lazy[2].name(), "attr2");
  ASSERT_TRUE(lazy[2].is_reference());
  ASSERT_TRUE(lazy[2].logical_size().has_value());
  ASSERT_EQ(*lazy[2].logical_size(), big2.size());

  // Nothing was pulled, so no blob payload was read.
  ASSERT_EQ(BlobBytesRead(options), 0U);
}

// Pull a single byte range from one blob column; only that column's blob is
// read, and the returned slice is exactly the requested sub-span.
TEST_F(DBLazyEntityTest, ResolveSingleColumnRange) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  std::string big(200, '\0');
  for (size_t i = 0; i < big.size(); ++i) {
    big[i] = static_cast<char>('0' + (i % 10));
  }
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  // Read the 50 bytes at offset 100 of the "data" column (index 1).
  PinnableSlice range;
  ASSERT_OK(lazy.ResolveColumnRange(lazy[1], /*offset=*/100,
                                    /*length=*/50, &range));
  ASSERT_EQ(range, Slice(big).ToString().substr(100, 50));

  // An offset at/past the end clamps to empty (not an error).
  PinnableSlice past_end;
  ASSERT_OK(lazy.ResolveColumnRange(lazy[1], /*offset=*/big.size(),
                                    /*length=*/10, &past_end));
  ASSERT_TRUE(past_end.empty());
}

// The headline example: resolve *many blobs in a single call*. One MultiResolve
// submits reads for several blob columns at once; the engine coalesces them
// (and, later, issues them asynchronously) under the hood, filling each
// request's out-params independently.
TEST_F(DBLazyEntityTest, MultiResolveResolvesManyBlobsInOneCall) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string v1(200, 'a');
  const std::string v2(250, 'b');
  const std::string v3(300, 'c');
  const WideColumns columns{
      {kDefaultWideColumnName, "inline"}, {"c1", v1}, {"c2", v2}, {"c3", v3}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  // Describe all three blob reads up front, then resolve them together in a
  // single call. Each read points at its own result/status out-params.
  std::array<PinnableSlice, 3> results;
  std::array<Status, 3> statuses;
  std::vector<LazyColumnReadRequest> reads(3);
  for (size_t i = 0; i < reads.size(); ++i) {
    reads[i].column = &lazy[i + 1];  // columns 1..3 are the blob columns
    reads[i].offset = 0;
    reads[i].length = kLazyWholeColumn;
    reads[i].result = &results[i];
    reads[i].status = &statuses[i];
  }

  ASSERT_OK(lazy.MultiResolve(reads));  // std::vector sugar overload

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(results[0], v1);
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(results[1], v2);
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(results[2], v3);
}

// A single MultiResolve can also mix multiple ranges from one column with reads
// of other columns -- all coalesced into one pass.
TEST_F(DBLazyEntityTest, MultiResolveMixesRangesAndColumns) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string a(400, 'a');
  const std::string b(200, 'b');
  const WideColumns columns{
      {kDefaultWideColumnName, "inline"}, {"a", a}, {"b", b}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  // Two disjoint ranges of column "a" (index 1) plus the whole of column "b"
  // (index 2), resolved together.
  std::array<PinnableSlice, 3> results;
  std::array<Status, 3> statuses;
  std::array<LazyColumnReadRequest, 3> reads;

  reads[0] = LazyColumnReadRequest{&lazy[1],      /*offset=*/0,
                                   /*length=*/50, /*force_verify=*/false,
                                   &results[0],   &statuses[0]};
  reads[1] = LazyColumnReadRequest{&lazy[1],       /*offset=*/300,
                                   /*length=*/100, /*force_verify=*/false,
                                   &results[1],    &statuses[1]};
  reads[2] = LazyColumnReadRequest{&lazy[2],
                                   /*offset=*/0,
                                   kLazyWholeColumn,
                                   /*force_verify=*/false,
                                   &results[2],
                                   &statuses[2]};

  ASSERT_OK(lazy.MultiResolve(reads.size(), reads.data()));

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(results[0], a.substr(0, 50));
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(results[1], a.substr(300, 100));
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(results[2], b);
}

// The cross-key peer: MultiGetEntityLazy returns a LazyWideColumnsBatch (N
// per-key entities), and one LazyWideColumnsBatch::MultiResolve call resolves
// blobs across all of them. Each read references its entity by index, so it
// cannot span batches.
TEST_F(DBLazyEntityTest, BatchResolveAcrossKeysInOneCall) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  const std::string key1 = "k1";
  const std::string key2 = "k2";
  const std::string v1(200, 'a');
  const std::string v2(250, 'b');
  const WideColumns columns1{{kDefaultWideColumnName, "inline1"}, {"data", v1}};
  const WideColumns columns2{{kDefaultWideColumnName, "inline2"}, {"data", v2}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  const std::array<Slice, 2> keys{Slice(key1), Slice(key2)};
  LazyWideColumnsBatch batch;
  std::array<Status, 2> get_statuses;
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                          keys.size(), keys.data(), &batch,
                          get_statuses.data());
  ASSERT_OK(get_statuses[0]);
  ASSERT_OK(get_statuses[1]);
  ASSERT_EQ(batch.size(), 2U);

  // No blobs read yet -- only inline columns + references were materialized.
  ASSERT_EQ(BlobBytesRead(options), 0U);

  // One batch resolves the "data" blob column of both keys together. Each read
  // names its target column, which identifies the owning entity.
  std::array<PinnableSlice, 2> results;
  std::array<Status, 2> statuses;
  std::array<LazyColumnReadRequest, 2> reads;
  for (size_t i = 0; i < reads.size(); ++i) {
    reads[i].column = &batch[i][1];  // "data" column of entity i
    reads[i].result = &results[i];
    reads[i].status = &statuses[i];
  }

  ASSERT_OK(batch.MultiResolve(reads.size(), reads.data()));

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(results[0], v1);
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(results[1], v2);
}

// The lazy result outlives the producing call: it pins the SuperVersion, so a
// deferred read still works after GetEntityLazy has returned and even after the
// result has been moved.
TEST_F(DBLazyEntityTest, ResultOutlivesGetEntityLazyCall) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(200, 'z');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns moved;
  {
    LazyWideColumns lazy;
    ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                                 &lazy));
    // Move the result out of the inner scope; the SuperVersion pin travels with
    // it, so the reference stays resolvable.
    moved = std::move(lazy);
  }

  PinnableSlice value;
  ASSERT_OK(moved.ResolveColumn(moved[1], &value));
  ASSERT_EQ(value, big);
}

// Columns that are never pulled are never read from storage.
TEST_F(DBLazyEntityTest, UnpulledColumnsAreNeverRead) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string wanted(200, 'w');
  const std::string unwanted(5000, 'u');
  const WideColumns columns{{kDefaultWideColumnName, "inline"},
                            {"unwanted", unwanted},
                            {"wanted", wanted}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  // Sorted order: ["", "unwanted", "wanted"] -> pull only index 2.
  PinnableSlice value;
  ASSERT_OK(lazy.ResolveColumn(lazy[2], &value));
  ASSERT_EQ(value, wanted);

  // Only ~200 bytes (the "wanted" blob) should have been read, never the 5000
  // bytes of "unwanted".
  ASSERT_GT(BlobBytesRead(options), 0U);
  ASSERT_LT(BlobBytesRead(options), unwanted.size());
}

// A repeated resolve of the same column reads its blob from storage only once;
// the second resolve is served from the cached bytes with no additional I/O.
TEST_F(DBLazyEntityTest, RepeatedResolutionReadsBlobOnce) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  PinnableSlice v1;
  ASSERT_OK(lazy.ResolveColumn(lazy[1], &v1));
  ASSERT_EQ(Slice(v1), big);
  const uint64_t after_first = BlobBytesRead(options);
  ASSERT_GT(after_first, 0U);

  // Resolving the same column again reuses the cached bytes -- no more blob
  // I/O.
  PinnableSlice v2;
  ASSERT_OK(lazy.ResolveColumn(lazy[1], &v2));
  ASSERT_EQ(Slice(v2), big);
  ASSERT_EQ(BlobBytesRead(options), after_first);
}

// A memtable-resident entity (never flushed) reads back correctly through the
// lazy API. Blobs are only created at flush, so the large value lives inline in
// the memtable: the column is not a reference and resolves with no blob I/O.
TEST_F(DBLazyEntityTest, MemtableResidentEntity) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  // No Flush(): the entity stays in the memtable.
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));
  ASSERT_EQ(lazy.size(), 2U);
  ASSERT_FALSE(lazy[1].is_reference());  // inline in memtable, not a blob ref

  PinnableSlice value;
  ASSERT_OK(lazy.ResolveColumn(lazy[1], &value));
  ASSERT_EQ(Slice(value), big);
  ASSERT_EQ(BlobBytesRead(options), 0U);  // no SST/blob files exist yet
}

// The lazy API requires max_open_files == -1 (immortal table readers) so
// embedded/same-file references stay resolvable lazily; otherwise it declines.
TEST_F(DBLazyEntityTest, RequiresMaxOpenFilesMinusOne) {
  Options options = GetLazyTestOptions();
  options.max_open_files = 1000;  // not -1
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string data(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", data}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns lazy;
  const Status s =
      db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key, &lazy);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

// Under ReadTier::kBlockCacheTier (taken from the originating GetEntityLazy()
// call), resolving a reference whose blob bytes are not cached yields
// Incomplete instead of doing I/O.
TEST_F(DBLazyEntityTest, BlockCacheTierYieldsIncompleteOnMiss) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Warm the SST blocks into the block cache with a normal lazy read. Because
  // it is lazy, the "data" blob is left unresolved, so the blob cache stays
  // cold.
  {
    LazyWideColumns warm;
    ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                                 &warm));
  }

  // A cache-only lazy read: the entity is served from the warm block cache, but
  // resolving the blob would require I/O, so GetColumn returns Incomplete.
  ReadOptions block_cache_only;
  block_cache_only.read_tier = kBlockCacheTier;
  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(block_cache_only, db_->DefaultColumnFamily(),
                               key, &lazy));

  PinnableSlice value;
  const Status s = lazy.ResolveColumn(lazy[1], &value);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
}

// A column that belongs to a different result is rejected with InvalidArgument
// on the per-read status (columns identify their owning result, so there is no
// untyped index to run out of range).
TEST_F(DBLazyEntityTest, ForeignColumnIsInvalidArgument) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string data(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", data}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));
  LazyWideColumns other;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &other));

  // Populate `value` with a successful resolve, then confirm a failed resolve
  // into the same output resets it (leaves no stale bytes).
  PinnableSlice value;
  ASSERT_OK(lazy.ResolveColumn(lazy[1], &value));
  ASSERT_FALSE(value.empty());

  // Resolving `other`'s column through `lazy` is rejected, and clears `value`.
  const Status s = lazy.ResolveColumn(other[0], &value);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_TRUE(value.empty());

  // A null column is likewise rejected, reported on the per-read status.
  PinnableSlice null_value;
  Status null_status;
  LazyColumnReadRequest read;
  read.column = nullptr;
  read.result = &null_value;
  read.status = &null_status;
  ASSERT_OK(lazy.MultiResolve(/*num_reads=*/1, &read));
  ASSERT_TRUE(null_status.IsInvalidArgument()) << null_status.ToString();
}

// A reused LazyWideColumns is reset (left empty) when a later GetEntityLazy
// call fails argument validation, rather than retaining the previous result.
TEST_F(DBLazyEntityTest, ReusedResultResetOnEarlyReturn) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string value(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"},
                            {"data", value}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));
  ASSERT_EQ(lazy.size(), 2U);

  // Reuse the same result for a call that fails argument validation (an
  // io_activity that is neither kUnknown nor kGetEntity); it must be reset to
  // empty, not left holding the previous entity.
  ReadOptions bad_read_options;
  bad_read_options.io_activity = Env::IOActivity::kGet;
  const Status s = db_->GetEntityLazy(bad_read_options,
                                      db_->DefaultColumnFamily(), key, &lazy);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_TRUE(lazy.empty());
  ASSERT_EQ(lazy.size(), 0U);
}

// A reused LazyWideColumnsBatch is reset (left empty) on MultiGetEntityLazy's
// early-return paths (num_keys == 0 and argument errors), not left stale.
TEST_F(DBLazyEntityTest, ReusedBatchResetOnEarlyReturn) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  const std::string k1 = "k1";
  const std::string k2 = "k2";
  const std::string d1(200, 'a');
  const std::string d2(200, 'b');
  const WideColumns c1{{kDefaultWideColumnName, "i1"}, {"data", d1}};
  const WideColumns c2{{kDefaultWideColumnName, "i2"}, {"data", d2}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), k1, c1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), k2, c2));
  ASSERT_OK(Flush());

  const std::array<Slice, 2> keys{Slice(k1), Slice(k2)};
  LazyWideColumnsBatch batch;
  std::array<Status, 2> statuses;

  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                          keys.size(), keys.data(), &batch, statuses.data());
  ASSERT_OK(statuses[0]);
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(batch.size(), 2U);

  // Reuse with num_keys == 0: the batch must be reset (empty), not stale.
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                          /*num_keys=*/0, keys.data(), &batch, statuses.data());
  ASSERT_TRUE(batch.empty());
  ASSERT_EQ(batch.size(), 0U);

  // Repopulate, then reuse for a call that fails argument validation; still
  // reset to empty.
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                          keys.size(), keys.data(), &batch, statuses.data());
  ASSERT_OK(statuses[0]);
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(batch.size(), 2U);

  ReadOptions bad_read_options;
  bad_read_options.io_activity = Env::IOActivity::kGet;
  db_->MultiGetEntityLazy(bad_read_options, db_->DefaultColumnFamily(),
                          keys.size(), keys.data(), &batch, statuses.data());
  ASSERT_TRUE(statuses[0].IsInvalidArgument()) << statuses[0].ToString();
  ASSERT_TRUE(statuses[1].IsInvalidArgument()) << statuses[1].ToString();
  ASSERT_TRUE(batch.empty());
}

// LazyWideColumnsBatch::MultiResolve rejects a null column and a column that
// belongs to a different result (not this batch), each on its per-read status,
// while still resolving the valid reads in the same call.
TEST_F(DBLazyEntityTest, BatchRejectsForeignAndNullColumns) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  const std::array<Slice, 1> keys{Slice(key)};
  LazyWideColumnsBatch batch;
  std::array<Status, 1> get_statuses;
  db_->MultiGetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(),
                          keys.size(), keys.data(), &batch,
                          get_statuses.data());
  ASSERT_OK(get_statuses[0]);

  // A standalone result whose column does not belong to this batch.
  LazyWideColumns standalone;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &standalone));

  PinnableSlice v_null;
  PinnableSlice v_foreign;
  PinnableSlice v_ok;
  Status s_null;
  Status s_foreign;
  Status s_ok;
  std::vector<LazyColumnReadRequest> reads(3);
  reads[0].column = nullptr;  // null
  reads[0].result = &v_null;
  reads[0].status = &s_null;
  reads[1].column = &standalone[1];  // foreign: belongs to `standalone`
  reads[1].result = &v_foreign;
  reads[1].status = &s_foreign;
  reads[2].column = &batch[0][1];  // valid: "data" of this batch's entity
  reads[2].result = &v_ok;
  reads[2].status = &s_ok;

  ASSERT_OK(batch.MultiResolve(reads));  // std::vector sugar overload
  ASSERT_TRUE(s_null.IsInvalidArgument()) << s_null.ToString();
  ASSERT_TRUE(s_foreign.IsInvalidArgument()) << s_foreign.ToString();
  ASSERT_OK(s_ok);
  ASSERT_EQ(Slice(v_ok), big);
}

// Wide-column entities do not support user-defined timestamps: PutEntity()
// rejects a UDT column family (see DBWideBasicTest.PutEntityTimestampError), so
// no entity can exist there. GetEntityLazy() inherits this feature-wide
// limitation and behaves at parity with GetEntity() on such a column family --
// it performs the same read-timestamp validation (both go through the same
// internal read path) and, for a plain value, returns a single default column.
TEST_F(DBLazyEntityTest, UserTimestampParityWithGetEntity) {
  Options options = GetLazyTestOptions();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  DestroyAndReopen(options);

  ColumnFamilyHandle* const cfh = db_->DefaultColumnFamily();
  constexpr char key[] = "entity";

  // Entities cannot be written under UDT.
  const WideColumns columns{{kDefaultWideColumnName, "value"}, {"attr", "x"}};
  ASSERT_EQ(db_->PutEntity(WriteOptions(), cfh, key, columns).code(),
            Status::kInvalidArgument);

  // Write a plain value with a timestamp.
  std::string write_ts;
  PutFixed64(&write_ts, 1);
  ASSERT_OK(db_->Put(WriteOptions(), cfh, key, write_ts, "plain"));
  ASSERT_OK(Flush());

  // Without a read timestamp, GetEntity and GetEntityLazy both reject the read
  // (the column family has a user-defined timestamp) -- i.e. at parity.
  {
    PinnableWideColumns eager;
    ASSERT_EQ(db_->GetEntity(ReadOptions(), cfh, key, &eager).code(),
              Status::kInvalidArgument);
    LazyWideColumns lazy;
    ASSERT_EQ(db_->GetEntityLazy(ReadOptions(), cfh, key, &lazy).code(),
              Status::kInvalidArgument);
  }

  // With a read timestamp, both succeed and return the plain value as a single
  // default column.
  std::string read_ts_buf;
  const Slice read_ts = EncodeU64Ts(2, &read_ts_buf);
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts;

  PinnableWideColumns eager;
  ASSERT_OK(db_->GetEntity(read_opts, cfh, key, &eager));

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(read_opts, cfh, key, &lazy));
  ASSERT_EQ(lazy.size(), eager.columns().size());
  ASSERT_EQ(lazy.size(), 1U);
  ASSERT_EQ(lazy[0].name(), kDefaultWideColumnName);

  PinnableSlice value;
  ASSERT_OK(lazy.ResolveColumn(lazy[0], &value));
  ASSERT_EQ(Slice(value), eager.columns()[0].value());
  ASSERT_EQ(Slice(value), "plain");
}

// The lazy read API must be forwarded through StackableDB wrappers (e.g.
// TransactionDB) to the underlying DB, not fall through to the DB base class
// default (Status::NotSupported). Exercises committed data read outside a
// transaction, including resolving a blob-backed column through the wrapper.
TEST_F(DBLazyEntityTest, StackableDBForwardsLazyReads) {
  Options options = GetLazyTestOptions();
  const std::string txn_dbname = dbname_ + "_txn";
  ASSERT_OK(DestroyDB(txn_dbname, options));

  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, TransactionDBOptions(), txn_dbname,
                                &txn_db));
  ASSERT_NE(txn_db, nullptr);
  std::unique_ptr<TransactionDB> txn_db_guard(txn_db);

  ColumnFamilyHandle* const cfh = txn_db->DefaultColumnFamily();

  constexpr char key[] = "entity";
  const std::string small = "inline";  // < min_blob_size: stays inline
  const std::string big(200, 'a');     // >= min_blob_size: blob reference
  const WideColumns columns{{kDefaultWideColumnName, small}, {"attr", big}};

  // Commit the entity via a transaction, then read it back as committed data.
  {
    std::unique_ptr<Transaction> txn(
        txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
    ASSERT_OK(txn->PutEntity(cfh, key, columns));
    ASSERT_OK(txn->Commit());
  }
  ASSERT_OK(txn_db->Flush(FlushOptions()));

  // Single-key: forwards to the underlying DB (not NotSupported), and the
  // blob-backed column resolves through the wrapper.
  {
    LazyWideColumns lazy;
    ASSERT_OK(txn_db->GetEntityLazy(ReadOptions(), cfh, key, &lazy));
    ASSERT_EQ(lazy.size(), 2U);

    PinnableSlice default_value;
    ASSERT_OK(lazy.ResolveColumn(lazy[0], &default_value));
    ASSERT_EQ(Slice(default_value), small);

    PinnableSlice attr_value;
    ASSERT_OK(lazy.ResolveColumn(lazy[1], &attr_value));
    ASSERT_EQ(Slice(attr_value), big);
  }

  // Batch analogue via the wrapper.
  {
    const std::array<Slice, 1> keys{Slice(key)};
    LazyWideColumnsBatch batch;
    std::array<Status, 1> statuses;
    txn_db->MultiGetEntityLazy(ReadOptions(), cfh, keys.size(), keys.data(),
                               &batch, statuses.data(),
                               /*sorted_input=*/false);
    ASSERT_OK(statuses[0]);
    ASSERT_EQ(batch.size(), 1U);
    ASSERT_EQ(batch[0].size(), 2U);

    PinnableSlice attr_value;
    ASSERT_OK(batch[0].ResolveColumn(batch[0][1], &attr_value));
    ASSERT_EQ(Slice(attr_value), big);
  }
}

// The lazy read API works on a read-only DB instance (opened with
// max_open_files == -1): the entity enumerates without blob I/O and its blob
// column resolves lazily after the call returns (the result's pin keeps it
// valid), matching the eager value.
TEST_F(DBLazyEntityTest, ReadOnlyInstance) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string small = "inline";
  const std::string big(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, small}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Reopen the same DB read-only; GetEntityLazy must be served by the read-only
  // GetImpl override (which forwards the lazy signal and transfers the pin).
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));
  ASSERT_EQ(lazy.size(), 2U);
  ASSERT_FALSE(lazy[0].is_reference());
  ASSERT_EQ(*lazy[0].inline_value(), small);
  ASSERT_TRUE(lazy[1].is_reference());
  ASSERT_EQ(lazy[1].name(), "data");

  // Enumeration alone reads no blob bytes.
  ASSERT_EQ(BlobBytesRead(options), 0U);

  PinnableSlice value;
  ASSERT_OK(lazy.ResolveColumn(lazy[1], &value));
  ASSERT_EQ(Slice(value), big);
  ASSERT_GT(BlobBytesRead(options), 0U);
}

// The lazy read API works on a secondary DB instance (opened with
// max_open_files == -1) after catching up to the primary.
TEST_F(DBLazyEntityTest, SecondaryInstance) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string small = "inline";
  const std::string big(250, 'b');
  const WideColumns columns{{kDefaultWideColumnName, small}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  const std::string secondary_path = dbname_ + "_secondary";
  ASSERT_OK(DestroyDB(secondary_path, options));
  std::unique_ptr<DB> secondary;
  ASSERT_OK(DB::OpenAsSecondary(options, dbname_, secondary_path, &secondary));
  ASSERT_OK(secondary->TryCatchUpWithPrimary());
  ASSERT_OK(options.statistics->Reset());

  LazyWideColumns lazy;
  ASSERT_OK(secondary->GetEntityLazy(
      ReadOptions(), secondary->DefaultColumnFamily(), key, &lazy));
  ASSERT_EQ(lazy.size(), 2U);
  ASSERT_TRUE(lazy[1].is_reference());
  ASSERT_EQ(BlobBytesRead(options), 0U);

  PinnableSlice value;
  ASSERT_OK(lazy.ResolveColumn(lazy[1], &value));
  ASSERT_EQ(Slice(value), big);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
