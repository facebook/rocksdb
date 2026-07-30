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

  ASSERT_EQ(lazy.num_columns(), 3U);

  // Column 0: the inline default column -- bytes available with no I/O.
  ASSERT_EQ(lazy.name(0), kDefaultWideColumnName);
  ASSERT_FALSE(lazy.is_reference(0));
  ASSERT_EQ(lazy.inline_value(0), small);

  // Columns 1 and 2: unresolved blob references with known logical sizes
  // (uncompressed), reported without reading the blob payloads.
  ASSERT_EQ(lazy.name(1), "attr1");
  ASSERT_TRUE(lazy.is_reference(1));
  ASSERT_TRUE(lazy.logical_size_known(1));
  ASSERT_EQ(lazy.logical_size(1), big1.size());
  ASSERT_EQ(lazy.compression(1), kNoCompression);

  ASSERT_EQ(lazy.name(2), "attr2");
  ASSERT_TRUE(lazy.is_reference(2));
  ASSERT_TRUE(lazy.logical_size_known(2));
  ASSERT_EQ(lazy.logical_size(2), big2.size());

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
  ASSERT_OK(lazy.GetColumnRange(ReadOptions(), /*column_index=*/1,
                                /*offset=*/100, /*length=*/50, &range));
  ASSERT_EQ(range, Slice(big).ToString().substr(100, 50));

  // An offset at/past the end clamps to empty (not an error).
  PinnableSlice past_end;
  ASSERT_OK(lazy.GetColumnRange(ReadOptions(), /*column_index=*/1,
                                /*offset=*/big.size(), /*length=*/10,
                                &past_end));
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
  std::array<LazyColumnReadRequest, 3> reads;
  for (size_t i = 0; i < reads.size(); ++i) {
    reads[i].column_index = i + 1;  // columns 1..3 are the blob columns
    reads[i].offset = 0;
    reads[i].length = kLazyWholeColumn;
    reads[i].result = &results[i];
    reads[i].status = &statuses[i];
  }

  ASSERT_OK(lazy.MultiResolve(ReadOptions(), reads.size(), reads.data()));

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

  reads[0] = LazyColumnReadRequest{
      /*column_index=*/1, /*offset=*/0,
      /*length=*/50,      /*force_verify=*/false,
      &results[0],        &statuses[0]};
  reads[1] = LazyColumnReadRequest{
      /*column_index=*/1, /*offset=*/300,
      /*length=*/100,     /*force_verify=*/false,
      &results[1],        &statuses[1]};
  reads[2] = LazyColumnReadRequest{
      /*column_index=*/2,     /*offset=*/0, kLazyWholeColumn,
      /*force_verify=*/false, &results[2],  &statuses[2]};

  ASSERT_OK(lazy.MultiResolve(ReadOptions(), reads.size(), reads.data()));

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(results[0], a.substr(0, 50));
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(results[1], a.substr(300, 100));
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(results[2], b);
}

// The cross-key peer: MultiGetEntityLazy returns a LazyWideColumnsBatch (one
// shared SuperVersion pin, N per-key entities), and one
// LazyWideColumnsBatch::MultiResolve call resolves blobs across *all* of them
// in a single pass (coalesced per blob file across keys; parallel/async under
// the hood). Reads reference entities by index, so they cannot span batches /
// SVs.
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
  ASSERT_EQ(batch.num_entities(), 2U);

  // No blobs read yet -- only inline columns + references were materialized.
  ASSERT_EQ(BlobBytesRead(options), 0U);

  // One batch resolves the "data" blob column of both keys together. Reads name
  // their entity by index into the batch.
  std::array<PinnableSlice, 2> results;
  std::array<Status, 2> statuses;
  std::array<LazyBatchColumnReadRequest, 2> reads;
  for (size_t i = 0; i < reads.size(); ++i) {
    reads[i].entity_index = i;
    reads[i].column_index = 1;  // "data"
    reads[i].result = &results[i];
    reads[i].status = &statuses[i];
  }

  ASSERT_OK(batch.MultiResolve(ReadOptions(), reads.size(), reads.data()));

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
  ASSERT_OK(moved.GetColumn(ReadOptions(), /*column_index=*/1, &value));
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
  ASSERT_OK(lazy.GetColumn(ReadOptions(), /*column_index=*/2, &value));
  ASSERT_EQ(value, wanted);

  // Only ~200 bytes (the "wanted" blob) should have been read, never the 5000
  // bytes of "unwanted".
  ASSERT_GT(BlobBytesRead(options), 0U);
  ASSERT_LT(BlobBytesRead(options), unwanted.size());
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

// Under ReadTier::kBlockCacheTier, a reference whose bytes are not cached
// yields Incomplete instead of doing I/O.
// TODO(lazy-blob-resolution-phase1): enable once resolve-time ReadOptions are
// honored. The current phase resolves through the read options captured at
// GetEntityLazy() time; per-resolve read_tier / verify_checksums semantics land
// with the partial-read work.
TEST_F(DBLazyEntityTest, DISABLED_BlockCacheTierYieldsIncompleteOnMiss) {
  Options options = GetLazyTestOptions();
  DestroyAndReopen(options);

  constexpr char key[] = "entity";
  const std::string big(200, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "inline"}, {"data", big}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  LazyWideColumns lazy;
  ASSERT_OK(db_->GetEntityLazy(ReadOptions(), db_->DefaultColumnFamily(), key,
                               &lazy));

  ReadOptions block_cache_only;
  block_cache_only.read_tier = kBlockCacheTier;
  PinnableSlice value;
  const Status s = lazy.GetColumn(block_cache_only, /*column_index=*/1, &value);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
}

// Out-of-range column index is an InvalidArgument on the per-read status.
TEST_F(DBLazyEntityTest, OutOfRangeColumnIndexIsInvalidArgument) {
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

  PinnableSlice value;
  const Status s = lazy.GetColumn(ReadOptions(), /*column_index=*/99, &value);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
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
  ASSERT_EQ(lazy.num_columns(), eager.columns().size());
  ASSERT_EQ(lazy.num_columns(), 1U);
  ASSERT_EQ(lazy.name(0), kDefaultWideColumnName);

  PinnableSlice value;
  ASSERT_OK(lazy.GetColumn(read_opts, /*column_index=*/0, &value));
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
    ASSERT_EQ(lazy.num_columns(), 2U);

    PinnableSlice default_value;
    ASSERT_OK(
        lazy.GetColumn(ReadOptions(), /*column_index=*/0, &default_value));
    ASSERT_EQ(Slice(default_value), small);

    PinnableSlice attr_value;
    ASSERT_OK(lazy.GetColumn(ReadOptions(), /*column_index=*/1, &attr_value));
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
    ASSERT_EQ(batch.num_entities(), 1U);
    ASSERT_EQ(batch.entity(0).num_columns(), 2U);

    PinnableSlice attr_value;
    ASSERT_OK(batch.entity(0).GetColumn(ReadOptions(), /*column_index=*/1,
                                        &attr_value));
    ASSERT_EQ(Slice(attr_value), big);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
