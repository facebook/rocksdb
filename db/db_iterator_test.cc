//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <functional>

#include "db/arena_wrapped_db_iter.h"
#include "db/db_iter.h"
#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "table/block_based/flush_block_policy_impl.h"
#include "util/random.h"
#include "utilities/merge_operators/string_append/stringappend2.h"

namespace ROCKSDB_NAMESPACE {

// A dumb ReadCallback which saying every key is committed.
class DummyReadCallback : public ReadCallback {
 public:
  DummyReadCallback() : ReadCallback(kMaxSequenceNumber) {}
  bool IsVisibleFullCheck(SequenceNumber /*seq*/) override { return true; }
  void SetSnapshot(SequenceNumber seq) { max_visible_seq_ = seq; }
};

class DBIteratorBaseTest : public DBTestBase {
 public:
  DBIteratorBaseTest()
      : DBTestBase("db_iterator_test", /*env_do_fsync=*/true) {}
};

TEST_F(DBIteratorBaseTest, APICallsWithPerfContext) {
  // Set up the DB
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  Random rnd(301);
  for (int i = 1; i <= 3; i++) {
    ASSERT_OK(Put(std::to_string(i), std::to_string(i)));
  }

  // Setup iterator and PerfContext
  Iterator* iter = db_->NewIterator(ReadOptions());
  std::string key_str = std::to_string(2);
  Slice key(key_str);
  SetPerfLevel(kEnableCount);
  get_perf_context()->Reset();

  // Initial PerfContext counters
  ASSERT_EQ(0, get_perf_context()->iter_seek_count);
  ASSERT_EQ(0, get_perf_context()->iter_next_count);
  ASSERT_EQ(0, get_perf_context()->iter_prev_count);

  // Test Seek-related API calls PerfContext counter
  iter->Seek(key);
  iter->SeekToFirst();
  iter->SeekToLast();
  iter->SeekForPrev(key);
  ASSERT_EQ(4, get_perf_context()->iter_seek_count);
  ASSERT_EQ(0, get_perf_context()->iter_next_count);
  ASSERT_EQ(0, get_perf_context()->iter_prev_count);

  // Test Next() calls PerfContext counter
  iter->Next();
  ASSERT_EQ(4, get_perf_context()->iter_seek_count);
  ASSERT_EQ(1, get_perf_context()->iter_next_count);
  ASSERT_EQ(0, get_perf_context()->iter_prev_count);

  // Test Prev() calls PerfContext counter
  iter->Prev();
  ASSERT_EQ(4, get_perf_context()->iter_seek_count);
  ASSERT_EQ(1, get_perf_context()->iter_next_count);
  ASSERT_EQ(1, get_perf_context()->iter_prev_count);

  delete iter;
}

// Test param:
//   bool: whether to pass read_callback to NewIterator().
class DBIteratorTest : public DBIteratorBaseTest,
                       public testing::WithParamInterface<bool> {
 public:
  DBIteratorTest() = default;

  Iterator* NewIterator(const ReadOptions& read_options,
                        ColumnFamilyHandle* column_family = nullptr) {
    if (column_family == nullptr) {
      column_family = db_->DefaultColumnFamily();
    }
    auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    auto* cfd = cfh->cfd();
    SequenceNumber seq = read_options.snapshot != nullptr
                             ? read_options.snapshot->GetSequenceNumber()
                             : db_->GetLatestSequenceNumber();
    bool use_read_callback = GetParam();
    DummyReadCallback* read_callback = nullptr;
    if (use_read_callback) {
      read_callback = new DummyReadCallback();
      read_callback->SetSnapshot(seq);
      InstrumentedMutexLock lock(&mutex_);
      read_callbacks_.push_back(
          std::unique_ptr<DummyReadCallback>(read_callback));
    }
    DBImpl* db_impl = dbfull();
    SuperVersion* super_version = cfd->GetReferencedSuperVersion(db_impl);
    return db_impl->NewIteratorImpl(read_options, cfh, super_version, seq,
                                    read_callback);
  }

 private:
  InstrumentedMutex mutex_;
  std::vector<std::unique_ptr<DummyReadCallback>> read_callbacks_;
};

TEST_P(DBIteratorTest, IteratorProperty) {
  // The test needs to be changed if kPersistedTier is supported in iterator.
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_OK(Put(1, "1", "2"));
  ASSERT_OK(Delete(1, "2"));
  ReadOptions ropt;
  ropt.pin_data = false;
  {
    std::unique_ptr<Iterator> iter(NewIterator(ropt, handles_[1]));
    iter->SeekToFirst();
    std::string prop_value;
    ASSERT_NOK(iter->GetProperty("non_existing.value", &prop_value));
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("0", prop_value);
    ASSERT_OK(
        iter->GetProperty("rocksdb.iterator.is-value-pinned", &prop_value));
    ASSERT_EQ("0", prop_value);
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.internal-key", &prop_value));
    ASSERT_EQ("1", prop_value);
    iter->Next();
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("Iterator is not valid.", prop_value);
    ASSERT_OK(
        iter->GetProperty("rocksdb.iterator.is-value-pinned", &prop_value));
    ASSERT_EQ("Iterator is not valid.", prop_value);

    // Get internal key at which the iteration stopped (tombstone in this case).
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.internal-key", &prop_value));
    ASSERT_EQ("2", prop_value);

    prop_value.clear();
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.write-time", &prop_value));
    uint64_t write_time;
    Slice prop_slice = prop_value;
    ASSERT_TRUE(GetFixed64(&prop_slice, &write_time));
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), write_time);
  }
  Close();
}

TEST_P(DBIteratorTest, PersistedTierOnIterator) {
  // The test needs to be changed if kPersistedTier is supported in iterator.
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  ReadOptions ropt;
  ropt.read_tier = kPersistedTier;

  auto* iter = db_->NewIterator(ropt, handles_[1]);
  ASSERT_TRUE(iter->status().IsNotSupported());
  delete iter;

  std::vector<Iterator*> iters;
  ASSERT_TRUE(db_->NewIterators(ropt, {handles_[1]}, &iters).IsNotSupported());
  Close();
}

TEST_P(DBIteratorTest, NonBlockingIteration) {
  do {
    ReadOptions non_blocking_opts, regular_opts;
    anon::OptionsOverride options_override;
    options_override.full_block_cache = true;
    Options options = CurrentOptions(options_override);
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    non_blocking_opts.read_tier = kBlockCacheTier;

    CreateAndReopenWithCF({"pikachu"}, options);
    // write one kv to the database.
    ASSERT_OK(Put(1, "a", "b"));

    // scan using non-blocking iterator. We should find it because
    // it is in memtable.
    Iterator* iter = NewIterator(non_blocking_opts, handles_[1]);
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 1);
    delete iter;

    // flush memtable to storage. Now, the key should not be in the
    // memtable neither in the block cache.
    ASSERT_OK(Flush(1));

    // verify that a non-blocking iterator does not find any
    // kvs. Neither does it do any IOs to storage.
    uint64_t numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    uint64_t cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      count++;
    }
    ASSERT_EQ(count, 0);
    ASSERT_TRUE(iter->status().IsIncomplete());
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;

    // read in the specified block via a regular get
    ASSERT_EQ(Get(1, "a"), "b");

    // verify that we can find it via a non-blocking scan
    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 1);
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;

    // This test verifies block cache behaviors, which is not used by plain
    // table format.
  } while (ChangeOptions(kSkipPlainTable | kSkipNoSeekToLast | kSkipMmapReads));
}

TEST_P(DBIteratorTest, IterSeekBeforePrev) {
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("0", "f"));
  ASSERT_OK(Put("1", "h"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("2", "j"));
  auto iter = NewIterator(ReadOptions());
  iter->Seek(Slice("c"));
  iter->Prev();
  iter->Seek(Slice("a"));
  iter->Prev();
  delete iter;
}

TEST_P(DBIteratorTest, IterReseekNewUpperBound) {
  Random rnd(301);
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  table_options.block_size_deviation = 50;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.compression = kNoCompression;
  Reopen(options);

  ASSERT_OK(Put("a", rnd.RandomString(400)));
  ASSERT_OK(Put("aabb", rnd.RandomString(400)));
  ASSERT_OK(Put("aaef", rnd.RandomString(400)));
  ASSERT_OK(Put("b", rnd.RandomString(400)));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ReadOptions opts;
  Slice ub = Slice("aa");
  opts.iterate_upper_bound = &ub;
  auto iter = NewIterator(opts);
  iter->Seek(Slice("a"));
  ub = Slice("b");
  iter->Seek(Slice("aabc"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "aaef");
  delete iter;
}

TEST_P(DBIteratorTest, IterSeekForPrevBeforeNext) {
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("0", "f"));
  ASSERT_OK(Put("1", "h"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("2", "j"));
  auto iter = NewIterator(ReadOptions());
  iter->SeekForPrev(Slice("0"));
  iter->Next();
  iter->SeekForPrev(Slice("1"));
  iter->Next();
  delete iter;
}

namespace {
std::string MakeLongKey(size_t length, char c) {
  return std::string(length, c);
}
}  // anonymous namespace

TEST_P(DBIteratorTest, IterLongKeys) {
  ASSERT_OK(Put(MakeLongKey(20, 0), "0"));
  ASSERT_OK(Put(MakeLongKey(32, 2), "2"));
  ASSERT_OK(Put("a", "b"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put(MakeLongKey(50, 1), "1"));
  ASSERT_OK(Put(MakeLongKey(127, 3), "3"));
  ASSERT_OK(Put(MakeLongKey(64, 4), "4"));
  auto iter = NewIterator(ReadOptions());

  // Create a key that needs to be skipped for Seq too new
  iter->Seek(MakeLongKey(20, 0));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(20, 0) + "->0");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(64, 4) + "->4");

  iter->SeekForPrev(MakeLongKey(127, 3));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  delete iter;

  iter = NewIterator(ReadOptions());
  iter->Seek(MakeLongKey(50, 1));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  delete iter;
}

TEST_P(DBIteratorTest, IterNextWithNewerSeq) {
  ASSERT_OK(Put("0", "0"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("d", "e"));
  auto iter = NewIterator(ReadOptions());

  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Seek(Slice("a"));
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->d");
  iter->SeekForPrev(Slice("b"));
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->d");

  delete iter;
}

TEST_P(DBIteratorTest, IterPrevWithNewerSeq) {
  ASSERT_OK(Put("0", "0"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("d", "e"));
  auto iter = NewIterator(ReadOptions());

  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Seek(Slice("d"));
  ASSERT_EQ(IterStatus(iter), "d->e");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "c->d");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Prev();
  iter->SeekForPrev(Slice("d"));
  ASSERT_EQ(IterStatus(iter), "d->e");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "c->d");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Prev();
  delete iter;
}

TEST_P(DBIteratorTest, IterPrevWithNewerSeq2) {
  ASSERT_OK(Put("0", "0"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("e", "f"));
  auto iter = NewIterator(ReadOptions());
  auto iter2 = NewIterator(ReadOptions());
  iter->Seek(Slice("c"));
  iter2->SeekForPrev(Slice("d"));
  ASSERT_EQ(IterStatus(iter), "c->d");
  ASSERT_EQ(IterStatus(iter2), "c->d");

  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Prev();
  iter2->Prev();
  ASSERT_EQ(IterStatus(iter2), "a->b");
  iter2->Prev();
  delete iter;
  delete iter2;
}

TEST_P(DBIteratorTest, IterEmpty) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("foo");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekForPrev("foo");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    ASSERT_OK(iter->status());

    delete iter;
  } while (ChangeCompactOptions());
}

TEST_P(DBIteratorTest, IterSingle) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "va"));
    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("b");
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("b");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

TEST_P(DBIteratorTest, IterMulti) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "va"));
    ASSERT_OK(Put(1, "b", "vb"));
    ASSERT_OK(Put(1, "c", "vc"));
    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Seek("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Seek("ax");
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->SeekForPrev("d");
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->SeekForPrev("c");
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->SeekForPrev("bx");
    ASSERT_EQ(IterStatus(iter), "b->vb");

    iter->Seek("b");
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Seek("z");
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("b");
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->SeekForPrev("");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    // Switch from reverse to forward
    iter->SeekToLast();
    iter->Prev();
    iter->Prev();
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");

    // Switch from forward to reverse
    iter->SeekToFirst();
    iter->Next();
    iter->Next();
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");

    // Make sure iter stays at snapshot
    ASSERT_OK(Put(1, "a", "va2"));
    ASSERT_OK(Put(1, "a2", "va3"));
    ASSERT_OK(Put(1, "b", "vb2"));
    ASSERT_OK(Put(1, "c", "vc2"));
    ASSERT_OK(Delete(1, "b"));
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

// Check that we can skip over a run of user keys
// by using reseek rather than sequential scan
TEST_P(DBIteratorTest, IterReseek) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  Options options = CurrentOptions(options_override);
  options.max_sequential_skip_in_iterations = 3;
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // insert three keys with same userkey and verify that
  // reseek is not invoked. For each of these test cases,
  // verify that we can find the next key "b".
  ASSERT_OK(Put(1, "a", "zero"));
  ASSERT_OK(Put(1, "a", "one"));
  ASSERT_OK(Put(1, "a", "two"));
  ASSERT_OK(Put(1, "b", "bone"));
  Iterator* iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "a->two");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // insert a total of three keys with same userkey and verify
  // that reseek is still not invoked.
  ASSERT_OK(Put(1, "a", "three"));
  iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->three");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // insert a total of four keys with same userkey and verify
  // that reseek is invoked.
  ASSERT_OK(Put(1, "a", "four"));
  iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->four");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // Testing reverse iterator
  // At this point, we have three versions of "a" and one version of "b".
  // The reseek statistics is already at 1.
  int num_reseeks = static_cast<int>(
      TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION));

  // Insert another version of b and assert that reseek is not invoked
  ASSERT_OK(Put(1, "b", "btwo"));
  iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "b->btwo");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks);
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks + 1);
  ASSERT_EQ(IterStatus(iter), "a->four");
  delete iter;

  // insert two more versions of b. This makes a total of 4 versions
  // of b and 4 versions of a.
  ASSERT_OK(Put(1, "b", "bthree"));
  ASSERT_OK(Put(1, "b", "bfour"));
  iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "b->bfour");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks + 2);
  iter->Prev();

  // the previous Prev call should have invoked reseek
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks + 3);
  ASSERT_EQ(IterStatus(iter), "a->four");
  delete iter;
}

TEST_F(DBIteratorTest, ReseekUponDirectionChange) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.merge_operator.reset(
      new StringAppendTESTOperator(/*delim_char=*/' '));
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value"));
  ASSERT_OK(Put("bar", "value"));
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToLast();
    it->Prev();
    it->Next();
  }
  ASSERT_EQ(1,
            options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));

  const std::string merge_key("good");
  ASSERT_OK(Put(merge_key, "orig"));
  ASSERT_OK(Merge(merge_key, "suffix"));
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->Seek(merge_key);
    ASSERT_TRUE(it->Valid());
    const uint64_t prev_reseek_count =
        options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION);
    it->Prev();
    ASSERT_EQ(prev_reseek_count + 1, options.statistics->getTickerCount(
                                         NUMBER_OF_RESEEKS_IN_ITERATION));
  }
}

TEST_P(DBIteratorTest, IterSmallAndLargeMix) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "va"));
    ASSERT_OK(Put(1, "b", std::string(100000, 'b')));
    ASSERT_OK(Put(1, "c", "vc"));
    ASSERT_OK(Put(1, "d", std::string(100000, 'd')));
    ASSERT_OK(Put(1, "e", std::string(100000, 'e')));

    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

TEST_P(DBIteratorTest, IterMultiWithDelete) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "ka", "va"));
    ASSERT_OK(Put(1, "kb", "vb"));
    ASSERT_OK(Put(1, "kc", "vc"));
    ASSERT_OK(Delete(1, "kb"));
    ASSERT_EQ("NOT_FOUND", Get(1, "kb"));

    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);
    iter->Seek("kc");
    ASSERT_EQ(IterStatus(iter), "kc->vc");
    if (!CurrentOptions().merge_operator) {
      // TODO: merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_ &&
          kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
          kHashLinkList != option_config_ &&
          kHashSkipList != option_config_) {  // doesn't support SeekToLast
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "ka->va");
      }
    }
    delete iter;
  } while (ChangeOptions());
}

TEST_P(DBIteratorTest, IterPrevMaxSkip) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    for (int i = 0; i < 2; i++) {
      ASSERT_OK(Put(1, "key1", "v1"));
      ASSERT_OK(Put(1, "key2", "v2"));
      ASSERT_OK(Put(1, "key3", "v3"));
      ASSERT_OK(Put(1, "key4", "v4"));
      ASSERT_OK(Put(1, "key5", "v5"));
    }

    VerifyIterLast("key5->v5", 1);

    ASSERT_OK(Delete(1, "key5"));
    VerifyIterLast("key4->v4", 1);

    ASSERT_OK(Delete(1, "key4"));
    VerifyIterLast("key3->v3", 1);

    ASSERT_OK(Delete(1, "key3"));
    VerifyIterLast("key2->v2", 1);

    ASSERT_OK(Delete(1, "key2"));
    VerifyIterLast("key1->v1", 1);

    ASSERT_OK(Delete(1, "key1"));
    VerifyIterLast("(invalid)", 1);
  } while (ChangeOptions(kSkipMergePut | kSkipNoSeekToLast));
}

TEST_P(DBIteratorTest, IterWithSnapshot) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    ASSERT_OK(Put(1, "key1", "val1"));
    ASSERT_OK(Put(1, "key2", "val2"));
    ASSERT_OK(Put(1, "key3", "val3"));
    ASSERT_OK(Put(1, "key4", "val4"));
    ASSERT_OK(Put(1, "key5", "val5"));

    const Snapshot* snapshot = db_->GetSnapshot();
    ReadOptions options;
    options.snapshot = snapshot;
    Iterator* iter = NewIterator(options, handles_[1]);

    ASSERT_OK(Put(1, "key0", "val0"));
    // Put more values after the snapshot
    ASSERT_OK(Put(1, "key100", "val100"));
    ASSERT_OK(Put(1, "key101", "val101"));

    iter->Seek("key5");
    ASSERT_EQ(IterStatus(iter), "key5->val5");
    if (!CurrentOptions().merge_operator) {
      // TODO: merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_ &&
          kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
          kHashLinkList != option_config_ && kHashSkipList != option_config_) {
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key4->val4");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key3->val3");

        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key4->val4");
        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key5->val5");
      }
      iter->Next();
      ASSERT_TRUE(!iter->Valid());
    }

    if (!CurrentOptions().merge_operator) {
      // TODO(gzh): merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_ &&
          kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
          kHashLinkList != option_config_ && kHashSkipList != option_config_) {
        iter->SeekForPrev("key1");
        ASSERT_EQ(IterStatus(iter), "key1->val1");
        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key2->val2");
        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key3->val3");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key2->val2");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key1->val1");
        iter->Prev();
        ASSERT_TRUE(!iter->Valid());
      }
    }
    db_->ReleaseSnapshot(snapshot);
    ASSERT_OK(iter->status());
    delete iter;
  } while (ChangeOptions());
}

TEST_P(DBIteratorTest, IteratorPinsRef) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "hello"));

    // Get iterator that will yield the current contents of the DB.
    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    // Write to force compactions
    ASSERT_OK(Put(1, "foo", "newvalue1"));
    for (int i = 0; i < 100; i++) {
      // 100K values
      ASSERT_OK(Put(1, Key(i), Key(i) + std::string(100000, 'v')));
    }
    ASSERT_OK(Put(1, "foo", "newvalue2"));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo", iter->key().ToString());
    ASSERT_EQ("hello", iter->value().ToString());
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
  } while (ChangeCompactOptions());
}

TEST_P(DBIteratorTest, IteratorDeleteAfterCfDelete) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

  ASSERT_OK(Put(1, "foo", "delete-cf-then-delete-iter"));
  ASSERT_OK(Put(1, "hello", "value2"));

  ColumnFamilyHandle* cf = handles_[1];
  ReadOptions ro;

  auto* iter = db_->NewIterator(ro, cf);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "foo->delete-cf-then-delete-iter");

  // delete CF handle
  EXPECT_OK(db_->DestroyColumnFamilyHandle(cf));
  handles_.erase(std::begin(handles_) + 1);

  // delete Iterator after CF handle is deleted
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "hello->value2");
  delete iter;
}

TEST_P(DBIteratorTest, IteratorDeleteAfterCfDrop) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

  ASSERT_OK(Put(1, "foo", "drop-cf-then-delete-iter"));

  ReadOptions ro;
  ColumnFamilyHandle* cf = handles_[1];

  auto* iter = db_->NewIterator(ro, cf);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "foo->drop-cf-then-delete-iter");

  // drop and delete CF
  EXPECT_OK(db_->DropColumnFamily(cf));
  EXPECT_OK(db_->DestroyColumnFamilyHandle(cf));
  handles_.erase(std::begin(handles_) + 1);

  // delete Iterator after CF handle is dropped
  delete iter;
}

// SetOptions not defined in ROCKSDB LITE
TEST_P(DBIteratorTest, DBIteratorBoundTest) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;

  options.prefix_extractor = nullptr;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "0"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("g1", "0"));

  // testing basic case with no iterate_upper_bound and no prefix_extractor
  {
    ReadOptions ro;
    ro.iterate_upper_bound = nullptr;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("g1")), 0);

    iter->SeekForPrev("g1");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("g1")), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);
  }

  // testing iterate_upper_bound and forward iterator
  // to make sure it stops at bound
  {
    ReadOptions ro;
    // iterate_upper_bound points beyond the last expected entry
    Slice prefix("foo2");
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("foo1")), 0);

    iter->Next();
    // should stop here...
    ASSERT_TRUE(!iter->Valid());
  }
  // Testing SeekToLast with iterate_upper_bound set
  {
    ReadOptions ro;

    Slice prefix("foo");
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("a")), 0);
  }

  // prefix is the first letter of the key
  ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:1"}}));
  ASSERT_OK(Put("a", "0"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("g1", "0"));

  // testing with iterate_upper_bound and prefix_extractor
  // Seek target and iterate_upper_bound are not is same prefix
  // This should be an error
  {
    ReadOptions ro;
    Slice upper_bound("g");
    ro.iterate_upper_bound = &upper_bound;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo", iter->key().ToString());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo1", iter->key().ToString());

    iter->Next();
    ASSERT_TRUE(!iter->Valid());
  }

  // testing that iterate_upper_bound prevents iterating over deleted items
  // if the bound has already reached
  {
    options.prefix_extractor = nullptr;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_OK(Put("b", "0"));
    ASSERT_OK(Put("b1", "0"));
    ASSERT_OK(Put("c", "0"));
    ASSERT_OK(Put("d", "0"));
    ASSERT_OK(Put("e", "0"));
    ASSERT_OK(Delete("c"));
    ASSERT_OK(Delete("d"));

    // base case with no bound
    ReadOptions ro;
    ro.iterate_upper_bound = nullptr;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("b")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("b1")), 0);

    get_perf_context()->Reset();
    iter->Next();

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(
        static_cast<int>(get_perf_context()->internal_delete_skipped_count), 2);

    // now testing with iterate_bound
    Slice prefix("c");
    ro.iterate_upper_bound = &prefix;

    iter.reset(NewIterator(ro));

    get_perf_context()->Reset();

    iter->Seek("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("b")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("b1")), 0);

    iter->Next();
    // the iteration should stop as soon as the bound key is reached
    // even though the key is deleted
    // hence internal_delete_skipped_count should be 0
    ASSERT_TRUE(!iter->Valid());
    ASSERT_EQ(
        static_cast<int>(get_perf_context()->internal_delete_skipped_count), 0);
  }
}

TEST_P(DBIteratorTest, DBIteratorBoundMultiSeek) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.prefix_extractor = nullptr;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "0"));
  ASSERT_OK(Put("z", "0"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("foo2", "bar2"));
  ASSERT_OK(Put("foo3", "bar3"));
  ASSERT_OK(Put("foo4", "bar4"));

  {
    std::string up_str = "foo5";
    Slice up(up_str);
    ReadOptions ro;
    ro.iterate_upper_bound = &up;
    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo1");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);

    uint64_t prev_block_cache_hit =
        TestGetTickerCount(options, BLOCK_CACHE_HIT);
    uint64_t prev_block_cache_miss =
        TestGetTickerCount(options, BLOCK_CACHE_MISS);

    ASSERT_GT(prev_block_cache_hit + prev_block_cache_miss, 0);

    iter->Seek("foo4");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo4")), 0);
    ASSERT_EQ(prev_block_cache_hit,
              TestGetTickerCount(options, BLOCK_CACHE_HIT));
    ASSERT_EQ(prev_block_cache_miss,
              TestGetTickerCount(options, BLOCK_CACHE_MISS));

    iter->Seek("foo2");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo2")), 0);
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo3")), 0);
    ASSERT_EQ(prev_block_cache_hit,
              TestGetTickerCount(options, BLOCK_CACHE_HIT));
    ASSERT_EQ(prev_block_cache_miss,
              TestGetTickerCount(options, BLOCK_CACHE_MISS));
  }
}

TEST_P(DBIteratorTest, DBIteratorBoundOptimizationTest) {
  for (auto format_version : {2, 3, 4}) {
    int upper_bound_hits = 0;
    Options options = CurrentOptions();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTableIterator:out_of_bound",
        [&upper_bound_hits](void*) { upper_bound_hits++; });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    options.env = env_;
    options.create_if_missing = true;
    options.prefix_extractor = nullptr;
    BlockBasedTableOptions table_options;
    table_options.format_version = format_version;
    table_options.flush_block_policy_factory =
        std::make_shared<FlushBlockEveryKeyPolicyFactory>();
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    DestroyAndReopen(options);
    ASSERT_OK(Put("foo1", "bar1"));
    ASSERT_OK(Put("foo2", "bar2"));
    ASSERT_OK(Put("foo4", "bar4"));
    ASSERT_OK(Flush());

    Slice ub("foo3");
    ReadOptions ro;
    ro.iterate_upper_bound = &ub;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);
    ASSERT_EQ(upper_bound_hits, 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo2")), 0);
    ASSERT_EQ(upper_bound_hits, 0);

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(upper_bound_hits, 1);
  }
}

// Enable kBinarySearchWithFirstKey, do some iterator operations and check that
// they don't do unnecessary block reads.
TEST_P(DBIteratorTest, IndexWithFirstKey) {
  for (int tailing = 0; tailing < 2; ++tailing) {
    SCOPED_TRACE("tailing = " + std::to_string(tailing));
    Options options = CurrentOptions();
    options.env = env_;
    options.create_if_missing = true;
    options.prefix_extractor = nullptr;
    options.merge_operator = MergeOperators::CreateStringAppendOperator();
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    Statistics* stats = options.statistics.get();
    BlockBasedTableOptions table_options;
    table_options.index_type =
        BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
    table_options.index_shortening =
        BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
    table_options.flush_block_policy_factory =
        std::make_shared<FlushBlockEveryKeyPolicyFactory>();
    table_options.block_cache =
        NewLRUCache(8000);  // fits all blocks and their cache metadata overhead
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    DestroyAndReopen(options);
    ASSERT_OK(Merge("a1", "x1"));
    ASSERT_OK(Merge("b1", "y1"));
    ASSERT_OK(Merge("c0", "z1"));
    ASSERT_OK(Flush());
    ASSERT_OK(Merge("a2", "x2"));
    ASSERT_OK(Merge("b2", "y2"));
    ASSERT_OK(Merge("c0", "z2"));
    ASSERT_OK(Flush());
    ASSERT_OK(Merge("a3", "x3"));
    ASSERT_OK(Merge("b3", "y3"));
    ASSERT_OK(Merge("c3", "z3"));
    ASSERT_OK(Flush());

    // Block cache is not important for this test.
    // We use BLOCK_CACHE_DATA_* counters just because they're the most readily
    // available way of counting block accesses.

    ReadOptions ropt;
    ropt.tailing = tailing;
    std::unique_ptr<Iterator> iter(NewIterator(ropt));

    ropt.read_tier = ReadTier::kBlockCacheTier;
    std::unique_ptr<Iterator> nonblocking_iter(NewIterator(ropt));

    iter->Seek("b10");
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("b2", iter->key().ToString());
    EXPECT_EQ("y2", iter->value().ToString());
    EXPECT_EQ(1, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    // The cache-only iterator should succeed too, using the blocks pulled into
    // the cache by the previous iterator.
    nonblocking_iter->Seek("b10");
    ASSERT_TRUE(nonblocking_iter->Valid());
    EXPECT_EQ("b2", nonblocking_iter->key().ToString());
    EXPECT_EQ("y2", nonblocking_iter->value().ToString());
    EXPECT_EQ(1, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // ... but it shouldn't be able to step forward since the next block is
    // not in cache yet.
    nonblocking_iter->Next();
    ASSERT_FALSE(nonblocking_iter->Valid());
    ASSERT_TRUE(nonblocking_iter->status().IsIncomplete());

    // ... nor should a seek to the next key succeed.
    nonblocking_iter->Seek("b20");
    ASSERT_FALSE(nonblocking_iter->Valid());
    ASSERT_TRUE(nonblocking_iter->status().IsIncomplete());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("b3", iter->key().ToString());
    EXPECT_EQ("y3", iter->value().ToString());
    EXPECT_EQ(4, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(1, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // After the blocking iterator loaded the next block, the nonblocking
    // iterator's seek should succeed.
    nonblocking_iter->Seek("b20");
    ASSERT_TRUE(nonblocking_iter->Valid());
    EXPECT_EQ("b3", nonblocking_iter->key().ToString());
    EXPECT_EQ("y3", nonblocking_iter->value().ToString());
    EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    iter->Seek("c0");
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("c0", iter->key().ToString());
    EXPECT_EQ("z1,z2", iter->value().ToString());
    EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    EXPECT_EQ(6, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("c3", iter->key().ToString());
    EXPECT_EQ("z3", iter->value().ToString());
    EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    EXPECT_EQ(7, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    iter.reset();

    // Enable iterate_upper_bound and check that iterator is not trying to read
    // blocks that are fully above upper bound.
    std::string ub = "b3";
    Slice ub_slice(ub);
    ropt.iterate_upper_bound = &ub_slice;
    iter.reset(NewIterator(ropt));

    iter->Seek("b2");
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("b2", iter->key().ToString());
    EXPECT_EQ("y2", iter->value().ToString());
    EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    EXPECT_EQ(7, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    EXPECT_EQ(7, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
  }
}

TEST_P(DBIteratorTest, IndexWithFirstKeyGet) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor = nullptr;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  Statistics* stats = options.statistics.get();
  BlockBasedTableOptions table_options;
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
  table_options.index_shortening =
      BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  table_options.block_cache = NewLRUCache(1000);  // fits all blocks
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  DestroyAndReopen(options);
  ASSERT_OK(Merge("a", "x1"));
  ASSERT_OK(Merge("c", "y1"));
  ASSERT_OK(Merge("e", "z1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("c", "y2"));
  ASSERT_OK(Merge("e", "z2"));
  ASSERT_OK(Flush());

  // Get() between blocks shouldn't read any blocks.
  ASSERT_EQ("NOT_FOUND", Get("b"));
  EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
  EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

  // Get() of an existing key shouldn't read any unnecessary blocks when there's
  // only one key per block.

  ASSERT_EQ("y1,y2", Get("c"));
  EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
  EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

  ASSERT_EQ("x1", Get("a"));
  EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
  EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

  EXPECT_EQ(std::vector<std::string>({"NOT_FOUND", "z1,z2"}),
            MultiGet({"b", "e"}));
}

// TODO(3.13): fix the issue of Seek() + Prev() which might not necessary
//             return the biggest key which is smaller than the seek key.
TEST_P(DBIteratorTest, PrevAfterAndNextAfterMerge) {
  Options options;
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreatePutOperator();
  options.env = env_;
  DestroyAndReopen(options);

  // write three entries with different keys using Merge()
  WriteOptions wopts;
  ASSERT_OK(db_->Merge(wopts, "1", "data1"));
  ASSERT_OK(db_->Merge(wopts, "2", "data2"));
  ASSERT_OK(db_->Merge(wopts, "3", "data3"));

  std::unique_ptr<Iterator> it(NewIterator(ReadOptions()));

  it->Seek("2");
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("2", it->key().ToString());

  it->Prev();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("1", it->key().ToString());

  it->SeekForPrev("1");
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("1", it->key().ToString());

  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("2", it->key().ToString());
}

class DBIteratorTestForPinnedData : public DBIteratorTest {
 public:
  enum TestConfig {
    NORMAL,
    CLOSE_AND_OPEN,
    COMPACT_BEFORE_READ,
    FLUSH_EVERY_1000,
    MAX
  };
  DBIteratorTestForPinnedData() : DBIteratorTest() {}
  void PinnedDataIteratorRandomized(TestConfig run_config) {
    // Generate Random data
    Random rnd(301);

    int puts = 100000;
    int key_pool = static_cast<int>(puts * 0.7);
    int key_size = 100;
    int val_size = 1000;
    int seeks_percentage = 20;   // 20% of keys will be used to test seek()
    int delete_percentage = 20;  // 20% of keys will be deleted
    int merge_percentage = 20;   // 20% of keys will be added using Merge()

    Options options = CurrentOptions();
    BlockBasedTableOptions table_options;
    table_options.use_delta_encoding = false;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.merge_operator = MergeOperators::CreatePutOperator();
    DestroyAndReopen(options);

    std::vector<std::string> generated_keys(key_pool);
    for (int i = 0; i < key_pool; i++) {
      generated_keys[i] = rnd.RandomString(key_size);
    }

    std::map<std::string, std::string> true_data;
    std::vector<std::string> random_keys;
    std::vector<std::string> deleted_keys;
    for (int i = 0; i < puts; i++) {
      auto& k = generated_keys[rnd.Next() % key_pool];
      auto v = rnd.RandomString(val_size);

      // Insert data to true_data map and to DB
      true_data[k] = v;
      if (rnd.PercentTrue(merge_percentage)) {
        ASSERT_OK(db_->Merge(WriteOptions(), k, v));
      } else {
        ASSERT_OK(Put(k, v));
      }

      // Pick random keys to be used to test Seek()
      if (rnd.PercentTrue(seeks_percentage)) {
        random_keys.push_back(k);
      }

      // Delete some random keys
      if (rnd.PercentTrue(delete_percentage)) {
        deleted_keys.push_back(k);
        true_data.erase(k);
        ASSERT_OK(Delete(k));
      }

      if (run_config == TestConfig::FLUSH_EVERY_1000) {
        if (i && i % 1000 == 0) {
          ASSERT_OK(Flush());
        }
      }
    }

    if (run_config == TestConfig::CLOSE_AND_OPEN) {
      Close();
      Reopen(options);
    } else if (run_config == TestConfig::COMPACT_BEFORE_READ) {
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }

    ReadOptions ro;
    ro.pin_data = true;
    auto iter = NewIterator(ro);

    {
      // Test Seek to random keys
      std::vector<Slice> keys_slices;
      std::vector<std::string> true_keys;
      for (auto& k : random_keys) {
        iter->Seek(k);
        if (!iter->Valid()) {
          ASSERT_EQ(true_data.lower_bound(k), true_data.end());
          continue;
        }
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        keys_slices.push_back(iter->key());
        true_keys.push_back(true_data.lower_bound(k)->first);
      }

      for (size_t i = 0; i < keys_slices.size(); i++) {
        ASSERT_EQ(keys_slices[i].ToString(), true_keys[i]);
      }
    }

    {
      // Test SeekForPrev to random keys
      std::vector<Slice> keys_slices;
      std::vector<std::string> true_keys;
      for (auto& k : random_keys) {
        iter->SeekForPrev(k);
        if (!iter->Valid()) {
          ASSERT_EQ(true_data.upper_bound(k), true_data.begin());
          continue;
        }
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        keys_slices.push_back(iter->key());
        true_keys.push_back((--true_data.upper_bound(k))->first);
      }

      for (size_t i = 0; i < keys_slices.size(); i++) {
        ASSERT_EQ(keys_slices[i].ToString(), true_keys[i]);
      }
    }

    {
      // Test iterating all data forward
      std::vector<Slice> all_keys;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        all_keys.push_back(iter->key());
      }
      ASSERT_EQ(all_keys.size(), true_data.size());

      // Verify that all keys slices are valid
      auto data_iter = true_data.begin();
      for (size_t i = 0; i < all_keys.size(); i++) {
        ASSERT_EQ(all_keys[i].ToString(), data_iter->first);
        data_iter++;
      }
    }

    {
      // Test iterating all data backward
      std::vector<Slice> all_keys;
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        all_keys.push_back(iter->key());
      }
      ASSERT_OK(iter->status());
      ASSERT_EQ(all_keys.size(), true_data.size());

      // Verify that all keys slices are valid (backward)
      auto data_iter = true_data.rbegin();
      for (size_t i = 0; i < all_keys.size(); i++) {
        ASSERT_EQ(all_keys[i].ToString(), data_iter->first);
        data_iter++;
      }
    }

    delete iter;
  }
};

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
TEST_P(DBIteratorTestForPinnedData, PinnedDataIteratorRandomizedNormal) {
  PinnedDataIteratorRandomized(TestConfig::NORMAL);
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_P(DBIteratorTestForPinnedData, PinnedDataIteratorRandomizedCLoseAndOpen) {
  PinnedDataIteratorRandomized(TestConfig::CLOSE_AND_OPEN);
}

TEST_P(DBIteratorTestForPinnedData,
       PinnedDataIteratorRandomizedCompactBeforeRead) {
  PinnedDataIteratorRandomized(TestConfig::COMPACT_BEFORE_READ);
}

TEST_P(DBIteratorTestForPinnedData, PinnedDataIteratorRandomizedFlush) {
  PinnedDataIteratorRandomized(TestConfig::FLUSH_EVERY_1000);
}

INSTANTIATE_TEST_CASE_P(DBIteratorTestForPinnedDataInstance,
                        DBIteratorTestForPinnedData,
                        testing::Values(true, false));

TEST_P(DBIteratorTest, PinnedDataIteratorMultipleFiles) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.disable_auto_compactions = true;
  options.write_buffer_size = 1024 * 1024 * 10;  // 10 Mb
  DestroyAndReopen(options);

  std::map<std::string, std::string> true_data;

  // Generate 4 sst files in L2
  Random rnd(301);
  for (int i = 1; i <= 1000; i++) {
    std::string k = Key(i * 3);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
    if (i % 250 == 0) {
      ASSERT_OK(Flush());
    }
  }
  ASSERT_EQ(FilesPerLevel(0), "4");
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(FilesPerLevel(0), "0,4");

  // Generate 4 sst files in L0
  for (int i = 1; i <= 1000; i++) {
    std::string k = Key(i * 2);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
    if (i % 250 == 0) {
      ASSERT_OK(Flush());
    }
  }
  ASSERT_EQ(FilesPerLevel(0), "4,4");

  // Add some keys/values in memtables
  for (int i = 1; i <= 1000; i++) {
    std::string k = Key(i);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
  }
  ASSERT_EQ(FilesPerLevel(0), "4,4");

  ReadOptions ro;
  ro.pin_data = true;
  auto iter = NewIterator(ro);

  std::vector<std::pair<Slice, Slice>> results;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string prop_value;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    ASSERT_OK(
        iter->GetProperty("rocksdb.iterator.is-value-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    results.emplace_back(iter->key(), iter->value());
  }

  ASSERT_EQ(results.size(), true_data.size());
  auto data_iter = true_data.begin();
  for (size_t i = 0; i < results.size(); i++, data_iter++) {
    auto& kv = results[i];
    ASSERT_EQ(kv.first, data_iter->first);
    ASSERT_EQ(kv.second, data_iter->second);
  }
  ASSERT_OK(iter->status());
  delete iter;
}

TEST_P(DBIteratorTest, PinnedDataIteratorMergeOperator) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.merge_operator = MergeOperators::CreateUInt64AddOperator();
  DestroyAndReopen(options);

  std::string numbers[7];
  for (int val = 0; val <= 6; val++) {
    PutFixed64(numbers + val, val);
  }

  // +1 all keys in range [ 0 => 999]
  for (int i = 0; i < 1000; i++) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[1]));
  }

  // +2 all keys divisible by 2 in range [ 0 => 999]
  for (int i = 0; i < 1000; i += 2) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[2]));
  }

  // +3 all keys divisible by 5 in range [ 0 => 999]
  for (int i = 0; i < 1000; i += 5) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[3]));
  }

  ReadOptions ro;
  ro.pin_data = true;
  auto iter = NewIterator(ro);

  std::vector<std::pair<Slice, std::string>> results;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string prop_value;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    ASSERT_OK(
        iter->GetProperty("rocksdb.iterator.is-value-pinned", &prop_value));
    ASSERT_EQ("0", prop_value);
    results.emplace_back(iter->key(), iter->value().ToString());
  }
  ASSERT_OK(iter->status());

  ASSERT_EQ(results.size(), 1000);
  for (size_t i = 0; i < results.size(); i++) {
    auto& kv = results[i];
    ASSERT_EQ(kv.first, Key(static_cast<int>(i)));
    int expected_val = 1;
    if (i % 2 == 0) {
      expected_val += 2;
    }
    if (i % 5 == 0) {
      expected_val += 3;
    }
    ASSERT_EQ(kv.second, numbers[expected_val]);
  }

  delete iter;
}

TEST_P(DBIteratorTest, PinnedDataIteratorReadAfterUpdate) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.write_buffer_size = 100000;
  DestroyAndReopen(options);

  Random rnd(301);

  std::map<std::string, std::string> true_data;
  for (int i = 0; i < 1000; i++) {
    std::string k = rnd.RandomString(10);
    std::string v = rnd.RandomString(1000);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
  }

  ReadOptions ro;
  ro.pin_data = true;
  auto iter = NewIterator(ro);

  // Delete 50% of the keys and update the other 50%
  for (auto& kv : true_data) {
    if (rnd.OneIn(2)) {
      ASSERT_OK(Delete(kv.first));
    } else {
      std::string new_val = rnd.RandomString(1000);
      ASSERT_OK(Put(kv.first, new_val));
    }
  }

  std::vector<std::pair<Slice, Slice>> results;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string prop_value;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    ASSERT_OK(
        iter->GetProperty("rocksdb.iterator.is-value-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    results.emplace_back(iter->key(), iter->value());
  }
  ASSERT_OK(iter->status());

  auto data_iter = true_data.begin();
  for (size_t i = 0; i < results.size(); i++, data_iter++) {
    auto& kv = results[i];
    ASSERT_EQ(kv.first, data_iter->first);
    ASSERT_EQ(kv.second, data_iter->second);
  }

  delete iter;
}

class SliceTransformLimitedDomainGeneric : public SliceTransform {
  const char* Name() const override {
    return "SliceTransformLimitedDomainGeneric";
  }

  Slice Transform(const Slice& src) const override {
    return Slice(src.data(), 1);
  }

  bool InDomain(const Slice& src) const override {
    // prefix will be x????
    return src.size() >= 1;
  }

  bool InRange(const Slice& dst) const override {
    // prefix will be x????
    return dst.size() == 1;
  }
};

TEST_P(DBIteratorTest, IterSeekForPrevCrossingFiles) {
  Options options = CurrentOptions();
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  options.disable_auto_compactions = true;
  // Enable prefix bloom for SST files
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "va1"));
  ASSERT_OK(Put("a2", "va2"));
  ASSERT_OK(Put("a3", "va3"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("b1", "vb1"));
  ASSERT_OK(Put("b2", "vb2"));
  ASSERT_OK(Put("b3", "vb3"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("b4", "vb4"));
  ASSERT_OK(Put("d1", "vd1"));
  ASSERT_OK(Put("d2", "vd2"));
  ASSERT_OK(Put("d4", "vd4"));
  ASSERT_OK(Flush());

  MoveFilesToLevel(1);
  {
    ReadOptions ro;
    Iterator* iter = NewIterator(ro);

    iter->SeekForPrev("a4");
    ASSERT_EQ(iter->key().ToString(), "a3");
    ASSERT_EQ(iter->value().ToString(), "va3");

    iter->SeekForPrev("c2");
    ASSERT_EQ(iter->key().ToString(), "b3");
    iter->SeekForPrev("d3");
    ASSERT_EQ(iter->key().ToString(), "d2");
    iter->SeekForPrev("b5");
    ASSERT_EQ(iter->key().ToString(), "b4");
    delete iter;
  }

  {
    ReadOptions ro;
    ro.prefix_same_as_start = true;
    Iterator* iter = NewIterator(ro);
    iter->SeekForPrev("c2");
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
    delete iter;
  }
}

TEST_P(DBIteratorTest, IterSeekForPrevCrossingFilesCustomPrefixExtractor) {
  Options options = CurrentOptions();
  options.prefix_extractor =
      std::make_shared<SliceTransformLimitedDomainGeneric>();
  options.disable_auto_compactions = true;
  // Enable prefix bloom for SST files
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "va1"));
  ASSERT_OK(Put("a2", "va2"));
  ASSERT_OK(Put("a3", "va3"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("b1", "vb1"));
  ASSERT_OK(Put("b2", "vb2"));
  ASSERT_OK(Put("b3", "vb3"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("b4", "vb4"));
  ASSERT_OK(Put("d1", "vd1"));
  ASSERT_OK(Put("d2", "vd2"));
  ASSERT_OK(Put("d4", "vd4"));
  ASSERT_OK(Flush());

  MoveFilesToLevel(1);
  {
    ReadOptions ro;
    Iterator* iter = NewIterator(ro);

    iter->SeekForPrev("a4");
    ASSERT_EQ(iter->key().ToString(), "a3");
    ASSERT_EQ(iter->value().ToString(), "va3");

    iter->SeekForPrev("c2");
    ASSERT_EQ(iter->key().ToString(), "b3");
    iter->SeekForPrev("d3");
    ASSERT_EQ(iter->key().ToString(), "d2");
    iter->SeekForPrev("b5");
    ASSERT_EQ(iter->key().ToString(), "b4");
    delete iter;
  }

  {
    ReadOptions ro;
    ro.prefix_same_as_start = true;
    Iterator* iter = NewIterator(ro);
    iter->SeekForPrev("c2");
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
    delete iter;
  }
}

TEST_P(DBIteratorTest, IterPrevKeyCrossingBlocks) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.block_size = 1;  // every block will contain one entry
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  options.disable_auto_compactions = true;
  options.max_sequential_skip_in_iterations = 8;

  DestroyAndReopen(options);

  // Putting such deletes will force DBIter::Prev() to fallback to a Seek
  for (int file_num = 0; file_num < 10; file_num++) {
    ASSERT_OK(Delete("key4"));
    ASSERT_OK(Flush());
  }

  // First File containing 5 blocks of puts
  ASSERT_OK(Put("key1", "val1.0"));
  ASSERT_OK(Put("key2", "val2.0"));
  ASSERT_OK(Put("key3", "val3.0"));
  ASSERT_OK(Put("key4", "val4.0"));
  ASSERT_OK(Put("key5", "val5.0"));
  ASSERT_OK(Flush());

  // Second file containing 9 blocks of merge operands
  ASSERT_OK(db_->Merge(WriteOptions(), "key1", "val1.1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key1", "val1.2"));

  ASSERT_OK(db_->Merge(WriteOptions(), "key2", "val2.1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key2", "val2.2"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key2", "val2.3"));

  ASSERT_OK(db_->Merge(WriteOptions(), "key3", "val3.1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key3", "val3.2"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key3", "val3.3"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key3", "val3.4"));
  ASSERT_OK(Flush());

  {
    ReadOptions ro;
    ro.fill_cache = false;
    Iterator* iter = NewIterator(ro);

    iter->SeekToLast();
    ASSERT_EQ(iter->key().ToString(), "key5");
    ASSERT_EQ(iter->value().ToString(), "val5.0");

    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "key4");
    ASSERT_EQ(iter->value().ToString(), "val4.0");

    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "key3");
    ASSERT_EQ(iter->value().ToString(), "val3.0,val3.1,val3.2,val3.3,val3.4");

    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "key2");
    ASSERT_EQ(iter->value().ToString(), "val2.0,val2.1,val2.2,val2.3");

    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "key1");
    ASSERT_EQ(iter->value().ToString(), "val1.0,val1.1,val1.2");

    delete iter;
  }
}

TEST_P(DBIteratorTest, IterPrevKeyCrossingBlocksRandomized) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  options.disable_auto_compactions = true;
  options.level0_slowdown_writes_trigger = (1 << 30);
  options.level0_stop_writes_trigger = (1 << 30);
  options.max_sequential_skip_in_iterations = 8;
  DestroyAndReopen(options);

  const int kNumKeys = 500;
  // Small number of merge operands to make sure that DBIter::Prev() don't
  // fall back to Seek()
  const int kNumMergeOperands = 3;
  // Use value size that will make sure that every block contain 1 key
  const int kValSize =
      static_cast<int>(BlockBasedTableOptions().block_size) * 4;
  // Percentage of keys that wont get merge operations
  const int kNoMergeOpPercentage = 20;
  // Percentage of keys that will be deleted
  const int kDeletePercentage = 10;

  // For half of the key range we will write multiple deletes first to
  // force DBIter::Prev() to fall back to Seek()
  for (int file_num = 0; file_num < 10; file_num++) {
    for (int i = 0; i < kNumKeys; i += 2) {
      ASSERT_OK(Delete(Key(i)));
    }
    ASSERT_OK(Flush());
  }

  Random rnd(301);
  std::map<std::string, std::string> true_data;
  std::string gen_key;
  std::string gen_val;

  for (int i = 0; i < kNumKeys; i++) {
    gen_key = Key(i);
    gen_val = rnd.RandomString(kValSize);

    ASSERT_OK(Put(gen_key, gen_val));
    true_data[gen_key] = gen_val;
  }
  ASSERT_OK(Flush());

  // Separate values and merge operands in different file so that we
  // make sure that we don't merge them while flushing but actually
  // merge them in the read path
  for (int i = 0; i < kNumKeys; i++) {
    if (rnd.PercentTrue(kNoMergeOpPercentage)) {
      // Dont give merge operations for some keys
      continue;
    }

    for (int j = 0; j < kNumMergeOperands; j++) {
      gen_key = Key(i);
      gen_val = rnd.RandomString(kValSize);

      ASSERT_OK(db_->Merge(WriteOptions(), gen_key, gen_val));
      true_data[gen_key] += "," + gen_val;
    }
  }
  ASSERT_OK(Flush());

  for (int i = 0; i < kNumKeys; i++) {
    if (rnd.PercentTrue(kDeletePercentage)) {
      gen_key = Key(i);

      ASSERT_OK(Delete(gen_key));
      true_data.erase(gen_key);
    }
  }
  ASSERT_OK(Flush());

  {
    ReadOptions ro;
    ro.fill_cache = false;
    Iterator* iter = NewIterator(ro);
    auto data_iter = true_data.rbegin();

    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      ASSERT_EQ(iter->key().ToString(), data_iter->first);
      ASSERT_EQ(iter->value().ToString(), data_iter->second);
      data_iter++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(data_iter, true_data.rend());

    delete iter;
  }

  {
    ReadOptions ro;
    ro.fill_cache = false;
    Iterator* iter = NewIterator(ro);
    auto data_iter = true_data.rbegin();

    int entries_right = 0;
    std::string seek_key;
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      // Verify key/value of current position
      ASSERT_EQ(iter->key().ToString(), data_iter->first);
      ASSERT_EQ(iter->value().ToString(), data_iter->second);

      bool restore_position_with_seek = rnd.Uniform(2);
      if (restore_position_with_seek) {
        seek_key = iter->key().ToString();
      }

      // Do some Next() operations the restore the iterator to orignal position
      int next_count =
          entries_right > 0 ? rnd.Uniform(std::min(entries_right, 10)) : 0;
      for (int i = 0; i < next_count; i++) {
        iter->Next();
        data_iter--;

        ASSERT_EQ(iter->key().ToString(), data_iter->first);
        ASSERT_EQ(iter->value().ToString(), data_iter->second);
      }

      if (restore_position_with_seek) {
        // Restore orignal position using Seek()
        iter->Seek(seek_key);
        for (int i = 0; i < next_count; i++) {
          data_iter++;
        }

        ASSERT_EQ(iter->key().ToString(), data_iter->first);
        ASSERT_EQ(iter->value().ToString(), data_iter->second);
      } else {
        // Restore original position using Prev()
        for (int i = 0; i < next_count; i++) {
          iter->Prev();
          data_iter++;

          ASSERT_EQ(iter->key().ToString(), data_iter->first);
          ASSERT_EQ(iter->value().ToString(), data_iter->second);
        }
      }

      entries_right++;
      data_iter++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(data_iter, true_data.rend());

    delete iter;
  }
}

TEST_P(DBIteratorTest, IteratorWithLocalStatistics) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 1000; i++) {
    // Key 10 bytes / Value 10 bytes
    ASSERT_OK(Put(rnd.RandomString(10), rnd.RandomString(10)));
  }

  std::atomic<uint64_t> total_next(0);
  std::atomic<uint64_t> total_next_found(0);
  std::atomic<uint64_t> total_prev(0);
  std::atomic<uint64_t> total_prev_found(0);
  std::atomic<uint64_t> total_bytes(0);

  std::vector<port::Thread> threads;
  std::function<void()> reader_func_next = [&]() {
    SetPerfLevel(kEnableCount);
    get_perf_context()->Reset();
    Iterator* iter = NewIterator(ReadOptions());

    iter->SeekToFirst();
    // Seek will bump ITER_BYTES_READ
    uint64_t bytes = 0;
    bytes += iter->key().size();
    bytes += iter->value().size();
    while (true) {
      iter->Next();
      total_next++;

      if (!iter->Valid()) {
        EXPECT_OK(iter->status());
        break;
      }
      total_next_found++;
      bytes += iter->key().size();
      bytes += iter->value().size();
    }

    delete iter;
    ASSERT_EQ(bytes, get_perf_context()->iter_read_bytes);
    SetPerfLevel(kDisable);
    total_bytes += bytes;
  };

  std::function<void()> reader_func_prev = [&]() {
    SetPerfLevel(kEnableCount);
    Iterator* iter = NewIterator(ReadOptions());

    iter->SeekToLast();
    // Seek will bump ITER_BYTES_READ
    uint64_t bytes = 0;
    bytes += iter->key().size();
    bytes += iter->value().size();
    while (true) {
      iter->Prev();
      total_prev++;

      if (!iter->Valid()) {
        EXPECT_OK(iter->status());
        break;
      }
      total_prev_found++;
      bytes += iter->key().size();
      bytes += iter->value().size();
    }

    delete iter;
    ASSERT_EQ(bytes, get_perf_context()->iter_read_bytes);
    SetPerfLevel(kDisable);
    total_bytes += bytes;
  };

  for (int i = 0; i < 10; i++) {
    threads.emplace_back(reader_func_next);
  }
  for (int i = 0; i < 15; i++) {
    threads.emplace_back(reader_func_prev);
  }

  for (auto& t : threads) {
    t.join();
  }

  ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_NEXT), (uint64_t)total_next);
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_NEXT_FOUND),
            (uint64_t)total_next_found);
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_PREV), (uint64_t)total_prev);
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_PREV_FOUND),
            (uint64_t)total_prev_found);
  ASSERT_EQ(TestGetTickerCount(options, ITER_BYTES_READ),
            (uint64_t)total_bytes);
}

TEST_P(DBIteratorTest, ReadAhead) {
  Options options;
  env_->count_random_reads_ = true;
  options.env = env_;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 4 << 20;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  table_options.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  std::string value(1024, 'a');
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());
  ASSERT_EQ("1,1,1", FilesPerLevel());

  env_->random_read_bytes_counter_ = 0;
  options.statistics->setTickerCount(NO_FILE_OPENS, 0);
  ReadOptions read_options;
  auto* iter = NewIterator(read_options);
  iter->SeekToFirst();
  int64_t num_file_opens = TestGetTickerCount(options, NO_FILE_OPENS);
  size_t bytes_read = env_->random_read_bytes_counter_;
  delete iter;

  env_->random_read_bytes_counter_ = 0;
  options.statistics->setTickerCount(NO_FILE_OPENS, 0);
  read_options.readahead_size = 1024 * 10;
  iter = NewIterator(read_options);
  iter->SeekToFirst();
  int64_t num_file_opens_readahead = TestGetTickerCount(options, NO_FILE_OPENS);
  size_t bytes_read_readahead = env_->random_read_bytes_counter_;
  delete iter;
  ASSERT_EQ(num_file_opens, num_file_opens_readahead);
  ASSERT_GT(bytes_read_readahead, bytes_read);
  ASSERT_GT(bytes_read_readahead, read_options.readahead_size * 3);

  // Verify correctness.
  iter = NewIterator(read_options);
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(value, iter->value());
    count++;
  }
  ASSERT_EQ(100, count);
  for (int i = 0; i < 100; i++) {
    iter->Seek(Key(i));
    ASSERT_EQ(value, iter->value());
  }
  delete iter;
}

// Insert a key, create a snapshot iterator, overwrite key lots of times,
// seek to a smaller key. Expect DBIter to fall back to a seek instead of
// going through all the overwrites linearly.
TEST_P(DBIteratorTest, DBIteratorSkipRecentDuplicatesTest) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.max_sequential_skip_in_iterations = 3;
  options.prefix_extractor = nullptr;
  options.write_buffer_size = 1 << 27;  // big enough to avoid flush
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);

  // Insert.
  ASSERT_OK(Put("b", "0"));

  // Create iterator.
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(NewIterator(ro));

  // Insert a lot.
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put("b", std::to_string(i + 1).c_str()));
  }

  // Check that memtable wasn't flushed.
  std::string val;
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &val));
  EXPECT_EQ("0", val);

  // Seek iterator to a smaller key.
  get_perf_context()->Reset();
  iter->Seek("a");
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ("b", iter->key().ToString());
  EXPECT_EQ("0", iter->value().ToString());

  // Check that the seek didn't do too much work.
  // Checks are not tight, just make sure that everything is well below 100.
  EXPECT_LT(get_perf_context()->internal_key_skipped_count, 4);
  EXPECT_LT(get_perf_context()->internal_recent_skipped_count, 8);
  EXPECT_LT(get_perf_context()->seek_on_memtable_count, 10);
  EXPECT_LT(get_perf_context()->next_on_memtable_count, 10);
  EXPECT_LT(get_perf_context()->prev_on_memtable_count, 10);

  // Check that iterator did something like what we expect.
  EXPECT_EQ(get_perf_context()->internal_delete_skipped_count, 0);
  EXPECT_EQ(get_perf_context()->internal_merge_count, 0);
  EXPECT_GE(get_perf_context()->internal_recent_skipped_count, 2);
  EXPECT_GE(get_perf_context()->seek_on_memtable_count, 2);
  EXPECT_EQ(1,
            options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));
}

TEST_P(DBIteratorTest, Refresh) {
  ASSERT_OK(Put("x", "y"));

  std::unique_ptr<Iterator> iter(NewIterator(ReadOptions()));
  ASSERT_OK(iter->status());
  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  ASSERT_OK(Put("c", "d"));

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  ASSERT_OK(iter->status());
  ASSERT_OK(iter->Refresh());

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("c")), 0);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  EXPECT_OK(dbfull()->Flush(FlushOptions()));

  ASSERT_OK(Put("m", "n"));

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("c")), 0);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  ASSERT_OK(iter->status());
  ASSERT_OK(iter->Refresh());

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("c")), 0);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("m")), 0);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  iter.reset();
}

TEST_P(DBIteratorTest, RefreshWithSnapshot) {
  // L1 file, uses LevelIterator internally
  ASSERT_OK(Put(Key(0), "val0"));
  ASSERT_OK(Put(Key(5), "val5"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  // L0 file, uses table iterator internally
  ASSERT_OK(Put(Key(1), "val1"));
  ASSERT_OK(Put(Key(4), "val4"));
  ASSERT_OK(Flush());

  // Memtable
  ASSERT_OK(Put(Key(2), "val2"));
  ASSERT_OK(Put(Key(3), "val3"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put(Key(2), "new val"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(4),
                             Key(7)));
  const Snapshot* snapshot2 = db_->GetSnapshot();

  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  ReadOptions options;
  options.snapshot = snapshot;
  Iterator* iter = NewIterator(options);
  ASSERT_OK(Put(Key(6), "val6"));
  ASSERT_OK(iter->status());

  auto verify_iter = [&](int start, int end, bool new_key2 = false) {
    for (int i = start; i < end; ++i) {
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), Key(i));
      if (i == 2 && new_key2) {
        ASSERT_EQ(iter->value(), "new val");
      } else {
        ASSERT_EQ(iter->value(), "val" + std::to_string(i));
      }
      iter->Next();
    }
  };

  for (int j = 0; j < 2; j++) {
    iter->Seek(Key(1));
    verify_iter(1, 3);
    // Refresh to same snapshot
    ASSERT_OK(iter->Refresh(snapshot));
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());
    iter->Seek(Key(3));
    verify_iter(3, 6);
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());

    // Refresh to a newer snapshot
    ASSERT_OK(iter->Refresh(snapshot2));
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());
    iter->SeekToFirst();
    verify_iter(0, 4, /*new_key2=*/true);
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());

    // Refresh to an older snapshot
    ASSERT_OK(iter->Refresh(snapshot));
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());
    iter->Seek(Key(3));
    verify_iter(3, 6);
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());

    // Refresh to no snapshot
    ASSERT_OK(iter->Refresh());
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());
    iter->Seek(Key(2));
    verify_iter(2, 4, /*new_key2=*/true);
    verify_iter(6, 7);
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());

    // Change LSM shape, new SuperVersion is created.
    ASSERT_OK(Flush());

    // Refresh back to original snapshot
    ASSERT_OK(iter->Refresh(snapshot));
  }

  delete iter;
  db_->ReleaseSnapshot(snapshot);
  db_->ReleaseSnapshot(snapshot2);
  ASSERT_OK(db_->Close());
}

TEST_P(DBIteratorTest, CreationFailure) {
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::NewInternalIterator:StatusCallback", [](void* arg) {
        *(static_cast<Status*>(arg)) = Status::Corruption("test status");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Iterator* iter = NewIterator(ReadOptions());
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
  delete iter;
}

TEST_P(DBIteratorTest, UpperBoundWithChangeDirection) {
  Options options = CurrentOptions();
  options.max_sequential_skip_in_iterations = 3;
  DestroyAndReopen(options);

  // write a bunch of kvs to the database.
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("y", "1"));
  ASSERT_OK(Put("y1", "1"));
  ASSERT_OK(Put("y2", "1"));
  ASSERT_OK(Put("y3", "1"));
  ASSERT_OK(Put("z", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("z", "1"));
  ASSERT_OK(Put("bar", "1"));
  ASSERT_OK(Put("foo", "1"));

  std::string upper_bound = "x";
  Slice ub_slice(upper_bound);
  ReadOptions ro;
  ro.iterate_upper_bound = &ub_slice;
  ro.max_skippable_internal_keys = 1000;

  Iterator* iter = NewIterator(ro);
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo", iter->key().ToString());

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("bar", iter->key().ToString());

  delete iter;
}

TEST_P(DBIteratorTest, TableFilter) {
  ASSERT_OK(Put("a", "1"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Put("c", "3"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("d", "4"));
  ASSERT_OK(Put("e", "5"));
  ASSERT_OK(Put("f", "6"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));

  // Ensure the table_filter callback is called once for each table.
  {
    std::set<uint64_t> unseen{1, 2, 3};
    ReadOptions opts;
    opts.table_filter = [&](const TableProperties& props) {
      auto it = unseen.find(props.num_entries);
      if (it == unseen.end()) {
        ADD_FAILURE() << "saw table properties with an unexpected "
                      << props.num_entries << " entries";
      } else {
        unseen.erase(it);
      }
      return true;
    };
    auto iter = NewIterator(opts);
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->1");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->2");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->3");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "d->4");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "e->5");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "f->6");
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_TRUE(unseen.empty());
    delete iter;
  }

  // Ensure returning false in the table_filter hides the keys from that table
  // during iteration.
  {
    ReadOptions opts;
    opts.table_filter = [](const TableProperties& props) {
      return props.num_entries != 2;
    };
    auto iter = NewIterator(opts);
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->1");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "d->4");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "e->5");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "f->6");
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    delete iter;
  }
}

TEST_P(DBIteratorTest, UpperBoundWithPrevReseek) {
  Options options = CurrentOptions();
  options.max_sequential_skip_in_iterations = 3;
  DestroyAndReopen(options);

  // write a bunch of kvs to the database.
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("y", "1"));
  ASSERT_OK(Put("z", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("z", "1"));
  ASSERT_OK(Put("bar", "1"));
  ASSERT_OK(Put("foo", "1"));
  ASSERT_OK(Put("foo", "2"));

  ASSERT_OK(Put("foo", "3"));
  ASSERT_OK(Put("foo", "4"));
  ASSERT_OK(Put("foo", "5"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put("foo", "6"));

  std::string upper_bound = "x";
  Slice ub_slice(upper_bound);
  ReadOptions ro;
  ro.snapshot = snapshot;
  ro.iterate_upper_bound = &ub_slice;

  Iterator* iter = NewIterator(ro);
  iter->SeekForPrev("goo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo", iter->key().ToString());
  iter->Prev();

  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar", iter->key().ToString());

  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_P(DBIteratorTest, SkipStatistics) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);

  int skip_count = 0;

  // write a bunch of kvs to the database.
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("b", "1"));
  ASSERT_OK(Put("c", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("d", "1"));
  ASSERT_OK(Put("e", "1"));
  ASSERT_OK(Put("f", "1"));
  ASSERT_OK(Put("a", "2"));
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("d"));
  ASSERT_OK(Delete("e"));
  ASSERT_OK(Delete("f"));

  Iterator* iter = NewIterator(ReadOptions());
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    count++;
  }
  ASSERT_EQ(count, 3);
  delete iter;
  skip_count += 8;  // 3 deletes + 3 original keys + 2 lower in sequence
  ASSERT_EQ(skip_count, TestGetTickerCount(options, NUMBER_ITER_SKIP));

  iter = NewIterator(ReadOptions());
  count = 0;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    ASSERT_OK(iter->status());
    count++;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 3);
  delete iter;
  skip_count += 8;  // Same as above, but in reverse order
  ASSERT_EQ(skip_count, TestGetTickerCount(options, NUMBER_ITER_SKIP));

  ASSERT_OK(Put("aa", "1"));
  ASSERT_OK(Put("ab", "1"));
  ASSERT_OK(Put("ac", "1"));
  ASSERT_OK(Put("ad", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("ab"));
  ASSERT_OK(Delete("ac"));
  ASSERT_OK(Delete("ad"));

  ReadOptions ro;
  Slice prefix("b");
  ro.iterate_upper_bound = &prefix;

  iter = NewIterator(ro);
  count = 0;
  for (iter->Seek("aa"); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    count++;
  }
  ASSERT_EQ(count, 1);
  delete iter;
  skip_count += 6;  // 3 deletes + 3 original keys
  ASSERT_EQ(skip_count, TestGetTickerCount(options, NUMBER_ITER_SKIP));

  iter = NewIterator(ro);
  count = 0;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    ASSERT_OK(iter->status());
    count++;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 2);
  delete iter;
  // 3 deletes + 3 original keys + lower sequence of "a"
  skip_count += 7;
  ASSERT_EQ(skip_count, TestGetTickerCount(options, NUMBER_ITER_SKIP));
}

TEST_P(DBIteratorTest, SeekAfterHittingManyInternalKeys) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ReadOptions ropts;
  ropts.max_skippable_internal_keys = 2;

  ASSERT_OK(Put("1", "val_1"));
  // Add more tombstones than max_skippable_internal_keys so that Next() fails.
  ASSERT_OK(Delete("2"));
  ASSERT_OK(Delete("3"));
  ASSERT_OK(Delete("4"));
  ASSERT_OK(Delete("5"));
  ASSERT_OK(Put("6", "val_6"));

  std::unique_ptr<Iterator> iter(NewIterator(ropts));
  iter->SeekToFirst();

  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "1");
  ASSERT_EQ(iter->value().ToString(), "val_1");

  // This should fail as incomplete due to too many non-visible internal keys on
  // the way to the next valid user key.
  iter->Next();
  ASSERT_TRUE(!iter->Valid());
  ASSERT_TRUE(iter->status().IsIncomplete());

  // Get the internal key at which Next() failed.
  std::string prop_value;
  ASSERT_OK(iter->GetProperty("rocksdb.iterator.internal-key", &prop_value));
  ASSERT_EQ("4", prop_value);

  // Create a new iterator to seek to the internal key.
  std::unique_ptr<Iterator> iter2(NewIterator(ropts));
  iter2->Seek(prop_value);
  ASSERT_TRUE(iter2->Valid());
  ASSERT_OK(iter2->status());

  ASSERT_EQ(iter2->key().ToString(), "6");
  ASSERT_EQ(iter2->value().ToString(), "val_6");
}

// Reproduces a former bug where iterator would skip some records when DBIter
// re-seeks subiterator with Incomplete status.
TEST_P(DBIteratorTest, NonBlockingIterationBugRepro) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  // Make sure the sst file has more than one block.
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  // Two records in sst file, each in its own block.
  ASSERT_OK(Put("b", ""));
  ASSERT_OK(Put("d", ""));
  ASSERT_OK(Flush());

  // Create a nonblocking iterator before writing to memtable.
  ReadOptions ropt;
  ropt.read_tier = kBlockCacheTier;
  std::unique_ptr<Iterator> iter(NewIterator(ropt));

  // Overwrite a key in memtable many times to hit
  // max_sequential_skip_in_iterations (which is 8 by default).
  for (int i = 0; i < 20; ++i) {
    ASSERT_OK(Put("c", ""));
  }

  // Load the second block in sst file into the block cache.
  {
    std::unique_ptr<Iterator> iter2(NewIterator(ReadOptions()));
    iter2->Seek("d");
  }

  // Finally seek the nonblocking iterator.
  iter->Seek("a");
  // With the bug, the status used to be OK, and the iterator used to point to
  // "d".
  EXPECT_TRUE(iter->status().IsIncomplete());
}

TEST_P(DBIteratorTest, SeekBackwardAfterOutOfUpperBound) {
  ASSERT_OK(Put("a", ""));
  ASSERT_OK(Put("b", ""));
  ASSERT_OK(Flush());

  ReadOptions ropt;
  Slice ub = "b";
  ropt.iterate_upper_bound = &ub;

  std::unique_ptr<Iterator> it(dbfull()->NewIterator(ropt));
  it->SeekForPrev("a");
  ASSERT_TRUE(it->Valid());
  ASSERT_OK(it->status());
  ASSERT_EQ("a", it->key().ToString());
  it->Next();
  ASSERT_FALSE(it->Valid());
  ASSERT_OK(it->status());
  it->SeekForPrev("a");
  ASSERT_OK(it->status());

  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("a", it->key().ToString());
}

TEST_P(DBIteratorTest, AvoidReseekLevelIterator) {
  Options options = CurrentOptions();
  options.compression = CompressionType::kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 800;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  Random rnd(301);
  std::string random_str = rnd.RandomString(180);

  ASSERT_OK(Put("1", random_str));
  ASSERT_OK(Put("2", random_str));
  ASSERT_OK(Put("3", random_str));
  ASSERT_OK(Put("4", random_str));
  // A new block
  ASSERT_OK(Put("5", random_str));
  ASSERT_OK(Put("6", random_str));
  ASSERT_OK(Put("7", random_str));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("8", random_str));
  ASSERT_OK(Put("9", random_str));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  int num_find_file_in_level = 0;
  int num_idx_blk_seek = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "LevelIterator::Seek:BeforeFindFile",
      [&](void* /*arg*/) { num_find_file_in_level++; });
  SyncPoint::GetInstance()->SetCallBack(
      "IndexBlockIter::Seek:0", [&](void* /*arg*/) { num_idx_blk_seek++; });
  SyncPoint::GetInstance()->EnableProcessing();

  {
    std::unique_ptr<Iterator> iter(NewIterator(ReadOptions()));
    iter->Seek("1");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(1, num_idx_blk_seek);

    iter->Seek("2");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(1, num_idx_blk_seek);

    iter->Seek("3");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(1, num_idx_blk_seek);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(1, num_idx_blk_seek);

    iter->Seek("5");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(2, num_idx_blk_seek);

    iter->Seek("6");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(2, num_idx_blk_seek);

    iter->Seek("7");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(3, num_idx_blk_seek);

    iter->Seek("8");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(2, num_find_file_in_level);
    // Still re-seek because "8" is the boundary key, which has
    // the same user key as the seek key.
    ASSERT_EQ(4, num_idx_blk_seek);

    iter->Seek("5");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(3, num_find_file_in_level);
    ASSERT_EQ(5, num_idx_blk_seek);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(3, num_find_file_in_level);
    ASSERT_EQ(5, num_idx_blk_seek);

    // Seek backward never triggers the index block seek to be skipped
    iter->Seek("5");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(3, num_find_file_in_level);
    ASSERT_EQ(6, num_idx_blk_seek);
  }

  SyncPoint::GetInstance()->DisableProcessing();
}

// MyRocks may change iterate bounds before seek. Simply test to make sure such
// usage doesn't break iterator.
TEST_P(DBIteratorTest, IterateBoundChangedBeforeSeek) {
  Options options = CurrentOptions();
  options.compression = CompressionType::kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 100;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  std::string value(50, 'v');
  Reopen(options);
  ASSERT_OK(Put("aaa", value));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("bbb", "v"));
  ASSERT_OK(Put("ccc", "v"));
  ASSERT_OK(Put("ddd", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("eee", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std::string ub1 = "e";
  std::string ub2 = "c";
  Slice ub(ub1);
  ReadOptions read_opts1;
  read_opts1.iterate_upper_bound = &ub;
  Iterator* iter = NewIterator(read_opts1);
  // Seek and iterate accross block boundary.
  iter->Seek("b");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("bbb", iter->key());
  ub = Slice(ub2);
  iter->Seek("b");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("bbb", iter->key());
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  delete iter;

  std::string lb1 = "a";
  std::string lb2 = "c";
  Slice lb(lb1);
  ReadOptions read_opts2;
  read_opts2.iterate_lower_bound = &lb;
  iter = NewIterator(read_opts2);
  iter->SeekForPrev("d");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("ccc", iter->key());
  lb = Slice(lb2);
  iter->SeekForPrev("d");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("ccc", iter->key());
  iter->Prev();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  delete iter;
}

TEST_P(DBIteratorTest, IterateWithLowerBoundAcrossFileBoundary) {
  ASSERT_OK(Put("aaa", "v"));
  ASSERT_OK(Put("bbb", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("ccc", "v"));
  ASSERT_OK(Put("ddd", "v"));
  ASSERT_OK(Flush());
  // Move both files to bottom level.
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  Slice lower_bound("b");
  ReadOptions read_opts;
  read_opts.iterate_lower_bound = &lower_bound;
  std::unique_ptr<Iterator> iter(NewIterator(read_opts));
  iter->SeekForPrev("d");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("ccc", iter->key());
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("bbb", iter->key());
  iter->Prev();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_P(DBIteratorTest, Blob) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.max_sequential_skip_in_iterations = 2;
  options.statistics = CreateDBStatistics();

  Reopen(options);

  // Note: we have 4 KVs (3 of which are hidden) for key "b" and
  // max_sequential_skip_in_iterations is set to 2. Thus, we need to do a reseek
  // anytime we move from "b" to "c" or vice versa.
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "vb0"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "vb1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "vb2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "vb3"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Flush());

  std::unique_ptr<Iterator> iter_guard(NewIterator(ReadOptions()));
  Iterator* const iter = iter_guard.get();

  iter->SeekToFirst();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->vb3");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToFirst();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->SeekToLast();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "b->vb3");
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToLast();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->Seek("");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Seek("a");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Seek("ax");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "b->vb3");

  iter->SeekForPrev("d");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->SeekForPrev("c");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->SeekForPrev("bx");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 3);
  ASSERT_EQ(IterStatus(iter), "b->vb3");

  iter->Seek("b");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 3);
  ASSERT_EQ(IterStatus(iter), "b->vb3");
  iter->Seek("z");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 3);
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekForPrev("b");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 4);
  ASSERT_EQ(IterStatus(iter), "b->vb3");
  iter->SeekForPrev("");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 4);
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  // Switch from reverse to forward
  iter->SeekToLast();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 4);
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 5);
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 5);
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 6);
  ASSERT_EQ(IterStatus(iter), "b->vb3");

  // Switch from forward to reverse
  iter->SeekToFirst();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 6);
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 6);
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 7);
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 8);
  ASSERT_EQ(IterStatus(iter), "b->vb3");
}

INSTANTIATE_TEST_CASE_P(DBIteratorTestInstance, DBIteratorTest,
                        testing::Values(true, false));

// Tests how DBIter work with ReadCallback
class DBIteratorWithReadCallbackTest : public DBIteratorTest {};

TEST_F(DBIteratorWithReadCallbackTest, ReadCallback) {
  class TestReadCallback : public ReadCallback {
   public:
    explicit TestReadCallback(SequenceNumber _max_visible_seq)
        : ReadCallback(_max_visible_seq) {}

    bool IsVisibleFullCheck(SequenceNumber seq) override {
      return seq <= max_visible_seq_;
    }
  };

  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("foo", "v2"));
  ASSERT_OK(Put("foo", "v3"));
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("z", "vz"));
  SequenceNumber seq1 = db_->GetLatestSequenceNumber();
  TestReadCallback callback1(seq1);
  ASSERT_OK(Put("foo", "v4"));
  ASSERT_OK(Put("foo", "v5"));
  ASSERT_OK(Put("bar", "v7"));

  SequenceNumber seq2 = db_->GetLatestSequenceNumber();
  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      db_->DefaultColumnFamily());
  auto* cfd = cfh->cfd();
  // The iterator are suppose to see data before seq1.
  DBImpl* db_impl = dbfull();
  SuperVersion* super_version = cfd->GetReferencedSuperVersion(db_impl);
  Iterator* iter = db_impl->NewIteratorImpl(ReadOptions(), cfh, super_version,
                                            seq2, &callback1);

  // Seek
  // The latest value of "foo" before seq1 is "v3"
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());
  // "bar" is not visible to the iterator. It will move on to the next key
  // "foo".
  iter->Seek("bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());

  // Next
  // Seek to "a"
  iter->Seek("a");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("va", iter->value());
  // "bar" is not visible to the iterator. It will move on to the next key
  // "foo".
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());

  // Prev
  // Seek to "z"
  iter->Seek("z");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("vz", iter->value());
  // The previous key is "foo", which is visible to the iterator.
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());
  // "bar" is not visible to the iterator. It will move on to the next key "a".
  iter->Prev();  // skipping "bar"
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("a", iter->key());
  ASSERT_EQ("va", iter->value());

  // SeekForPrev
  // The previous key is "foo", which is visible to the iterator.
  iter->SeekForPrev("y");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());
  // "bar" is not visible to the iterator. It will move on to the next key "a".
  iter->SeekForPrev("bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("a", iter->key());
  ASSERT_EQ("va", iter->value());

  delete iter;

  // Prev beyond max_sequential_skip_in_iterations
  uint64_t num_versions =
      CurrentOptions().max_sequential_skip_in_iterations + 10;
  for (uint64_t i = 0; i < num_versions; i++) {
    ASSERT_OK(Put("bar", std::to_string(i)));
  }
  SequenceNumber seq3 = db_->GetLatestSequenceNumber();
  TestReadCallback callback2(seq3);
  ASSERT_OK(Put("bar", "v8"));
  SequenceNumber seq4 = db_->GetLatestSequenceNumber();

  // The iterator is suppose to see data before seq3.
  super_version = cfd->GetReferencedSuperVersion(db_impl);
  iter = db_impl->NewIteratorImpl(ReadOptions(), cfh, super_version, seq4,
                                  &callback2);
  // Seek to "z", which is visible.
  iter->Seek("z");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("vz", iter->value());
  // Previous key is "foo" and the last value "v5" is visible.
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v5", iter->value());
  // Since the number of values of "bar" is more than
  // max_sequential_skip_in_iterations, Prev() will ultimately fallback to
  // seek in forward direction. Here we test the fallback seek is correct.
  // The last visible value should be (num_versions - 1), as "v8" is not
  // visible.
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("bar", iter->key());
  ASSERT_EQ(std::to_string(num_versions - 1), iter->value());

  delete iter;
}

TEST_F(DBIteratorTest, BackwardIterationOnInplaceUpdateMemtable) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.inplace_update_support = false;
  options.env = env_;
  DestroyAndReopen(options);
  constexpr int kNumKeys = 10;

  // Write kNumKeys to WAL.
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), "val"));
  }
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    int count = 0;
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      ++count;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(kNumKeys, count);
  }

  // Reopen and rebuild the memtable from WAL.
  options.create_if_missing = false;
  options.avoid_flush_during_recovery = true;
  options.inplace_update_support = true;
  options.allow_concurrent_memtable_write = false;
  Reopen(options);
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->SeekToLast();
    // Backward iteration not supported due to inplace_update_support = true.
    ASSERT_TRUE(iter->status().IsNotSupported());
    ASSERT_FALSE(iter->Valid());
  }
}

TEST_F(DBIteratorTest, IteratorRefreshReturnSV) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  std::unique_ptr<Iterator> iter{db_->NewIterator(ReadOptions())};
  SyncPoint::GetInstance()->SetCallBack(
      "ArenaWrappedDBIter::Refresh:SV", [&](void*) {
        ASSERT_OK(db_->Put(WriteOptions(), "dummy", "new SV"));
        // This makes the local SV obselete.
        ASSERT_OK(Flush());
        SyncPoint::GetInstance()->DisableProcessing();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(iter->Refresh());
  iter.reset();
  // iter used to not cleanup SV, so the Close() below would hit an assertion
  // error.
  Close();
}

TEST_F(DBIteratorTest, ErrorWhenReadFile) {
  // This is to test a bug that is fixed in
  // https://github.com/facebook/rocksdb/pull/11782.
  //
  // Ingest error when reading from a file, and
  // see if Iterator handles it correctly.
  Options opts = CurrentOptions();
  opts.num_levels = 7;
  opts.compression = kNoCompression;
  BlockBasedTableOptions bbto;
  // Always do I/O
  bbto.no_block_cache = true;
  opts.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(opts);

  // Set up LSM
  // L5: F1 [key0, key99], F2 [key100, key199]
  // L6:        F3 [key50, key149]
  Random rnd(301);
  const int kValLen = 100;
  for (int i = 50; i < 150; ++i) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(kValLen)));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  std::vector<std::string> values;
  for (int i = 0; i < 100; ++i) {
    values.emplace_back(rnd.RandomString(kValLen));
    ASSERT_OK(Put(Key(i), values.back()));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);

  for (int i = 100; i < 200; ++i) {
    values.emplace_back(rnd.RandomString(kValLen));
    ASSERT_OK(Put(Key(i), values.back()));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);

  ASSERT_EQ(2, NumTableFilesAtLevel(5));
  ASSERT_EQ(1, NumTableFilesAtLevel(6));

  std::vector<LiveFileMetaData> files;
  db_->GetLiveFilesMetaData(&files);
  // Get file names for F1, F2 and F3.
  // These are file names, not full paths.
  std::string f1, f2, f3;
  for (auto& file_meta : files) {
    if (file_meta.level == 6) {
      f3 = file_meta.name;
    } else {
      if (file_meta.smallestkey == Key(0)) {
        f1 = file_meta.name;
      } else {
        f2 = file_meta.name;
      }
    }
  }
  ASSERT_TRUE(!f1.empty());
  ASSERT_TRUE(!f2.empty());
  ASSERT_TRUE(!f3.empty());

  std::string error_file;
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::Read::BeforeReturn",
      [&error_file](void* io_s_ptr) {
        auto p = static_cast<std::pair<std::string*, IOStatus*>*>(io_s_ptr);
        if (p->first->find(error_file) != std::string::npos) {
          *p->second = IOStatus::IOError();
          p->second->SetRetryable(true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  // Error reading F1
  error_file = f1;
  std::unique_ptr<Iterator> iter{db_->NewIterator(ReadOptions())};
  iter->SeekToFirst();
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  // This does not require reading the first block.
  iter->Seek(Key(90));
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[90]);
  // iter has ok status before this Seek.
  iter->Seek(Key(1));
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());

  // Error reading F2
  error_file = f2;
  iter.reset(db_->NewIterator(ReadOptions()));
  iter->Seek(Key(99));
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[99]);
  // Need to read from F2.
  iter->Next();
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  iter->Seek(Key(190));
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[190]);
  // Seek for first key of F2.
  iter->Seek(Key(100));
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  iter->SeekToLast();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[199]);
  // SeekForPrev for first key of F2.
  iter->SeekForPrev(Key(100));
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  // Does not read first block (offset 0).
  iter->SeekForPrev(Key(98));
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[98]);

  // Error reading F3
  error_file = f3;
  iter.reset(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  iter->Seek(Key(50));
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  iter->SeekForPrev(Key(50));
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  // Does not read file 3
  iter->Seek(Key(150));
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[150]);

  // Test when file read error occurs during Prev().
  // This requires returning an error when reading near the end of a file
  // instead of offset 0.
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::Read::AnyOffset", [&f1](void* pair_ptr) {
        auto p = static_cast<std::pair<std::string*, IOStatus*>*>(pair_ptr);
        if (p->first->find(f1) != std::string::npos) {
          *p->second = IOStatus::IOError();
          p->second->SetRetryable(true);
        }
      });
  iter->SeekForPrev(Key(101));
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[101]);
  // DBIter will not stop at Key(100) since it needs
  // to make sure the key it returns has the max sequence number for Key(100).
  // So it will call MergingIterator::Prev() which will read F1.
  iter->Prev();
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  iter->Reset();
}

TEST_F(DBIteratorTest, IteratorsConsistentViewImplicitSnapshot) {
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"cf_1", "cf_2"}, options);

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkFlush:done",
        "DBImpl::MultiCFSnapshot::BeforeCheckingSnapshot"}});

  bool flushed = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiCFSnapshot::AfterRefSV", [&](void* /*arg*/) {
        if (!flushed) {
          for (int i = 0; i < 3; ++i) {
            ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                          "cf" + std::to_string(i) + "_val_new"));
          }
          // After SV is obtained for the first CF, flush for the second CF
          ASSERT_OK(Flush(1));
          flushed = true;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ReadOptions read_options;
  std::vector<Iterator*> iters;
  ASSERT_OK(db_->NewIterators(read_options, handles_, &iters));

  for (int i = 0; i < 3; ++i) {
    auto iter = iters[i];
    ASSERT_OK(iter->status());
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "cf" + std::to_string(i) + "_key->cf" +
                                    std::to_string(i) + "_val_new");
  }
  for (auto* iter : iters) {
    delete iter;
  }

  // Thread-local SVs are no longer obsolete nor in use
  for (int i = 0; i < 3; ++i) {
    auto* cfd =
        static_cast_with_check<ColumnFamilyHandleImpl>(handles_[i])->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVObsolete);
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
  }
}

TEST_F(DBIteratorTest, IteratorsConsistentViewExplicitSnapshot) {
  Options options = GetDefaultOptions();
  options.atomic_flush = true;
  CreateAndReopenWithCF({"cf_1", "cf_2"}, options);

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkFlush:done",
        "DBImpl::MultiCFSnapshot::BeforeCheckingSnapshot"}});

  bool flushed = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiCFSnapshot::AfterRefSV", [&](void* /*arg*/) {
        if (!flushed) {
          for (int i = 0; i < 3; ++i) {
            ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                          "cf" + std::to_string(i) + "_val_new"));
          }
          // After SV is obtained for the first CF, do the atomic flush()
          ASSERT_OK(Flush());
          flushed = true;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  // Explicit snapshot wouldn't force reloading all svs. We should expect old
  // values
  const Snapshot* snapshot = db_->GetSnapshot();
  ReadOptions read_options;
  read_options.snapshot = snapshot;
  std::vector<Iterator*> iters;
  ASSERT_OK(db_->NewIterators(read_options, handles_, &iters));

  for (int i = 0; i < 3; ++i) {
    auto iter = iters[i];
    ASSERT_OK(iter->status());
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "cf" + std::to_string(i) + "_key->cf" +
                                    std::to_string(i) + "_val");
  }
  db_->ReleaseSnapshot(snapshot);
  for (auto* iter : iters) {
    delete iter;
  }

  // Thread-local SV for cf_0 is obsolete (atomic flush happened after the first
  // SV Ref)
  auto* cfd0 =
      static_cast_with_check<ColumnFamilyHandleImpl>(handles_[0])->cfd();
  ASSERT_EQ(cfd0->TEST_GetLocalSV()->Get(), SuperVersion::kSVObsolete);
  ASSERT_NE(cfd0->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);

  // Rest are not InUse nor Obsolete
  for (int i = 1; i < 3; ++i) {
    auto* cfd =
        static_cast_with_check<ColumnFamilyHandleImpl>(handles_[i])->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVObsolete);
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
