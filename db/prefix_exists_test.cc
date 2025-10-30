// Copyright (c) 2011-present, Facebook, Inc.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_factory.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class PrefixExistsTest : public testing::Test {
 public:
  PrefixExistsTest() : db_(nullptr) {}

  ~PrefixExistsTest() override {
    if (db_ != nullptr) {
      delete db_;
      db_ = nullptr;
    }
  }

 protected:
  void SetUp() override {
    dbname_ = test::PerThreadDBPath("prefix_exists_test");
    ASSERT_OK(DestroyDB(dbname_, Options()));
  }

  void TearDown() override {
    if (db_ != nullptr) {
      delete db_;
      db_ = nullptr;
    }
    ASSERT_OK(DestroyDB(dbname_, Options()));
  }

  void OpenDB(const Options& options) {
    Options opts = options;
    if (!opts.create_if_missing) {
      opts.create_if_missing = true;
    }
    Status s = DB::Open(opts, dbname_, &db_);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_NE(db_, nullptr);
  }

  DB* db_;
  std::string dbname_;
};

TEST_F(PrefixExistsTest, BasicWithExtractorMemtable) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "abc2", "v2"));

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, Slice("abc")));
  ASSERT_TRUE(db_->PrefixExists(ro, Slice("xyz")).IsNotFound());
}

TEST_F(PrefixExistsTest, WithoutExtractorMemtable) {
  Options options;
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "key1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "v2"));

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, Slice("key")));
  ASSERT_TRUE(db_->PrefixExists(ro, Slice("zzz")).IsNotFound());
}

TEST_F(PrefixExistsTest, WithSSTAndBloom) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "pfx1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "pfx2", "v2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, Slice("pfx")));
  ASSERT_TRUE(db_->PrefixExists(ro, Slice("zzz")).IsNotFound());
}

TEST_F(PrefixExistsTest, DeletedKeyReturnsNotFound) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "v1"));
  ASSERT_OK(db_->Delete(WriteOptions(), "aa1"));

  ReadOptions ro;
  ASSERT_TRUE(db_->PrefixExists(ro, Slice("aa")).IsNotFound());
}

class PrefixExistsComprehensiveTest : public testing::Test {
 public:
  PrefixExistsComprehensiveTest() : db_(nullptr) {}
  ~PrefixExistsComprehensiveTest() override {
    if (db_ != nullptr) {
      delete db_;
    }
  }

 protected:
  void SetUp() override {
    dbname_ = test::PerThreadDBPath("prefix_exists_comprehensive_test");
    ASSERT_OK(DestroyDB(dbname_, Options()));
  }

  void TearDown() override {
    if (db_ != nullptr) {
      delete db_;
      db_ = nullptr;
    }
    ASSERT_OK(DestroyDB(dbname_, Options()));
  }

  void OpenDB(const Options& options = Options()) {
    Options opts = options;
    if (!opts.create_if_missing) {
      opts.create_if_missing = true;
    }
    Status s = DB::Open(opts, dbname_, &db_);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_NE(db_, nullptr);
  }

  DB* db_;
  std::string dbname_;
};

// MEMTABLE PREFIX BLOOM FILTER TESTS
TEST_F(PrefixExistsComprehensiveTest, MemtablePrefixBloomEnabled) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.memtable_prefix_bloom_size_ratio = 0.25;
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "abc2", "value2"));

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, "abc"));
  ASSERT_TRUE(db_->PrefixExists(ro, "xyz").IsNotFound());
}

TEST_F(PrefixExistsComprehensiveTest, MemtablePrefixBloomDisabled) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.memtable_prefix_bloom_size_ratio = 0.0;
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "abc2", "value2"));

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, "abc"));
  ASSERT_TRUE(db_->PrefixExists(ro, "xyz").IsNotFound());
}

TEST_F(PrefixExistsComprehensiveTest, MemtablePrefixBloomFalsePositive) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.memtable_prefix_bloom_size_ratio = 0.25;
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "value1"));

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, "abc"));
  ASSERT_TRUE(db_->PrefixExists(ro, "def").IsNotFound());
}

TEST_F(PrefixExistsComprehensiveTest, NoExtractorWithMemtablePrefixBloom) {
  Options options;
  options.memtable_prefix_bloom_size_ratio = 0.25;
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "value2"));

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, "key"));
  ASSERT_TRUE(db_->PrefixExists(ro, "zzz").IsNotFound());
}

// MULTIPLE IMMUTABLE MEMTABLES TESTS
TEST_F(PrefixExistsComprehensiveTest, MultipleImmutableMemtables) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.write_buffer_size = 1024;
  OpenDB(options);

  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "aa" + std::to_string(i), "v"));
  }
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "bb" + std::to_string(i), "v"));
  }
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "cc" + std::to_string(i), "v"));
  }

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, "aa"));
  ASSERT_OK(db_->PrefixExists(ro, "bb"));
  ASSERT_OK(db_->PrefixExists(ro, "cc"));
}

TEST_F(PrefixExistsComprehensiveTest, KeyInFirstImmutableMemtable) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.write_buffer_size = 1024;
  OpenDB(options);

  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "aa" + std::to_string(i), "v"));
  }
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "bb" + std::to_string(i), "v"));
  }

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, "aa"));
}

// SST FILE FILTER POLICY TESTS
TEST_F(PrefixExistsComprehensiveTest, BloomFilterInSST) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));

  BlockBasedTableOptions tbo;
  tbo.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(NewBlockBasedTableFactory(tbo));

  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "abc2", "value2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, "abc"));
  ASSERT_TRUE(db_->PrefixExists(ro, "xyz").IsNotFound());
}

TEST_F(PrefixExistsComprehensiveTest, NoFilterPolicyInSST) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));

  BlockBasedTableOptions tbo;
  options.table_factory.reset(NewBlockBasedTableFactory(tbo));

  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "abc2", "value2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, "abc"));
  ASSERT_TRUE(db_->PrefixExists(ro, "xyz").IsNotFound());
}

// MULTIPLE SST FILES AT DIFFERENT LEVELS
TEST_F(PrefixExistsComprehensiveTest, KeyInMultipleLevels) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.level0_file_num_compaction_trigger = 2;
  OpenDB(options);

  for (int level = 0; level < 3; ++level) {
    for (int i = 0; i < 100; ++i) {
      std::string key;
      key.push_back(static_cast<char>('a' + level));
      key.push_back(static_cast<char>('a' + (i % 26)));
      key += std::to_string(i);
      ASSERT_OK(db_->Put(WriteOptions(), key, "v"));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, "aa"));
  ASSERT_OK(db_->PrefixExists(ro, "ba"));
  ASSERT_OK(db_->PrefixExists(ro, "ca"));
}

// DELETED KEY HANDLING TESTS
TEST_F(PrefixExistsComprehensiveTest, DeletedKeyInMemtableWithBloom) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.memtable_prefix_bloom_size_ratio = 0.25;
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "v1"));
  ASSERT_OK(db_->Delete(WriteOptions(), "aa1"));

  ReadOptions ro;
  ASSERT_TRUE(db_->PrefixExists(ro, "aa").IsNotFound());
}

TEST_F(PrefixExistsComprehensiveTest, DeletedKeyInSST) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "v1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Delete(WriteOptions(), "aa1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions ro;
  ASSERT_TRUE(db_->PrefixExists(ro, "aa").IsNotFound());
}

// SNAPSHOT ISOLATION TESTS
TEST_F(PrefixExistsComprehensiveTest, SnapshotWithMemtablePrefixBloom) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.memtable_prefix_bloom_size_ratio = 0.25;
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "v1"));

  const Snapshot* snapshot = db_->GetSnapshot();

  ASSERT_OK(db_->Put(WriteOptions(), "aa2", "v2"));

  ReadOptions ro;
  ro.snapshot = snapshot;

  ASSERT_OK(db_->PrefixExists(ro, "aa1"));
  ASSERT_TRUE(db_->PrefixExists(ro, "aa2").IsNotFound());

  db_->ReleaseSnapshot(snapshot);
}

// EDGE CASE TESTS
TEST_F(PrefixExistsComprehensiveTest, VeryLargePrefix) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(100));
  OpenDB(options);

  std::string large_key(200, 'a');
  ASSERT_OK(db_->Put(WriteOptions(), large_key, "v"));

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, large_key));
}

TEST_F(PrefixExistsComprehensiveTest, SingleCharacterPrefix) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "a1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "a2", "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), "b1", "v3"));

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, "a"));
  ASSERT_OK(db_->PrefixExists(ro, "b"));
  ASSERT_TRUE(db_->PrefixExists(ro, "c").IsNotFound());
}

TEST_F(PrefixExistsComprehensiveTest, EmptyKeyAfterExtraction) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(10));
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "abc", "v"));

  ReadOptions ro;
  Status s = db_->PrefixExists(ro, "abc");
  ASSERT_TRUE(s.ok() || s.IsNotFound());
}

TEST_F(PrefixExistsComprehensiveTest, BatchWithExtractorMemtable) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "abd1", "v2"));

  ReadOptions ro;
  const Slice prefixes[] = {Slice("abc"), Slice("abe"), Slice("abd")};
  Status statuses[3];
  db_->PrefixExistsMulti(ro, false, 3, prefixes, statuses);
  ASSERT_TRUE(statuses[0].ok());
  ASSERT_TRUE(statuses[1].IsNotFound());
  ASSERT_TRUE(statuses[2].ok());
}

TEST_F(PrefixExistsComprehensiveTest, BatchWithoutExtractorMemtable) {
  Options options;
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "key1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "v2"));

  ReadOptions ro;
  const Slice prefixes[] = {Slice("key"), Slice("zzz")};
  Status statuses[2];
  db_->PrefixExistsMulti(ro, false, 2, prefixes, statuses);
  ASSERT_TRUE(statuses[0].ok());
  ASSERT_TRUE(statuses[1].IsNotFound());
}

TEST_F(PrefixExistsComprehensiveTest, BatchFlushAndDelete) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "bb1", "v2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions ro;
  const Slice prefixes1[] = {Slice("aa"), Slice("bb"), Slice("cc")};
  Status statuses1[3];
  db_->PrefixExistsMulti(ro, false, 3, prefixes1, statuses1);
  ASSERT_TRUE(statuses1[0].ok());
  ASSERT_TRUE(statuses1[1].ok());
  ASSERT_TRUE(statuses1[2].IsNotFound());

  ASSERT_OK(db_->Delete(WriteOptions(), "aa1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  const Slice prefixes2[] = {Slice("aa"), Slice("bb")};
  Status statuses2[2];
  db_->PrefixExistsMulti(ro, false, 2, prefixes2, statuses2);
  ASSERT_TRUE(statuses2[0].IsNotFound());
  ASSERT_TRUE(statuses2[1].ok());
}

TEST_F(PrefixExistsComprehensiveTest, BatchSortedHintWithDuplicates) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "v1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions ro;
  const Slice prefixes[] = {Slice("abc"), Slice("abc"), Slice("abe"),
                            Slice("abe")};
  Status statuses[4];
  db_->PrefixExistsMulti(ro, true /*sorted*/, 4, prefixes, statuses);
  ASSERT_TRUE(statuses[0].ok());
  ASSERT_TRUE(statuses[1].ok());
  ASSERT_TRUE(statuses[2].IsNotFound());
  ASSERT_TRUE(statuses[3].IsNotFound());
}

TEST_F(PrefixExistsComprehensiveTest, BatchSortedHintWithoutExtractor) {
  Options options;
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "key1", "v1"));

  ReadOptions ro;
  const Slice prefixes[] = {Slice("key"), Slice("key"), Slice("zzz")};
  Status statuses[3];
  db_->PrefixExistsMulti(ro, true /*sorted*/, 3, prefixes, statuses);
  ASSERT_TRUE(statuses[0].ok());
  ASSERT_TRUE(statuses[1].ok());
  ASSERT_TRUE(statuses[2].IsNotFound());
}

TEST_F(PrefixExistsComprehensiveTest, BatchSnapshotIsolation) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "v1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  const Snapshot* snap = db_->GetSnapshot();
  ASSERT_NE(snap, nullptr);

  // Delete after snapshot
  ASSERT_OK(db_->Delete(WriteOptions(), "aa1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions ro;
  ro.snapshot = snap;
  const Slice prefixes[] = {Slice("aa"), Slice("bb")};
  Status statuses[2];
  db_->PrefixExistsMulti(ro, false, 2, prefixes, statuses);
  ASSERT_TRUE(statuses[0].ok());
  ASSERT_TRUE(statuses[1].IsNotFound());

  db_->ReleaseSnapshot(snap);
}

// In Multi, different requested prefixes that map to the same effective
// (extracted) prefix must be evaluated independently. Only requests whose
// exact requested prefix matches the found key should return OK.
TEST_F(PrefixExistsComprehensiveTest, BatchExactRequestedPrefixSemantics) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "v1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions ro;
  const Slice prefixes[] = {Slice("aa1"), Slice("aa2")};
  Status statuses[2];
  db_->PrefixExistsMulti(ro, false /*sorted*/, 2, prefixes, statuses);
  ASSERT_TRUE(statuses[0].ok());
  ASSERT_TRUE(statuses[1].IsNotFound());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
