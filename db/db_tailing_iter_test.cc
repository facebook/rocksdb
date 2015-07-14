//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Introduction of SyncPoint effectively disabled building and running this test
// in Release build.
// which is a pity, it is a good test
#if !(defined NDEBUG) || !defined(OS_WIN)

#include "port/stack_trace.h"
#include "util/db_test_util.h"

namespace rocksdb {

class DBTestTailingIterator : public DBTestBase {
 public:
  DBTestTailingIterator() : DBTestBase("/db_tailing_iterator_test") {}
};

TEST_F(DBTestTailingIterator, TailingIteratorSingle) {
  ReadOptions read_options;
  read_options.tailing = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->SeekToFirst();
  ASSERT_TRUE(!iter->Valid());

  // add a record and check that iter can see it
  ASSERT_OK(db_->Put(WriteOptions(), "mirko", "fodor"));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "mirko");

  iter->Next();
  ASSERT_TRUE(!iter->Valid());
}

TEST_F(DBTestTailingIterator, TailingIteratorKeepAdding) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
  std::string value(1024, 'a');

  const int num_records = 10000;
  for (int i = 0; i < num_records; ++i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%016d", i);

    Slice key(buf, 16);
    ASSERT_OK(Put(1, key, value));

    iter->Seek(key);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
}

TEST_F(DBTestTailingIterator, TailingIteratorSeekToNext) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
  std::string value(1024, 'a');

  const int num_records = 1000;
  for (int i = 1; i < num_records; ++i) {
    char buf1[32];
    char buf2[32];
    snprintf(buf1, sizeof(buf1), "00a0%016d", i * 5);

    Slice key(buf1, 20);
    ASSERT_OK(Put(1, key, value));

    if (i % 100 == 99) {
      ASSERT_OK(Flush(1));
    }

    snprintf(buf2, sizeof(buf2), "00a0%016d", i * 5 - 2);
    Slice target(buf2, 20);
    iter->Seek(target);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
  for (int i = 2 * num_records; i > 0; --i) {
    char buf1[32];
    char buf2[32];
    snprintf(buf1, sizeof(buf1), "00a0%016d", i * 5);

    Slice key(buf1, 20);
    ASSERT_OK(Put(1, key, value));

    if (i % 100 == 99) {
      ASSERT_OK(Flush(1));
    }

    snprintf(buf2, sizeof(buf2), "00a0%016d", i * 5 - 2);
    Slice target(buf2, 20);
    iter->Seek(target);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
}

TEST_F(DBTestTailingIterator, TailingIteratorDeletes) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));

  // write a single record, read it using the iterator, then delete it
  ASSERT_OK(Put(1, "0test", "test"));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "0test");
  ASSERT_OK(Delete(1, "0test"));

  // write many more records
  const int num_records = 10000;
  std::string value(1024, 'A');

  for (int i = 0; i < num_records; ++i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "1%015d", i);

    Slice key(buf, 16);
    ASSERT_OK(Put(1, key, value));
  }

  // force a flush to make sure that no records are read from memtable
  ASSERT_OK(Flush(1));

  // skip "0test"
  iter->Next();

  // make sure we can read all new records using the existing iterator
  int count = 0;
  for (; iter->Valid(); iter->Next(), ++count) ;

  ASSERT_EQ(count, num_records);
}

TEST_F(DBTestTailingIterator, TailingIteratorPrefixSeek) {
  XFUNC_TEST("", "dbtest_prefix", prefix_skip1, XFuncPoint::SetSkip,
             kSkipNoPrefix);
  ReadOptions read_options;
  read_options.tailing = true;

  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.memtable_factory.reset(NewHashSkipListRepFactory(16));
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
  ASSERT_OK(Put(1, "0101", "test"));

  ASSERT_OK(Flush(1));

  ASSERT_OK(Put(1, "0202", "test"));

  // Seek(0102) shouldn't find any records since 0202 has a different prefix
  iter->Seek("0102");
  ASSERT_TRUE(!iter->Valid());

  iter->Seek("0202");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "0202");

  iter->Next();
  ASSERT_TRUE(!iter->Valid());
  XFUNC_TEST("", "dbtest_prefix", prefix_skip1, XFuncPoint::SetSkip, 0);
}

TEST_F(DBTestTailingIterator, TailingIteratorIncomplete) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.read_tier = kBlockCacheTier;

  std::string key("key");
  std::string value("value");

  ASSERT_OK(db_->Put(WriteOptions(), key, value));

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->SeekToFirst();
  // we either see the entry or it's not in cache
  ASSERT_TRUE(iter->Valid() || iter->status().IsIncomplete());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  iter->SeekToFirst();
  // should still be true after compaction
  ASSERT_TRUE(iter->Valid() || iter->status().IsIncomplete());
}

TEST_F(DBTestTailingIterator, TailingIteratorSeekToSame) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 1000;
  CreateAndReopenWithCF({"pikachu"}, options);

  ReadOptions read_options;
  read_options.tailing = true;

  const int NROWS = 10000;
  // Write rows with keys 00000, 00002, 00004 etc.
  for (int i = 0; i < NROWS; ++i) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%05d", 2*i);
    std::string key(buf);
    std::string value("value");
    ASSERT_OK(db_->Put(WriteOptions(), key, value));
  }

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  // Seek to 00001.  We expect to find 00002.
  std::string start_key = "00001";
  iter->Seek(start_key);
  ASSERT_TRUE(iter->Valid());

  std::string found = iter->key().ToString();
  ASSERT_EQ("00002", found);

  // Now seek to the same key.  The iterator should remain in the same
  // position.
  iter->Seek(found);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(found, iter->key().ToString());
}

TEST_F(DBTestTailingIterator, ManagedTailingIteratorSingle) {
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->SeekToFirst();
  ASSERT_TRUE(!iter->Valid());

  // add a record and check that iter can see it
  ASSERT_OK(db_->Put(WriteOptions(), "mirko", "fodor"));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "mirko");

  iter->Next();
  ASSERT_TRUE(!iter->Valid());
}

TEST_F(DBTestTailingIterator, ManagedTailingIteratorKeepAdding) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
  std::string value(1024, 'a');

  const int num_records = 10000;
  for (int i = 0; i < num_records; ++i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%016d", i);

    Slice key(buf, 16);
    ASSERT_OK(Put(1, key, value));

    iter->Seek(key);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
}

TEST_F(DBTestTailingIterator, ManagedTailingIteratorSeekToNext) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
  std::string value(1024, 'a');

  const int num_records = 1000;
  for (int i = 1; i < num_records; ++i) {
    char buf1[32];
    char buf2[32];
    snprintf(buf1, sizeof(buf1), "00a0%016d", i * 5);

    Slice key(buf1, 20);
    ASSERT_OK(Put(1, key, value));

    if (i % 100 == 99) {
      ASSERT_OK(Flush(1));
    }

    snprintf(buf2, sizeof(buf2), "00a0%016d", i * 5 - 2);
    Slice target(buf2, 20);
    iter->Seek(target);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
  for (int i = 2 * num_records; i > 0; --i) {
    char buf1[32];
    char buf2[32];
    snprintf(buf1, sizeof(buf1), "00a0%016d", i * 5);

    Slice key(buf1, 20);
    ASSERT_OK(Put(1, key, value));

    if (i % 100 == 99) {
      ASSERT_OK(Flush(1));
    }

    snprintf(buf2, sizeof(buf2), "00a0%016d", i * 5 - 2);
    Slice target(buf2, 20);
    iter->Seek(target);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
}

TEST_F(DBTestTailingIterator, ManagedTailingIteratorDeletes) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));

  // write a single record, read it using the iterator, then delete it
  ASSERT_OK(Put(1, "0test", "test"));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "0test");
  ASSERT_OK(Delete(1, "0test"));

  // write many more records
  const int num_records = 10000;
  std::string value(1024, 'A');

  for (int i = 0; i < num_records; ++i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "1%015d", i);

    Slice key(buf, 16);
    ASSERT_OK(Put(1, key, value));
  }

  // force a flush to make sure that no records are read from memtable
  ASSERT_OK(Flush(1));

  // skip "0test"
  iter->Next();

  // make sure we can read all new records using the existing iterator
  int count = 0;
  for (; iter->Valid(); iter->Next(), ++count) {
  }

  ASSERT_EQ(count, num_records);
}

TEST_F(DBTestTailingIterator, ManagedTailingIteratorPrefixSeek) {
  XFUNC_TEST("", "dbtest_prefix", prefix_skip1, XFuncPoint::SetSkip,
             kSkipNoPrefix);
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;

  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.memtable_factory.reset(NewHashSkipListRepFactory(16));
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
  ASSERT_OK(Put(1, "0101", "test"));

  ASSERT_OK(Flush(1));

  ASSERT_OK(Put(1, "0202", "test"));

  // Seek(0102) shouldn't find any records since 0202 has a different prefix
  iter->Seek("0102");
  ASSERT_TRUE(!iter->Valid());

  iter->Seek("0202");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "0202");

  iter->Next();
  ASSERT_TRUE(!iter->Valid());
  XFUNC_TEST("", "dbtest_prefix", prefix_skip1, XFuncPoint::SetSkip, 0);
}

TEST_F(DBTestTailingIterator, ManagedTailingIteratorIncomplete) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;
  read_options.read_tier = kBlockCacheTier;

  std::string key = "key";
  std::string value = "value";

  ASSERT_OK(db_->Put(WriteOptions(), key, value));

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->SeekToFirst();
  // we either see the entry or it's not in cache
  ASSERT_TRUE(iter->Valid() || iter->status().IsIncomplete());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  iter->SeekToFirst();
  // should still be true after compaction
  ASSERT_TRUE(iter->Valid() || iter->status().IsIncomplete());
}

TEST_F(DBTestTailingIterator, ManagedTailingIteratorSeekToSame) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 1000;
  CreateAndReopenWithCF({"pikachu"}, options);

  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;

  const int NROWS = 10000;
  // Write rows with keys 00000, 00002, 00004 etc.
  for (int i = 0; i < NROWS; ++i) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%05d", 2 * i);
    std::string key(buf);
    std::string value("value");
    ASSERT_OK(db_->Put(WriteOptions(), key, value));
  }

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  // Seek to 00001.  We expect to find 00002.
  std::string start_key = "00001";
  iter->Seek(start_key);
  ASSERT_TRUE(iter->Valid());

  std::string found = iter->key().ToString();
  ASSERT_EQ("00002", found);

  // Now seek to the same key.  The iterator should remain in the same
  // position.
  iter->Seek(found);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(found, iter->key().ToString());
}

}  // namespace rocksdb

#endif  // !(defined NDEBUG) || !defined(OS_WIN)

int main(int argc, char** argv) {
#if !(defined NDEBUG) || !defined(OS_WIN)
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
#else
  return 0;
#endif
}
