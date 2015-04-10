//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include <vector>
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/version_set.h"
#include "db/write_controller.h"
#include "db/writebuffer.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "util/testharness.h"

namespace rocksdb {

class DumbLogger : public Logger {
 public:
  using Logger::Logv;
  virtual void Logv(const char* format, va_list ap) override {}
  virtual size_t GetLogFileSize() const override { return 0; }
};

class MemTableListTest : public testing::Test {
 public:
  std::string dbname;
  DB* db;
  Options options;

  MemTableListTest() : db(nullptr) {
    dbname = test::TmpDir() + "/memtable_list_test";
  }

  // Create a test db if not yet created
  void CreateDB() {
    if (db == nullptr) {
      options.create_if_missing = true;
      DestroyDB(dbname, options);
      Status s = DB::Open(options, dbname, &db);
      EXPECT_OK(s);
    }
  }

  ~MemTableListTest() {
    if (db) {
      delete db;
      DestroyDB(dbname, options);
    }
  }

  // Calls MemTableList::InstallMemtableFlushResults() and sets up all
  // structures needed to call this function.
  Status Mock_InstallMemtableFlushResults(
      MemTableList* list, const MutableCFOptions& mutable_cf_options,
      const autovector<MemTable*>& m, autovector<MemTable*>* to_delete) {
    // Create a mock Logger
    DumbLogger logger;
    LogBuffer log_buffer(DEBUG_LEVEL, &logger);

    // Create a mock VersionSet
    DBOptions db_options;
    EnvOptions env_options;
    shared_ptr<Cache> table_cache(NewLRUCache(50000, 16));
    WriteBuffer write_buffer(db_options.db_write_buffer_size);
    WriteController write_controller;

    CreateDB();
    VersionSet versions(dbname, &db_options, env_options, table_cache.get(),
                        &write_buffer, &write_controller);

    // Create mock default ColumnFamilyData
    ColumnFamilyOptions cf_options;
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
    EXPECT_OK(versions.Recover(column_families, false));

    auto column_family_set = versions.GetColumnFamilySet();
    auto cfd = column_family_set->GetColumnFamily(0);
    EXPECT_TRUE(cfd != nullptr);

    // Create dummy mutex.
    InstrumentedMutex mutex;
    InstrumentedMutexLock l(&mutex);

    return list->InstallMemtableFlushResults(cfd, mutable_cf_options, m,
                                             &versions, &mutex, 1, to_delete,
                                             nullptr, &log_buffer);
  }
};

TEST_F(MemTableListTest, Empty) {
  // Create an empty MemTableList and validate basic functions.
  MemTableList list(1);

  ASSERT_EQ(0, list.size());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));
  ASSERT_FALSE(list.IsFlushPending());

  autovector<MemTable*> mems;
  list.PickMemtablesToFlush(&mems);
  ASSERT_EQ(0, mems.size());

  autovector<MemTable*> to_delete;
  list.current()->Unref(&to_delete);
  ASSERT_EQ(0, to_delete.size());
}

TEST_F(MemTableListTest, GetTest) {
  // Create MemTableList
  int min_write_buffer_number_to_merge = 2;
  MemTableList list(min_write_buffer_number_to_merge);

  SequenceNumber seq = 1;
  std::string value;
  Status s;
  MergeContext merge_context;

  LookupKey lkey("key1", seq);
  bool found = list.current()->Get(lkey, &value, &s, &merge_context);
  ASSERT_FALSE(found);

  // Create a MemTable
  InternalKeyComparator cmp(BytewiseComparator());
  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  ImmutableCFOptions ioptions(options);

  WriteBuffer wb(options.db_write_buffer_size);
  MemTable* mem =
      new MemTable(cmp, ioptions, MutableCFOptions(options, ioptions), &wb);
  mem->Ref();

  // Write some keys to this memtable.
  mem->Add(++seq, kTypeDeletion, "key1", "");
  mem->Add(++seq, kTypeValue, "key2", "value2");
  mem->Add(++seq, kTypeValue, "key1", "value1");
  mem->Add(++seq, kTypeValue, "key2", "value2.2");

  // Fetch the newly written keys
  merge_context.Clear();
  found = mem->Get(LookupKey("key1", seq), &value, &s, &merge_context);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ(value, "value1");

  merge_context.Clear();
  found = mem->Get(LookupKey("key1", 2), &value, &s, &merge_context);
  // MemTable found out that this key is *not* found (at this sequence#)
  ASSERT_TRUE(found && s.IsNotFound());

  merge_context.Clear();
  found = mem->Get(LookupKey("key2", seq), &value, &s, &merge_context);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ(value, "value2.2");

  ASSERT_EQ(4, mem->num_entries());
  ASSERT_EQ(1, mem->num_deletes());

  // Add memtable to list
  list.Add(mem);

  SequenceNumber saved_seq = seq;

  // Create another memtable and write some keys to it
  WriteBuffer wb2(options.db_write_buffer_size);
  MemTable* mem2 =
      new MemTable(cmp, ioptions, MutableCFOptions(options, ioptions), &wb2);
  mem2->Ref();

  mem2->Add(++seq, kTypeDeletion, "key1", "");
  mem2->Add(++seq, kTypeValue, "key2", "value2.3");

  // Add second memtable to list
  list.Add(mem2);

  // Fetch keys via MemTableList
  merge_context.Clear();
  found =
      list.current()->Get(LookupKey("key1", seq), &value, &s, &merge_context);
  ASSERT_TRUE(found && s.IsNotFound());

  merge_context.Clear();
  found = list.current()->Get(LookupKey("key1", saved_seq), &value, &s,
                              &merge_context);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ("value1", value);

  merge_context.Clear();
  found =
      list.current()->Get(LookupKey("key2", seq), &value, &s, &merge_context);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ(value, "value2.3");

  merge_context.Clear();
  found = list.current()->Get(LookupKey("key2", 1), &value, &s, &merge_context);
  ASSERT_FALSE(found);

  ASSERT_EQ(2, list.size());

  autovector<MemTable*> to_delete;
  list.current()->Unref(&to_delete);
  for (MemTable* m : to_delete) {
    delete m;
  }
}

TEST_F(MemTableListTest, FlushPendingTest) {
  const int num_tables = 5;
  SequenceNumber seq = 1;
  Status s;

  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  ImmutableCFOptions ioptions(options);
  InternalKeyComparator cmp(BytewiseComparator());
  WriteBuffer wb(options.db_write_buffer_size);

  // Create MemTableList
  int min_write_buffer_number_to_merge = 3;
  MemTableList list(min_write_buffer_number_to_merge);

  // Create some MemTables
  std::vector<MemTable*> tables;
  for (int i = 0; i < num_tables; i++) {
    MemTable* mem =
        new MemTable(cmp, ioptions, MutableCFOptions(options, ioptions), &wb);
    mem->Ref();

    std::string value;
    MergeContext merge_context;

    mem->Add(++seq, kTypeValue, "key1", std::to_string(i));
    mem->Add(++seq, kTypeValue, "keyN" + std::to_string(i), "valueN");
    mem->Add(++seq, kTypeValue, "keyX" + std::to_string(i), "value");
    mem->Add(++seq, kTypeValue, "keyM" + std::to_string(i), "valueM");
    mem->Add(++seq, kTypeDeletion, "keyX" + std::to_string(i), "");

    tables.push_back(mem);
  }

  // Nothing to flush
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));
  autovector<MemTable*> to_flush;
  list.PickMemtablesToFlush(&to_flush);
  ASSERT_EQ(0, to_flush.size());

  // Request a flush even though there is nothing to flush
  list.FlushRequested();
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Attempt to 'flush' to clear request for flush
  list.PickMemtablesToFlush(&to_flush);
  ASSERT_EQ(0, to_flush.size());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Request a flush again
  list.FlushRequested();
  // No flush pending since the list is empty.
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Add 2 tables
  list.Add(tables[0]);
  list.Add(tables[1]);
  ASSERT_EQ(2, list.size());

  // Even though we have less than the minimum to flush, a flush is
  // pending since we had previously requested a flush and never called
  // PickMemtablesToFlush() to clear the flush.
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush
  list.PickMemtablesToFlush(&to_flush);
  ASSERT_EQ(2, to_flush.size());
  ASSERT_EQ(2, list.size());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Revert flush
  list.RollbackMemtableFlush(to_flush, 0);
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));
  to_flush.clear();

  // Add another table
  list.Add(tables[2]);
  // We now have the minimum to flush regardles of whether FlushRequested()
  // was called.
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush
  list.PickMemtablesToFlush(&to_flush);
  ASSERT_EQ(3, to_flush.size());
  ASSERT_EQ(3, list.size());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush again
  autovector<MemTable*> to_flush2;
  list.PickMemtablesToFlush(&to_flush2);
  ASSERT_EQ(0, to_flush2.size());
  ASSERT_EQ(3, list.size());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Add another table
  list.Add(tables[3]);
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Request a flush again
  list.FlushRequested();
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush again
  list.PickMemtablesToFlush(&to_flush2);
  ASSERT_EQ(1, to_flush2.size());
  ASSERT_EQ(4, list.size());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Rollback first pick of tables
  list.RollbackMemtableFlush(to_flush, 0);
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));
  to_flush.clear();

  // Add another tables
  list.Add(tables[4]);
  ASSERT_EQ(5, list.size());
  // We now have the minimum to flush regardles of whether FlushRequested()
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush
  list.PickMemtablesToFlush(&to_flush);
  // Should pick 4 of 5 since 1 table has been picked in to_flush2
  ASSERT_EQ(4, to_flush.size());
  ASSERT_EQ(5, list.size());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush again
  autovector<MemTable*> to_flush3;
  ASSERT_EQ(0, to_flush3.size());  // nothing not in progress of being flushed
  ASSERT_EQ(5, list.size());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  autovector<MemTable*> to_delete;

  // Flush the 4 memtables that were picked in to_flush
  s = Mock_InstallMemtableFlushResults(
      &list, MutableCFOptions(options, ioptions), to_flush, &to_delete);
  ASSERT_OK(s);

  // Note:  now to_flush contains tables[0,1,2,4].  to_flush2 contains
  // tables[3].
  // Current implementation will only commit memtables in the order they were
  // created.  So InstallMemtableFlushResults will install the first 3 tables
  // in to_flush and stop when it encounters a table not yet flushed.
  ASSERT_EQ(3, to_delete.size());
  ASSERT_EQ(2, list.size());

  for (const auto& m : to_delete) {
    // Refcount should be 0 after calling InstallMemtableFlushResults.
    // Verify this, by Ref'ing then UnRef'ing:
    m->Ref();
    ASSERT_EQ(m, m->Unref());
    delete m;
  }
  to_delete.clear();

  // Request a flush again. Should be nothing to flush
  list.FlushRequested();
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Flush the 1 memtable that was picked in to_flush2
  s = MemTableListTest::Mock_InstallMemtableFlushResults(
      &list, MutableCFOptions(options, ioptions), to_flush2, &to_delete);
  ASSERT_OK(s);

  // This will actually intall 2 tables.  The 1 we told it to flush, and also
  // tables[4] which has been waiting for tables[3] to commit.
  ASSERT_EQ(2, to_delete.size());
  ASSERT_EQ(0, list.size());

  for (const auto& m : to_delete) {
    // Refcount should be 0 after calling InstallMemtableFlushResults.
    // Verify this, by Ref'ing then UnRef'ing:
    m->Ref();
    ASSERT_EQ(m, m->Unref());
    delete m;
  }
  to_delete.clear();

  list.current()->Unref(&to_delete);
  ASSERT_EQ(0, to_delete.size());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
