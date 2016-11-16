//  Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <stdio.h>

#if !defined(ROCKSDB_LITE)

#if defined(LUA)

#include <string>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/lua/rocks_lua_compaction_filter.h"
#include "util/testharness.h"

namespace rocksdb {

class StopOnErrorLogger : public Logger {
 public:
  using Logger::Logv;
  virtual void Logv(const char* format, va_list ap) override {
    vfprintf(stderr, format, ap);
    fprintf(stderr, "\n");
    ASSERT_TRUE(false);
  }
};


class RocksLuaTest : public testing::Test {
 public:
  RocksLuaTest() : rnd_(301) {
    temp_dir_ = test::TmpDir(Env::Default());
    db_ = nullptr;
  }

  std::string RandomString(int len) {
    std::string res;
    for (int i = 0; i < len; ++i) {
      res += rnd_.Uniform(26) + 'a';
    }
    return res;
  }

  void CreateDBWithLuaCompactionFilter(
      const lua::RocksLuaCompactionFilterOptions& lua_opt,
      const std::string& db_path,
      std::unordered_map<std::string, std::string>* kvs,
      const int kNumFlushes = 5,
      std::shared_ptr<rocksdb::lua::RocksLuaCompactionFilterFactory>*
          output_factory = nullptr) {
    const int kKeySize = 10;
    const int kValueSize = 50;
    const int kKeysPerFlush = 2;
    auto factory =
        std::make_shared<rocksdb::lua::RocksLuaCompactionFilterFactory>(
            lua_opt);
    if (output_factory != nullptr) {
      *output_factory = factory;
    }

    options_ = Options();
    options_.create_if_missing = true;
    options_.compaction_filter_factory = factory;
    options_.max_bytes_for_level_base =
        (kKeySize + kValueSize) * kKeysPerFlush * 2;
    options_.max_bytes_for_level_multiplier = 2;
    options_.target_file_size_base = (kKeySize + kValueSize) * kKeysPerFlush;
    options_.level0_file_num_compaction_trigger = 2;
    DestroyDB(db_path, options_);
    ASSERT_OK(DB::Open(options_, db_path, &db_));

    for (int f = 0; f < kNumFlushes; ++f) {
      for (int i = 0; i < kKeysPerFlush; ++i) {
        std::string key = RandomString(kKeySize);
        std::string value = RandomString(kValueSize);
        kvs->insert({key, value});
        ASSERT_OK(db_->Put(WriteOptions(), key, value));
      }
      db_->Flush(FlushOptions());
    }
  }

  ~RocksLuaTest() {
    if (db_) {
      delete db_;
    }
  }
  std::string temp_dir_;
  DB* db_;
  Random rnd_;
  Options options_;
};

TEST_F(RocksLuaTest, Default) {
  // If nothing is set in the LuaCompactionFilterOptions, then
  // RocksDB will keep all the key / value pairs, but it will also
  // print our error log indicating failure.
  std::string db_path = temp_dir_ + "/rocks_lua_test";

  lua::RocksLuaCompactionFilterOptions lua_opt;

  std::unordered_map<std::string, std::string> kvs;
  CreateDBWithLuaCompactionFilter(lua_opt, db_path, &kvs);

  for (auto const& entry : kvs) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), entry.first, &value));
    ASSERT_EQ(value, entry.second);
  }
}

TEST_F(RocksLuaTest, KeepsAll) {
  std::string db_path = temp_dir_ + "/rocks_lua_test";

  lua::RocksLuaCompactionFilterOptions lua_opt;
  lua_opt.error_log = std::make_shared<StopOnErrorLogger>();
  // keeps all the key value pairs
  lua_opt.lua_script =
      "function Filter(level, key, existing_value)\n"
      "  return false, false, \"\"\n"
      "end\n"
      "\n"
      "function FilterMergeOperand(level, key, operand)\n"
      "  return false\n"
      "end\n"
      "function Name()\n"
      "  return \"KeepsAll\"\n"
      "end\n"
      "\n";

  std::unordered_map<std::string, std::string> kvs;
  CreateDBWithLuaCompactionFilter(lua_opt, db_path, &kvs);

  for (auto const& entry : kvs) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), entry.first, &value));
    ASSERT_EQ(value, entry.second);
  }
}

TEST_F(RocksLuaTest, GetName) {
  std::string db_path = temp_dir_ + "/rocks_lua_test";

  lua::RocksLuaCompactionFilterOptions lua_opt;
  lua_opt.error_log = std::make_shared<StopOnErrorLogger>();
  const std::string kScriptName = "SimpleLuaCompactionFilter";
  lua_opt.lua_script =
      std::string(
          "function Filter(level, key, existing_value)\n"
          "  return false, false, \"\"\n"
          "end\n"
          "\n"
          "function FilterMergeOperand(level, key, operand)\n"
          "  return false\n"
          "end\n"
          "function Name()\n"
          "  return \"") + kScriptName + "\"\n"
      "end\n"
      "\n";

  std::shared_ptr<CompactionFilterFactory> factory =
      std::make_shared<lua::RocksLuaCompactionFilterFactory>(lua_opt);
  std::string factory_name(factory->Name());
  ASSERT_NE(factory_name.find(kScriptName), std::string::npos);
}

TEST_F(RocksLuaTest, RemovesAll) {
  std::string db_path = temp_dir_ + "/rocks_lua_test";

  lua::RocksLuaCompactionFilterOptions lua_opt;
  lua_opt.error_log = std::make_shared<StopOnErrorLogger>();
  // removes all the key value pairs
  lua_opt.lua_script =
      "function Filter(level, key, existing_value)\n"
      "  return true, false, \"\"\n"
      "end\n"
      "\n"
      "function FilterMergeOperand(level, key, operand)\n"
      "  return false\n"
      "end\n"
      "function Name()\n"
      "  return \"RemovesAll\"\n"
      "end\n"
      "\n";

  std::unordered_map<std::string, std::string> kvs;
  CreateDBWithLuaCompactionFilter(lua_opt, db_path, &kvs);
  // Issue full compaction and expect nothing is in the DB.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (auto const& entry : kvs) {
    std::string value;
    auto s = db_->Get(ReadOptions(), entry.first, &value);
    ASSERT_TRUE(s.IsNotFound());
  }
}

TEST_F(RocksLuaTest, FilterByKey) {
  std::string db_path = temp_dir_ + "/rocks_lua_test";

  lua::RocksLuaCompactionFilterOptions lua_opt;
  lua_opt.error_log = std::make_shared<StopOnErrorLogger>();
  // removes all keys whose initial is less than 'r'
  lua_opt.lua_script =
      "function Filter(level, key, existing_value)\n"
      "  if key:sub(1,1) < 'r' then\n"
      "    return true, false, \"\"\n"
      "  end\n"
      "  return false, false, \"\"\n"
      "end\n"
      "\n"
      "function FilterMergeOperand(level, key, operand)\n"
      "  return false\n"
      "end\n"
      "function Name()\n"
      "  return \"KeepsAll\"\n"
      "end\n";

  std::unordered_map<std::string, std::string> kvs;
  CreateDBWithLuaCompactionFilter(lua_opt, db_path, &kvs);
  // Issue full compaction and expect nothing is in the DB.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (auto const& entry : kvs) {
    std::string value;
    auto s = db_->Get(ReadOptions(), entry.first, &value);
    if (entry.first[0] < 'r') {
      ASSERT_TRUE(s.IsNotFound());
    } else {
      ASSERT_TRUE(s.ok());
      ASSERT_TRUE(value == entry.second);
    }
  }
}

TEST_F(RocksLuaTest, FilterByValue) {
  std::string db_path = temp_dir_ + "/rocks_lua_test";

  lua::RocksLuaCompactionFilterOptions lua_opt;
  lua_opt.error_log = std::make_shared<StopOnErrorLogger>();
  // removes all values whose initial is less than 'r'
  lua_opt.lua_script =
      "function Filter(level, key, existing_value)\n"
      "  if existing_value:sub(1,1) < 'r' then\n"
      "    return true, false, \"\"\n"
      "  end\n"
      "  return false, false, \"\"\n"
      "end\n"
      "\n"
      "function FilterMergeOperand(level, key, operand)\n"
      "  return false\n"
      "end\n"
      "function Name()\n"
      "  return \"FilterByValue\"\n"
      "end\n"
      "\n";

  std::unordered_map<std::string, std::string> kvs;
  CreateDBWithLuaCompactionFilter(lua_opt, db_path, &kvs);
  // Issue full compaction and expect nothing is in the DB.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (auto const& entry : kvs) {
    std::string value;
    auto s = db_->Get(ReadOptions(), entry.first, &value);
    if (entry.second[0] < 'r') {
      ASSERT_TRUE(s.IsNotFound());
    } else {
      ASSERT_TRUE(s.ok());
      ASSERT_EQ(value, entry.second);
    }
  }
}

TEST_F(RocksLuaTest, ChangeValue) {
  std::string db_path = temp_dir_ + "/rocks_lua_test";

  lua::RocksLuaCompactionFilterOptions lua_opt;
  lua_opt.error_log = std::make_shared<StopOnErrorLogger>();
  // Replace all values by their reversed key
  lua_opt.lua_script =
      "function Filter(level, key, existing_value)\n"
      "  return false, true, key:reverse()\n"
      "end\n"
      "\n"
      "function FilterMergeOperand(level, key, operand)\n"
      "  return false\n"
      "end\n"
      "function Name()\n"
      "  return \"ChangeValue\"\n"
      "end\n"
      "\n";

  std::unordered_map<std::string, std::string> kvs;
  CreateDBWithLuaCompactionFilter(lua_opt, db_path, &kvs);
  // Issue full compaction and expect nothing is in the DB.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (auto const& entry : kvs) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), entry.first, &value));
    std::string new_value = entry.first;
    std::reverse(new_value.begin(), new_value.end());
    ASSERT_EQ(value, new_value);
  }
}

TEST_F(RocksLuaTest, ConditionallyChangeAndFilterValue) {
  std::string db_path = temp_dir_ + "/rocks_lua_test";

  lua::RocksLuaCompactionFilterOptions lua_opt;
  lua_opt.error_log = std::make_shared<StopOnErrorLogger>();
  // Performs the following logic:
  // If key[0] < 'h' --> replace value by reverse key
  // If key[0] >= 'r' --> keep the original key value
  // Otherwise, filter the key value
  lua_opt.lua_script =
      "function Filter(level, key, existing_value)\n"
      "  if key:sub(1,1) < 'h' then\n"
      "    return false, true, key:reverse()\n"
      "  elseif key:sub(1,1) < 'r' then\n"
      "    return true, false, \"\"\n"
      "  end\n"
      "  return false, false, \"\"\n"
      "end\n"
      "function Name()\n"
      "  return \"ConditionallyChangeAndFilterValue\"\n"
      "end\n"
      "\n";

  std::unordered_map<std::string, std::string> kvs;
  CreateDBWithLuaCompactionFilter(lua_opt, db_path, &kvs);
  // Issue full compaction and expect nothing is in the DB.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (auto const& entry : kvs) {
    std::string value;
    auto s = db_->Get(ReadOptions(), entry.first, &value);
    if (entry.first[0] < 'h') {
      ASSERT_TRUE(s.ok());
      std::string new_value = entry.first;
      std::reverse(new_value.begin(), new_value.end());
      ASSERT_EQ(value, new_value);
    } else if (entry.first[0] < 'r') {
      ASSERT_TRUE(s.IsNotFound());
    } else {
      ASSERT_TRUE(s.ok());
      ASSERT_EQ(value, entry.second);
    }
  }
}

TEST_F(RocksLuaTest, DynamicChangeScript) {
  std::string db_path = temp_dir_ + "/rocks_lua_test";

  lua::RocksLuaCompactionFilterOptions lua_opt;
  lua_opt.error_log = std::make_shared<StopOnErrorLogger>();
  // keeps all the key value pairs
  lua_opt.lua_script =
      "function Filter(level, key, existing_value)\n"
      "  return false, false, \"\"\n"
      "end\n"
      "\n"
      "function FilterMergeOperand(level, key, operand)\n"
      "  return false\n"
      "end\n"
      "function Name()\n"
      "  return \"KeepsAll\"\n"
      "end\n"
      "\n";

  std::unordered_map<std::string, std::string> kvs;
  std::shared_ptr<rocksdb::lua::RocksLuaCompactionFilterFactory> factory;
  CreateDBWithLuaCompactionFilter(lua_opt, db_path, &kvs, 30, &factory);
  uint64_t count = 0;
  ASSERT_TRUE(db_->GetIntProperty(
      rocksdb::DB::Properties::kNumEntriesActiveMemTable, &count));
  ASSERT_EQ(count, 0);
  ASSERT_TRUE(db_->GetIntProperty(
      rocksdb::DB::Properties::kNumEntriesImmMemTables, &count));
  ASSERT_EQ(count, 0);

  CompactRangeOptions cr_opt;
  cr_opt.bottommost_level_compaction =
      rocksdb::BottommostLevelCompaction::kForce;

  // Issue full compaction and expect everything is in the DB.
  ASSERT_OK(db_->CompactRange(cr_opt, nullptr, nullptr));

  for (auto const& entry : kvs) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), entry.first, &value));
    ASSERT_EQ(value, entry.second);
  }

  // change the lua script to removes all the key value pairs
  factory->SetScript(
      "function Filter(level, key, existing_value)\n"
      "  return true, false, \"\"\n"
      "end\n"
      "\n"
      "function FilterMergeOperand(level, key, operand)\n"
      "  return false\n"
      "end\n"
      "function Name()\n"
      "  return \"RemovesAll\"\n"
      "end\n"
      "\n");
  {
    std::string key = "another-key";
    std::string value = "another-value";
    kvs.insert({key, value});
    ASSERT_OK(db_->Put(WriteOptions(), key, value));
    db_->Flush(FlushOptions());
  }

  cr_opt.change_level = true;
  cr_opt.target_level = 5;
  // Issue full compaction and expect nothing is in the DB.
  ASSERT_OK(db_->CompactRange(cr_opt, nullptr, nullptr));

  for (auto const& entry : kvs) {
    std::string value;
    auto s = db_->Get(ReadOptions(), entry.first, &value);
    ASSERT_TRUE(s.IsNotFound());
  }
}

TEST_F(RocksLuaTest, LuaConditionalTypeError) {
  std::string db_path = temp_dir_ + "/rocks_lua_test";

  lua::RocksLuaCompactionFilterOptions lua_opt;
  // Filter() error when input key's initial >= 'r'
  lua_opt.lua_script =
      "function Filter(level, key, existing_value)\n"
      "  if existing_value:sub(1,1) >= 'r' then\n"
      "    return true, 2, \"\" -- incorrect type of 2nd return value\n"
      "  end\n"
      "  return true, false, \"\"\n"
      "end\n"
      "\n"
      "function FilterMergeOperand(level, key, operand)\n"
      "  return false\n"
      "end\n"
      "function Name()\n"
      "  return \"BuggyCode\"\n"
      "end\n"
      "\n";

  std::unordered_map<std::string, std::string> kvs;
  // Create DB with 10 files
  CreateDBWithLuaCompactionFilter(lua_opt, db_path, &kvs, 10);

  // Issue full compaction and expect all keys which initial is < 'r'
  // will be deleted as we keep the key value when we hit an error.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (auto const& entry : kvs) {
    std::string value;
    auto s = db_->Get(ReadOptions(), entry.first, &value);
    if (entry.second[0] < 'r') {
      ASSERT_TRUE(s.IsNotFound());
    } else {
      ASSERT_TRUE(s.ok());
      ASSERT_EQ(value, entry.second);
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else

int main(int argc, char** argv) {
  printf("LUA_PATH is not set.  Ignoring the test.\n");
}

#endif  // defined(LUA)

#else

int main(int argc, char** argv) {
  printf("Lua is not supported in RocksDBLite.  Ignoring the test.\n");
}

#endif  // !defined(ROCKSDB_LITE)
