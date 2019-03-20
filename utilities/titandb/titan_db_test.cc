#include <inttypes.h>
#include <options/cf_options.h>

#include "rocksdb/utilities/titandb/db.h"
#include "util/filename.h"
#include "util/random.h"
#include "util/testharness.h"

namespace rocksdb {
namespace titandb {

void DeleteDir(Env* env, const std::string& dirname) {
  std::vector<std::string> filenames;
  env->GetChildren(dirname, &filenames);
  for (auto& fname : filenames) {
    env->DeleteFile(dirname + "/" + fname);
  }
  env->DeleteDir(dirname);
}

class TitanDBTest : public testing::Test {
 public:
  TitanDBTest() : dbname_(test::TmpDir()) {
    options_.dirname = dbname_ + "/titandb";
    options_.create_if_missing = true;
    options_.min_blob_size = 32;
    options_.min_gc_batch_size = 1;
    options_.blob_file_compression = CompressionType::kLZ4Compression;
    DeleteDir(env_, options_.dirname);
    DeleteDir(env_, dbname_);
  }

  ~TitanDBTest() { Close(); }

  void Open() {
    if (cf_names_.empty()) {
      ASSERT_OK(TitanDB::Open(options_, dbname_, &db_));
    } else {
      TitanDBOptions db_options(options_);
      TitanCFOptions cf_options(options_);
      cf_names_.clear();
      ASSERT_OK(DB::ListColumnFamilies(db_options, dbname_, &cf_names_));
      std::vector<TitanCFDescriptor> descs;
      for (auto& name : cf_names_) {
        descs.emplace_back(name, cf_options);
      }
      cf_handles_.clear();
      ASSERT_OK(TitanDB::Open(db_options, dbname_, descs, &cf_handles_, &db_));
    }
  }

  void Close() {
    if (!db_) return;
    for (auto& handle : cf_handles_) {
      db_->DestroyColumnFamilyHandle(handle);
    }
    ASSERT_OK(db_->Close());
    delete db_;
    db_ = nullptr;
  }

  void Reopen() {
    Close();
    Open();
  }

  void AddCF(const std::string& name) {
    TitanCFDescriptor desc(name, options_);
    ColumnFamilyHandle* handle = nullptr;
    ASSERT_OK(db_->CreateColumnFamily(desc, &handle));
    cf_names_.emplace_back(name);
    cf_handles_.emplace_back(handle);
  }

  void DropCF(const std::string& name) {
    for (size_t i = 0; i < cf_names_.size(); i++) {
      if (cf_names_[i] != name) continue;
      auto handle = cf_handles_[i];
      ASSERT_OK(db_->DropColumnFamily(handle));
      db_->DestroyColumnFamilyHandle(handle);
      cf_names_.erase(cf_names_.begin() + i);
      cf_handles_.erase(cf_handles_.begin() + i);
      break;
    }
  }

  void Put(uint64_t k, std::map<std::string, std::string>* data = nullptr) {
    WriteOptions wopts;
    std::string key = GenKey(k);
    std::string value = GenValue(k);
    ASSERT_OK(db_->Put(wopts, key, value));
    for (auto& handle : cf_handles_) {
      ASSERT_OK(db_->Put(wopts, handle, key, value));
    }
    if (data != nullptr) {
      data->emplace(key, value);
    }
  }

  void Flush() {
    FlushOptions fopts;
    ASSERT_OK(db_->Flush(fopts));
    for (auto& handle : cf_handles_) {
      ASSERT_OK(db_->Flush(fopts, handle));
    }
  }

  void VerifyDB(const std::map<std::string, std::string>& data) {
    ReadOptions ropts;

    for (auto& kv : data) {
      std::string value;
      ASSERT_OK(db_->Get(ropts, kv.first, &value));
      ASSERT_EQ(value, kv.second);
      for (auto& handle : cf_handles_) {
        ASSERT_OK(db_->Get(ropts, handle, kv.first, &value));
        ASSERT_EQ(value, kv.second);
      }
      std::vector<Slice> keys(cf_handles_.size(), kv.first);
      std::vector<std::string> values;
      auto res = db_->MultiGet(ropts, cf_handles_, keys, &values);
      for (auto& s : res) ASSERT_OK(s);
      for (auto& v : values) ASSERT_EQ(v, kv.second);
    }

    std::vector<Iterator*> iterators;
    db_->NewIterators(ropts, cf_handles_, &iterators);
    iterators.emplace_back(db_->NewIterator(ropts));
    for (auto& handle : cf_handles_) {
      iterators.emplace_back(db_->NewIterator(ropts, handle));
    }
    for (auto& iter : iterators) {
      iter->SeekToFirst();
      for (auto& kv : data) {
        ASSERT_EQ(iter->Valid(), true);
        ASSERT_EQ(iter->key(), kv.first);
        ASSERT_EQ(iter->value(), kv.second);
        iter->Next();
      }
      delete iter;
    }
  }

  std::string GenKey(uint64_t i) {
    char buf[64];
    snprintf(buf, sizeof(buf), "k-%08" PRIu64, i);
    return buf;
  }

  std::string GenValue(uint64_t k) {
    if (k % 2 == 0) {
      return std::string(options_.min_blob_size - 1, 'v');
    } else {
      return std::string(options_.min_blob_size + 1, 'v');
    }
  }

  void TestTableFactory() {
    DeleteDir(env_, options_.dirname);
    DeleteDir(env_, dbname_);
    Options options;
    options.create_if_missing = true;
    options.table_factory.reset(
        NewBlockBasedTableFactory(BlockBasedTableOptions()));
    auto* original_table_factory = options.table_factory.get();
    TitanDB* db;
    ASSERT_OK(TitanDB::Open(TitanOptions(options), dbname_, &db));
    auto cf_options = db->GetOptions(db->DefaultColumnFamily());
    auto db_options = db->GetDBOptions();
    ImmutableCFOptions immu_cf_options(ImmutableDBOptions(db_options),
                                       cf_options);
    ASSERT_EQ(original_table_factory, immu_cf_options.original_table_factory);
    ASSERT_NE(original_table_factory, immu_cf_options.table_factory);
    ASSERT_OK(db->Close());

    DeleteDir(env_, options_.dirname);
    DeleteDir(env_, dbname_);
    DB* db2;
    ASSERT_OK(DB::Open(options, dbname_, &db2));
    auto cf_options2 = db2->GetOptions(db2->DefaultColumnFamily());
    auto db_options2 = db2->GetDBOptions();
    ImmutableCFOptions immu_cf_options2(ImmutableDBOptions(db_options2),
                                        cf_options2);
    ASSERT_EQ(nullptr, immu_cf_options2.original_table_factory);
    ASSERT_EQ(original_table_factory, immu_cf_options2.table_factory);
    ASSERT_OK(db2->Close());
  }

  Env* env_{Env::Default()};
  std::string dbname_;
  TitanOptions options_;
  TitanDB* db_{nullptr};
  std::vector<std::string> cf_names_;
  std::vector<ColumnFamilyHandle*> cf_handles_;
};

TEST_F(TitanDBTest, Basic) {
  const uint64_t kNumKeys = 100;
  std::map<std::string, std::string> data;
  for (auto i = 0; i < 6; i++) {
    if (i == 0) {
      Open();
    } else {
      Reopen();
      VerifyDB(data);
      AddCF(std::to_string(i));
      if (i % 3 == 0) {
        DropCF(std::to_string(i - 1));
        DropCF(std::to_string(i - 2));
      }
    }
    for (uint64_t k = 1; k <= kNumKeys; k++) {
      Put(k, &data);
    }
    Flush();
    VerifyDB(data);
  }
}

TEST_F(TitanDBTest, TableFactory) { TestTableFactory(); }

TEST_F(TitanDBTest, DbIter) {
  Open();
  std::map<std::string, std::string> data;
  const int kNumEntries = 100;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  ASSERT_EQ(kNumEntries, data.size());
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  for (const auto& it : data) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(it.first, iter->key());
    ASSERT_EQ(it.second, iter->value());
    iter->Next();
  }
  ASSERT_FALSE(iter->Valid());
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
