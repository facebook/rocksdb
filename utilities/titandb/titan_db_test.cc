#include <inttypes.h>
#include <options/cf_options.h>

#include "rocksdb/utilities/titandb/db.h"
#include "utilities/titandb/db_impl.h"
#include "utilities/titandb/titan_fault_injection_test_env.h"
#include "util/filename.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/sync_point.h"
#include "blob_file_reader.h"
#include "blob_file_iterator.h"
#include "db_iter.h"

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
      db_impl_ = reinterpret_cast<TitanDBImpl*>(db_);
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

  std::weak_ptr<BlobStorage> GetBlobStorage(ColumnFamilyHandle* cf_handle = nullptr) {
    if(cf_handle == nullptr) {
      cf_handle = db_->DefaultColumnFamily();
    }
    return db_impl_->vset_->GetBlobStorage(cf_handle->GetID());
  }

  void VerifyDB(const std::map<std::string, std::string>& data, ReadOptions ropts = ReadOptions()) {
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

  void VerifyBlob(
      uint64_t file_number,
      const std::map<std::string, std::string>& data) {
    // Open blob file and iterate in-file records
    EnvOptions env_opt;
    uint64_t file_size = 0;
    std::map<std::string, std::string> file_data;
    std::unique_ptr<RandomAccessFileReader> readable_file;
    std::string file_name = BlobFileName(options_.dirname, file_number);
    ASSERT_OK(env_->GetFileSize(file_name, &file_size));
    NewBlobFileReader(file_number, 0, options_, env_opt, env_,
                      &readable_file);
    BlobFileIterator iter(std::move(readable_file), 
                          file_number,
                          file_size,
                          options_
                          );
    iter.SeekToFirst();
    for(auto& kv : data) {
      if(kv.second.size() < options_.min_blob_size) {
        continue;
      }
      ASSERT_EQ(iter.Valid(), true);
      ASSERT_EQ(iter.key(), kv.first);
      ASSERT_EQ(iter.value(), kv.second);
      iter.Next();
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
    ASSERT_EQ(original_table_factory, immu_cf_options.table_factory);
    ASSERT_OK(db->Close());

    DeleteDir(env_, options_.dirname);
    DeleteDir(env_, dbname_);
  }

  Env* env_{Env::Default()};
  std::string dbname_;
  TitanOptions options_;
  TitanDB* db_{nullptr};
  TitanDBImpl* db_impl_{nullptr};
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

TEST_F(TitanDBTest, Snapshot) {
  Open();
  std::map<std::string, std::string> data;
  Put(1, &data);
  ASSERT_EQ(1, data.size());

  const Snapshot* snapshot(db_->GetSnapshot());
  ReadOptions ropts;
  ropts.snapshot = snapshot;

  VerifyDB(data, ropts);
  Flush();
  VerifyDB(data, ropts);
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(TitanDBTest, IngestExternalFiles) {
  Open();
  SstFileWriter sst_file_writer(EnvOptions(), options_);
  ASSERT_EQ(sst_file_writer.FileSize(), 0);

  const uint64_t kNumEntries = 100;
  std::map<std::string, std::string> total_data;
  std::map<std::string, std::string> original_data;
  std::map<std::string, std::string> ingested_data;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &original_data);
  }
  ASSERT_EQ(kNumEntries, original_data.size());
  total_data.insert(original_data.begin(), original_data.end());
  VerifyDB(total_data);
  Flush();
  VerifyDB(total_data);

  const uint64_t kNumIngestedEntries = 100;
  // Make sure that keys in SST overlaps with existing keys
  const uint64_t kIngestedStart = kNumEntries - kNumEntries / 2;
  std::string sst_file = options_.dirname + "/for_ingest.sst";
  ASSERT_OK(sst_file_writer.Open(sst_file));
  for (uint64_t i = 1; i <= kNumIngestedEntries; i++) {
    std::string key = GenKey(kIngestedStart + i);
    std::string value = GenValue(kIngestedStart + i);
    ASSERT_OK(sst_file_writer.Put(key, value));
    total_data[key] = value;
    ingested_data.emplace(key, value);
  }
  ASSERT_OK(sst_file_writer.Finish());
  IngestExternalFileOptions ifo;
  ASSERT_OK(db_->IngestExternalFile({sst_file}, ifo));
  VerifyDB(total_data);
  Flush();
  VerifyDB(total_data);
  for(auto& handle : cf_handles_) {
    auto blob = GetBlobStorage(handle);
    ASSERT_EQ(1, blob.lock()->NumBlobFiles());
  }

  CompactRangeOptions copt;
  ASSERT_OK(db_->CompactRange(copt, nullptr, nullptr));
  VerifyDB(total_data);
  for(auto& handle : cf_handles_) {
    auto blob = GetBlobStorage(handle);
    ASSERT_EQ(2, blob.lock()->NumBlobFiles());
    std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
    blob.lock()->ExportBlobFiles(blob_files);
    ASSERT_EQ(2, blob_files.size());
    auto bf = blob_files.begin();
    VerifyBlob(bf->first, original_data);
    bf ++;
    VerifyBlob(bf->first, ingested_data);
  }
}

TEST_F(TitanDBTest, DISABLED_ReadAfterDropCF) {
  Open();
  const uint64_t kNumCF = 3;
  for(uint64_t i = 1; i <= kNumCF; i++) {
    AddCF(std::to_string(i));
  }
  const uint64_t kNumEntries = 100;
  std::map<std::string, std::string> data;
  for(uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  VerifyDB(data);
  Flush();
  VerifyDB(data);
  for(auto& handle : cf_handles_) {
    ASSERT_OK(db_->DropColumnFamily(handle));
    VerifyDB(data);
  }
}

#ifndef NDEBUG
TEST_F(TitanDBTest, BlobFileIOError) {
  std::unique_ptr<TitanFaultInjectionTestEnv> mock_env(
      new TitanFaultInjectionTestEnv(env_));
  options_.env = mock_env.get();
  options_.disable_background_gc = true; // avoid abort by BackgroundGC
  Open();

  std::map<std::string, std::string> data;
  const int kNumEntries = 100;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  ASSERT_EQ(kNumEntries, data.size());
  CompactRangeOptions copts;
  ASSERT_OK(db_->CompactRange(copts, nullptr, nullptr));
  VerifyDB(data);

  SyncPoint::GetInstance()->SetCallBack(
    "BlobFileReader::Get", [&](void *) {
      mock_env->SetFilesystemActive(
        false, 
        Status::IOError("Injected error")
        );
    });
  SyncPoint::GetInstance()->EnableProcessing();
  for(auto& it : data) {
    std::string value;
    if(it.second.size() > options_.min_blob_size) {
      ASSERT_TRUE(db_->Get(ReadOptions(), it.first, &value).IsIOError());
      mock_env->SetFilesystemActive(true);
    }
  }
  SyncPoint::GetInstance()->DisableProcessing();
  mock_env->SetFilesystemActive(true);

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  SyncPoint::GetInstance()->EnableProcessing();
  iter->SeekToFirst();
  ASSERT_TRUE(iter->status().IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  mock_env->SetFilesystemActive(true);

  iter.reset(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  SyncPoint::GetInstance()->EnableProcessing();
  iter->Next(); // second value (k=2) is inlined
  ASSERT_TRUE(iter->Valid());
  iter->Next();
  ASSERT_TRUE(iter->status().IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  mock_env->SetFilesystemActive(true);

  options_.env = env_;
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // env must be destructed AFTER db is closed to avoid
  // `pure abstract method called` complaint.
  iter.reset(nullptr); // early release to avoid outstanding reference
  Close();
  db_ = nullptr;
}

TEST_F(TitanDBTest, FlushWriteIOErrorHandling) {
  std::unique_ptr<TitanFaultInjectionTestEnv> mock_env(
      new TitanFaultInjectionTestEnv(env_));
  options_.env = mock_env.get();
  options_.disable_background_gc = true; // avoid abort by BackgroundGC
  Open();

  std::map<std::string, std::string> data;
  const int kNumEntries = 100;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  ASSERT_EQ(kNumEntries, data.size());
  CompactRangeOptions copts;
  // no compaction to enable Flush
  VerifyDB(data);

  SyncPoint::GetInstance()->SetCallBack(
    "FlushJob::Start", [&](void *) {
      mock_env->SetFilesystemActive(
        false,
        Status::IOError("FlushJob injected error")
        );
    });
  SyncPoint::GetInstance()->EnableProcessing();
  FlushOptions fopts;
  ASSERT_TRUE(db_->Flush(fopts).IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  mock_env->SetFilesystemActive(true);
  // subsequent writes return error too
  WriteOptions wopts;
  std::string key = "key_after_flush";
  std::string value = "value_after_flush";
  ASSERT_TRUE(db_->Put(wopts, key, value).IsIOError());

  options_.env = env_;
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // env must be destructed AFTER db is closed to avoid
  // `pure abstract method called` complaint.
  Close();
  db_ = nullptr;
}

TEST_F(TitanDBTest, CompactionWriteIOErrorHandling) {
    std::unique_ptr<TitanFaultInjectionTestEnv> mock_env(
      new TitanFaultInjectionTestEnv(env_));
  options_.env = mock_env.get();
  options_.disable_background_gc = true; // avoid abort by BackgroundGC
  Open();

  std::map<std::string, std::string> data;
  const int kNumEntries = 100;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  ASSERT_EQ(kNumEntries, data.size());
  CompactRangeOptions copts;
  // do not compact to enable following Compaction
  VerifyDB(data);

  SyncPoint::GetInstance()->SetCallBack(
    "BackgroundCallCompaction:0", [&](void *) {
      mock_env->SetFilesystemActive(
        false,
        Status::IOError("Compaction injected error")
        );
    });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_TRUE(db_->CompactRange(copts, nullptr, nullptr).IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  mock_env->SetFilesystemActive(true);
  // subsequent writes return error too
  WriteOptions wopts;
  std::string key = "key_after_compaction";
  std::string value = "value_after_compaction";
  ASSERT_TRUE(db_->Put(wopts, key, value).IsIOError());

  options_.env = env_;
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // env must be destructed AFTER db is closed to avoid
  // `pure abstract method called` complaint.
  Close();
  db_ = nullptr;
}

TEST_F(TitanDBTest, BlobFileCorruptionErrorHandling) {
  options_.disable_background_gc = true; // avoid abort by BackgroundGC
  Open();
  std::map<std::string, std::string> data;
  const int kNumEntries = 100;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  ASSERT_EQ(kNumEntries, data.size());
  CompactRangeOptions copt;
  ASSERT_OK(db_->CompactRange(copt, nullptr, nullptr));
  VerifyDB(data);

  // Modify the checksum data to reproduce a mismatch
  SyncPoint::GetInstance()->SetCallBack(
    "BlobDecoder::DecodeRecord", [&](void* arg) {
      auto* crc = reinterpret_cast<uint32_t*>(arg);
      *crc = *crc + 1;
    });

  SyncPoint::GetInstance()->EnableProcessing();
  for (auto& it : data) {
    std::string value;
    if(it.second.size() < options_.min_blob_size) {
        continue;
    }
    ASSERT_TRUE(db_->Get(ReadOptions(), it.first, &value).IsCorruption());
  }
  SyncPoint::GetInstance()->DisableProcessing();

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  SyncPoint::GetInstance()->EnableProcessing();
  iter->SeekToFirst();
  ASSERT_TRUE(iter->status().IsCorruption());
  SyncPoint::GetInstance()->DisableProcessing();

  iter.reset(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  SyncPoint::GetInstance()->EnableProcessing();
  iter->Next(); // second value (k=2) is inlined
  ASSERT_TRUE(iter->Valid());
  iter->Next();
  ASSERT_TRUE(iter->status().IsCorruption());
  SyncPoint::GetInstance()->DisableProcessing();

  SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif // !NDEBUG

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
