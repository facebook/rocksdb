//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/flush_job.h"

#include <algorithm>
#include <array>
#include <map>
#include <string>

#include "db/blob/blob_index.h"
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/version_set.h"
#include "file/writable_file_writer.h"
#include "rocksdb/cache.h"
#include "rocksdb/file_system.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/mock_table.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// TODO(icanadi) Mock out everything else:
// 1. VersionSet
// 2. Memtable
class FlushJobTestBase : public testing::Test {
 protected:
  FlushJobTestBase(std::string dbname, const Comparator* ucmp)
      : env_(Env::Default()),
        fs_(env_->GetFileSystem()),
        dbname_(std::move(dbname)),
        ucmp_(ucmp),
        options_(),
        db_options_(options_),
        column_family_names_({kDefaultColumnFamilyName, "foo", "bar"}),
        table_cache_(NewLRUCache(50000, 16)),
        write_buffer_manager_(db_options_.db_write_buffer_size),
        shutting_down_(false),
        mock_table_factory_(new mock::MockTableFactory()) {}

  virtual ~FlushJobTestBase() {
    if (getenv("KEEP_DB")) {
      fprintf(stdout, "db is still in %s\n", dbname_.c_str());
    } else {
      // destroy versions_ to release all file handles
      versions_.reset();
      EXPECT_OK(DestroyDir(env_, dbname_));
    }
  }

  void NewDB() {
    ASSERT_OK(SetIdentityFile(env_, dbname_));
    VersionEdit new_db;

    new_db.SetLogNumber(0);
    new_db.SetNextFile(2);
    new_db.SetLastSequence(0);

    autovector<VersionEdit> new_cfs;
    SequenceNumber last_seq = 1;
    uint32_t cf_id = 1;
    for (size_t i = 1; i != column_family_names_.size(); ++i) {
      VersionEdit new_cf;
      new_cf.AddColumnFamily(column_family_names_[i]);
      new_cf.SetColumnFamily(cf_id++);
      new_cf.SetComparatorName(ucmp_->Name());
      new_cf.SetLogNumber(0);
      new_cf.SetNextFile(2);
      new_cf.SetLastSequence(last_seq++);
      new_cfs.emplace_back(new_cf);
    }

    const std::string manifest = DescriptorFileName(dbname_, 1);
    const auto& fs = env_->GetFileSystem();
    std::unique_ptr<WritableFileWriter> file_writer;
    Status s = WritableFileWriter::Create(
        fs, manifest, fs->OptimizeForManifestWrite(env_options_), &file_writer,
        nullptr);
    ASSERT_OK(s);

    {
      log::Writer log(std::move(file_writer), 0, false);
      std::string record;
      new_db.EncodeTo(&record);
      s = log.AddRecord(record);
      ASSERT_OK(s);

      for (const auto& e : new_cfs) {
        record.clear();
        e.EncodeTo(&record);
        s = log.AddRecord(record);
        ASSERT_OK(s);
      }
    }
    ASSERT_OK(s);
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(fs_.get(), dbname_, 1, nullptr);
    ASSERT_OK(s);
  }

  void SetUp() override {
    EXPECT_OK(env_->CreateDirIfMissing(dbname_));

    // TODO(icanadi) Remove this once we mock out VersionSet
    NewDB();

    db_options_.env = env_;
    db_options_.fs = fs_;
    db_options_.db_paths.emplace_back(dbname_,
                                      std::numeric_limits<uint64_t>::max());
    db_options_.statistics = CreateDBStatistics();

    cf_options_.comparator = ucmp_;

    std::vector<ColumnFamilyDescriptor> column_families;
    cf_options_.table_factory = mock_table_factory_;
    for (const auto& cf_name : column_family_names_) {
      column_families.emplace_back(cf_name, cf_options_);
    }

    versions_.reset(
        new VersionSet(dbname_, &db_options_, env_options_, table_cache_.get(),
                       &write_buffer_manager_, &write_controller_,
                       /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr));
    EXPECT_OK(versions_->Recover(column_families, false));
  }

  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  std::string dbname_;
  const Comparator* const ucmp_;
  EnvOptions env_options_;
  Options options_;
  ImmutableDBOptions db_options_;
  const std::vector<std::string> column_family_names_;
  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  WriteBufferManager write_buffer_manager_;
  ColumnFamilyOptions cf_options_;
  std::unique_ptr<VersionSet> versions_;
  InstrumentedMutex mutex_;
  std::atomic<bool> shutting_down_;
  std::shared_ptr<mock::MockTableFactory> mock_table_factory_;
};

class FlushJobTest : public FlushJobTestBase {
 public:
  FlushJobTest()
      : FlushJobTestBase(test::PerThreadDBPath("flush_job_test"),
                         BytewiseComparator()) {}
};

TEST_F(FlushJobTest, Empty) {
  JobContext job_context(0);
  auto cfd = versions_->GetColumnFamilySet()->GetDefault();
  EventLogger event_logger(db_options_.info_log.get());
  SnapshotChecker* snapshot_checker = nullptr;  // not relavant
  FlushJob flush_job(
      dbname_, versions_->GetColumnFamilySet()->GetDefault(), db_options_,
      *cfd->GetLatestMutableCFOptions(), port::kMaxUint64 /* memtable_id */,
      env_options_, versions_.get(), &mutex_, &shutting_down_, {},
      kMaxSequenceNumber, snapshot_checker, &job_context, nullptr, nullptr,
      nullptr, kNoCompression, nullptr, &event_logger, false,
      true /* sync_output_directory */, true /* write_manifest */,
      Env::Priority::USER, nullptr /*IOTracer*/);
  {
    InstrumentedMutexLock l(&mutex_);
    flush_job.PickMemTable();
    ASSERT_OK(flush_job.Run());
  }
  job_context.Clean();
}

TEST_F(FlushJobTest, NonEmpty) {
  JobContext job_context(0);
  auto cfd = versions_->GetColumnFamilySet()->GetDefault();
  auto new_mem = cfd->ConstructNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                           kMaxSequenceNumber);
  new_mem->Ref();
  auto inserted_keys = mock::MakeMockFile();
  // Test data:
  //   seqno [    1,    2 ... 8998, 8999, 9000, 9001, 9002 ... 9999 ]
  //   key   [ 1001, 1002 ... 9998, 9999,    0,    1,    2 ...  999 ]
  //   range-delete "9995" -> "9999" at seqno 10000
  //   blob references with seqnos 10001..10006
  for (int i = 1; i < 10000; ++i) {
    std::string key(ToString((i + 1000) % 10000));
    std::string value("value" + key);
    ASSERT_OK(new_mem->Add(SequenceNumber(i), kTypeValue, key, value,
                           nullptr /* kv_prot_info */));
    if ((i + 1000) % 10000 < 9995) {
      InternalKey internal_key(key, SequenceNumber(i), kTypeValue);
      inserted_keys.push_back({internal_key.Encode().ToString(), value});
    }
  }

  {
    ASSERT_OK(new_mem->Add(SequenceNumber(10000), kTypeRangeDeletion, "9995",
                           "9999a", nullptr /* kv_prot_info */));
    InternalKey internal_key("9995", SequenceNumber(10000), kTypeRangeDeletion);
    inserted_keys.push_back({internal_key.Encode().ToString(), "9999a"});
  }

  // Note: the first two blob references will not be considered when resolving
  // the oldest blob file referenced (the first one is inlined TTL, while the
  // second one is TTL and thus points to a TTL blob file).
  constexpr std::array<uint64_t, 6> blob_file_numbers{{
      kInvalidBlobFileNumber, 5, 103, 17, 102, 101}};
  for (size_t i = 0; i < blob_file_numbers.size(); ++i) {
    std::string key(ToString(i + 10001));
    std::string blob_index;
    if (i == 0) {
      BlobIndex::EncodeInlinedTTL(&blob_index, /* expiration */ 1234567890ULL,
                                  "foo");
    } else if (i == 1) {
      BlobIndex::EncodeBlobTTL(&blob_index, /* expiration */ 1234567890ULL,
                               blob_file_numbers[i], /* offset */ i << 10,
                               /* size */ i << 20, kNoCompression);
    } else {
      BlobIndex::EncodeBlob(&blob_index, blob_file_numbers[i],
                            /* offset */ i << 10, /* size */ i << 20,
                            kNoCompression);
    }

    const SequenceNumber seq(i + 10001);
    ASSERT_OK(new_mem->Add(seq, kTypeBlobIndex, key, blob_index,
                           nullptr /* kv_prot_info */));

    InternalKey internal_key(key, seq, kTypeBlobIndex);
    inserted_keys.push_back({internal_key.Encode().ToString(), blob_index});
  }
  mock::SortKVVector(&inserted_keys);

  autovector<MemTable*> to_delete;
  cfd->imm()->Add(new_mem, &to_delete);
  for (auto& m : to_delete) {
    delete m;
  }

  EventLogger event_logger(db_options_.info_log.get());
  SnapshotChecker* snapshot_checker = nullptr;  // not relavant
  FlushJob flush_job(
      dbname_, versions_->GetColumnFamilySet()->GetDefault(), db_options_,
      *cfd->GetLatestMutableCFOptions(), port::kMaxUint64 /* memtable_id */,
      env_options_, versions_.get(), &mutex_, &shutting_down_, {},
      kMaxSequenceNumber, snapshot_checker, &job_context, nullptr, nullptr,
      nullptr, kNoCompression, db_options_.statistics.get(), &event_logger,
      true, true /* sync_output_directory */, true /* write_manifest */,
      Env::Priority::USER, nullptr /*IOTracer*/);

  HistogramData hist;
  FileMetaData file_meta;
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run(nullptr, &file_meta));
  mutex_.Unlock();
  db_options_.statistics->histogramData(FLUSH_TIME, &hist);
  ASSERT_GT(hist.average, 0.0);

  ASSERT_EQ(ToString(0), file_meta.smallest.user_key().ToString());
  ASSERT_EQ("9999a", file_meta.largest.user_key().ToString());
  ASSERT_EQ(1, file_meta.fd.smallest_seqno);
  ASSERT_EQ(10006, file_meta.fd.largest_seqno);
  ASSERT_EQ(17, file_meta.oldest_blob_file_number);
  mock_table_factory_->AssertSingleFile(inserted_keys);
  job_context.Clean();
}

TEST_F(FlushJobTest, FlushMemTablesSingleColumnFamily) {
  const size_t num_mems = 2;
  const size_t num_mems_to_flush = 1;
  const size_t num_keys_per_table = 100;
  JobContext job_context(0);
  ColumnFamilyData* cfd = versions_->GetColumnFamilySet()->GetDefault();
  std::vector<uint64_t> memtable_ids;
  std::vector<MemTable*> new_mems;
  for (size_t i = 0; i != num_mems; ++i) {
    MemTable* mem = cfd->ConstructNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                              kMaxSequenceNumber);
    mem->SetID(i);
    mem->Ref();
    new_mems.emplace_back(mem);
    memtable_ids.push_back(mem->GetID());

    for (size_t j = 0; j < num_keys_per_table; ++j) {
      std::string key(ToString(j + i * num_keys_per_table));
      std::string value("value" + key);
      ASSERT_OK(mem->Add(SequenceNumber(j + i * num_keys_per_table), kTypeValue,
                         key, value, nullptr /* kv_prot_info */));
    }
  }

  autovector<MemTable*> to_delete;
  for (auto mem : new_mems) {
    cfd->imm()->Add(mem, &to_delete);
  }

  EventLogger event_logger(db_options_.info_log.get());
  SnapshotChecker* snapshot_checker = nullptr;  // not relavant

  assert(memtable_ids.size() == num_mems);
  uint64_t smallest_memtable_id = memtable_ids.front();
  uint64_t flush_memtable_id = smallest_memtable_id + num_mems_to_flush - 1;
  FlushJob flush_job(
      dbname_, versions_->GetColumnFamilySet()->GetDefault(), db_options_,
      *cfd->GetLatestMutableCFOptions(), flush_memtable_id, env_options_,
      versions_.get(), &mutex_, &shutting_down_, {}, kMaxSequenceNumber,
      snapshot_checker, &job_context, nullptr, nullptr, nullptr, kNoCompression,
      db_options_.statistics.get(), &event_logger, true,
      true /* sync_output_directory */, true /* write_manifest */,
      Env::Priority::USER, nullptr /*IOTracer*/);
  HistogramData hist;
  FileMetaData file_meta;
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run(nullptr /* prep_tracker */, &file_meta));
  mutex_.Unlock();
  db_options_.statistics->histogramData(FLUSH_TIME, &hist);
  ASSERT_GT(hist.average, 0.0);

  ASSERT_EQ(ToString(0), file_meta.smallest.user_key().ToString());
  ASSERT_EQ("99", file_meta.largest.user_key().ToString());
  ASSERT_EQ(0, file_meta.fd.smallest_seqno);
  ASSERT_EQ(SequenceNumber(num_mems_to_flush * num_keys_per_table - 1),
            file_meta.fd.largest_seqno);
  ASSERT_EQ(kInvalidBlobFileNumber, file_meta.oldest_blob_file_number);

  for (auto m : to_delete) {
    delete m;
  }
  to_delete.clear();
  job_context.Clean();
}

TEST_F(FlushJobTest, FlushMemtablesMultipleColumnFamilies) {
  autovector<ColumnFamilyData*> all_cfds;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    all_cfds.push_back(cfd);
  }
  const std::vector<size_t> num_memtables = {2, 1, 3};
  assert(num_memtables.size() == column_family_names_.size());
  const size_t num_keys_per_memtable = 1000;
  JobContext job_context(0);
  std::vector<uint64_t> memtable_ids;
  std::vector<SequenceNumber> smallest_seqs;
  std::vector<SequenceNumber> largest_seqs;
  autovector<MemTable*> to_delete;
  SequenceNumber curr_seqno = 0;
  size_t k = 0;
  for (auto cfd : all_cfds) {
    smallest_seqs.push_back(curr_seqno);
    for (size_t i = 0; i != num_memtables[k]; ++i) {
      MemTable* mem = cfd->ConstructNewMemtable(
          *cfd->GetLatestMutableCFOptions(), kMaxSequenceNumber);
      mem->SetID(i);
      mem->Ref();

      for (size_t j = 0; j != num_keys_per_memtable; ++j) {
        std::string key(ToString(j + i * num_keys_per_memtable));
        std::string value("value" + key);
        ASSERT_OK(mem->Add(curr_seqno++, kTypeValue, key, value,
                           nullptr /* kv_prot_info */));
      }

      cfd->imm()->Add(mem, &to_delete);
    }
    largest_seqs.push_back(curr_seqno - 1);
    memtable_ids.push_back(num_memtables[k++] - 1);
  }

  EventLogger event_logger(db_options_.info_log.get());
  SnapshotChecker* snapshot_checker = nullptr;  // not relevant
  std::vector<std::unique_ptr<FlushJob>> flush_jobs;
  k = 0;
  for (auto cfd : all_cfds) {
    std::vector<SequenceNumber> snapshot_seqs;
    flush_jobs.emplace_back(new FlushJob(
        dbname_, cfd, db_options_, *cfd->GetLatestMutableCFOptions(),
        memtable_ids[k], env_options_, versions_.get(), &mutex_,
        &shutting_down_, snapshot_seqs, kMaxSequenceNumber, snapshot_checker,
        &job_context, nullptr, nullptr, nullptr, kNoCompression,
        db_options_.statistics.get(), &event_logger, true,
        false /* sync_output_directory */, false /* write_manifest */,
        Env::Priority::USER, nullptr /*IOTracer*/));
    k++;
  }
  HistogramData hist;
  std::vector<FileMetaData> file_metas;
  // Call reserve to avoid auto-resizing
  file_metas.reserve(flush_jobs.size());
  mutex_.Lock();
  for (auto& job : flush_jobs) {
    job->PickMemTable();
  }
  for (auto& job : flush_jobs) {
    FileMetaData meta;
    // Run will release and re-acquire  mutex
    ASSERT_OK(job->Run(nullptr /**/, &meta));
    file_metas.emplace_back(meta);
  }
  autovector<FileMetaData*> file_meta_ptrs;
  for (auto& meta : file_metas) {
    file_meta_ptrs.push_back(&meta);
  }
  autovector<const autovector<MemTable*>*> mems_list;
  for (size_t i = 0; i != all_cfds.size(); ++i) {
    const auto& mems = flush_jobs[i]->GetMemTables();
    mems_list.push_back(&mems);
  }
  autovector<const MutableCFOptions*> mutable_cf_options_list;
  for (auto cfd : all_cfds) {
    mutable_cf_options_list.push_back(cfd->GetLatestMutableCFOptions());
  }

  Status s = InstallMemtableAtomicFlushResults(
      nullptr /* imm_lists */, all_cfds, mutable_cf_options_list, mems_list,
      versions_.get(), nullptr /* prep_tracker */, &mutex_, file_meta_ptrs,
      &job_context.memtables_to_free, nullptr /* db_directory */,
      nullptr /* log_buffer */);
  ASSERT_OK(s);

  mutex_.Unlock();
  db_options_.statistics->histogramData(FLUSH_TIME, &hist);
  ASSERT_GT(hist.average, 0.0);
  k = 0;
  for (const auto& file_meta : file_metas) {
    ASSERT_EQ(ToString(0), file_meta.smallest.user_key().ToString());
    ASSERT_EQ("999", file_meta.largest.user_key()
                         .ToString());  // max key by bytewise comparator
    ASSERT_EQ(smallest_seqs[k], file_meta.fd.smallest_seqno);
    ASSERT_EQ(largest_seqs[k], file_meta.fd.largest_seqno);
    // Verify that imm is empty
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              all_cfds[k]->imm()->GetEarliestMemTableID());
    ASSERT_EQ(0, all_cfds[k]->imm()->GetLatestMemTableID());
    ++k;
  }

  for (auto m : to_delete) {
    delete m;
  }
  to_delete.clear();
  job_context.Clean();
}

TEST_F(FlushJobTest, Snapshots) {
  JobContext job_context(0);
  auto cfd = versions_->GetColumnFamilySet()->GetDefault();
  auto new_mem = cfd->ConstructNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                           kMaxSequenceNumber);

  std::set<SequenceNumber> snapshots_set;
  int keys = 10000;
  int max_inserts_per_keys = 8;

  Random rnd(301);
  for (int i = 0; i < keys / 2; ++i) {
    snapshots_set.insert(rnd.Uniform(keys * (max_inserts_per_keys / 2)) + 1);
  }
  // set has already removed the duplicate snapshots
  std::vector<SequenceNumber> snapshots(snapshots_set.begin(),
                                        snapshots_set.end());

  new_mem->Ref();
  SequenceNumber current_seqno = 0;
  auto inserted_keys = mock::MakeMockFile();
  for (int i = 1; i < keys; ++i) {
    std::string key(ToString(i));
    int insertions = rnd.Uniform(max_inserts_per_keys);
    for (int j = 0; j < insertions; ++j) {
      std::string value(rnd.HumanReadableString(10));
      auto seqno = ++current_seqno;
      ASSERT_OK(new_mem->Add(SequenceNumber(seqno), kTypeValue, key, value,
                             nullptr /* kv_prot_info */));
      // a key is visible only if:
      // 1. it's the last one written (j == insertions - 1)
      // 2. there's a snapshot pointing at it
      bool visible = (j == insertions - 1) ||
                     (snapshots_set.find(seqno) != snapshots_set.end());
      if (visible) {
        InternalKey internal_key(key, seqno, kTypeValue);
        inserted_keys.push_back({internal_key.Encode().ToString(), value});
      }
    }
  }
  mock::SortKVVector(&inserted_keys);

  autovector<MemTable*> to_delete;
  cfd->imm()->Add(new_mem, &to_delete);
  for (auto& m : to_delete) {
    delete m;
  }

  EventLogger event_logger(db_options_.info_log.get());
  SnapshotChecker* snapshot_checker = nullptr;  // not relavant
  FlushJob flush_job(
      dbname_, versions_->GetColumnFamilySet()->GetDefault(), db_options_,
      *cfd->GetLatestMutableCFOptions(), port::kMaxUint64 /* memtable_id */,
      env_options_, versions_.get(), &mutex_, &shutting_down_, snapshots,
      kMaxSequenceNumber, snapshot_checker, &job_context, nullptr, nullptr,
      nullptr, kNoCompression, db_options_.statistics.get(), &event_logger,
      true, true /* sync_output_directory */, true /* write_manifest */,
      Env::Priority::USER, nullptr /*IOTracer*/);
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run());
  mutex_.Unlock();
  mock_table_factory_->AssertSingleFile(inserted_keys);
  HistogramData hist;
  db_options_.statistics->histogramData(FLUSH_TIME, &hist);
  ASSERT_GT(hist.average, 0.0);
  job_context.Clean();
}

class FlushJobTimestampTest : public FlushJobTestBase {
 public:
  FlushJobTimestampTest()
      : FlushJobTestBase(test::PerThreadDBPath("flush_job_ts_gc_test"),
                         test::ComparatorWithU64Ts()) {}

  void AddKeyValueToMemtable(MemTable* memtable, std::string key, uint64_t ts,
                             SequenceNumber seq, ValueType value_type,
                             Slice value) {
    std::string key_str(std::move(key));
    PutFixed64(&key_str, ts);
    ASSERT_OK(memtable->Add(seq, value_type, key_str, value,
                            nullptr /* kv_prot_info */));
  }

 protected:
  static constexpr uint64_t kStartTs = 10;
  static constexpr SequenceNumber kStartSeq = 0;
  SequenceNumber curr_seq_{kStartSeq};
  std::atomic<uint64_t> curr_ts_{kStartTs};
};

TEST_F(FlushJobTimestampTest, AllKeysExpired) {
  ColumnFamilyData* cfd = versions_->GetColumnFamilySet()->GetDefault();
  autovector<MemTable*> to_delete;

  {
    MemTable* new_mem = cfd->ConstructNewMemtable(
        *cfd->GetLatestMutableCFOptions(), kMaxSequenceNumber);
    new_mem->Ref();
    for (int i = 0; i < 100; ++i) {
      uint64_t ts = curr_ts_.fetch_add(1);
      SequenceNumber seq = (curr_seq_++);
      AddKeyValueToMemtable(new_mem, test::EncodeInt(0), ts, seq,
                            ValueType::kTypeValue, "0_value");
    }
    uint64_t ts = curr_ts_.fetch_add(1);
    SequenceNumber seq = (curr_seq_++);
    AddKeyValueToMemtable(new_mem, test::EncodeInt(0), ts, seq,
                          ValueType::kTypeDeletionWithTimestamp, "");
    cfd->imm()->Add(new_mem, &to_delete);
  }

  std::vector<SequenceNumber> snapshots;
  constexpr SnapshotChecker* const snapshot_checker = nullptr;
  JobContext job_context(0);
  EventLogger event_logger(db_options_.info_log.get());
  std::string full_history_ts_low;
  PutFixed64(&full_history_ts_low, std::numeric_limits<uint64_t>::max());
  FlushJob flush_job(
      dbname_, cfd, db_options_, *cfd->GetLatestMutableCFOptions(),
      port::kMaxUint64 /* memtable_id */, env_options_, versions_.get(),
      &mutex_, &shutting_down_, snapshots, kMaxSequenceNumber, snapshot_checker,
      &job_context, nullptr, nullptr, nullptr, kNoCompression,
      db_options_.statistics.get(), &event_logger, true,
      true /* sync_output_directory */, true /* write_manifest */,
      Env::Priority::USER, nullptr /*IOTracer*/, /*db_id=*/"",
      /*db_session_id=*/"", full_history_ts_low);

  FileMetaData fmeta;
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run(/*prep_tracker=*/nullptr, &fmeta));
  mutex_.Unlock();

  {
    std::string key = test::EncodeInt(0);
    key.append(test::EncodeInt(curr_ts_.load(std::memory_order_relaxed) - 1));
    InternalKey ikey(key, curr_seq_ - 1, ValueType::kTypeDeletionWithTimestamp);
    ASSERT_EQ(ikey.Encode(), fmeta.smallest.Encode());
    ASSERT_EQ(ikey.Encode(), fmeta.largest.Encode());
  }

  job_context.Clean();
  ASSERT_TRUE(to_delete.empty());
}

TEST_F(FlushJobTimestampTest, NoKeyExpired) {
  ColumnFamilyData* cfd = versions_->GetColumnFamilySet()->GetDefault();
  autovector<MemTable*> to_delete;

  {
    MemTable* new_mem = cfd->ConstructNewMemtable(
        *cfd->GetLatestMutableCFOptions(), kMaxSequenceNumber);
    new_mem->Ref();
    for (int i = 0; i < 100; ++i) {
      uint64_t ts = curr_ts_.fetch_add(1);
      SequenceNumber seq = (curr_seq_++);
      AddKeyValueToMemtable(new_mem, test::EncodeInt(0), ts, seq,
                            ValueType::kTypeValue, "0_value");
    }
    cfd->imm()->Add(new_mem, &to_delete);
  }

  std::vector<SequenceNumber> snapshots;
  SnapshotChecker* const snapshot_checker = nullptr;
  JobContext job_context(0);
  EventLogger event_logger(db_options_.info_log.get());
  std::string full_history_ts_low;
  PutFixed64(&full_history_ts_low, 0);
  FlushJob flush_job(
      dbname_, cfd, db_options_, *cfd->GetLatestMutableCFOptions(),
      port::kMaxUint64 /* memtable_id */, env_options_, versions_.get(),
      &mutex_, &shutting_down_, snapshots, kMaxSequenceNumber, snapshot_checker,
      &job_context, nullptr, nullptr, nullptr, kNoCompression,
      db_options_.statistics.get(), &event_logger, true,
      true /* sync_output_directory */, true /* write_manifest */,
      Env::Priority::USER, nullptr /*IOTracer*/, /*db_id=*/"",
      /*db_session_id=*/"", full_history_ts_low);

  FileMetaData fmeta;
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run(/*prep_tracker=*/nullptr, &fmeta));
  mutex_.Unlock();

  {
    std::string ukey = test::EncodeInt(0);
    std::string smallest_key =
        ukey + test::EncodeInt(curr_ts_.load(std::memory_order_relaxed) - 1);
    std::string largest_key = ukey + test::EncodeInt(kStartTs);
    InternalKey smallest(smallest_key, curr_seq_ - 1, ValueType::kTypeValue);
    InternalKey largest(largest_key, kStartSeq, ValueType::kTypeValue);
    ASSERT_EQ(smallest.Encode(), fmeta.smallest.Encode());
    ASSERT_EQ(largest.Encode(), fmeta.largest.Encode());
  }
  job_context.Clean();
  ASSERT_TRUE(to_delete.empty());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
