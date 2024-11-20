// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).


#include "rocksdb/sst_file_reader.h"

#include <cinttypes>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/utilities/types_util.h"
#include "table/sst_file_writer_collectors.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

std::string EncodeAsString(uint64_t v) {
  char buf[16];
  snprintf(buf, sizeof(buf), "%08" PRIu64, v);
  return std::string(buf);
}

std::string EncodeAsUint64(uint64_t v) {
  std::string dst;
  PutFixed64(&dst, v);
  return dst;
}

class SstFileReaderTest : public testing::Test {
 public:
  SstFileReaderTest() {
    options_.merge_operator = MergeOperators::CreateUInt64AddOperator();
    sst_name_ = test::PerThreadDBPath("sst_file");

    Env* base_env = Env::Default();
    EXPECT_OK(
        test::CreateEnvFromSystem(ConfigOptions(), &base_env, &env_guard_));
    EXPECT_NE(nullptr, base_env);
    env_ = base_env;
    options_.env = env_;
  }

  ~SstFileReaderTest() {
    Status s = env_->DeleteFile(sst_name_);
    EXPECT_OK(s);
  }

  void CreateFile(const std::string& file_name,
                  const std::vector<std::string>& keys) {
    SstFileWriter writer(soptions_, options_);
    ASSERT_OK(writer.Open(file_name));
    for (size_t i = 0; i + 2 < keys.size(); i += 3) {
      ASSERT_OK(writer.Put(keys[i], keys[i]));
      ASSERT_OK(writer.Merge(keys[i + 1], EncodeAsUint64(i + 1)));
      ASSERT_OK(writer.Delete(keys[i + 2]));
    }
    ASSERT_OK(writer.Finish());
  }

  void CheckFile(const std::string& file_name,
                 const std::vector<std::string>& keys,
                 bool check_global_seqno = false) {
    ReadOptions ropts;
    SstFileReader reader(options_);
    ASSERT_OK(reader.Open(file_name));
    ASSERT_OK(reader.VerifyChecksum());
    std::unique_ptr<Iterator> iter(reader.NewIterator(ropts));
    iter->SeekToFirst();
    for (size_t i = 0; i + 2 < keys.size(); i += 3) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().compare(keys[i]), 0);
      ASSERT_EQ(iter->value().compare(keys[i]), 0);
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().compare(keys[i + 1]), 0);
      ASSERT_EQ(iter->value().compare(EncodeAsUint64(i + 1)), 0);
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    if (check_global_seqno) {
      auto properties = reader.GetTableProperties();
      ASSERT_TRUE(properties);
      std::string hostname;
      ASSERT_OK(env_->GetHostNameString(&hostname));
      ASSERT_EQ(properties->db_host_id, hostname);
      auto& user_properties = properties->user_collected_properties;
      ASSERT_TRUE(
          user_properties.count(ExternalSstFilePropertyNames::kGlobalSeqno));
    }
  }

  void CreateFileAndCheck(const std::vector<std::string>& keys) {
    CreateFile(sst_name_, keys);
    CheckFile(sst_name_, keys);
  }

 protected:
  Options options_;
  EnvOptions soptions_;
  std::string sst_name_;
  std::shared_ptr<Env> env_guard_;
  Env* env_;
};

const uint64_t kNumKeys = 100;

TEST_F(SstFileReaderTest, Basic) {
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsString(i));
  }
  CreateFileAndCheck(keys);
}

TEST_F(SstFileReaderTest, Uint64Comparator) {
  options_.comparator = test::Uint64Comparator();
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsUint64(i));
  }
  CreateFileAndCheck(keys);
}

TEST_F(SstFileReaderTest, ReadOptionsOutOfScope) {
  // Repro a bug where the SstFileReader depended on its configured ReadOptions
  // outliving it.
  options_.comparator = test::Uint64Comparator();
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsUint64(i));
  }
  CreateFile(sst_name_, keys);

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));
  std::unique_ptr<Iterator> iter;
  {
    // Make sure ReadOptions go out of scope ASAP so we know the iterator
    // operations do not depend on it.
    ReadOptions ropts;
    iter.reset(reader.NewIterator(ropts));
  }
  iter->SeekToFirst();
  while (iter->Valid()) {
    iter->Next();
  }
}

TEST_F(SstFileReaderTest, ReadFileWithGlobalSeqno) {
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsString(i));
  }
  // Generate a SST file.
  CreateFile(sst_name_, keys);

  // Ingest the file into a db, to assign it a global sequence number.
  Options options;
  options.create_if_missing = true;
  std::string db_name = test::PerThreadDBPath("test_db");
  DB* db;
  ASSERT_OK(DB::Open(options, db_name, &db));
  // Bump sequence number.
  ASSERT_OK(db->Put(WriteOptions(), keys[0], "foo"));
  ASSERT_OK(db->Flush(FlushOptions()));
  // Ingest the file.
  IngestExternalFileOptions ingest_options;
  ingest_options.write_global_seqno = true;
  ASSERT_OK(db->IngestExternalFile({sst_name_}, ingest_options));
  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  ASSERT_OK(db->GetLiveFiles(live_files, &manifest_file_size));
  // Get the ingested file.
  std::string ingested_file;
  for (auto& live_file : live_files) {
    if (live_file.substr(live_file.size() - 4, std::string::npos) == ".sst") {
      if (ingested_file.empty() || ingested_file < live_file) {
        ingested_file = live_file;
      }
    }
  }
  ASSERT_FALSE(ingested_file.empty());
  delete db;

  // Verify the file can be open and read by SstFileReader.
  CheckFile(db_name + ingested_file, keys, true /* check_global_seqno */);

  // Cleanup.
  ASSERT_OK(DestroyDB(db_name, options));
}

TEST_F(SstFileReaderTest, TimestampSizeMismatch) {
  SstFileWriter writer(soptions_, options_);

  ASSERT_OK(writer.Open(sst_name_));

  // Comparator is not timestamp-aware; calls to APIs taking timestamps should
  // fail.
  ASSERT_NOK(writer.Put("key", EncodeAsUint64(100), "value"));
  ASSERT_NOK(writer.Delete("another_key", EncodeAsUint64(200)));
}

class SstFileReaderTimestampTest : public testing::Test {
 public:
  SstFileReaderTimestampTest() {
    Env* env = Env::Default();
    EXPECT_OK(test::CreateEnvFromSystem(ConfigOptions(), &env, &env_guard_));
    EXPECT_NE(nullptr, env);

    options_.env = env;

    options_.comparator = test::BytewiseComparatorWithU64TsWrapper();

    sst_name_ = test::PerThreadDBPath("sst_file_ts");
  }

  ~SstFileReaderTimestampTest() {
    EXPECT_OK(options_.env->DeleteFile(sst_name_));
  }

  struct KeyValueDesc {
    KeyValueDesc(std::string k, std::string ts, std::string v)
        : key(std::move(k)), timestamp(std::move(ts)), value(std::move(v)) {}

    std::string key;
    std::string timestamp;
    std::string value;
  };

  struct InputKeyValueDesc : public KeyValueDesc {
    InputKeyValueDesc(std::string k, std::string ts, std::string v, bool is_del,
                      bool use_contig_buf)
        : KeyValueDesc(std::move(k), std::move(ts), std::move(v)),
          is_delete(is_del),
          use_contiguous_buffer(use_contig_buf) {}

    bool is_delete = false;
    bool use_contiguous_buffer = false;
  };

  struct OutputKeyValueDesc : public KeyValueDesc {
    OutputKeyValueDesc(std::string k, std::string ts, std::string v)
        : KeyValueDesc(std::move(k), std::string(ts), std::string(v)) {}
  };

  void CreateFile(const std::vector<InputKeyValueDesc>& descs,
                  ExternalSstFileInfo* file_info = nullptr) {
    SstFileWriter writer(soptions_, options_);

    ASSERT_OK(writer.Open(sst_name_));

    for (const auto& desc : descs) {
      if (desc.is_delete) {
        if (desc.use_contiguous_buffer) {
          std::string key_with_ts(desc.key + desc.timestamp);
          ASSERT_OK(writer.Delete(Slice(key_with_ts.data(), desc.key.size()),
                                  Slice(key_with_ts.data() + desc.key.size(),
                                        desc.timestamp.size())));
        } else {
          ASSERT_OK(writer.Delete(desc.key, desc.timestamp));
        }
      } else {
        if (desc.use_contiguous_buffer) {
          std::string key_with_ts(desc.key + desc.timestamp);
          ASSERT_OK(writer.Put(Slice(key_with_ts.data(), desc.key.size()),
                               Slice(key_with_ts.data() + desc.key.size(),
                                     desc.timestamp.size()),
                               desc.value));
        } else {
          ASSERT_OK(writer.Put(desc.key, desc.timestamp, desc.value));
        }
      }
    }

    ASSERT_OK(writer.Finish(file_info));
  }

  void CheckFile(const std::string& timestamp,
                 const std::vector<OutputKeyValueDesc>& descs) {
    SstFileReader reader(options_);

    ASSERT_OK(reader.Open(sst_name_));
    ASSERT_OK(reader.VerifyChecksum());

    Slice ts_slice(timestamp);

    ReadOptions read_options;
    read_options.timestamp = &ts_slice;

    std::unique_ptr<Iterator> iter(reader.NewIterator(read_options));
    iter->SeekToFirst();

    for (const auto& desc : descs) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), desc.key);
      ASSERT_EQ(iter->timestamp(), desc.timestamp);
      ASSERT_EQ(iter->value(), desc.value);

      iter->Next();
    }

    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

 protected:
  std::shared_ptr<Env> env_guard_;
  Options options_;
  EnvOptions soptions_;
  std::string sst_name_;
};

TEST_F(SstFileReaderTimestampTest, Basic) {
  std::vector<InputKeyValueDesc> input_descs;

  for (uint64_t k = 0; k < kNumKeys; k += 4) {
    // A Put with key k, timestamp k that gets overwritten by a subsequent Put
    // with timestamp (k + 1). Note that the comparator uses descending order
    // for the timestamp part, so we add the later Put first.
    input_descs.emplace_back(
        /* key */ EncodeAsString(k), /* timestamp */ EncodeAsUint64(k + 1),
        /* value */ EncodeAsString(k * 2), /* is_delete */ false,
        /* use_contiguous_buffer */ false);
    input_descs.emplace_back(
        /* key */ EncodeAsString(k), /* timestamp */ EncodeAsUint64(k),
        /* value */ EncodeAsString(k * 3), /* is_delete */ false,
        /* use_contiguous_buffer */ true);

    // A Put with key (k + 2), timestamp (k + 2) that gets cancelled out by a
    // Delete with timestamp (k + 3).  Note that the comparator uses descending
    // order for the timestamp part, so we add the Delete first.
    input_descs.emplace_back(/* key */ EncodeAsString(k + 2),
                             /* timestamp */ EncodeAsUint64(k + 3),
                             /* value */ std::string(), /* is_delete */ true,
                             /* use_contiguous_buffer */ (k % 8) == 0);
    input_descs.emplace_back(
        /* key */ EncodeAsString(k + 2), /* timestamp */ EncodeAsUint64(k + 2),
        /* value */ EncodeAsString(k * 5), /* is_delete */ false,
        /* use_contiguous_buffer */ (k % 8) != 0);
  }

  CreateFile(input_descs);

  // Note: below, we check the results as of each timestamp in the range,
  // updating the expected result as needed.
  std::vector<OutputKeyValueDesc> output_descs;

  for (uint64_t ts = 0; ts < kNumKeys; ++ts) {
    const uint64_t k = ts - (ts % 4);

    switch (ts % 4) {
      case 0:  // Initial Put for key k
        output_descs.emplace_back(/* key */ EncodeAsString(k),
                                  /* timestamp */ EncodeAsUint64(ts),
                                  /* value */ EncodeAsString(k * 3));
        break;

      case 1:  // Second Put for key k
        assert(output_descs.back().key == EncodeAsString(k));
        assert(output_descs.back().timestamp == EncodeAsUint64(ts - 1));
        assert(output_descs.back().value == EncodeAsString(k * 3));
        output_descs.back().timestamp = EncodeAsUint64(ts);
        output_descs.back().value = EncodeAsString(k * 2);
        break;

      case 2:  // Put for key (k + 2)
        output_descs.emplace_back(/* key */ EncodeAsString(k + 2),
                                  /* timestamp */ EncodeAsUint64(ts),
                                  /* value */ EncodeAsString(k * 5));
        break;

      case 3:  // Delete for key (k + 2)
        assert(output_descs.back().key == EncodeAsString(k + 2));
        assert(output_descs.back().timestamp == EncodeAsUint64(ts - 1));
        assert(output_descs.back().value == EncodeAsString(k * 5));
        output_descs.pop_back();
        break;
    }

    CheckFile(EncodeAsUint64(ts), output_descs);
  }
}

TEST_F(SstFileReaderTimestampTest, TimestampsOutOfOrder) {
  SstFileWriter writer(soptions_, options_);

  ASSERT_OK(writer.Open(sst_name_));

  // Note: KVs that have the same user key disregarding timestamps should be in
  // descending order of timestamps.
  ASSERT_OK(writer.Put("key", EncodeAsUint64(1), "value1"));
  ASSERT_NOK(writer.Put("key", EncodeAsUint64(2), "value2"));
}

TEST_F(SstFileReaderTimestampTest, TimestampSizeMismatch) {
  SstFileWriter writer(soptions_, options_);

  ASSERT_OK(writer.Open(sst_name_));

  // Comparator expects 64-bit timestamps; timestamps with other sizes as well
  // as calls to the timestamp-less APIs should be rejected.
  ASSERT_NOK(writer.Put("key", "not_an_actual_64_bit_timestamp", "value"));
  ASSERT_NOK(writer.Delete("another_key", "timestamp_of_unexpected_size"));

  ASSERT_NOK(writer.Put("key_without_timestamp", "value"));
  ASSERT_NOK(writer.Merge("another_key_missing_a_timestamp", "merge_operand"));
  ASSERT_NOK(writer.Delete("yet_another_key_still_no_timestamp"));
  ASSERT_NOK(writer.DeleteRange("begin_key_timestamp_absent",
                                "end_key_with_a_complete_lack_of_timestamps"));
}

class SstFileReaderTimestampNotPersistedTest
    : public SstFileReaderTimestampTest {
 public:
  SstFileReaderTimestampNotPersistedTest() {
    Env* env = Env::Default();
    EXPECT_OK(test::CreateEnvFromSystem(ConfigOptions(), &env, &env_guard_));
    EXPECT_NE(nullptr, env);

    options_.env = env;

    options_.comparator = test::BytewiseComparatorWithU64TsWrapper();

    options_.persist_user_defined_timestamps = false;

    sst_name_ = test::PerThreadDBPath("sst_file_ts_not_persisted");
  }

  ~SstFileReaderTimestampNotPersistedTest() = default;
};

TEST_F(SstFileReaderTimestampNotPersistedTest, Basic) {
  std::vector<InputKeyValueDesc> input_descs;

  for (uint64_t k = 0; k < kNumKeys; k++) {
    input_descs.emplace_back(
        /* key */ EncodeAsString(k), /* timestamp */ EncodeAsUint64(0),
        /* value */ EncodeAsString(k), /* is_delete */ false,
        /* use_contiguous_buffer */ (k % 2) == 0);
  }

  ExternalSstFileInfo external_sst_file_info;

  CreateFile(input_descs, &external_sst_file_info);
  std::vector<OutputKeyValueDesc> output_descs;

  for (uint64_t k = 0; k < kNumKeys; k++) {
    output_descs.emplace_back(/* key */ EncodeAsString(k),
                              /* timestamp */ EncodeAsUint64(0),
                              /* value */ EncodeAsString(k));
  }
  CheckFile(EncodeAsUint64(0), output_descs);
  ASSERT_EQ(external_sst_file_info.smallest_key, EncodeAsString(0));
  ASSERT_EQ(external_sst_file_info.largest_key, EncodeAsString(kNumKeys - 1));
  ASSERT_EQ(external_sst_file_info.smallest_range_del_key, "");
  ASSERT_EQ(external_sst_file_info.largest_range_del_key, "");
}

TEST_F(SstFileReaderTimestampNotPersistedTest, NonMinTimestampNotAllowed) {
  SstFileWriter writer(soptions_, options_);

  ASSERT_OK(writer.Open(sst_name_));

  ASSERT_NOK(writer.Delete("baz", EncodeAsUint64(2)));
  ASSERT_OK(writer.Put("baz", EncodeAsUint64(0), "foo_val"));

  ASSERT_NOK(writer.Put("key", EncodeAsUint64(2), "value1"));
  ASSERT_OK(writer.Put("key", EncodeAsUint64(0), "value2"));

  // The `SstFileWriter::DeleteRange` API documentation specifies that
  // a range deletion tombstone added in the file does NOT delete point
  // (Put/Merge/Delete) keys in the same file. While there is no checks in
  // `SstFileWriter` to ensure this requirement is met, when such a range
  // deletion does exist, it will get over-written by point data in the same
  // file after ingestion because they have the same sequence number.
  // We allow having a point data entry and having a range deletion entry for
  // a key in the same file when timestamps are removed for the same reason.
  // After the file is ingested, the range deletion will effectively get
  // over-written by the point data since they will have the same sequence
  // number and the same user-defined timestamps.
  ASSERT_NOK(writer.DeleteRange("bar", "foo", EncodeAsUint64(2)));
  ASSERT_OK(writer.DeleteRange("bar", "foo", EncodeAsUint64(0)));

  ExternalSstFileInfo external_sst_file_info;

  ASSERT_OK(writer.Finish(&external_sst_file_info));
  ASSERT_EQ(external_sst_file_info.smallest_key, "baz");
  ASSERT_EQ(external_sst_file_info.largest_key, "key");
  ASSERT_EQ(external_sst_file_info.smallest_range_del_key, "bar");
  ASSERT_EQ(external_sst_file_info.largest_range_del_key, "foo");
}

TEST_F(SstFileReaderTimestampNotPersistedTest, KeyWithoutTimestampOutOfOrder) {
  SstFileWriter writer(soptions_, options_);

  ASSERT_OK(writer.Open(sst_name_));

  ASSERT_OK(writer.Put("foo", EncodeAsUint64(0), "value1"));
  ASSERT_NOK(writer.Put("bar", EncodeAsUint64(0), "value2"));
}

TEST_F(SstFileReaderTimestampNotPersistedTest, IncompatibleTimestampFormat) {
  SstFileWriter writer(soptions_, options_);

  ASSERT_OK(writer.Open(sst_name_));

  // Even though in this mode timestamps are not persisted, we require users
  // to call the timestamp-aware APIs only.
  ASSERT_TRUE(writer.Put("key", "not_an_actual_64_bit_timestamp", "value")
                  .IsInvalidArgument());
  ASSERT_TRUE(writer.Delete("another_key", "timestamp_of_unexpected_size")
                  .IsInvalidArgument());

  ASSERT_TRUE(writer.Put("key_without_timestamp", "value").IsInvalidArgument());
  ASSERT_TRUE(writer.Merge("another_key_missing_a_timestamp", "merge_operand")
                  .IsInvalidArgument());
  ASSERT_TRUE(
      writer.Delete("yet_another_key_still_no_timestamp").IsInvalidArgument());
  ASSERT_TRUE(writer
                  .DeleteRange("begin_key_timestamp_absent",
                               "end_key_with_a_complete_lack_of_timestamps")
                  .IsInvalidArgument());
}

TEST_F(SstFileReaderTest, VerifyNumEntriesBasic) {
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsUint64(i));
  }
  CreateFile(sst_name_, keys);
  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));
  ASSERT_OK(reader.VerifyNumEntries(ReadOptions()));
}

TEST_F(SstFileReaderTest, VerifyNumEntriesDeleteRange) {
  SstFileWriter writer(soptions_, options_);
  ASSERT_OK(writer.Open(sst_name_));

  for (uint64_t i = 0; i < kNumKeys; i++) {
    ASSERT_OK(writer.Put(EncodeAsUint64(i), EncodeAsUint64(i + 1)));
  }
  ASSERT_OK(
      writer.DeleteRange(EncodeAsUint64(0), EncodeAsUint64(kNumKeys / 2)));
  ASSERT_OK(writer.Finish());
  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));
  ASSERT_OK(reader.VerifyNumEntries(ReadOptions()));
}

TEST_F(SstFileReaderTest, VerifyNumEntriesCorruption) {
  const int num_keys = 99;
  const int corrupted_num_keys = num_keys + 2;
  SyncPoint::GetInstance()->SetCallBack(
      "PropertyBlockBuilder::AddTableProperty:Start", [&](void* arg) {
        TableProperties* props = reinterpret_cast<TableProperties*>(arg);
        props->num_entries = corrupted_num_keys;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < num_keys; i++) {
    keys.emplace_back(EncodeAsUint64(i));
  }
  CreateFile(sst_name_, keys);
  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));
  Status s = reader.VerifyNumEntries(ReadOptions());
  ASSERT_TRUE(s.IsCorruption());
  std::ostringstream oss;
  oss << "Table property expects " << corrupted_num_keys
      << " entries when excluding range deletions,"
      << " but scanning the table returned " << num_keys << " entries";
  ASSERT_TRUE(std::strstr(oss.str().c_str(), s.getState()));
}

class SstFileReaderTableIteratorTest : public DBTestBase {
 public:
  SstFileReaderTableIteratorTest()
      : DBTestBase("sst_file_reader_table_iterator_test",
                   /*env_do_fsync=*/false) {}

  void VerifyTableEntry(Iterator* iter, const std::string& user_key,
                        ValueType value_type,
                        std::optional<std::string> expected_value,
                        bool backward_iteration = false) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->status().ok());
    ParsedInternalKey pikey;
    ASSERT_OK(ParseInternalKey(iter->key(), &pikey, /*log_err_key=*/false));
    ASSERT_EQ(pikey.user_key, user_key);
    ASSERT_EQ(pikey.type, value_type);
    if (expected_value.has_value()) {
      ASSERT_EQ(iter->value(), expected_value.value());
    }
    if (!backward_iteration) {
      iter->Next();
    } else {
      iter->Prev();
    }
  }
};

TEST_F(SstFileReaderTableIteratorTest, Basic) {
  Options options = CurrentOptions();
  const Comparator* ucmp = BytewiseComparator();
  options.comparator = ucmp;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);

  // Create a L0 sst file with 4 entries, two for each user key.
  // The file should have these entries in ascending internal key order:
  // 'bar, seq: 4, type: kTypeValue => val2'
  // 'bar, seq: 3, type: kTypeDeletion'
  // 'foo, seq: 2, type: kTypeDeletion'
  // 'foo, seq: 1, type: kTypeValue => val1'
  ASSERT_OK(Put("foo", "val1"));
  const Snapshot* snapshot1 = dbfull()->GetSnapshot();
  ASSERT_OK(Delete("foo"));
  ASSERT_OK(Delete("bar"));
  const Snapshot* snapshot2 = dbfull()->GetSnapshot();
  ASSERT_OK(Put("bar", "val2"));

  ASSERT_OK(Flush());

  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_TRUE(files.size() == 1);
  ASSERT_TRUE(files[0].level == 0);
  std::string file_name = files[0].directory + "/" + files[0].relative_filename;

  SstFileReader reader(options);
  ASSERT_OK(reader.Open(file_name));
  ASSERT_OK(reader.VerifyChecksum());

  // When iterating the file as a DB iterator, only one data entry for "bar" is
  // visible.
  std::unique_ptr<Iterator> db_iter(reader.NewIterator(ReadOptions()));
  db_iter->SeekToFirst();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key(), "bar");
  ASSERT_EQ(db_iter->value(), "val2");
  db_iter->Next();
  ASSERT_FALSE(db_iter->Valid());
  db_iter.reset();

  // When iterating the file with a raw table iterator, all the data entries are
  // surfaced in ascending internal key order.
  std::unique_ptr<Iterator> table_iter = reader.NewTableIterator();

  table_iter->SeekToFirst();
  VerifyTableEntry(table_iter.get(), "bar", kTypeValue, "val2");
  VerifyTableEntry(table_iter.get(), "bar", kTypeDeletion, std::nullopt);
  VerifyTableEntry(table_iter.get(), "foo", kTypeDeletion, std::nullopt);
  VerifyTableEntry(table_iter.get(), "foo", kTypeValue, "val1");
  ASSERT_FALSE(table_iter->Valid());

  std::string seek_key_buf;
  ASSERT_OK(GetInternalKeyForSeek("foo", ucmp, &seek_key_buf));
  Slice seek_target = seek_key_buf;
  table_iter->Seek(seek_target);
  VerifyTableEntry(table_iter.get(), "foo", kTypeDeletion, std::nullopt);
  VerifyTableEntry(table_iter.get(), "foo", kTypeValue, "val1");
  ASSERT_FALSE(table_iter->Valid());

  ASSERT_OK(GetInternalKeyForSeekForPrev("bar", ucmp, &seek_key_buf));
  Slice seek_for_prev_target = seek_key_buf;
  table_iter->SeekForPrev(seek_for_prev_target);
  VerifyTableEntry(table_iter.get(), "bar", kTypeDeletion, std::nullopt,
                   /*backward_iteration=*/true);
  VerifyTableEntry(table_iter.get(), "bar", kTypeValue, "val2",
                   /*backward_iteration=*/true);
  ASSERT_FALSE(table_iter->Valid());

  dbfull()->ReleaseSnapshot(snapshot1);
  dbfull()->ReleaseSnapshot(snapshot2);
  Close();
}

TEST_F(SstFileReaderTableIteratorTest, UserDefinedTimestampsEnabled) {
  Options options = CurrentOptions();
  const Comparator* ucmp = test::BytewiseComparatorWithU64TsWrapper();
  options.comparator = ucmp;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);

  // Create a L0 sst file with 4 entries, two for each user key.
  // The file should have these entries in ascending internal key order:
  // 'bar, ts=3, seq: 4, type: kTypeValue => val2'
  // 'bar, ts=2, seq: 3, type: kTypeDeletionWithTimestamp'
  // 'foo, ts=4, seq: 2, type: kTypeDeletionWithTimestamp'
  // 'foo, ts=3, seq: 1, type: kTypeValue => val1'
  WriteOptions wopt;
  ColumnFamilyHandle* cfd = db_->DefaultColumnFamily();
  ASSERT_OK(db_->Put(wopt, cfd, "foo", EncodeAsUint64(3), "val1"));
  ASSERT_OK(db_->Delete(wopt, cfd, "foo", EncodeAsUint64(4)));
  ASSERT_OK(db_->Delete(wopt, cfd, "bar", EncodeAsUint64(2)));
  ASSERT_OK(db_->Put(wopt, cfd, "bar", EncodeAsUint64(3), "val2"));

  ASSERT_OK(Flush());

  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_TRUE(files.size() == 1);
  ASSERT_TRUE(files[0].level == 0);
  std::string file_name = files[0].directory + "/" + files[0].relative_filename;

  SstFileReader reader(options);
  ASSERT_OK(reader.Open(file_name));
  ASSERT_OK(reader.VerifyChecksum());

  // When iterating the file as a DB iterator, only one data entry for "bar" is
  // visible.
  ReadOptions ropts;
  std::string read_ts = EncodeAsUint64(4);
  Slice read_ts_slice = read_ts;
  ropts.timestamp = &read_ts_slice;
  std::unique_ptr<Iterator> db_iter(reader.NewIterator(ropts));
  db_iter->SeekToFirst();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key(), "bar");
  ASSERT_EQ(db_iter->value(), "val2");
  ASSERT_EQ(db_iter->timestamp(), EncodeAsUint64(3));
  db_iter->Next();
  ASSERT_FALSE(db_iter->Valid());
  db_iter.reset();

  std::unique_ptr<Iterator> table_iter = reader.NewTableIterator();

  table_iter->SeekToFirst();
  VerifyTableEntry(table_iter.get(), "bar" + EncodeAsUint64(3), kTypeValue,
                   "val2");
  VerifyTableEntry(table_iter.get(), "bar" + EncodeAsUint64(2),
                   kTypeDeletionWithTimestamp, std::nullopt);
  VerifyTableEntry(table_iter.get(), "foo" + EncodeAsUint64(4),
                   kTypeDeletionWithTimestamp, std::nullopt);
  VerifyTableEntry(table_iter.get(), "foo" + EncodeAsUint64(3), kTypeValue,
                   "val1");
  ASSERT_FALSE(table_iter->Valid());

  std::string seek_key_buf;
  ASSERT_OK(GetInternalKeyForSeek("foo", ucmp, &seek_key_buf));
  Slice seek_target = seek_key_buf;
  table_iter->Seek(seek_target);
  VerifyTableEntry(table_iter.get(), "foo" + EncodeAsUint64(4),
                   kTypeDeletionWithTimestamp, std::nullopt);
  VerifyTableEntry(table_iter.get(), "foo" + EncodeAsUint64(3), kTypeValue,
                   "val1");
  ASSERT_FALSE(table_iter->Valid());

  ASSERT_OK(GetInternalKeyForSeekForPrev("bar", ucmp, &seek_key_buf));
  Slice seek_for_prev_target = seek_key_buf;
  table_iter->SeekForPrev(seek_for_prev_target);
  VerifyTableEntry(table_iter.get(), "bar" + EncodeAsUint64(2),
                   kTypeDeletionWithTimestamp, std::nullopt,
                   /*backward_iteration=*/true);
  VerifyTableEntry(table_iter.get(), "bar" + EncodeAsUint64(3), kTypeValue,
                   "val2", /*backward_iteration=*/true);
  ASSERT_FALSE(table_iter->Valid());

  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}

