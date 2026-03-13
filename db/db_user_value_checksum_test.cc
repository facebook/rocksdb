//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/user_value_checksum.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

// Shared CRC32c trailing checksum validation logic.
// Value format: [user_data][4-byte CRC32c checksum]
inline Status ValidateTrailingCRC32c(const Slice& value) {
  if (value.size() < 4) {
    return Status::Corruption("Value too small for checksum");
  }
  size_t data_sz = value.size() - 4;
  uint32_t stored = DecodeFixed32(value.data() + data_sz);
  uint32_t computed = crc32c::Value(value.data(), data_sz);
  if (stored != computed) {
    return Status::Corruption("CRC32c checksum mismatch");
  }
  return Status::OK();
}

// Test UserValueChecksum implementation: value format is
// [user_data][4-byte CRC32c checksum]
class TestUserValueChecksum : public UserValueChecksum {
 public:
  const char* Name() const override { return "TestUserValueChecksum"; }

  Status Validate(const Slice& /*key*/, const Slice& value,
                  ChecksumValueType /*value_type*/) const override {
    return ValidateTrailingCRC32c(value);
  }

  // Helper to create a checksummed value
  static std::string MakeValue(const std::string& data) {
    std::string result = data;
    uint32_t crc = crc32c::Value(data.data(), data.size());
    PutFixed32(&result, crc);
    return result;
  }

  // Helper to create a corrupt value (wrong checksum)
  static std::string MakeCorruptValue(const std::string& data) {
    std::string result = data;
    PutFixed32(&result, 0xDEADBEEF);  // bad checksum
    return result;
  }
};

// Merge operator that preserves CRC32c checksums
class ChecksumPreservingMergeOp : public MergeOperator {
 public:
  bool FullMergeV3(const MergeOperationInputV3& merge_in,
                   MergeOperationOutputV3* merge_out) const override {
    // Concatenate all values, then recompute checksum
    std::string merged_data;
    if (auto* existing = std::get_if<Slice>(&merge_in.existing_value)) {
      if (existing->size() >= 4) {
        // Strip old checksum
        merged_data.assign(existing->data(), existing->size() - 4);
      }
    }
    for (const auto& operand : merge_in.operand_list) {
      if (operand.size() >= 4) {
        merged_data.append(operand.data(), operand.size() - 4);
      }
    }
    // Recompute checksum
    std::string result = merged_data;
    uint32_t crc = crc32c::Value(result.data(), result.size());
    PutFixed32(&result, crc);
    merge_out->new_value = std::move(result);
    return true;
  }

  const char* Name() const override { return "ChecksumPreservingMergeOp"; }
};

// UserValueChecksum that always validates successfully.
class AlwaysValidChecksum : public UserValueChecksum {
 public:
  const char* Name() const override { return "AlwaysValidChecksum"; }

  Status Validate(const Slice& /*key*/, const Slice& /*value*/,
                  ChecksumValueType /*value_type*/) const override {
    return Status::OK();
  }
};

class UserValueChecksumTest : public DBTestBase {
 public:
  UserValueChecksumTest()
      : DBTestBase("db_user_value_checksum_test", /*env_do_fsync=*/false) {}

  // Write two overlapping L0 files with corrupt or valid values.
  // Used by tests that need a non-trivial compaction input.
  void WriteOverlappingL0Files(bool corrupt = true) {
    auto make = corrupt ? TestUserValueChecksum::MakeCorruptValue
                        : TestUserValueChecksum::MakeValue;
    ASSERT_OK(Put("key1", make("value1")));
    ASSERT_OK(Put("key2", make("value2")));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("key1", make("value1b")));
    ASSERT_OK(Put("key3", make("value3")));
    ASSERT_OK(Flush());
  }
};

TEST_F(UserValueChecksumTest, FlushValidationCorruptAndValid) {
  for (bool corrupt : {false, true}) {
    SCOPED_TRACE(corrupt ? "corrupt" : "valid");
    Options options = CurrentOptions();
    options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
    options.verify_user_value_checksum_on_flush = true;
    options.statistics = CreateDBStatistics();
    DestroyAndReopen(options);

    auto make = corrupt ? TestUserValueChecksum::MakeCorruptValue
                        : TestUserValueChecksum::MakeValue;
    ASSERT_OK(Put("key1", make("value1")));
    ASSERT_OK(Put("key2", make("value2")));
    ASSERT_OK(Put("key3", make("value3")));

    Status s = Flush();
    if (corrupt) {
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    } else {
      ASSERT_OK(s);
      ASSERT_GE(
          options.statistics->getTickerCount(USER_VALUE_CHECKSUM_COMPUTE_COUNT),
          3);
      ASSERT_EQ(options.statistics->getTickerCount(
                    USER_VALUE_CHECKSUM_MISMATCH_COUNT),
                0);
      ASSERT_EQ(Get("key1"), TestUserValueChecksum::MakeValue("value1"));
      ASSERT_EQ(Get("key2"), TestUserValueChecksum::MakeValue("value2"));
    }
  }
}

TEST_F(UserValueChecksumTest, CompactionOutputVerificationVariants) {
  struct TestCase {
    const char* label;
    VerifyOutputFlags flags;
    bool paranoid_file_checks;
    bool corrupt;
    bool expect_corruption;
    int min_checksum_count;  // 0 = don't check
  };
  TestCase cases[] = {
      {"ParanoidCorrupt", VerifyOutputFlags::kVerifyNone,
       /*paranoid_file_checks=*/true,
       /*corrupt=*/true,
       /*expect_corruption=*/true,
       /*min_checksum_count=*/0},
      {"ParanoidValid", VerifyOutputFlags::kVerifyNone,
       /*paranoid_file_checks=*/true,
       /*corrupt=*/false,
       /*expect_corruption=*/false,
       /*min_checksum_count=*/2},
      {"BlockChecksumCorrupt",
       VerifyOutputFlags::kVerifyBlockChecksum |
           VerifyOutputFlags::kEnableForLocalCompaction,
       /*paranoid_file_checks=*/false,
       /*corrupt=*/true,
       /*expect_corruption=*/true,
       /*min_checksum_count=*/0},
      {"BlockChecksumValid",
       VerifyOutputFlags::kVerifyBlockChecksum |
           VerifyOutputFlags::kEnableForLocalCompaction,
       /*paranoid_file_checks=*/false,
       /*corrupt=*/false,
       /*expect_corruption=*/false,
       /*min_checksum_count=*/2},
      {"StandaloneCorrupt", VerifyOutputFlags::kVerifyNone,
       /*paranoid_file_checks=*/false,
       /*corrupt=*/true,
       /*expect_corruption=*/true,
       /*min_checksum_count=*/0},
      {"StandaloneValid", VerifyOutputFlags::kVerifyNone,
       /*paranoid_file_checks=*/false,
       /*corrupt=*/false,
       /*expect_corruption=*/false,
       /*min_checksum_count=*/3},
  };
  for (const auto& tc : cases) {
    SCOPED_TRACE(tc.label);
    Options options = CurrentOptions();
    options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
    options.verify_user_value_checksum_on_compaction = true;
    options.verify_user_value_checksum_on_flush = false;
    options.disable_auto_compactions = true;
    options.verify_output_flags = tc.flags;
    options.paranoid_file_checks = tc.paranoid_file_checks;
    options.statistics = CreateDBStatistics();
    DestroyAndReopen(options);

    ASSERT_NO_FATAL_FAILURE(WriteOverlappingL0Files(tc.corrupt));
    Status s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    if (tc.expect_corruption) {
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    } else {
      ASSERT_OK(s);
      if (tc.min_checksum_count > 0) {
        ASSERT_GE(options.statistics->getTickerCount(
                      USER_VALUE_CHECKSUM_COMPUTE_COUNT),
                  tc.min_checksum_count);
      }
      ASSERT_EQ(options.statistics->getTickerCount(
                    USER_VALUE_CHECKSUM_MISMATCH_COUNT),
                0);
    }
  }
}

TEST_F(UserValueChecksumTest, FlushOnlyEnabled) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = true;
  options.verify_user_value_checksum_on_compaction = false;
  options.level0_file_num_compaction_trigger = 2;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", TestUserValueChecksum::MakeValue("value1")));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", TestUserValueChecksum::MakeValue("value2")));
  ASSERT_OK(Flush());

  // Compaction should succeed even though compaction checksum is disabled
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(UserValueChecksumTest, DisabledByDefault) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  // Both flags default to false
  DestroyAndReopen(options);

  // Put a value with a CORRUPT checksum — flush should still succeed since
  // validation is disabled, proving it is truly skipped
  ASSERT_OK(Put("key1", TestUserValueChecksum::MakeCorruptValue("bad_data")));
  ASSERT_OK(Flush());
}

TEST_F(UserValueChecksumTest, NullValidator) {
  Options options = CurrentOptions();
  // user_value_checksum defaults to nullptr
  options.verify_user_value_checksum_on_flush = true;
  options.verify_user_value_checksum_on_compaction = true;
  DestroyAndReopen(options);

  // Should work fine — flags are true but no validator is set
  ASSERT_OK(Put("key1", "plain_value"));
  ASSERT_OK(Flush());
}

TEST_F(UserValueChecksumTest, EmptyValueSkipped) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = true;
  DestroyAndReopen(options);

  // Put an empty value — should be skipped during validation
  ASSERT_OK(Put("key1", ""));
  ASSERT_OK(Put("key2", TestUserValueChecksum::MakeValue("value2")));
  ASSERT_OK(Flush());

  ASSERT_EQ(Get("key1"), "");
  ASSERT_EQ(Get("key2"), TestUserValueChecksum::MakeValue("value2"));
}

TEST_F(UserValueChecksumTest, DeleteSkipped) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", TestUserValueChecksum::MakeValue("value1")));
  ASSERT_OK(Delete("key1"));
  ASSERT_OK(Flush());

  ASSERT_EQ(Get("key1"), "NOT_FOUND");
}

TEST_F(UserValueChecksumTest, MergeOperatorCompatible) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.merge_operator = std::make_shared<ChecksumPreservingMergeOp>();
  options.verify_user_value_checksum_on_flush = true;
  options.verify_user_value_checksum_on_compaction = true;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", TestUserValueChecksum::MakeValue("base")));
  ASSERT_OK(Merge("key1", TestUserValueChecksum::MakeValue("operand")));
  ASSERT_OK(Flush());

  // Verify validation actually ran
  ASSERT_GE(
      options.statistics->getTickerCount(USER_VALUE_CHECKSUM_COMPUTE_COUNT), 1);
  ASSERT_EQ(
      options.statistics->getTickerCount(USER_VALUE_CHECKSUM_MISMATCH_COUNT),
      0);

  // The merge result should have a valid checksum
  std::string value = Get("key1");
  ASSERT_NE(value, "NOT_FOUND");

  // Verify the merged value actually has a valid checksum
  TestUserValueChecksum validator;
  ASSERT_OK(validator.Validate("key1", value, ChecksumValueType::kValue));
}

TEST_F(UserValueChecksumTest, DynamicToggle) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = false;
  DestroyAndReopen(options);

  // Start disabled
  ASSERT_OK(Put("key1", "no_checksum_value"));
  ASSERT_OK(Flush());

  // Enable via SetOptions
  ASSERT_OK(
      dbfull()->SetOptions({{"verify_user_value_checksum_on_flush", "true"}}));

  // Prove the toggle works: write a corrupt value and verify flush fails
  ASSERT_OK(Put("key2", TestUserValueChecksum::MakeCorruptValue("value2")));
  Status s = Flush();
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();

  // Reopen since the DB is in error state after corruption
  Reopen(options);

  // Re-enable and verify valid checksums pass after toggle
  ASSERT_OK(
      dbfull()->SetOptions({{"verify_user_value_checksum_on_flush", "true"}}));
  ASSERT_OK(Put("key3", TestUserValueChecksum::MakeValue("value3")));
  ASSERT_OK(Flush());
}

TEST_F(UserValueChecksumTest, CorruptChecksumWithParanoidChecks) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = true;
  options.paranoid_file_checks = true;
  DestroyAndReopen(options);

  // Write values with corrupt checksums
  ASSERT_OK(Put("key1", TestUserValueChecksum::MakeCorruptValue("value1")));

  // Flush should fail — both paranoid_file_checks and user checksum
  // verification are enabled, and they share the same iteration pass
  Status s = Flush();
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

// Test that TimedPut (kTypeValuePreferredSeqno) values are correctly
// unpacked before checksum validation. The helper strips the trailing
// uint64 seqno before calling Validate().
TEST_F(UserValueChecksumTest, TimedPutValidationCorruptAndValid) {
  for (bool corrupt : {false, true}) {
    SCOPED_TRACE(corrupt ? "corrupt" : "valid");
    Options options = CurrentOptions();
    options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
    options.verify_user_value_checksum_on_flush = true;
    DestroyAndReopen(options);

    WriteBatch batch;
    std::string val = corrupt
                          ? TestUserValueChecksum::MakeCorruptValue("timed_val")
                          : TestUserValueChecksum::MakeValue("timed_val");
    ASSERT_OK(batch.TimedPut(db_->DefaultColumnFamily(), "timed_key", val,
                             /*write_unix_time=*/1000));
    ASSERT_OK(db_->Write(WriteOptions(), &batch));

    Status s = Flush();
    if (corrupt) {
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    } else {
      ASSERT_OK(s);
    }
  }
}

// UserValueChecksum that validates wide-column entities by checking for a
// "checksum" column containing a CRC32c over the "data" column.
class WideColumnChecksum : public UserValueChecksum {
 public:
  const char* Name() const override { return "WideColumnChecksum"; }

  Status Validate(const Slice& /*key*/, const Slice& value,
                  ChecksumValueType /*value_type*/) const override {
    return ValidateTrailingCRC32c(value);
  }

  Status ValidateWideColumns(const Slice& /*key*/,
                             const WideColumns& columns) const override {
    // Look for "data" and "checksum" columns
    Slice data_val;
    Slice checksum_val;
    bool has_data = false;
    bool has_checksum = false;
    for (const auto& col : columns) {
      if (col.name() == "data") {
        data_val = col.value();
        has_data = true;
      } else if (col.name() == "checksum") {
        checksum_val = col.value();
        has_checksum = true;
      }
    }
    if (!has_data || !has_checksum) {
      return Status::OK();  // No data/checksum columns — skip
    }
    if (checksum_val.size() != sizeof(uint32_t)) {
      return Status::Corruption("Checksum column wrong size");
    }
    uint32_t stored = DecodeFixed32(checksum_val.data());
    uint32_t computed = crc32c::Value(data_val.data(), data_val.size());
    if (stored != computed) {
      return Status::Corruption("Wide-column CRC32c checksum mismatch");
    }
    return Status::OK();
  }
};

// Test that PutEntity (wide-column) values are validated during flush
// via the ValidateWideColumns() callback.
TEST_F(UserValueChecksumTest, PutEntityValidation) {
  // Test 1: Valid entity with AlwaysValidChecksum (default ValidateWideColumns
  // returns OK)
  {
    Options opts = CurrentOptions();
    opts.user_value_checksum = std::make_shared<AlwaysValidChecksum>();
    opts.verify_user_value_checksum_on_flush = true;
    DestroyAndReopen(opts);

    WideColumns cols{{kDefaultWideColumnName, "default_val"},
                     {"col1", "val1"},
                     {"col2", "val2"}};
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                             "entity_key", cols));
    ASSERT_OK(Flush());
  }

  // Test 2: Valid entity with WideColumnChecksum — data/checksum columns match
  {
    Options opts = CurrentOptions();
    opts.user_value_checksum = std::make_shared<WideColumnChecksum>();
    opts.verify_user_value_checksum_on_flush = true;
    DestroyAndReopen(opts);

    std::string data_value = "hello_world";
    uint32_t crc = crc32c::Value(data_value.data(), data_value.size());
    std::string checksum_bytes;
    PutFixed32(&checksum_bytes, crc);

    WideColumns cols{{"checksum", checksum_bytes}, {"data", data_value}};
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                             "entity_key", cols));
    ASSERT_OK(Flush());
  }

  // Test 3: Invalid entity with WideColumnChecksum — wrong checksum
  {
    Options opts = CurrentOptions();
    opts.user_value_checksum = std::make_shared<WideColumnChecksum>();
    opts.verify_user_value_checksum_on_flush = true;
    DestroyAndReopen(opts);

    std::string data_value = "hello_world";
    std::string bad_checksum;
    PutFixed32(&bad_checksum, 0xDEADBEEF);

    WideColumns cols{{"checksum", bad_checksum}, {"data", data_value}};
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                             "entity_key", cols));
    Status s = Flush();
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  }
}

// Test dynamic toggle of verify_user_value_checksum_on_compaction.
TEST_F(UserValueChecksumTest, DynamicCompactionToggle) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = false;
  options.verify_user_value_checksum_on_compaction = false;
  options.disable_auto_compactions = true;
  options.paranoid_file_checks = true;
  DestroyAndReopen(options);

  // Write corrupt values and flush (no flush verification)
  ASSERT_NO_FATAL_FAILURE(WriteOverlappingL0Files(/*corrupt=*/true));

  // Enable compaction checksum via SetOptions
  ASSERT_OK(dbfull()->SetOptions(
      {{"verify_user_value_checksum_on_compaction", "true"}}));

  // Compaction should now detect corrupt checksums
  Status s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

// Test that user value checksum validation works correctly after DB reopen.
TEST_F(UserValueChecksumTest, ReopenPreservesValidation) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = true;
  options.verify_user_value_checksum_on_compaction = true;
  DestroyAndReopen(options);

  // Write data and flush
  ASSERT_OK(Put("key1", TestUserValueChecksum::MakeValue("value1")));
  ASSERT_OK(Flush());

  // Reopen with same options
  Reopen(options);

  // Verify data from before reopen
  ASSERT_EQ(Get("key1"), TestUserValueChecksum::MakeValue("value1"));

  // Verify validation still works after reopen: corrupt value should fail
  ASSERT_OK(Put("key2", TestUserValueChecksum::MakeCorruptValue("value2")));
  Status s = Flush();
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

// Test that corrupt values written before a crash are detected during
// recovery flush (WAL replay path via WriteLevel0TableForRecovery).
TEST_F(UserValueChecksumTest, RecoveryFlushCorruptAndValid) {
  for (bool corrupt : {false, true}) {
    SCOPED_TRACE(corrupt ? "corrupt" : "valid");
    Options options = CurrentOptions();
    options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
    options.verify_user_value_checksum_on_flush = true;
    options.avoid_flush_during_recovery = false;
    DestroyAndReopen(options);

    // Write valid data and flush to establish a stable baseline
    ASSERT_OK(Put("key1", TestUserValueChecksum::MakeValue("value1")));
    ASSERT_OK(Flush());

    // Write a value to WAL (not flushed) — corrupt or valid
    auto make = corrupt ? TestUserValueChecksum::MakeCorruptValue
                        : TestUserValueChecksum::MakeValue;
    ASSERT_OK(Put("key2", make("value2")));

    // Simulate crash by reopening without clean shutdown
    Status s = TryReopen(options);
    if (corrupt) {
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    } else {
      ASSERT_OK(s);
      ASSERT_EQ(Get("key1"), TestUserValueChecksum::MakeValue("value1"));
      ASSERT_EQ(Get("key2"), TestUserValueChecksum::MakeValue("value2"));
    }
  }
}

// Test that blob values (kTypeBlobIndex) are correctly skipped during
// user value checksum validation.
TEST_F(UserValueChecksumTest, BlobValueSkipped) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = true;
  options.verify_user_value_checksum_on_compaction = true;
  // Enable blob files to produce kTypeBlobIndex entries
  options.enable_blob_files = true;
  options.min_blob_size = 0;  // All values go to blob files
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Write values that will become blob references. The blob value content
  // is stored in the blob file; the SST contains a blob index (which has
  // no user checksum). Validation should skip these.
  ASSERT_OK(Put("key1", TestUserValueChecksum::MakeValue("blob_value1")));
  ASSERT_OK(Put("key2", TestUserValueChecksum::MakeValue("blob_value2")));
  ASSERT_OK(Flush());

  // Verify data is readable (blob references resolved transparently)
  ASSERT_EQ(Get("key1"), TestUserValueChecksum::MakeValue("blob_value1"));
  ASSERT_EQ(Get("key2"), TestUserValueChecksum::MakeValue("blob_value2"));
}

// Test that SingleDelete entries are correctly skipped during validation.
TEST_F(UserValueChecksumTest, SingleDeleteSkipped) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", TestUserValueChecksum::MakeValue("value1")));
  ASSERT_OK(db_->SingleDelete(WriteOptions(), "key1"));
  ASSERT_OK(Flush());

  ASSERT_EQ(Get("key1"), "NOT_FOUND");
}

// Test that DeleteRange entries are correctly skipped during validation.
TEST_F(UserValueChecksumTest, DeleteRangeSkipped) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", TestUserValueChecksum::MakeValue("value1")));
  ASSERT_OK(Put("key3", TestUserValueChecksum::MakeValue("value3")));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "key1",
                             "key2"));
  ASSERT_OK(Flush());

  ASSERT_EQ(Get("key1"), "NOT_FOUND");
  ASSERT_EQ(Get("key3"), TestUserValueChecksum::MakeValue("value3"));
}

// Test that ingested SST files bypass user checksum validation during
// ingestion but are validated during subsequent compaction.
TEST_F(UserValueChecksumTest, IngestExternalFileBypass) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = true;
  options.verify_user_value_checksum_on_compaction = true;
  options.disable_auto_compactions = true;
  options.paranoid_file_checks = true;
  DestroyAndReopen(options);

  // Create an SST file with a value that has NO valid checksum
  std::string sst_file = dbname_ + "/ingest.sst";
  SstFileWriter sst_writer(EnvOptions(), options);
  ASSERT_OK(sst_writer.Open(sst_file));
  // Write a value without a valid CRC32c checksum
  ASSERT_OK(sst_writer.Put("ingest_key", "no_checksum_value"));
  ASSERT_OK(sst_writer.Finish());

  // Ingestion should succeed — user checksum is NOT validated here
  IngestExternalFileOptions ingest_opts;
  ASSERT_OK(db_->IngestExternalFile({sst_file}, ingest_opts));

  // Verify the ingested data is readable
  ASSERT_EQ(Get("ingest_key"), "no_checksum_value");

  // Write another L0 file with overlapping key to force non-trivial compaction
  ASSERT_OK(Put("ingest_key", TestUserValueChecksum::MakeValue("new_value")));
  ASSERT_OK(Flush());

  // Compaction should succeed because the ingested value without checksum
  // will be superseded by the newer value with a valid checksum
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
}

// Test that ingested SST files with corrupt checksums are detected during
// subsequent compaction. This verifies the end-to-end path: ingest bypasses
// validation, but compaction catches corruption.
TEST_F(UserValueChecksumTest,
       IngestExternalFileCorruptionDetectedOnCompaction) {
  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = false;
  options.verify_user_value_checksum_on_compaction = true;
  options.disable_auto_compactions = true;
  options.paranoid_file_checks = true;
  options.num_levels = 3;
  DestroyAndReopen(options);

  // Pre-populate L1 with data covering the ingested key range.
  ASSERT_OK(Put("key1", TestUserValueChecksum::MakeValue("l1_value1")));
  ASSERT_OK(Put("key2", TestUserValueChecksum::MakeValue("l1_value2")));
  ASSERT_OK(Flush());
  // Move L0 file to L1
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Create an SST file with corrupt checksums
  std::string sst_file = dbname_ + "/ingest_corrupt.sst";
  SstFileWriter sst_writer(EnvOptions(), options);
  ASSERT_OK(sst_writer.Open(sst_file));
  ASSERT_OK(
      sst_writer.Put("key1", TestUserValueChecksum::MakeCorruptValue("bad1")));
  ASSERT_OK(
      sst_writer.Put("key2", TestUserValueChecksum::MakeCorruptValue("bad2")));
  ASSERT_OK(sst_writer.Finish());

  // Ingest to L0 (bypasses user checksum validation)
  IngestExternalFileOptions ingest_opts;
  ASSERT_OK(db_->IngestExternalFile({sst_file}, ingest_opts));

  // Compact: ingested L0 file merges with L1 data, triggering
  // user checksum verification on the output — should detect corruption
  Status s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

// Randomized test: write random keys and values of varying sizes with valid
// CRC32c checksums, flush, and compact.
TEST_F(UserValueChecksumTest, RandomizedValueSizes) {
  uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);

  Options options = CurrentOptions();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = true;
  options.verify_user_value_checksum_on_compaction = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Generate random keys and values of varying sizes (1-1024 bytes)
  constexpr int kNumKeys = 50;
  for (int i = 0; i < kNumKeys; i++) {
    int key_len = 1 + rnd.Uniform(64);
    int val_len = 1 + rnd.Uniform(1024);
    std::string key = rnd.RandomString(key_len);
    std::string val_data = rnd.RandomString(val_len);
    std::string val = TestUserValueChecksum::MakeValue(val_data);
    ASSERT_OK(Put(key, val));
  }
  ASSERT_OK(Flush());

  // Write a second batch with some overlapping keys
  for (int i = 0; i < kNumKeys; i++) {
    int key_len = 1 + rnd.Uniform(64);
    int val_len = 1 + rnd.Uniform(1024);
    std::string key = rnd.RandomString(key_len);
    std::string val_data = rnd.RandomString(val_len);
    std::string val = TestUserValueChecksum::MakeValue(val_data);
    ASSERT_OK(Put(key, val));
  }
  ASSERT_OK(Flush());

  // Compact — should succeed with all valid checksums
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
}

TEST_F(UserValueChecksumTest, StatisticsCounters) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = true;
  options.verify_user_value_checksum_on_compaction = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Write valid checksummed values
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put("key" + std::to_string(i), TestUserValueChecksum::MakeValue(
                                                 "value" + std::to_string(i))));
  }
  ASSERT_OK(Flush());

  // Flush should have validated all 10 entries
  ASSERT_GE(
      options.statistics->getTickerCount(USER_VALUE_CHECKSUM_COMPUTE_COUNT),
      10);
  ASSERT_EQ(
      options.statistics->getTickerCount(USER_VALUE_CHECKSUM_MISMATCH_COUNT),
      0);

  // Write another batch and flush for compaction input
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(
        Put("key" + std::to_string(i),
            TestUserValueChecksum::MakeValue("value_new" + std::to_string(i))));
  }
  ASSERT_OK(Flush());

  uint64_t count_before =
      options.statistics->getTickerCount(USER_VALUE_CHECKSUM_COMPUTE_COUNT);

  // Compact — should validate all output entries
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_GT(
      options.statistics->getTickerCount(USER_VALUE_CHECKSUM_COMPUTE_COUNT),
      count_before);
  ASSERT_EQ(
      options.statistics->getTickerCount(USER_VALUE_CHECKSUM_MISMATCH_COUNT),
      0);
}

TEST_F(UserValueChecksumTest, StatisticsCountersMismatch) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  options.user_value_checksum = std::make_shared<TestUserValueChecksum>();
  options.verify_user_value_checksum_on_flush = true;
  DestroyAndReopen(options);

  // Write a value with an invalid checksum
  ASSERT_OK(Put("key1", TestUserValueChecksum::MakeCorruptValue("bad_value")));

  // Flush should fail due to checksum mismatch
  Status s = Flush();
  ASSERT_NOK(s);

  ASSERT_GE(
      options.statistics->getTickerCount(USER_VALUE_CHECKSUM_MISMATCH_COUNT),
      1);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
