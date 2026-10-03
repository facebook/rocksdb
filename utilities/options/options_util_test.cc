//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/options_util.h"

#include <algorithm>
#include <cctype>
#include <cinttypes>
#include <unordered_map>

#include "db/db_impl/db_impl.h"
#include "env/mock_env.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "options/options_parser.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"

#ifndef GFLAGS
bool FLAGS_enable_print = false;
#else
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace ROCKSDB_NAMESPACE {
class OptionsUtilTest : public testing::Test {
 public:
  OptionsUtilTest() : rnd_(0xFB) {
    env_.reset(NewMemEnv(Env::Default()));
    dbname_ = test::PerThreadDBPath("options_util_test");
  }

 protected:
  Options NewTestOptions(bool track_options_file_number) const {
    Options options;
    options.env = env_.get();
    options.create_if_missing = true;
    options.track_options_file_number_in_manifest = track_options_file_number;
    return options;
  }

  void CreateAndCloseDb(const Options& options) {
    std::unique_ptr<DB> db;
    ASSERT_OK(DB::Open(options, dbname_, &db));
  }

  void PersistDefaultOptions(const Options& options, uint64_t file_number) {
    ASSERT_OK(PersistRocksDBOptions(
        WriteOptions(), DBOptions(options), {kDefaultColumnFamilyName},
        {ColumnFamilyOptions(options)}, OptionsFileName(dbname_, file_number),
        env_->GetFileSystem().get()));
  }

  std::unique_ptr<Env> env_;
  std::string dbname_;
  Random rnd_;
};

TEST_F(OptionsUtilTest, SaveAndLoad) {
  const size_t kCFCount = 5;

  DBOptions db_opt;
  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_opts;
  test::RandomInitDBOptions(&db_opt, &rnd_);
  for (size_t i = 0; i < kCFCount; ++i) {
    cf_names.push_back(i == 0 ? kDefaultColumnFamilyName
                              : test::RandomName(&rnd_, 10));
    cf_opts.emplace_back();
    test::RandomInitCFOptions(&cf_opts.back(), db_opt, &rnd_);
  }

  const std::string kFileName = "OPTIONS-123456";
  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), db_opt, cf_names, cf_opts,
                                  kFileName, env_->GetFileSystem().get()));

  DBOptions loaded_db_opt;
  std::vector<ColumnFamilyDescriptor> loaded_cf_descs;
  ConfigOptions config_options;
  config_options.ignore_unknown_options = false;
  config_options.input_strings_escaped = true;
  config_options.env = env_.get();
  ASSERT_OK(LoadOptionsFromFile(config_options, kFileName, &loaded_db_opt,
                                &loaded_cf_descs));
  ConfigOptions exact;
  exact.sanity_level = ConfigOptions::kSanityLevelExactMatch;
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(exact, db_opt, loaded_db_opt));
  test::RandomInitDBOptions(&db_opt, &rnd_);
  ASSERT_NOK(
      RocksDBOptionsParser::VerifyDBOptions(exact, db_opt, loaded_db_opt));

  for (size_t i = 0; i < kCFCount; ++i) {
    ASSERT_EQ(cf_names[i], loaded_cf_descs[i].name);
    ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
        exact, cf_opts[i], loaded_cf_descs[i].options));
    ASSERT_OK(RocksDBOptionsParser::VerifyTableFactory(
        exact, cf_opts[i].table_factory.get(),
        loaded_cf_descs[i].options.table_factory.get()));
    test::RandomInitCFOptions(&cf_opts[i], db_opt, &rnd_);
    ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
        exact, cf_opts[i], loaded_cf_descs[i].options));
  }

  ASSERT_OK(DestroyDB(dbname_, Options(db_opt, cf_opts[0])));
  for (size_t i = 0; i < kCFCount; ++i) {
    if (cf_opts[i].compaction_filter) {
      delete cf_opts[i].compaction_filter;
    }
  }
}

TEST_F(OptionsUtilTest, SaveAndLoadWithCacheCheck) {
  // creating db
  DBOptions db_opt;
  db_opt.create_if_missing = true;
  // initialize BlockBasedTableOptions
  std::shared_ptr<Cache> cache = NewLRUCache(1 * 1024);
  BlockBasedTableOptions bbt_opts;
  bbt_opts.block_size = 32 * 1024;
  // saving cf options
  std::vector<ColumnFamilyOptions> cf_opts;
  ColumnFamilyOptions default_column_family_opt = ColumnFamilyOptions();
  default_column_family_opt.table_factory.reset(
      NewBlockBasedTableFactory(bbt_opts));
  cf_opts.push_back(default_column_family_opt);

  ColumnFamilyOptions cf_opt_sample = ColumnFamilyOptions();
  cf_opt_sample.table_factory.reset(NewBlockBasedTableFactory(bbt_opts));
  cf_opts.push_back(cf_opt_sample);

  ColumnFamilyOptions cf_opt_plain_table_opt = ColumnFamilyOptions();
  cf_opt_plain_table_opt.table_factory.reset(NewPlainTableFactory());
  cf_opts.push_back(cf_opt_plain_table_opt);

  std::vector<std::string> cf_names;
  cf_names.push_back(kDefaultColumnFamilyName);
  cf_names.emplace_back("cf_sample");
  cf_names.emplace_back("cf_plain_table_sample");
  // Saving DB in file
  const std::string kFileName = "OPTIONS-LOAD_CACHE_123456";
  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), db_opt, cf_names, cf_opts,
                                  kFileName, env_->GetFileSystem().get()));
  DBOptions loaded_db_opt;
  std::vector<ColumnFamilyDescriptor> loaded_cf_descs;

  ConfigOptions config_options;
  config_options.ignore_unknown_options = false;
  config_options.input_strings_escaped = true;
  config_options.env = env_.get();
  ASSERT_OK(LoadOptionsFromFile(config_options, kFileName, &loaded_db_opt,
                                &loaded_cf_descs, &cache));
  for (size_t i = 0; i < loaded_cf_descs.size(); i++) {
    auto* loaded_bbt_opt =
        loaded_cf_descs[i]
            .options.table_factory->GetOptions<BlockBasedTableOptions>();
    // Expect the same cache will be loaded
    if (loaded_bbt_opt != nullptr) {
      ASSERT_EQ(loaded_bbt_opt->block_cache.get(), cache.get());
    }
  }
  ASSERT_OK(DestroyDB(dbname_, Options(loaded_db_opt, cf_opts[0])));
}

namespace {
class DummyTableFactory : public TableFactory {
 public:
  DummyTableFactory() = default;
  ~DummyTableFactory() override = default;

  const char* Name() const override { return "DummyTableFactory"; }

  using TableFactory::NewTableReader;
  Status NewTableReader(
      const ReadOptions& /*ro*/,
      const TableReaderOptions& /*table_reader_options*/,
      std::unique_ptr<RandomAccessFileReader>&& /*file*/,
      uint64_t /*file_size*/, std::unique_ptr<TableReader>* /*table_reader*/,
      bool /*prefetch_index_and_filter_in_cache*/) const override {
    return Status::NotSupported();
  }

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& /*table_builder_options*/,
      WritableFileWriter* /*file*/) const override {
    return nullptr;
  }

  Status ValidateOptions(
      const DBOptions& /*db_opts*/,
      const ColumnFamilyOptions& /*cf_opts*/) const override {
    return Status::NotSupported();
  }

  std::string GetPrintableOptions() const override { return ""; }

  std::unique_ptr<TableFactory> Clone() const override { return nullptr; }
};

class DummyMergeOperator : public MergeOperator {
 public:
  DummyMergeOperator() = default;
  ~DummyMergeOperator() override = default;

  bool FullMergeV2(const MergeOperationInput& /*merge_in*/,
                   MergeOperationOutput* /*merge_out*/) const override {
    return false;
  }

  bool PartialMergeMulti(const Slice& /*key*/,
                         const std::deque<Slice>& /*operand_list*/,
                         std::string* /*new_value*/,
                         Logger* /*logger*/) const override {
    return false;
  }

  const char* Name() const override { return "DummyMergeOperator"; }
};

class DummySliceTransform : public SliceTransform {
 public:
  DummySliceTransform() = default;
  ~DummySliceTransform() override = default;

  // Return the name of this transformation.
  const char* Name() const override { return "DummySliceTransform"; }

  // transform a src in domain to a dst in the range
  Slice Transform(const Slice& src) const override { return src; }

  // determine whether this is a valid src upon the function applies
  bool InDomain(const Slice& /*src*/) const override { return false; }
};

}  // namespace

TEST_F(OptionsUtilTest, SanityCheck) {
  DBOptions db_opt;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  const size_t kCFCount = 5;
  for (size_t i = 0; i < kCFCount; ++i) {
    cf_descs.emplace_back();
    cf_descs.back().name =
        (i == 0) ? kDefaultColumnFamilyName : test::RandomName(&rnd_, 10);

    cf_descs.back().options.table_factory.reset(NewBlockBasedTableFactory());
    // Assign non-null values to prefix_extractors except the first cf.
    cf_descs.back().options.prefix_extractor.reset(
        i != 0 ? test::RandomSliceTransform(&rnd_) : nullptr);
    cf_descs.back().options.merge_operator.reset(
        test::RandomMergeOperator(&rnd_));
  }

  db_opt.create_missing_column_families = true;
  db_opt.create_if_missing = true;

  ASSERT_OK(DestroyDB(dbname_, Options(db_opt, cf_descs[0].options)));
  std::unique_ptr<DB> db;
  std::vector<ColumnFamilyHandle*> handles;
  // open and persist the options
  ASSERT_OK(DB::Open(db_opt, dbname_, cf_descs, &handles, &db));

  // close the db
  for (auto* handle : handles) {
    delete handle;
  }
  db.reset();

  ConfigOptions config_options;
  config_options.ignore_unknown_options = false;
  config_options.input_strings_escaped = true;
  config_options.sanity_level = ConfigOptions::kSanityLevelLooselyCompatible;
  // perform sanity check
  ASSERT_OK(
      CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));

  ASSERT_GE(kCFCount, 5);
  // merge operator
  {
    std::shared_ptr<MergeOperator> merge_op =
        cf_descs[0].options.merge_operator;

    ASSERT_NE(merge_op.get(), nullptr);
    cf_descs[0].options.merge_operator.reset();
    ASSERT_NOK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));

    cf_descs[0].options.merge_operator.reset(new DummyMergeOperator());
    ASSERT_NOK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));

    cf_descs[0].options.merge_operator = merge_op;
    ASSERT_OK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));
  }

  // prefix extractor
  {
    std::shared_ptr<const SliceTransform> prefix_extractor =
        cf_descs[1].options.prefix_extractor;

    // It's okay to set prefix_extractor to nullptr.
    ASSERT_NE(prefix_extractor, nullptr);
    cf_descs[1].options.prefix_extractor.reset();
    ASSERT_OK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));

    cf_descs[1].options.prefix_extractor.reset(new DummySliceTransform());
    ASSERT_OK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));

    cf_descs[1].options.prefix_extractor = prefix_extractor;
    ASSERT_OK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));
  }

  // prefix extractor nullptr case
  {
    std::shared_ptr<const SliceTransform> prefix_extractor =
        cf_descs[0].options.prefix_extractor;

    // It's okay to set prefix_extractor to nullptr.
    ASSERT_EQ(prefix_extractor, nullptr);
    cf_descs[0].options.prefix_extractor.reset();
    ASSERT_OK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));

    // It's okay to change prefix_extractor from nullptr to non-nullptr
    cf_descs[0].options.prefix_extractor.reset(new DummySliceTransform());
    ASSERT_OK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));

    cf_descs[0].options.prefix_extractor = prefix_extractor;
    ASSERT_OK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));
  }

  // comparator
  {
    test::SimpleSuffixReverseComparator comparator;

    auto* prev_comparator = cf_descs[2].options.comparator;
    cf_descs[2].options.comparator = &comparator;
    ASSERT_NOK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));

    cf_descs[2].options.comparator = prev_comparator;
    ASSERT_OK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));
  }

  // table factory
  {
    std::shared_ptr<TableFactory> table_factory =
        cf_descs[3].options.table_factory;

    ASSERT_NE(table_factory, nullptr);
    cf_descs[3].options.table_factory.reset(new DummyTableFactory());
    ASSERT_NOK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));

    cf_descs[3].options.table_factory = table_factory;
    ASSERT_OK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));
  }

  // persist_user_defined_timestamps
  {
    bool prev_persist_user_defined_timestamps =
        cf_descs[2].options.persist_user_defined_timestamps;
    cf_descs[2].options.persist_user_defined_timestamps = false;
    ASSERT_NOK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));

    cf_descs[2].options.persist_user_defined_timestamps =
        prev_persist_user_defined_timestamps;
    ASSERT_OK(
        CheckOptionsCompatibility(config_options, dbname_, db_opt, cf_descs));
  }

  ASSERT_OK(DestroyDB(dbname_, Options(db_opt, cf_descs[0].options)));
}

TEST_F(OptionsUtilTest, LatestOptionsNotFound) {
  std::unique_ptr<Env> env(NewMemEnv(Env::Default()));
  Status s;
  Options options;
  ConfigOptions config_opts;
  std::vector<ColumnFamilyDescriptor> cf_descs;

  options.env = env.get();
  options.create_if_missing = true;
  config_opts.env = options.env;
  config_opts.ignore_unknown_options = false;

  std::vector<std::string> children;

  std::string options_file_name;
  ASSERT_OK(DestroyDB(dbname_, options));
  // First, test where the db directory does not exist
  ASSERT_NOK(options.env->GetChildren(dbname_, &children));

  s = GetLatestOptionsFileName(dbname_, options.env, &options_file_name);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(s.IsPathNotFound());

  s = LoadLatestOptions(config_opts, dbname_, &options, &cf_descs);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(s.IsPathNotFound());

  s = GetLatestOptionsFileName(dbname_, options.env, &options_file_name);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(s.IsPathNotFound());

  // Second, test where the db directory exists but is empty
  ASSERT_OK(options.env->CreateDir(dbname_));

  s = GetLatestOptionsFileName(dbname_, options.env, &options_file_name);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(s.IsPathNotFound());

  s = LoadLatestOptions(config_opts, dbname_, &options, &cf_descs);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(s.IsPathNotFound());

  // Finally, test where a file exists but is not an "Options" file
  std::unique_ptr<WritableFile> file;
  ASSERT_OK(
      options.env->NewWritableFile(dbname_ + "/temp.txt", &file, EnvOptions()));
  ASSERT_OK(file->Close());
  s = GetLatestOptionsFileName(dbname_, options.env, &options_file_name);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(s.IsPathNotFound());

  s = LoadLatestOptions(config_opts, dbname_, &options, &cf_descs);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(s.IsPathNotFound());
  ASSERT_OK(options.env->DeleteFile(dbname_ + "/temp.txt"));
  ASSERT_OK(options.env->DeleteDir(dbname_));
}

TEST_F(OptionsUtilTest, LegacyScanRecognizesHigherUnpreparedOptions) {
  Options options = NewTestOptions(false);
  CreateAndCloseDb(options);

  bool activated_binding = false;
  std::string effective_name;
  std::string orphan_name;
  SyncPoint::GetInstance()->SetCallBack(
      "GetLatestOptionsFileName:AfterPointerAbsentManifestReplay", [&](void*) {
        ASSERT_FALSE(activated_binding);
        activated_binding = true;
        Options tracked_options = options;
        tracked_options.create_if_missing = false;
        tracked_options.track_options_file_number_in_manifest = true;
        std::unique_ptr<DB> writer;
        ASSERT_OK(DB::Open(tracked_options, dbname_, &writer));
        auto* writer_impl = static_cast_with_check<DBImpl>(writer->GetRootDB());
        const uint64_t effective_number =
            writer_impl->GetVersionSet()->options_file_number();
        effective_name = OptionsFileName(effective_number);

        const uint64_t orphan_number = effective_number + 1000000;
        orphan_name = OptionsFileName(orphan_number);
        PersistDefaultOptions(tracked_options, orphan_number);
        writer.reset();
        OptionsFileProtocolState protocol_state;
        ASSERT_OK(VersionSet::GetOptionsFileProtocolState(
            dbname_, env_->GetFileSystem().get(), &protocol_state));
        ASSERT_TRUE(protocol_state.has_effective_options_file_number);
        ASSERT_EQ(effective_number,
                  protocol_state.effective_options_file_number);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::string selected_name;
  ASSERT_OK(GetLatestOptionsFileName(dbname_, env_.get(), &selected_name));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(activated_binding);
  ASSERT_NE(effective_name, selected_name);
  ASSERT_EQ(orphan_name, selected_name);
  ASSERT_OK(DestroyDB(dbname_, options));
}

TEST_F(OptionsUtilTest, ReadOnlyRecognizesHigherUnpreparedOptions) {
  Options options = NewTestOptions(false);
  CreateAndCloseDb(options);

  bool activated_binding = false;
  uint64_t effective_number = 0;
  uint64_t orphan_number = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::Recover:BeforeReadOnlyLegacyOptionsDirectoryScan", [&](void*) {
        if (activated_binding) {
          return;
        }
        activated_binding = true;
        Options tracked_options = options;
        tracked_options.create_if_missing = false;
        tracked_options.track_options_file_number_in_manifest = true;
        std::unique_ptr<DB> writer;
        ASSERT_OK(DB::Open(tracked_options, dbname_, &writer));
        auto* writer_impl = static_cast_with_check<DBImpl>(writer->GetRootDB());
        effective_number = writer_impl->GetVersionSet()->options_file_number();

        orphan_number = effective_number + 1000000;
        PersistDefaultOptions(tracked_options, orphan_number);
        writer.reset();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::unique_ptr<DB> read_only_db;
  Status first_open = DB::OpenForReadOnly(options, dbname_, &read_only_db);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(activated_binding);
  ASSERT_OK(first_open);
  ASSERT_NE(nullptr, read_only_db);
  // The optimized read-only attempt retries through the full recovery path.
  // The higher unprepared file is treated as output from a legacy writer.
  auto* read_only_impl =
      static_cast_with_check<DBImpl>(read_only_db->GetRootDB());
  ASSERT_FALSE(
      read_only_impl->GetVersionSet()->has_manifest_options_file_number());
  ASSERT_NE(effective_number,
            read_only_impl->GetVersionSet()->options_file_number());
  ASSERT_EQ(orphan_number,
            read_only_impl->GetVersionSet()->options_file_number());
  read_only_db.reset();
  ASSERT_OK(DestroyDB(dbname_, options));
}

TEST_F(OptionsUtilTest, BestEffortsReadOnlyUsesRecoveredManifest) {
  Options options = NewTestOptions(false);
  CreateAndCloseDb(options);

  std::string current_manifest;
  ASSERT_OK(ReadFileToString(env_.get(), CurrentFileName(dbname_),
                             &current_manifest));
  ASSERT_FALSE(current_manifest.empty());
  if (current_manifest.back() == '\n') {
    current_manifest.pop_back();
  }
  uint64_t manifest_number = 0;
  FileType manifest_type = kTempFile;
  ASSERT_TRUE(
      ParseFileName(current_manifest, &manifest_number, &manifest_type));
  ASSERT_EQ(kDescriptorFile, manifest_type);
  const std::string stale_current_target = dbname_ + "/" + current_manifest;
  const std::string surviving_manifest =
      DescriptorFileName(dbname_, manifest_number + 1);
  ASSERT_OK(env_->RenameFile(stale_current_target, surviving_manifest));
  ASSERT_TRUE(env_->FileExists(stale_current_target).IsNotFound());

  Options best_efforts_options = options;
  best_efforts_options.create_if_missing = false;
  best_efforts_options.best_efforts_recovery = true;
  std::unique_ptr<DB> read_only_db;
  ASSERT_OK(DB::OpenForReadOnly(best_efforts_options, dbname_, &read_only_db));
  read_only_db.reset();
  ASSERT_OK(DestroyDB(dbname_, options));
}

TEST_F(OptionsUtilTest, LoadLatestOptions) {
  Options options;
  options.OptimizeForSmallDb();
  ColumnFamilyDescriptor cf_desc;
  ConfigOptions config_opts;
  DBOptions db_opts;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> db;
  options.create_if_missing = true;

  ASSERT_OK(DestroyDB(dbname_, options));

  cf_descs.emplace_back();
  cf_descs.back().name = kDefaultColumnFamilyName;
  cf_descs.back().options.table_factory.reset(NewBlockBasedTableFactory());
  cf_descs.emplace_back();
  cf_descs.back().name = "Plain";
  cf_descs.back().options.table_factory.reset(NewPlainTableFactory());
  db_opts.create_missing_column_families = true;
  db_opts.create_if_missing = true;

  // open and persist the options
  ASSERT_OK(DB::Open(db_opts, dbname_, cf_descs, &handles, &db));

  std::string options_file_name;
  std::string new_options_file;

  ASSERT_OK(GetLatestOptionsFileName(dbname_, options.env, &options_file_name));
  ASSERT_OK(LoadLatestOptions(config_opts, dbname_, &db_opts, &cf_descs));
  ASSERT_EQ(cf_descs.size(), 2U);
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(config_opts,
                                                  db->GetDBOptions(), db_opts));
  ASSERT_OK(handles[0]->GetDescriptor(&cf_desc));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_opts, cf_desc.options,
                                                  cf_descs[0].options));
  ASSERT_OK(handles[1]->GetDescriptor(&cf_desc));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_opts, cf_desc.options,
                                                  cf_descs[1].options));

  // Now change some of the DBOptions
  ASSERT_OK(db->SetDBOptions(
      {{"delayed_write_rate", "1234"}, {"bytes_per_sync", "32768"}}));
  ASSERT_OK(GetLatestOptionsFileName(dbname_, options.env, &new_options_file));
  ASSERT_NE(options_file_name, new_options_file);
  ASSERT_OK(LoadLatestOptions(config_opts, dbname_, &db_opts, &cf_descs));
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(config_opts,
                                                  db->GetDBOptions(), db_opts));
  options_file_name = new_options_file;

  // Now change some of the ColumnFamilyOptions
  ASSERT_OK(db->SetOptions(handles[1], {{"write_buffer_size", "32768"}}));
  ASSERT_OK(GetLatestOptionsFileName(dbname_, options.env, &new_options_file));
  ASSERT_NE(options_file_name, new_options_file);
  ASSERT_OK(LoadLatestOptions(config_opts, dbname_, &db_opts, &cf_descs));
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(config_opts,
                                                  db->GetDBOptions(), db_opts));
  ASSERT_OK(handles[0]->GetDescriptor(&cf_desc));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_opts, cf_desc.options,
                                                  cf_descs[0].options));
  ASSERT_OK(handles[1]->GetDescriptor(&cf_desc));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_opts, cf_desc.options,
                                                  cf_descs[1].options));

  // close the db
  for (auto* handle : handles) {
    delete handle;
  }
  db.reset();
  ASSERT_OK(DestroyDB(dbname_, options, cf_descs));
}

TEST_F(OptionsUtilTest, ManifestSelectsEffectiveOptionsFile) {
  DBOptions db_options;
  db_options.env = env_.get();
  db_options.create_if_missing = true;
  db_options.track_options_file_number_in_manifest = true;
  db_options.max_manifest_file_size = 1;
  ColumnFamilyOptions cf_options;
  Options options(db_options, cf_options);
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname_, &db));

  ColumnFamilyHandle* new_cf = nullptr;
  ASSERT_OK(db->CreateColumnFamily(cf_options, "one", &new_cf));
  ASSERT_NE(nullptr, new_cf);

  std::string effective_name;
  ASSERT_OK(GetLatestOptionsFileName(dbname_, env_.get(), &effective_name));
  uint64_t effective_number = 0;
  FileType type = kTempFile;
  ASSERT_TRUE(ParseFileName(effective_name, &effective_number, &type));
  ASSERT_EQ(kOptionsFile, type);

  // An unrelated MANIFEST rotation must carry the pointer into its snapshot.
  ASSERT_OK(db->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db->Flush(FlushOptions()));
  std::string selected_after_manifest_rotation;
  ASSERT_OK(GetLatestOptionsFileName(dbname_, env_.get(),
                                     &selected_after_manifest_rotation));
  ASSERT_EQ(effective_name, selected_after_manifest_rotation);

  // Generic cleanup must retain the MANIFEST-selected file.
  auto* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
  db_impl->TEST_DeleteObsoleteFiles();
  ASSERT_OK(db_impl->TEST_WaitForPurge());
  ASSERT_OK(env_->FileExists(dbname_ + "/" + effective_name));

  std::string selected_name;
  ASSERT_OK(GetLatestOptionsFileName(dbname_, env_.get(), &selected_name));
  ASSERT_EQ(effective_name, selected_name);

  ConfigOptions config_options;
  config_options.env = env_.get();
  DBOptions loaded_db_options;
  std::vector<ColumnFamilyDescriptor> loaded_cf_descs;
  ASSERT_OK(LoadLatestOptions(config_options, dbname_, &loaded_db_options,
                              &loaded_cf_descs));
  ASSERT_FALSE(loaded_db_options.track_options_file_number_in_manifest);
  ASSERT_EQ(2U, loaded_cf_descs.size());
  ASSERT_EQ("one", loaded_cf_descs[1].name);
  const std::vector<ColumnFamilyDescriptor> explicit_cf_descs = loaded_cf_descs;

  delete new_cf;
  new_cf = nullptr;
  db.reset();

  std::vector<std::string> files_before_read_only_open;
  ASSERT_OK(env_->GetChildren(dbname_, &files_before_read_only_open));
  std::vector<ColumnFamilyHandle*> read_only_handles;
  std::unique_ptr<DB> read_only_db;
  ASSERT_OK(DB::OpenForReadOnly(db_options, dbname_, loaded_cf_descs,
                                &read_only_handles, &read_only_db));
  for (auto* handle : read_only_handles) {
    delete handle;
  }
  read_only_db.reset();
  std::vector<std::string> files_after_read_only_open;
  ASSERT_OK(env_->GetChildren(dbname_, &files_after_read_only_open));
  std::sort(files_before_read_only_open.begin(),
            files_before_read_only_open.end());
  std::sort(files_after_read_only_open.begin(),
            files_after_read_only_open.end());
  ASSERT_EQ(files_before_read_only_open, files_after_read_only_open);

  // Corruption in the selected file is terminal.
  ASSERT_OK(CreateFile(env_->GetFileSystem(), dbname_ + "/" + effective_name,
                       "not an OPTIONS file", /*use_fsync=*/false));
  ASSERT_NOK(LoadLatestOptions(config_options, dbname_, &loaded_db_options,
                               &loaded_cf_descs));

  // A missing selected file fails closed when no legacy writer superseded it.
  ASSERT_OK(env_->DeleteFile(dbname_ + "/" + effective_name));
  ASSERT_TRUE(LoadLatestOptions(config_options, dbname_, &loaded_db_options,
                                &loaded_cf_descs)
                  .IsCorruption());

  // DB::Open consumes the caller's complete options, not the operational
  // OPTIONS snapshot. The missing pointer target is terminal only for APIs
  // that explicitly load persisted options. GetLiveFiles snapshots names and
  // does not stat immutable files while holding the DB mutex.
  std::vector<ColumnFamilyHandle*> explicit_handles;
  std::unique_ptr<DB> explicit_db;
  ASSERT_OK(DB::OpenForReadOnly(db_options, dbname_, explicit_cf_descs,
                                &explicit_handles, &explicit_db));
  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  ASSERT_OK(explicit_db->GetLiveFiles(live_files, &manifest_file_size,
                                      /*flush_memtable=*/false));
  ASSERT_NE(live_files.end(), std::find(live_files.begin(), live_files.end(),
                                        OptionsFileName("", effective_number)));
  for (auto* explicit_handle : explicit_handles) {
    delete explicit_handle;
  }
  explicit_db.reset();
  ASSERT_OK(DestroyDB(dbname_, options));
}

TEST_F(OptionsUtilTest, DisabledTrackingUsesLegacyWritesAndCanReenable) {
  Options tracked = NewTestOptions(true);
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(tracked, dbname_, &db));
  auto* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
  ASSERT_TRUE(db_impl->GetVersionSet()->has_manifest_options_file_number());
  const uint64_t initial_committed_number =
      db_impl->GetVersionSet()->options_file_number();
  db.reset();

  Options legacy = tracked;
  legacy.create_if_missing = false;
  legacy.track_options_file_number_in_manifest = false;
  ASSERT_OK(DB::Open(legacy, dbname_, &db));
  for (int i = 0; i < 20; ++i) {
    ASSERT_OK(db->SetDBOptions(
        {{"stats_dump_period_sec", std::to_string(1001 + i)}}));
  }
  db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
  ASSERT_FALSE(db_impl->GetVersionSet()->has_manifest_options_file_number());
  const uint64_t legacy_number =
      db_impl->GetVersionSet()->options_file_number();
  ASSERT_GT(legacy_number, initial_committed_number);
  db.reset();

  OptionsFileProtocolState protocol_state;
  ASSERT_OK(VersionSet::GetOptionsFileProtocolState(
      dbname_, env_->GetFileSystem().get(), &protocol_state));
  ASSERT_TRUE(protocol_state.has_effective_options_file_number);
  ASSERT_EQ(initial_committed_number,
            protocol_state.effective_options_file_number);

  std::string selected_name;
  ASSERT_OK(GetLatestOptionsFileName(dbname_, env_.get(), &selected_name));
  ASSERT_EQ(OptionsFileName(legacy_number), selected_name);

  tracked.create_if_missing = false;
  ASSERT_OK(DB::Open(tracked, dbname_, &db));
  ColumnFamilyHandle* new_cf = nullptr;
  ASSERT_OK(db->CreateColumnFamily(ColumnFamilyOptions(), "one", &new_cf));
  ASSERT_NE(nullptr, new_cf);
  db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
  ASSERT_TRUE(db_impl->GetVersionSet()->has_manifest_options_file_number());
  const uint64_t reenabled_number =
      db_impl->GetVersionSet()->options_file_number();
  ASSERT_GT(reenabled_number, legacy_number);
  ASSERT_OK(db->DestroyColumnFamilyHandle(new_cf));
  db.reset();

  ASSERT_OK(GetLatestOptionsFileName(dbname_, env_.get(), &selected_name));
  ASSERT_EQ(OptionsFileName(reenabled_number), selected_name);
  ConfigOptions config_options;
  config_options.env = env_.get();
  DBOptions loaded_db_options;
  std::vector<ColumnFamilyDescriptor> loaded_cfs;
  ASSERT_OK(LoadLatestOptions(config_options, dbname_, &loaded_db_options,
                              &loaded_cfs));
  ASSERT_EQ(2U, loaded_cfs.size());
  ASSERT_EQ("one", loaded_cfs[1].name);
  ASSERT_OK(DestroyDB(dbname_, tracked));
}

TEST_F(OptionsUtilTest, ManifestPublishProtectsPendingOptionsFile) {
  Options options = NewTestOptions(true);
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname_, &db));

  auto* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
  const uint64_t current_number =
      db_impl->GetVersionSet()->options_file_number();
  for (uint64_t offset : {1000000U, 1000001U}) {
    PersistDefaultOptions(options, current_number + offset);
  }

  bool ran_cleanup = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteOptionsFile:AfterPersistOptions", [&](void*) {
        ran_cleanup = true;
        db_impl->TEST_DeleteObsoleteFiles();
        ASSERT_OK(db_impl->TEST_WaitForPurge());
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db->SetDBOptions({{"stats_dump_period_sec", "1001"}}));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(ran_cleanup);

  std::string effective_name;
  ASSERT_OK(GetLatestOptionsFileName(dbname_, env_.get(), &effective_name));
  ASSERT_OK(env_->FileExists(dbname_ + "/" + effective_name));

  db.reset();
  ASSERT_OK(DestroyDB(dbname_, options));
}

TEST_F(OptionsUtilTest, AmbiguousManifestPublishRetainsOptionsGcRoot) {
  Options options = NewTestOptions(true);
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname_, &db));

  auto* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
  bool injected = false;
  int manifest_sync_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        ++manifest_sync_count;
        // The first sync prepares the file number. Inject after the second
        // sync, where the effective pointer is committed.
        if (manifest_sync_count == 2) {
          injected = true;
          auto* status = static_cast<IOStatus*>(arg);
          assert(status != nullptr);
          ASSERT_OK(*status);
          *status = IOStatus::IOError("injected post-sync failure");
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_NOK(db->SetDBOptions({{"stats_dump_period_sec", "1001"}}));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(injected);

  // The edit is physically recoverable despite the injected return status.
  // Its target must remain protected even though the in-memory pointer was
  // not installed.
  std::string ambiguous_options_name;
  ASSERT_OK(
      GetLatestOptionsFileName(dbname_, env_.get(), &ambiguous_options_name));
  uint64_t ambiguous_options_number = 0;
  FileType type = kTempFile;
  ASSERT_TRUE(
      ParseFileName(ambiguous_options_name, &ambiguous_options_number, &type));
  ASSERT_EQ(kOptionsFile, type);
  ASSERT_NE(db_impl->GetVersionSet()->options_file_number(),
            ambiguous_options_number);

  for (uint64_t offset : {1000000U, 1000001U}) {
    PersistDefaultOptions(options, ambiguous_options_number + offset);
  }

  db_impl->TEST_DeleteObsoleteFiles();
  ASSERT_OK(db_impl->TEST_WaitForPurge());
  ASSERT_OK(env_->FileExists(dbname_ + "/" + ambiguous_options_name));

  DBOptions loaded_db_options;
  std::vector<ColumnFamilyDescriptor> loaded_cfs;
  ConfigOptions config_options;
  config_options.env = env_.get();
  ASSERT_OK(LoadLatestOptions(config_options, dbname_, &loaded_db_options,
                              &loaded_cfs));
  ASSERT_EQ(1U, loaded_cfs.size());

  ASSERT_OK(db->SetDBOptions({{"stats_dump_period_sec", "1002"}}));
  db_impl->TEST_DeleteObsoleteFiles();
  ASSERT_OK(db_impl->TEST_WaitForPurge());
  ASSERT_TRUE(
      env_->FileExists(dbname_ + "/" + ambiguous_options_name).IsNotFound());

  db.reset();
  ASSERT_OK(DestroyDB(dbname_, options));
}

static void WriteOptionsFile(Env* env, const std::string& path,
                             const std::string& options_file, int major,
                             int minor, const std::string& db_opts,
                             const std::string& cf_opts,
                             const std::string& bbt_opts = "") {
  std::string options_file_header =
      "\n"
      "[Version]\n"
      "  rocksdb_version=" +
      std::to_string(major) + "." + std::to_string(minor) +
      ".0\n"
      "  options_file_version=1\n";

  std::unique_ptr<WritableFile> wf;
  ASSERT_OK(env->NewWritableFile(path + "/" + options_file, &wf, EnvOptions()));
  ASSERT_OK(
      wf->Append(options_file_header + "[ DBOptions ]\n" + db_opts + "\n"));
  ASSERT_OK(wf->Append(
      "[CFOptions   \"default\"]  # column family must be specified\n" +
      cf_opts + "\n"));
  ASSERT_OK(wf->Append("[TableOptions/BlockBasedTable   \"default\"]\n" +
                       bbt_opts + "\n"));
  ASSERT_OK(wf->Close());

  std::string latest_options_file;
  ASSERT_OK(GetLatestOptionsFileName(path, env, &latest_options_file));
  ASSERT_EQ(latest_options_file, options_file);
}

TEST_F(OptionsUtilTest, BadLatestOptions) {
  Status s;
  ConfigOptions config_opts;
  DBOptions db_opts;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  Options options;
  options.env = env_.get();
  config_opts.env = env_.get();
  config_opts.ignore_unknown_options = false;
  config_opts.delimiter = "\n";

  ConfigOptions ignore_opts = config_opts;
  ignore_opts.ignore_unknown_options = true;

  std::string options_file_name;

  // Test where the db directory exists but is empty
  ASSERT_OK(options.env->CreateDir(dbname_));
  ASSERT_NOK(
      GetLatestOptionsFileName(dbname_, options.env, &options_file_name));
  ASSERT_NOK(LoadLatestOptions(config_opts, dbname_, &db_opts, &cf_descs));

  // Write an options file for a previous major release with an unknown DB
  // Option
  WriteOptionsFile(options.env, dbname_, "OPTIONS-0001", ROCKSDB_MAJOR - 1,
                   ROCKSDB_MINOR, "unknown_db_opt=true", "");
  s = LoadLatestOptions(config_opts, dbname_, &db_opts, &cf_descs);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  // Even though ignore_unknown_options=true, we still return an error...
  s = LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  // Write an options file for a previous minor release with an unknown CF
  // Option
  WriteOptionsFile(options.env, dbname_, "OPTIONS-0002", ROCKSDB_MAJOR,
                   ROCKSDB_MINOR - 1, "", "unknown_cf_opt=true");
  s = LoadLatestOptions(config_opts, dbname_, &db_opts, &cf_descs);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  // Even though ignore_unknown_options=true, we still return an error...
  s = LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  // Write an options file for a previous minor release with an unknown BBT
  // Option
  WriteOptionsFile(options.env, dbname_, "OPTIONS-0003", ROCKSDB_MAJOR,
                   ROCKSDB_MINOR - 1, "", "", "unknown_bbt_opt=true");
  s = LoadLatestOptions(config_opts, dbname_, &db_opts, &cf_descs);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  // Even though ignore_unknown_options=true, we still return an error...
  s = LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Write an options file for the current release with an unknown DB Option
  WriteOptionsFile(options.env, dbname_, "OPTIONS-0004", ROCKSDB_MAJOR,
                   ROCKSDB_MINOR, "unknown_db_opt=true", "");
  s = LoadLatestOptions(config_opts, dbname_, &db_opts, &cf_descs);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  // Even though ignore_unknown_options=true, we still return an error...
  s = LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Write an options file for the current release with an unknown CF Option
  WriteOptionsFile(options.env, dbname_, "OPTIONS-0005", ROCKSDB_MAJOR,
                   ROCKSDB_MINOR, "", "unknown_cf_opt=true");
  s = LoadLatestOptions(config_opts, dbname_, &db_opts, &cf_descs);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  // Even though ignore_unknown_options=true, we still return an error...
  s = LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Write an options file for the current release with an invalid DB Option
  WriteOptionsFile(options.env, dbname_, "OPTIONS-0006", ROCKSDB_MAJOR,
                   ROCKSDB_MINOR, "create_if_missing=hello", "");
  s = LoadLatestOptions(config_opts, dbname_, &db_opts, &cf_descs);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  // Even though ignore_unknown_options=true, we still return an error...
  s = LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Write an options file for the next release with an invalid DB Option
  WriteOptionsFile(options.env, dbname_, "OPTIONS-0007", ROCKSDB_MAJOR,
                   ROCKSDB_MINOR + 1, "create_if_missing=hello", "");
  ASSERT_NOK(LoadLatestOptions(config_opts, dbname_, &db_opts, &cf_descs));
  ASSERT_OK(LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs));

  // Write an options file for the next release with an unknown DB Option
  WriteOptionsFile(options.env, dbname_, "OPTIONS-0008", ROCKSDB_MAJOR,
                   ROCKSDB_MINOR + 1, "unknown_db_opt=true", "");
  ASSERT_NOK(LoadLatestOptions(config_opts, dbname_, &db_opts, &cf_descs));
  // Ignore the errors for future releases when ignore_unknown_options=true
  ASSERT_OK(LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs));

  // Write an options file for the next major release with an unknown CF Option
  WriteOptionsFile(options.env, dbname_, "OPTIONS-0009", ROCKSDB_MAJOR + 1,
                   ROCKSDB_MINOR, "", "unknown_cf_opt=true");
  ASSERT_NOK(LoadLatestOptions(config_opts, dbname_, &db_opts, &cf_descs));
  // Ignore the errors for future releases when ignore_unknown_options=true
  ASSERT_OK(LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs));
}

TEST_F(OptionsUtilTest, RenameDatabaseDirectory) {
  std::unique_ptr<DB> db;
  Options options;
  DBOptions db_opts;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  std::vector<ColumnFamilyHandle*> handles;
  ConfigOptions ignore_opts;
  ignore_opts.ignore_unknown_options = false;
  ignore_opts.env = options.env;

  options.create_if_missing = true;

  ASSERT_OK(DB::Open(options, dbname_, &db));
  ASSERT_OK(db->Put(WriteOptions(), "foo", "value0"));
  db.reset();

  auto new_dbname = dbname_ + "_2";

  ASSERT_OK(options.env->RenameFile(dbname_, new_dbname));
  ASSERT_OK(LoadLatestOptions(ignore_opts, new_dbname, &db_opts, &cf_descs));
  ASSERT_EQ(cf_descs.size(), 1U);

  db_opts.create_if_missing = false;
  ASSERT_OK(DB::Open(db_opts, new_dbname, cf_descs, &handles, &db));
  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "foo", &value));
  ASSERT_EQ("value0", value);
  // close the db
  for (auto* handle : handles) {
    delete handle;
  }
  db.reset();
  Options new_options(db_opts, cf_descs[0].options);
  ASSERT_OK(DestroyDB(new_dbname, new_options, cf_descs));
  ASSERT_OK(DestroyDB(dbname_, options));
}

TEST_F(OptionsUtilTest, WalDirSettings) {
  std::unique_ptr<DB> db;
  Options options;
  DBOptions db_opts;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  std::vector<ColumnFamilyHandle*> handles;
  ConfigOptions ignore_opts;
  ignore_opts.ignore_unknown_options = false;
  ignore_opts.env = options.env;

  options.create_if_missing = true;

  // Open a DB with no wal dir set.  The wal_dir should stay empty
  ASSERT_OK(DB::Open(options, dbname_, &db));
  db.reset();
  ASSERT_OK(LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs));
  ASSERT_EQ(db_opts.wal_dir, "");

  // Open a DB with wal_dir == dbname.  The wal_dir should be set to empty
  options.wal_dir = dbname_;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  db.reset();
  ASSERT_OK(LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs));
  ASSERT_EQ(db_opts.wal_dir, "");

  // Open a DB with no wal_dir but a db_path==dbname_.  The wal_dir should be
  // empty
  options.wal_dir = "";
  options.db_paths.emplace_back(dbname_, std::numeric_limits<uint64_t>::max());
  ASSERT_OK(DB::Open(options, dbname_, &db));
  db.reset();
  ASSERT_OK(LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs));
  ASSERT_EQ(db_opts.wal_dir, "");

  // Open a DB with no wal_dir==dbname_ and db_path==dbname_.  The wal_dir
  // should be empty
  options.wal_dir = dbname_ + "/";
  options.db_paths.emplace_back(dbname_, std::numeric_limits<uint64_t>::max());
  ASSERT_OK(DB::Open(options, dbname_, &db));
  db.reset();
  ASSERT_OK(LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs));
  ASSERT_EQ(db_opts.wal_dir, "");
  ASSERT_OK(DestroyDB(dbname_, options));

  // Open a DB with no wal_dir but db_path != db_name.  The wal_dir == dbname_
  options.wal_dir = "";
  options.db_paths.clear();
  options.db_paths.emplace_back(dbname_ + "_0",
                                std::numeric_limits<uint64_t>::max());
  ASSERT_OK(DB::Open(options, dbname_, &db));
  db.reset();
  ASSERT_OK(LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs));
  ASSERT_EQ(db_opts.wal_dir, dbname_);
  ASSERT_OK(DestroyDB(dbname_, options));

  // Open a DB with wal_dir != db_name.  The wal_dir remains unchanged
  options.wal_dir = dbname_ + "/wal";
  options.db_paths.clear();
  ASSERT_OK(DB::Open(options, dbname_, &db));
  db.reset();
  ASSERT_OK(LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs));
  ASSERT_EQ(db_opts.wal_dir, dbname_ + "/wal");
  ASSERT_OK(DestroyDB(dbname_, options));
}

TEST_F(OptionsUtilTest, WalDirInOptins) {
  std::unique_ptr<DB> db;
  Options options;
  DBOptions db_opts;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  std::vector<ColumnFamilyHandle*> handles;
  ConfigOptions ignore_opts;
  ignore_opts.ignore_unknown_options = false;
  ignore_opts.env = options.env;

  // Store an options file with wal_dir=dbname_ and make sure it still loads
  // when the input wal_dir is empty
  options.create_if_missing = true;
  options.wal_dir = "";
  ASSERT_OK(DB::Open(options, dbname_, &db));
  db.reset();
  options.wal_dir = dbname_;
  std::string options_file;
  ASSERT_OK(GetLatestOptionsFileName(dbname_, options.env, &options_file));
  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), options, {"default"},
                                  {options}, dbname_ + "/" + options_file,
                                  options.env->GetFileSystem().get()));
  ASSERT_OK(LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs));
  ASSERT_EQ(db_opts.wal_dir, dbname_);
  options.wal_dir = "";
  ASSERT_OK(DB::Open(options, dbname_, &db));
  db.reset();
  ASSERT_OK(LoadLatestOptions(ignore_opts, dbname_, &db_opts, &cf_descs));
  ASSERT_EQ(db_opts.wal_dir, "");
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
