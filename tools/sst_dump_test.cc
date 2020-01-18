//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include <stdint.h>
#include "rocksdb/sst_dump_tool.h"

#include "file/random_access_file_reader.h"
#include "port/stack_trace.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/table_builder.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace rocksdb {

const uint32_t optLength = 100;

namespace {
static std::string MakeKey(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "k_%04d", i);
  InternalKey key(std::string(buf), 0, ValueType::kTypeValue);
  return key.Encode().ToString();
}

static std::string MakeValue(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "v_%04d", i);
  InternalKey key(std::string(buf), 0, ValueType::kTypeValue);
  return key.Encode().ToString();
}

void createSST(const Options& opts, const std::string& file_name) {
  Env* env = opts.env;
  EnvOptions env_options(opts);
  ReadOptions read_options;
  const ImmutableCFOptions imoptions(opts);
  const MutableCFOptions moptions(opts);
  rocksdb::InternalKeyComparator ikc(opts.comparator);
  std::unique_ptr<TableBuilder> tb;

  std::unique_ptr<WritableFile> file;
  ASSERT_OK(env->NewWritableFile(file_name, &file, env_options));

  std::vector<std::unique_ptr<IntTblPropCollectorFactory> >
      int_tbl_prop_collector_factories;
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      NewLegacyWritableFileWrapper(std::move(file)), file_name, EnvOptions()));
  std::string column_family_name;
  int unknown_level = -1;
  tb.reset(opts.table_factory->NewTableBuilder(
      TableBuilderOptions(
          imoptions, moptions, ikc, &int_tbl_prop_collector_factories,
          CompressionType::kNoCompression, 0 /* sample_for_compression */,
          CompressionOptions(), false /* skip_filters */, column_family_name,
          unknown_level),
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
      file_writer.get()));

  // Populate slightly more than 1K keys
  uint32_t num_keys = 1024;
  for (uint32_t i = 0; i < num_keys; i++) {
    tb->Add(MakeKey(i), MakeValue(i));
  }
  tb->Finish();
  file_writer->Close();
}

void cleanup(const Options& opts, const std::string& file_name) {
  Env* env = opts.env;
  env->DeleteFile(file_name);
  std::string outfile_name = file_name.substr(0, file_name.length() - 4);
  outfile_name.append("_dump.txt");
  env->DeleteFile(outfile_name);
}
}  // namespace

// Test for sst dump tool "raw" mode
class SSTDumpToolTest : public testing::Test {
  std::string test_dir_;
  Env* env_;
  std::shared_ptr<Env> env_guard_;

 public:
  SSTDumpToolTest() : env_(Env::Default()) {
    const char* test_env_uri = getenv("TEST_ENV_URI");
    if (test_env_uri) {
      Env::LoadEnv(test_env_uri, &env_, &env_guard_);
    }
    test_dir_ = test::PerThreadDBPath(env_, "sst_dump_test_db");
    Status s = env_->CreateDirIfMissing(test_dir_);
    EXPECT_OK(s);
  }

  ~SSTDumpToolTest() override {
    if (getenv("KEEP_DB")) {
      fprintf(stdout, "Data is still at %s\n", test_dir_.c_str());
    } else {
      EXPECT_OK(env_->DeleteDir(test_dir_));
    }
  }

  Env* env() { return env_; }

  std::string MakeFilePath(const std::string& file_name) const {
    std::string path(test_dir_);
    path.append("/").append(file_name);
    return path;
  }

  template <std::size_t N>
  void PopulateCommandArgs(const std::string& file_path, const char* command,
                           char* (&usage)[N]) const {
    for (int i = 0; i < static_cast<int>(N); ++i) {
      usage[i] = new char[optLength];
    }
    snprintf(usage[0], optLength, "./sst_dump");
    snprintf(usage[1], optLength, "%s", command);
    snprintf(usage[2], optLength, "--file=%s", file_path.c_str());
  }
};

TEST_F(SSTDumpToolTest, EmptyFilter) {
  Options opts;
  opts.env = env();
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path);

  char* usage[3];
  PopulateCommandArgs(file_path, "--command=raw", usage);

  rocksdb::SSTDumpTool tool;
  ASSERT_TRUE(!tool.Run(3, usage, opts));

  cleanup(opts, file_path);
  for (int i = 0; i < 3; i++) {
    delete[] usage[i];
  }
}

TEST_F(SSTDumpToolTest, FilterBlock) {
  Options opts;
  opts.env = env();
  BlockBasedTableOptions table_opts;
  table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
  opts.table_factory.reset(new BlockBasedTableFactory(table_opts));
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path);

  char* usage[3];
  PopulateCommandArgs(file_path, "--command=raw", usage);

  rocksdb::SSTDumpTool tool;
  ASSERT_TRUE(!tool.Run(3, usage, opts));

  cleanup(opts, file_path);
  for (int i = 0; i < 3; i++) {
    delete[] usage[i];
  }
}

TEST_F(SSTDumpToolTest, FullFilterBlock) {
  Options opts;
  opts.env = env();
  BlockBasedTableOptions table_opts;
  table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  opts.table_factory.reset(new BlockBasedTableFactory(table_opts));
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path);

  char* usage[3];
  PopulateCommandArgs(file_path, "--command=raw", usage);

  rocksdb::SSTDumpTool tool;
  ASSERT_TRUE(!tool.Run(3, usage, opts));

  cleanup(opts, file_path);
  for (int i = 0; i < 3; i++) {
    delete[] usage[i];
  }
}

TEST_F(SSTDumpToolTest, GetProperties) {
  Options opts;
  opts.env = env();
  BlockBasedTableOptions table_opts;
  table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  opts.table_factory.reset(new BlockBasedTableFactory(table_opts));
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path);

  char* usage[3];
  PopulateCommandArgs(file_path, "--show_properties", usage);

  rocksdb::SSTDumpTool tool;
  ASSERT_TRUE(!tool.Run(3, usage, opts));

  cleanup(opts, file_path);
  for (int i = 0; i < 3; i++) {
    delete[] usage[i];
  }
}

TEST_F(SSTDumpToolTest, CompressedSizes) {
  Options opts;
  opts.env = env();
  BlockBasedTableOptions table_opts;
  table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  opts.table_factory.reset(new BlockBasedTableFactory(table_opts));
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path);

  char* usage[3];
  PopulateCommandArgs(file_path, "--command=recompress", usage);

  rocksdb::SSTDumpTool tool;
  ASSERT_TRUE(!tool.Run(3, usage, opts));

  cleanup(opts, file_path);
  for (int i = 0; i < 3; i++) {
    delete[] usage[i];
  }
}

TEST_F(SSTDumpToolTest, MemEnv) {
  std::unique_ptr<Env> mem_env(NewMemEnv(env()));
  Options opts;
  opts.env = mem_env.get();
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path);

  char* usage[3];
  PopulateCommandArgs(file_path, "--command=verify_checksum", usage);

  rocksdb::SSTDumpTool tool;
  ASSERT_TRUE(!tool.Run(3, usage, opts));

  cleanup(opts, file_path);
  for (int i = 0; i < 3; i++) {
    delete[] usage[i];
  }
}

}  // namespace rocksdb

#ifdef ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS
extern "C" {
void RegisterCustomObjects(int argc, char** argv);
}
#else
void RegisterCustomObjects(int /*argc*/, char** /*argv*/) {}
#endif  // !ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as SSTDumpTool is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE  return RUN_ALL_TESTS();
