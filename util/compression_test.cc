// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#include "util/compression.h"

#include "port/stack_trace.h"
#include "rocksdb/configurable.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/utilities/options_util.h"
#include "test_util/testharness.h"
#include "util/compressor.h"

namespace ROCKSDB_NAMESPACE {

TEST(Compression, CreateFromString) {
  ConfigOptions config_options;
  config_options.ignore_unsupported_options = false;
  config_options.ignore_unknown_options = false;

  for (auto type : {kSnappyCompression, kZlibCompression, kBZip2Compression,
                    kLZ4Compression, kLZ4HCCompression, kXpressCompression,
                    kZSTD, kZSTDNotFinalCompression, kNoCompression}) {
    std::shared_ptr<Compressor> base, copy;
    std::string name, nickname;
    ASSERT_TRUE(BuiltinCompressor::TypeToString(type, true, &name));
    ASSERT_OK(Compressor::CreateFromString(config_options, name, &base));
    // Was compressor created?
    ASSERT_NE(base, nullptr);
    ASSERT_EQ(base->GetCompressionType(), type);
    ASSERT_TRUE(base->IsInstanceOf(name));
    if (BuiltinCompressor::TypeToString(type, false, &nickname)) {
      ASSERT_OK(Compressor::CreateFromString(config_options, nickname, &copy));
      ASSERT_NE(copy, nullptr);
      ASSERT_EQ(base.get(), copy.get());
    }
    std::string value = base->ToString(config_options);
    ASSERT_OK(Compressor::CreateFromString(config_options, value, &copy));
    ASSERT_NE(copy, nullptr);
    ASSERT_EQ(base.get(), copy.get());
  }
}

TEST(Compression, TestBuiltinCompressors) {
  std::string mismatch;
  ConfigOptions config_options;
  config_options.ignore_unsupported_options = false;
  config_options.ignore_unknown_options = false;
  CompressionOptions compression_opts{1, 2, 3, 4, 5, 6, false, 7, false, 8};

  for (auto type : {kSnappyCompression, kZlibCompression, kBZip2Compression,
                    kLZ4Compression, kLZ4HCCompression, kXpressCompression,
                    kZSTD, kZSTDNotFinalCompression}) {
    std::shared_ptr<Compressor> copy;
    auto compressor1 = BuiltinCompressor::GetCompressor(type);
    ASSERT_NE(compressor1, nullptr);
    ASSERT_EQ(compressor1->GetCompressionType(), type);
    auto compressor2 = BuiltinCompressor::GetCompressor(type, compression_opts);
    ASSERT_NE(compressor2, nullptr);
    ASSERT_EQ(compressor2->GetCompressionType(), type);
    ASSERT_EQ(compressor2->GetCompressionType(),
              compressor1->GetCompressionType());
    ASSERT_NE(compressor1.get(), compressor2.get());
    ASSERT_FALSE(compressor1->AreEquivalent(config_options, compressor2.get(),
                                            &mismatch));
    std::string value = compressor1->ToString(config_options);
    ASSERT_OK(Compressor::CreateFromString(config_options, value, &copy));
    ASSERT_EQ(compressor1.get(), copy.get());

    value = compressor2->ToString(config_options);
    ASSERT_OK(Compressor::CreateFromString(config_options, value, &copy));
    ASSERT_EQ(compressor2.get(), copy.get());
  }
}

TEST(Compression, GetSupportedCompressions) {
  std::vector<CompressionType> types = GetSupportedCompressions();
  std::vector<std::string> names = Compressor::GetSupported();
  for (const auto& n : names) {
    CompressionType type;
    if (BuiltinCompressor::StringToType(n, &type)) {
      bool found = false;
      for (auto& t : types) {
        if (t == type) {
          found = true;
          break;
        }
      }
      ASSERT_TRUE(found) << "Missing Compression Type: " << n;
    }
  }
}

static void WriteDBAndFlush(DB* db, int num_keys, const std::string& val) {
  WriteOptions wo;
  for (int i = 0; i < num_keys; i++) {
    std::string key = std::to_string(i);
    Status s = db->Put(wo, Slice(key), Slice(val));
    ASSERT_OK(s);
  }
  // Flush all data from memtable so that an SST file is written
  ASSERT_OK(db->Flush(FlushOptions()));
}

static void CloseDB(DB* db) {
  Status s = db->Close();
  ASSERT_OK(s);
  delete db;
}

TEST(Compression, DBWithZlibAndCompressionOptions) {
  if (!BuiltinCompressor::TypeSupported(kZlibCompression)) {
    ROCKSDB_GTEST_BYPASS("Test requires ZLIB compression");
    return;
  }

  Options options;
  std::string dbname = test::PerThreadDBPath("compression_test");
  ASSERT_OK(DestroyDB(dbname, options));

  // Select Zlib through options.compression and options.compression_opts
  options.create_if_missing = true;
  options.compression = kZlibCompression;
  options.compression_opts.window_bits = -13;

  // Open database
  DB* db = nullptr;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_NE(db, nullptr);

  // Write 200 values, each 20 bytes
  WriteDBAndFlush(db, 200, "aaaaaaaaaabbbbbbbbbb");

  // Verify table properties
  TablePropertiesCollection all_tables_props;
  s = db->GetPropertiesOfAllTables(&all_tables_props);
  ASSERT_OK(s);
  for (auto it = all_tables_props.begin(); it != all_tables_props.end(); ++it) {
    ASSERT_EQ(it->second->compression_name,
              BuiltinCompressor::TypeToString(kZlibCompression));
  }
  // Verify options file
  DBOptions db_options;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  ConfigOptions config_options;
  s = LoadLatestOptions(config_options, db->GetName(), &db_options, &cf_descs);
  ASSERT_OK(s);
  ASSERT_EQ(cf_descs[0].options.compression, kZlibCompression);
  ASSERT_EQ(cf_descs[0].options.compression_opts.window_bits, -13);
  CloseDB(db);
  ASSERT_OK(DestroyDB(dbname, options));
}

TEST(Compression, DBWithCompressionPerLevel) {
  if (!BuiltinCompressor::TypeSupported(kSnappyCompression)) {
    ROCKSDB_GTEST_BYPASS("Test requires Snappy compression");
    return;
  }

  Options options;
  std::string dbname = test::PerThreadDBPath("compression_test");
  ASSERT_OK(DestroyDB(dbname, options));

  options.create_if_missing = true;
  options.compression_per_level.push_back(kNoCompression);
  options.compression_per_level.push_back(kSnappyCompression);

  DB* db = nullptr;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_NE(db, nullptr);

  CloseDB(db);

  // Test an invalid selection for compression_per_level
  options.compression_per_level.push_back(static_cast<CompressionType>(254));
  s = DB::Open(options, dbname, &db);
  ASSERT_NOK(s);
  ASSERT_EQ(s.ToString(), "Invalid argument: Compression type is invalid.");

  ASSERT_OK(DestroyDB(dbname, options));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
