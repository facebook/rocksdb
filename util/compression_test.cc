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
#include "rocksdb/compressor.h"
#include "rocksdb/configurable.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/utilities/options_util.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

// Registers a factory function for a Compressor in the default ObjectLibrary.
// The factory function calls the Compressor constructor.
template <typename T>
void AddCompressorFactory(std::shared_ptr<ObjectLibrary> library) {
  library->AddFactory<Compressor>(
      T::kClassName(),
      [](const std::string& /* uri */, std::unique_ptr<Compressor>* c,
         std::string* /* errmsg */) {
        c->reset(new T());
        return c->get();
      });
}

struct DummyCompressorOptions {
  static const char* kName() { return "DummyCompressorOptions"; };
  std::string option_str = "default";
  int option_int = 0;
};

static std::unordered_map<std::string, OptionTypeInfo>
    dummy_compressor_type_info = {
        {"option_str",
         {offsetof(struct DummyCompressorOptions, option_str),
          OptionType::kString, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"option_int",
         {offsetof(struct DummyCompressorOptions, option_int), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}}};

template <const int id>
class DummyCompressor : public Compressor {
 public:
  DummyCompressor() {
    RegisterOptions(&options_, &dummy_compressor_type_info);
  };

  static const char* kClassName() {
    name_ = "DummyCompressor" + std::to_string(id);
    return name_.c_str();
  }

  const char* Name() const override { return kClassName(); }

  Status Compress(const CompressionInfo& info, const Slice& input,
                  std::string* output) override {
    (void)info;
    (void)input;
    (void)output;
    return Status::OK();
  }

  Status Uncompress(const UncompressionInfo& info, const char* input,
                    size_t input_length, char** output,
                    size_t* output_length) override {
    (void)info;
    (void)input;
    (void)input_length;
    (void)output;
    (void)output_length;
    return Status::OK();
  }

 private:
  static std::string name_;
  DummyCompressorOptions options_;
};

template <const int id>
std::string DummyCompressor<id>::name_;

struct DummyDictionaryCompressorOptions {
  static const char* kName() { return "DummyDictionaryCompressorOptions"; };
  uint32_t max_dict_bytes = 0;
  uint32_t max_train_bytes = 0;
  uint64_t max_dict_buffer_bytes = 0;
};

static std::unordered_map<std::string, OptionTypeInfo>
    dummy_dictionary_compressor_type_info = {
        {"max_dict_bytes",
         {offsetof(struct DummyDictionaryCompressorOptions, max_dict_bytes),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_train_bytes",
         {offsetof(struct DummyDictionaryCompressorOptions, max_train_bytes),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_dict_buffer_bytes",
         {offsetof(struct DummyDictionaryCompressorOptions,
                   max_dict_buffer_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}}};

class DummyDictionaryCompressor : public Compressor {
 public:
  DummyDictionaryCompressor() {
    RegisterOptions(&options_, &dummy_dictionary_compressor_type_info);
  };

  static const char* kClassName() { return "DummyDictionaryCompressor"; }

  const char* Name() const override { return kClassName(); }

  bool DictCompressionSupported() const override { return true; }

  Status Compress(const CompressionInfo& info, const Slice& input,
                  std::string* output) override {
    (void)info;
    output->append(input.data(), input.size());
    return Status::OK();
  }

  Status Uncompress(const UncompressionInfo& info, const char* input,
                    size_t input_length, char** output,
                    size_t* output_length) override {
    *output = Allocate(input_length, info.GetMemoryAllocator());
    if (!*output) {
      return Status::MemoryLimit();
    }
    memcpy(*output, input, input_length);
    *output_length = input_length;
    return Status::OK();
  }

  Status CreateDict(std::vector<std::string>& /*data_block_buffers*/,
                    std::unique_ptr<CompressionDict>* dict) override {
    *dict = NewCompressionDict(std::string());
    num_create_dictionary_calls++;
    return Status::OK();
  }

  uint64_t GetMaxDictBufferBytes() const override {
    return options_.max_dict_buffer_bytes;
  }

  int num_create_dictionary_calls = 0;

 private:
  uint32_t GetMaxDictBytes() const override { return options_.max_dict_bytes; }

  uint32_t GetMaxTrainBytes() const override {
    return options_.max_train_bytes;
  }

  DummyDictionaryCompressorOptions options_;
};

// Simple RLE compressor for testing purposes.
// It needs to compress enough to pass GoodCompressionRatio check.
class SimpleRLECompressor : public Compressor {
 public:
  static const char* kClassName() { return "SimpleRLECompressor"; }

  const char* Name() const override { return kClassName(); }

  Status Compress(const CompressionInfo& info, const Slice& input,
                  std::string* output) override;

  Status Uncompress(const UncompressionInfo& info, const char* input,
                    size_t input_length, char** output,
                    size_t* output_length) override;

  std::atomic<int> num_compress_calls = 0;
  std::atomic<int> num_uncompress_calls = 0;

 private:
  const char delim_ = '~';

  void outputSeq(char last, char seq, std::string* output);
};

Status SimpleRLECompressor::Compress(const CompressionInfo& info,
                                     const Slice& input, std::string* output) {
  (void)info;

  output->clear();
  char last = input[0];
  char seq = 0;
  for (size_t i = 0; i < input.size(); i++) {
    if (input[i] == last && seq < delim_ - 1) {
      seq++;
    } else {
      outputSeq(last, seq, output);
      seq = 1;
    }
    last = input[i];
  }
  outputSeq(last, seq, output);

  num_compress_calls++;
  return Status::OK();
}

Status SimpleRLECompressor::Uncompress(const UncompressionInfo& info,
                                       const char* input, size_t input_length,
                                       char** output, size_t* output_length) {
  (void)info;

  std::string uncompressed;
  size_t i = 0;
  while (i < input_length) {
    if (i < input_length - 1 && input[i] == delim_ && input[i + 1] == delim_) {
      uncompressed += delim_;
      i += 2;
    } else if (i < input_length - 2 && input[i] == delim_) {
      uncompressed.append(input[i + 1], input[i + 2]);
      i += 3;
    } else {
      uncompressed += input[i];
      i++;
    }
  }

  *output = Allocate(uncompressed.length(), info.GetMemoryAllocator());
  if (!*output) {
    return Status::MemoryLimit();
  }
  memcpy(*output, uncompressed.c_str(), uncompressed.length());
  *output_length = uncompressed.length();
  num_uncompress_calls++;
  return Status::OK();
}

void SimpleRLECompressor::outputSeq(char last, char seq, std::string* output) {
  if (last != delim_) {
    if (seq >= 4) {
      *output += delim_;
      *output += seq;
      *output += last;
    } else {
      output->append(seq, last);
    }
  } else {
    if (seq >= 2) {
      *output += delim_;
      *output += seq;
      *output += last;
    } else {
      output->append(seq * 2, last);
    }
  }
}

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

TEST(Compression, SimpleRLECompressor) {
  SimpleRLECompressor compressor;
  char input[] = "aaaaaaaaaabbbbbbbbbb";
  size_t input_length = 21;
  std::string compressed;
  CompressionInfo compression_info(CompressionDict::GetEmptyDict(), 0, 0);
  Slice data(input, input_length);
  Status s = compressor.Compress(compression_info, data, &compressed);
  ASSERT_OK(s);
  ASSERT_STREQ(compressed.c_str(), "~\na~\nb");
  char* decompress_data = nullptr;
  size_t decompress_size = 0;
  UncompressionInfo uncompression_info(UncompressionDict::GetEmptyDict(), 0);
  s = compressor.Uncompress(uncompression_info, compressed.c_str(),
                            compressed.length(), &decompress_data,
                            &decompress_size);
  CacheAllocationPtr original;
  original.reset(decompress_data);
  ASSERT_OK(s);
  ASSERT_NE(original, nullptr);
  ASSERT_EQ(decompress_size, input_length);
  ASSERT_STREQ(original.get(), input);
}

TEST(Compression, CreatePluginCompressor) {
  ConfigOptions config_options;
  auto library = config_options.registry->AddLibrary("CreatePluginCompressor");
  std::shared_ptr<Compressor> compressor;
  std::vector<std::shared_ptr<Compressor>> compressors;

  AddCompressorFactory<SimpleRLECompressor>(library);
  std::string compressor_name = SimpleRLECompressor::kClassName();
  Status s = Compressor::CreateFromString(config_options, compressor_name,
                                          &compressor);

  // Was compressor created?
  ASSERT_OK(s);
  ASSERT_NE(compressor, nullptr);
  ASSERT_EQ(compressor->Name(), compressor_name);

  // Was compressor assigned the expected numeric type?
  CompressionType compression_type = compressor->GetCompressionType();
  ASSERT_EQ(compression_type, kPluginCompression);

  // Was factory added to ObjectLibrary?
  auto entry = library->FindFactory<Compressor>(compressor_name);
  ASSERT_NE(entry, nullptr);
}

TEST(Compression, CreateCompressorsWithDifferentOptions) {
  ConfigOptions config_options;
  auto library = config_options.registry->AddLibrary(
      "CreateCompressorsWithDifferentOptions");
  std::vector<std::shared_ptr<Compressor>> compressors;

  AddCompressorFactory<DummyCompressor<1>>(library);
  std::string compressor_name = DummyCompressor<1>::kClassName();

  // Create DummyCompressor with default configuration
  std::shared_ptr<Compressor> compressor_config0;
  Status s = Compressor::CreateFromString(config_options, compressor_name,
                                          &compressor_config0);
  ASSERT_OK(s);
  ASSERT_NE(compressor_config0, nullptr);

  // Create DummyCompressor with configuration 1
  std::shared_ptr<Compressor> compressor_config1;
  s = Compressor::CreateFromString(
      config_options,
      "id=" + std::string(DummyCompressor<1>::kClassName()) +
          ";option_str=str1;option_int=1",
      &compressor_config1);
  ASSERT_OK(s);
  ASSERT_NE(compressor_config1, nullptr);

  // Create DummyCompressor with configuration 2
  std::shared_ptr<Compressor> compressor_config2;
  s = Compressor::CreateFromString(
      config_options,
      "id=" + std::string(DummyCompressor<1>::kClassName()) +
          ";option_str=str2;option_int=2",
      &compressor_config2);
  ASSERT_OK(s);
  ASSERT_NE(compressor_config2, nullptr);

  // Verify default configuration
  ASSERT_EQ(
      compressor_config0->GetOptions<DummyCompressorOptions>()->option_str,
      DummyCompressorOptions().option_str);
  ASSERT_EQ(
      compressor_config0->GetOptions<DummyCompressorOptions>()->option_int,
      DummyCompressorOptions().option_int);

  // Verify configuration 1
  ASSERT_STREQ(compressor_config1->GetOptions<DummyCompressorOptions>()
                   ->option_str.c_str(),
               "str1");
  ASSERT_EQ(
      compressor_config1->GetOptions<DummyCompressorOptions>()->option_int, 1);

  // Verify configuration 2
  ASSERT_STREQ(compressor_config2->GetOptions<DummyCompressorOptions>()
                   ->option_str.c_str(),
               "str2");
  ASSERT_EQ(
      compressor_config2->GetOptions<DummyCompressorOptions>()->option_int, 2);
}

TEST(Compression, ColumnFamilyOptionsFromStringWithMissingCompressor) {
  std::vector<std::shared_ptr<Compressor>> compressors;
  ColumnFamilyOptions options, new_options;
  ConfigOptions config_options;
  config_options.ignore_unsupported_options = false;

  Status s = GetColumnFamilyOptionsFromString(
      config_options, options, "compressor=MissingCompressor;", &new_options);
  ASSERT_NOK(s);
  ASSERT_EQ(s.ToString(),
            "Invalid argument: Cannot find compressor : MissingCompressor");
}

TEST(Compression, ColumnFamilyOptionsFromStringWithPluginCompressor) {
  ConfigOptions config_options;
  auto library = config_options.registry->AddLibrary(
      "ColumnFamilyOptionsFromStringWithPluginCompressor");
  std::vector<std::shared_ptr<Compressor>> compressors;
  ColumnFamilyOptions options, new_options;
  config_options.ignore_unsupported_options = false;

  AddCompressorFactory<SimpleRLECompressor>(library);
  std::string compressor_name = SimpleRLECompressor::kClassName();
  Status s = GetColumnFamilyOptionsFromString(
      config_options, options, "compressor=" + compressor_name + ";",
      &new_options);
  ASSERT_OK(s);
  ASSERT_NE(new_options.compressor, nullptr);
  ASSERT_EQ(new_options.compressor->Name(), compressor_name);
  ASSERT_EQ(new_options.compressor->GetCompressionType(), kPluginCompression);
}

TEST(Compression, ColumnFamilyOptionsFromStringWithCompression) {
  ConfigOptions config_options;
  auto library = config_options.registry->AddLibrary(
      "ColumnFamilyOptionsFromStringWithCompression");
  ColumnFamilyOptions options, new_options;
  config_options.ignore_unsupported_options = false;

  Status s = GetColumnFamilyOptionsFromString(
      config_options, options, "compression=kZlibCompression", &new_options);
  ASSERT_OK(s);
  ASSERT_EQ(new_options.compression, kZlibCompression);
  ASSERT_EQ(new_options.compressor, nullptr);
}

TEST(Compression, StringFromColumnFamilyOptions) {
  ConfigOptions config_options;
  auto library =
      config_options.registry->AddLibrary("StringFromColumnFamilyOptions");
  std::shared_ptr<Compressor> compressor;
  std::vector<std::shared_ptr<Compressor>> compressors;

  // Select compressor with options
  AddCompressorFactory<DummyCompressor<1>>(library);
  std::string compressor_name = DummyCompressor<1>::kClassName();
  std::string opts_str =
      "{id=" + compressor_name + ";option_int=1;option_str=str1;}";

  ColumnFamilyOptions options, new_options;
  Status s = GetColumnFamilyOptionsFromString(
      config_options, options, "compressor=" + opts_str, &new_options);
  ASSERT_OK(s);
  ASSERT_NE(new_options.compressor, nullptr);
  ASSERT_EQ(new_options.compressor->Name(), compressor_name);
  ASSERT_EQ(new_options.compressor->GetCompressionType(), kPluginCompression);

  // Verify compressor was configured with options
  ASSERT_STREQ(new_options.compressor->GetOptions<DummyCompressorOptions>()
                   ->option_str.c_str(),
               "str1");
  ASSERT_EQ(
      new_options.compressor->GetOptions<DummyCompressorOptions>()->option_int,
      1);

  // Verify serialization of options
  std::string opts_serialized;
  s = GetStringFromColumnFamilyOptions(config_options, new_options,
                                       &opts_serialized);
  ASSERT_OK(s);
  ASSERT_TRUE(opts_serialized.find("compressor=" + opts_str) !=
              std::string::npos);
  ASSERT_TRUE(opts_serialized.find("bottommost_compressor=nullptr") !=
              std::string::npos);
  ASSERT_TRUE(opts_serialized.find("blob_compressor=nullptr") !=
              std::string::npos);

  // Re-parse serialized options
  s = GetColumnFamilyOptionsFromString(config_options, options, opts_serialized,
                                       &new_options);
  ASSERT_OK(s);
}

TEST(Compression, ColumnFamilyOptionsFromStringWithCompressionPerLevel) {
  ConfigOptions config_options;
  auto library = config_options.registry->AddLibrary(
      "ColumnFamilyOptionsFromStringWithCompressionPerLevel");
  std::vector<std::shared_ptr<Compressor>> compressors;

  // Select two different compressor configurations (per level)
  AddCompressorFactory<DummyCompressor<1>>(library);
  std::string opts_str1 =
      "{id=" + std::string(DummyCompressor<1>::kClassName()) +
      ";option_int=1;option_str=str1;}";
  std::string opts_str2 =
      "{id=" + std::string(DummyCompressor<1>::kClassName()) +
      ";option_int=2;option_str=str2;}";

  ColumnFamilyOptions options, new_options;
  std::string opts = "compressor_per_level={NoCompression:" + opts_str1 + ":" +
                     opts_str2 + "}";
  Status s = GetColumnFamilyOptionsFromString(config_options, options, opts,
                                              &new_options);
  ASSERT_OK(s);

  std::shared_ptr<Compressor> compressor1 = new_options.compressor_per_level[1];
  ASSERT_NE(compressor1, nullptr);
  std::shared_ptr<Compressor> compressor2 = new_options.compressor_per_level[2];
  ASSERT_NE(compressor2, nullptr);

  // Verify compressors were set up with options
  ASSERT_STREQ(
      compressor1->GetOptions<DummyCompressorOptions>()->option_str.c_str(),
      "str1");
  ASSERT_EQ(compressor1->GetOptions<DummyCompressorOptions>()->option_int, 1);

  ASSERT_STREQ(
      compressor2->GetOptions<DummyCompressorOptions>()->option_str.c_str(),
      "str2");
  ASSERT_EQ(compressor2->GetOptions<DummyCompressorOptions>()->option_int, 2);

  // Verify serialization of options
  std::string opts_serialized;
  s = GetStringFromColumnFamilyOptions(config_options, new_options,
                                       &opts_serialized);
  ASSERT_OK(s);
  ASSERT_TRUE(
      opts_serialized.find(
          "compressor_per_level={{id=NoCompression;parallel_threads=1;}:" +
          opts_str1 + ":" + opts_str2 + "}") != std::string::npos);
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

TEST(Compression, DBWithSimpleRLECompressor) {
  Options options;
  std::string dbname = test::PerThreadDBPath("compression_test");
  ASSERT_OK(DestroyDB(dbname, options));

  // Select SimpleRLECompressor through options.compressor
  options.create_if_missing = true;
  ConfigOptions config_options;
  auto library =
      config_options.registry->AddLibrary("DBWithSimpleRLECompressor");
  AddCompressorFactory<SimpleRLECompressor>(library);
  Status s = Compressor::CreateFromString(
      config_options, SimpleRLECompressor::kClassName(), &options.compressor);
  ASSERT_OK(s);
  ASSERT_NE(options.compressor, nullptr);
  SimpleRLECompressor* compressor =
      reinterpret_cast<SimpleRLECompressor*>(options.compressor.get());
  unsigned int compression_type = options.compressor->GetCompressionType();

  // Open database
  DB* db = nullptr;
  s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_NE(db, nullptr);
  ASSERT_EQ(compressor->num_compress_calls, 0);
  ASSERT_EQ(compressor->num_uncompress_calls, 0);

  // Write 200 values, each 20 bytes
  int num_keys = 200;
  std::string val = "aaaaaaaaaabbbbbbbbbb";
  WriteDBAndFlush(db, num_keys, val);
  ASSERT_EQ(compressor->num_compress_calls, 3);

  // Read and verify
  ReadOptions ro;
  std::string result;
  for (int i = 0; i < num_keys; i++) {
    std::string key = std::to_string(i);
    s = db->Get(ro, key, &result);
    ASSERT_OK(s);
    ASSERT_EQ(result, val);
  }
  // Index block not compressed, as not passing GoodCompressionRatio test
  ASSERT_EQ(compressor->num_uncompress_calls, 2);

  // Verify options file
  DBOptions db_options;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  s = LoadLatestOptions(config_options, db->GetName(), &db_options, &cf_descs);
  ASSERT_OK(s);
  if (BuiltinCompressor::TypeSupported(kSnappyCompression)) {
    ASSERT_EQ(cf_descs[0].options.compression, kSnappyCompression);
  } else {
    ASSERT_EQ(cf_descs[0].options.compression, kNoCompression);
  }
  ASSERT_EQ(cf_descs[0].options.compressor->GetCompressionType(),
            compression_type);

  CloseDB(db);

  // Reopen database
  compressor->num_compress_calls = 0;
  compressor->num_uncompress_calls = 0;
  db = nullptr;
  Options reopen_options;
  s = DB::Open(reopen_options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_NE(db, nullptr);

  // Verify table properties
  TablePropertiesCollection all_tables_props;
  s = db->GetPropertiesOfAllTables(&all_tables_props);
  ASSERT_OK(s);
  for (auto it = all_tables_props.begin(); it != all_tables_props.end(); ++it) {
    ASSERT_EQ(it->second->compression_name, std::string(compressor->Name()));
  }

  // Read and verify
  for (int i = 0; i < num_keys; i++) {
    std::string key = std::to_string(i);
    s = db->Get(ro, key, &result);
    ASSERT_OK(s);
    ASSERT_EQ(result, val);
  }
  ASSERT_EQ(compressor->num_compress_calls, 0);
  ASSERT_EQ(compressor->num_uncompress_calls, 2);

  CloseDB(db);
  ASSERT_OK(DestroyDB(dbname, options));
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
  ASSERT_EQ(cf_descs[0].options.compressor, nullptr);

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

  // Verify options file
  DBOptions db_options;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  ConfigOptions config_options;
  s = LoadLatestOptions(config_options, db->GetName(), &db_options, &cf_descs);
  ASSERT_OK(s);
  ASSERT_EQ(cf_descs[0].options.compression_per_level.size(), 2);
  ASSERT_EQ(cf_descs[0].options.compression_per_level[0], kNoCompression);
  ASSERT_EQ(cf_descs[0].options.compression_per_level[1], kSnappyCompression);
  ASSERT_EQ(cf_descs[0].options.compressor_per_level.size(), 0);

  CloseDB(db);

  // Test an invalid selection for compression_per_level
  options.compression_per_level.push_back(static_cast<CompressionType>(254));
  s = DB::Open(options, dbname, &db);
  ASSERT_NOK(s);
  ASSERT_EQ(s.ToString(), "Invalid argument: Compression type is invalid.");

  ASSERT_OK(DestroyDB(dbname, options));
}

TEST(Compression, DBwithZlibAndCompressorOptions) {
  if (!ZlibCompressor().Supported()) {
    return;
  }

  Options options;
  std::string dbname = test::PerThreadDBPath("compression_test");
  ASSERT_OK(DestroyDB(dbname, options));

  // Select Zlib and its options through options.compressor
  options.create_if_missing = true;
  Status s;
  ConfigOptions config_options;
  s = Compressor::CreateFromString(
      config_options,
      "id=" + std::string(ZlibCompressor::kClassName()) + ";window_bits=-13",
      &options.compressor);
  ASSERT_OK(s);
  ASSERT_NE(options.compressor, nullptr);

  // Open database
  DB* db = nullptr;
  s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_NE(db, nullptr);

  // Write 200 values, each 20 bytes
  WriteDBAndFlush(db, 200, "aaaaaaaaaabbbbbbbbbb");

  CompressionType compression_type = options.compressor->GetCompressionType();

  // Verify ZlibCompressor created with selected options
  auto compressor = BuiltinCompressor::GetCompressor(compression_type);
  ASSERT_NE(compressor, nullptr);
  std::string value;
  s = compressor->GetOption(config_options, "window_bits", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "-13");

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
  s = LoadLatestOptions(config_options, db->GetName(), &db_options, &cf_descs);
  ASSERT_OK(s);
  // compression and compression_opts are still default
  ASSERT_EQ(cf_descs[0].options.compression, kSnappyCompression);
  ASSERT_EQ(cf_descs[0].options.compression_opts.window_bits, -14);
  ASSERT_STREQ(cf_descs[0].options.compressor->ToString(config_options).c_str(),
               "{id=Zlib;parallel_threads=1;use_zstd_dict_trainer=true;max_"
               "dict_buffer_bytes=0;max_train_bytes=0;max_dict_bytes=0;level="
               "32767;window_bits=-13;strategy=0;}");

  CloseDB(db);

  // Reopen database
  db = nullptr;
  Options reopen_options;
  s = DB::Open(reopen_options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_NE(db, nullptr);

  // Verify table properties
  s = db->GetPropertiesOfAllTables(&all_tables_props);
  ASSERT_OK(s);
  for (auto it = all_tables_props.begin(); it != all_tables_props.end(); ++it) {
    ASSERT_EQ(it->second->compression_name,
              BuiltinCompressor::TypeToString(kZlibCompression));
  }

  CloseDB(db);
  ASSERT_OK(DestroyDB(dbname, options));
}

TEST(Compression, DBWithCompressorPerLevel) {
  if (!BuiltinCompressor::TypeSupported(kSnappyCompression)) {
    ROCKSDB_GTEST_BYPASS("Test requires Snappy compression");
    return;
  }

  Options options;
  std::string dbname = test::PerThreadDBPath("compression_test");
  ASSERT_OK(DestroyDB(dbname, options));

  auto no_compressor = BuiltinCompressor::GetCompressor(kNoCompression);
  auto snappy_compressor = BuiltinCompressor::GetCompressor(kSnappyCompression);

  options.create_if_missing = true;
  options.compressor_per_level.push_back(no_compressor);
  options.compressor_per_level.push_back(snappy_compressor);

  DB* db = nullptr;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_NE(db, nullptr);

  // Verify options file
  DBOptions db_options;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  ConfigOptions config_options;
  s = LoadLatestOptions(config_options, db->GetName(), &db_options, &cf_descs);
  ASSERT_OK(s);
  ASSERT_EQ(cf_descs[0].options.compression_per_level.size(), 0);
  ASSERT_EQ(cf_descs[0].options.compressor_per_level.size(), 2);
  ASSERT_STREQ(cf_descs[0]
                   .options.compressor_per_level[0]
                   ->ToString(config_options)
                   .c_str(),
               "{id=NoCompression;parallel_threads=1;}");
  ASSERT_STREQ(cf_descs[0]
                   .options.compressor_per_level[1]
                   ->ToString(config_options)
                   .c_str(),
               "{id=Snappy;parallel_threads=1;}");

  CloseDB(db);
  ASSERT_OK(DestroyDB(dbname, options));
}

TEST(Compression, DBWithDictionaryCompression) {
  Options options;
  std::string dbname = test::PerThreadDBPath("compression_test");
  ASSERT_OK(DestroyDB(dbname, options));

  // Select compressor that supports dictionary compression
  options.create_if_missing = true;
  ConfigOptions config_options;
  // If the factory is not added to the default library, DB::Open fails as
  // unable to persist options.
  AddCompressorFactory<DummyDictionaryCompressor>(ObjectLibrary::Default());
  Status s = Compressor::CreateFromString(
      config_options,
      "id=" + std::string(DummyDictionaryCompressor::kClassName()) +
          ";max_dict_bytes=4096;",
      &options.compressor);
  ASSERT_OK(s);
  ASSERT_NE(options.compressor, nullptr);
  DummyDictionaryCompressor* compressor =
      reinterpret_cast<DummyDictionaryCompressor*>(options.compressor.get());

  // Open database
  DB* db = nullptr;
  s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_NE(db, nullptr);

  // Write 2000 values, each 20 bytes
  int num_keys = 2000;
  std::string val = "aaaaaaaaaabbbbbbbbbb";
  WriteDBAndFlush(db, num_keys, val);

  // Dictionary was enabled
  ASSERT_EQ(compressor->num_create_dictionary_calls, 1);

  CloseDB(db);
  ASSERT_OK(DestroyDB(dbname, options));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
