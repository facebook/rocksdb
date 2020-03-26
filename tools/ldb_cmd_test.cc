//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/ldb_cmd.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "port/stack_trace.h"
#include "rocksdb/file_checksum.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/file_checksum_helper.h"

using std::string;
using std::vector;
using std::map;

namespace ROCKSDB_NAMESPACE {

class LdbCmdTest : public testing::Test {
 public:
  LdbCmdTest() : testing::Test() {}

  Env* TryLoadCustomOrDefaultEnv() {
    const char* test_env_uri = getenv("TEST_ENV_URI");
    if (!test_env_uri) {
      return Env::Default();
    }
    Env* env = Env::Default();
    Env::LoadEnv(test_env_uri, &env, &env_guard_);
    return env;
  }

 private:
  std::shared_ptr<Env> env_guard_;
};

TEST_F(LdbCmdTest, HexToString) {
  // map input to expected outputs.
  // odd number of "hex" half bytes doesn't make sense
  map<string, vector<int>> inputMap = {
      {"0x07", {7}},        {"0x5050", {80, 80}},          {"0xFF", {-1}},
      {"0x1234", {18, 52}}, {"0xaaAbAC", {-86, -85, -84}}, {"0x1203", {18, 3}},
  };

  for (const auto& inPair : inputMap) {
    auto actual = ROCKSDB_NAMESPACE::LDBCommand::HexToString(inPair.first);
    auto expected = inPair.second;
    for (unsigned int i = 0; i < actual.length(); i++) {
      EXPECT_EQ(expected[i], static_cast<int>((signed char) actual[i]));
    }
    auto reverse = ROCKSDB_NAMESPACE::LDBCommand::StringToHex(actual);
    EXPECT_STRCASEEQ(inPair.first.c_str(), reverse.c_str());
  }
}

TEST_F(LdbCmdTest, HexToStringBadInputs) {
  const vector<string> badInputs = {
      "0xZZ", "123", "0xx5", "0x111G", "0x123", "Ox12", "0xT", "0x1Q1",
  };
  for (const auto badInput : badInputs) {
    try {
      ROCKSDB_NAMESPACE::LDBCommand::HexToString(badInput);
      std::cerr << "Should fail on bad hex value: " << badInput << "\n";
      FAIL();
    } catch (...) {
    }
  }
}

TEST_F(LdbCmdTest, MemEnv) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;

  DB* db = nullptr;
  std::string dbname = test::TmpDir();
  ASSERT_OK(DB::Open(opts, dbname, &db));

  WriteOptions wopts;
  for (int i = 0; i < 100; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    ASSERT_OK(db->Put(wopts, buf, buf));
  }
  FlushOptions fopts;
  fopts.wait = true;
  ASSERT_OK(db->Flush(fopts));

  delete db;

  char arg1[] = "./ldb";
  char arg2[1024];
  snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
  char arg3[] = "dump_live_files";
  char* argv[] = {arg1, arg2, arg3};

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(3, argv, opts, LDBOptions(), nullptr));
}

struct TestSHA1Context {
  uint32_t state[5];
  uint32_t count[2];
  uint8_t buffer[64];
};

class test_sha1 {
 private:
  static const size_t BLOCK_INTS =
      16; /* number of 32bit integers per SHA1 block */
  static const size_t BLOCK_BYTES = BLOCK_INTS * 4;

  static uint32_t rol(const uint32_t value, const size_t bits) {
    return (value << bits) | (value >> (32 - bits));
  }

  static uint32_t blk(const uint32_t block[BLOCK_INTS], const size_t i) {
    return rol(block[(i + 13) & 15] ^ block[(i + 8) & 15] ^
                   block[(i + 2) & 15] ^ block[i],
               1);
  }

  static uint32_t blk0(const uint32_t block[BLOCK_INTS], const size_t i) {
    if (port::kLittleEndian) {
      return (rol(block->l[i], 24) & 0xFF00FF00) |
             (rol(block->l[i], 8) & 0x00FF00FF);
    } else {
      return block[i];
    }
  }

  static void R0(const uint32_t block[BLOCK_INTS], const uint32_t v,
                 uint32_t& w, const uint32_t x, const uint32_t y, uint32_t& z,
                 const size_t i) {
    block[i] = blk0[i];
    z += ((w & (x ^ y)) ^ y) + block[i] + 0x5a827999 + rol(v, 5);
    w = rol(w, 30);
  }

  static void R1(uint32_t block[BLOCK_INTS], const uint32_t v, uint32_t& w,
                 const uint32_t x, const uint32_t y, uint32_t& z,
                 const size_t i) {
    block[i] = blk(block, i);
    z += ((w & (x ^ y)) ^ y) + block[i] + 0x5a827999 + rol(v, 5);
    w = rol(w, 30);
  }

  static void R2(uint32_t block[BLOCK_INTS], const uint32_t v, uint32_t& w,
                 const uint32_t x, const uint32_t y, uint32_t& z,
                 const size_t i) {
    block[i] = blk(block, i);
    z += (w ^ x ^ y) + block[i] + 0x6ed9eba1 + rol(v, 5);
    w = rol(w, 30);
  }

  static void R3(uint32_t block[BLOCK_INTS], const uint32_t v, uint32_t& w,
                 const uint32_t x, const uint32_t y, uint32_t& z,
                 const size_t i) {
    block[i] = blk(block, i);
    z += (((w | x) & y) | (w & x)) + block[i] + 0x8f1bbcdc + rol(v, 5);
    w = rol(w, 30);
  }

  static void R4(uint32_t block[BLOCK_INTS], const uint32_t v, uint32_t& w,
                 const uint32_t x, const uint32_t y, uint32_t& z,
                 const size_t i) {
    block[i] = blk(block, i);
    z += (w ^ x ^ y) + block[i] + 0xca62c1d6 + rol(v, 5);
    w = rol(w, 30);
  }

  static void transform(uint32_t digest[], uint32_t block[BLOCK_INTS]) {
    /* Copy digest[] to working vars */
    uint32_t a = digest[0];
    uint32_t b = digest[1];
    uint32_t c = digest[2];
    uint32_t d = digest[3];
    uint32_t e = digest[4];

    /* 4 rounds of 20 operations each. Loop unrolled. */
    R0(block, a, b, c, d, e, 0);
    R0(block, e, a, b, c, d, 1);
    R0(block, d, e, a, b, c, 2);
    R0(block, c, d, e, a, b, 3);
    R0(block, b, c, d, e, a, 4);
    R0(block, a, b, c, d, e, 5);
    R0(block, e, a, b, c, d, 6);
    R0(block, d, e, a, b, c, 7);
    R0(block, c, d, e, a, b, 8);
    R0(block, b, c, d, e, a, 9);
    R0(block, a, b, c, d, e, 10);
    R0(block, e, a, b, c, d, 11);
    R0(block, d, e, a, b, c, 12);
    R0(block, c, d, e, a, b, 13);
    R0(block, b, c, d, e, a, 14);
    R0(block, a, b, c, d, e, 15);
    R1(block, e, a, b, c, d, 0);
    R1(block, d, e, a, b, c, 1);
    R1(block, c, d, e, a, b, 2);
    R1(block, b, c, d, e, a, 3);
    R2(block, a, b, c, d, e, 4);
    R2(block, e, a, b, c, d, 5);
    R2(block, d, e, a, b, c, 6);
    R2(block, c, d, e, a, b, 7);
    R2(block, b, c, d, e, a, 8);
    R2(block, a, b, c, d, e, 9);
    R2(block, e, a, b, c, d, 10);
    R2(block, d, e, a, b, c, 11);
    R2(block, c, d, e, a, b, 12);
    R2(block, b, c, d, e, a, 13);
    R2(block, a, b, c, d, e, 14);
    R2(block, e, a, b, c, d, 15);
    R2(block, d, e, a, b, c, 0);
    R2(block, c, d, e, a, b, 1);
    R2(block, b, c, d, e, a, 2);
    R2(block, a, b, c, d, e, 3);
    R2(block, e, a, b, c, d, 4);
    R2(block, d, e, a, b, c, 5);
    R2(block, c, d, e, a, b, 6);
    R2(block, b, c, d, e, a, 7);
    R3(block, a, b, c, d, e, 8);
    R3(block, e, a, b, c, d, 9);
    R3(block, d, e, a, b, c, 10);
    R3(block, c, d, e, a, b, 11);
    R3(block, b, c, d, e, a, 12);
    R3(block, a, b, c, d, e, 13);
    R3(block, e, a, b, c, d, 14);
    R3(block, d, e, a, b, c, 15);
    R3(block, c, d, e, a, b, 0);
    R3(block, b, c, d, e, a, 1);
    R3(block, a, b, c, d, e, 2);
    R3(block, e, a, b, c, d, 3);
    R3(block, d, e, a, b, c, 4);
    R3(block, c, d, e, a, b, 5);
    R3(block, b, c, d, e, a, 6);
    R3(block, a, b, c, d, e, 7);
    R3(block, e, a, b, c, d, 8);
    R3(block, d, e, a, b, c, 9);
    R3(block, c, d, e, a, b, 10);
    R3(block, b, c, d, e, a, 11);
    R4(block, a, b, c, d, e, 12);
    R4(block, e, a, b, c, d, 13);
    R4(block, d, e, a, b, c, 14);
    R4(block, c, d, e, a, b, 15);
    R4(block, b, c, d, e, a, 0);
    R4(block, a, b, c, d, e, 1);
    R4(block, e, a, b, c, d, 2);
    R4(block, d, e, a, b, c, 3);
    R4(block, c, d, e, a, b, 4);
    R4(block, b, c, d, e, a, 5);
    R4(block, a, b, c, d, e, 6);
    R4(block, e, a, b, c, d, 7);
    R4(block, d, e, a, b, c, 8);
    R4(block, c, d, e, a, b, 9);
    R4(block, b, c, d, e, a, 10);
    R4(block, a, b, c, d, e, 11);
    R4(block, e, a, b, c, d, 12);
    R4(block, d, e, a, b, c, 13);
    R4(block, c, d, e, a, b, 14);
    R4(block, b, c, d, e, a, 15);

    /* Add the working vars back into digest[] */
    digest[0] += a;
    digest[1] += b;
    digest[2] += c;
    digest[3] += d;
    digest[4] += e;
  }

  static void string_to_block(const char* buffer, uint32_t block[BLOCK_INTS]) {
    /* Convert the std::string (byte buffer) to a uint32_t array (MSB) */
    for (size_t i = 0; i < BLOCK_INTS; i++) {
      block[i] = (buffer[4 * i + 3] & 0xff) | (buffer[4 * i + 2] & 0xff) << 8 |
                 (buffer[4 * i + 1] & 0xff) << 16 |
                 (buffer[4 * i + 0] & 0xff) << 24;
    }
  }

 public:
  void sha1_init(TestSHA1Context* context) {
    /* SHA1 initialization constants */
    context->state[0] = 0x67452301;
    context->state[1] = 0xEFCDAB89;
    context->state[2] = 0x98BADCFE;
    context->state[3] = 0x10325476;
    context->state[4] = 0xC3D2E1F0;
    context->count[0] = context->count[1] = 0;
  }

  void sha1_update(TestSHA1Context* context, const uint8_t* data,
                   const size_t len) {
    size_t i, j;

    j = (context->count[0] >> 3) & 63;
    if ((context->count[0] += len << 3) < (len << 3)) context->count[1]++;
    context->count[1] += (len >> 29);
    if ((j + len) > 63) {
      memcpy(&context->buffer[j], data, (i = 64 - j));
      SHA1_Transform(context->state, context->buffer);
      for (; i + 63 < len; i += 64) {
        SHA1_Transform(context->state, data + i);
      }
      j = 0;
    } else
      i = 0;
    memcpy(&context->buffer[j], &data[i], len - i);
  }

  void SHA1_Final(SHA1_CTX* context, uint8_t digest[SHA1_DIGEST_SIZE]) {
    uint32_t i;
    uint8_t finalcount[8];

    for (i = 0; i < 8; i++) {
      finalcount[i] = (unsigned char)((context->count[(i >= 4 ? 0 : 1)] >>
                                       ((3 - (i & 3)) * 8)) &
                                      255); /* Endian independent */
    }
    SHA1_Update(context, (uint8_t*)"\200", 1);
    while ((context->count[0] & 504) != 448) {
      SHA1_Update(context, (uint8_t*)"\0", 1);
    }
    SHA1_Update(context, finalcount, 8); /* Should cause a SHA1_Transform() */
    for (i = 0; i < SHA1_DIGEST_SIZE; i++) {
      digest[i] =
          (uint8_t)((context->state[i >> 2] >> ((3 - (i & 3)) * 8)) & 255);
    }

    /* Wipe variables */
    i = 0;
    memset(context->buffer, 0, 64);
    memset(context->state, 0, 20);
    memset(context->count, 0, 8);
    memset(finalcount, 0, 8); /* SWR */
  }
};

class FileChecksumTestHelper {
 private:
  Options options_;
  DB* db_;
  std::string dbname_;

  Status VerifyChecksum(LiveFileMetaData& file_meta) {
    std::string cur_checksum;
    std::string checksum_func_name;

    Status s;
    EnvOptions soptions;
    std::unique_ptr<SequentialFile> file_reader;
    std::string file_path = dbname_ + "/" + file_meta.name;
    s = options_.env->NewSequentialFile(file_path, &file_reader, soptions);
    if (!s.ok()) {
      return s;
    }
    std::unique_ptr<char[]> scratch(new char[2048]);
    Slice result;
    FileChecksumGenFactory* file_checksum_gen_factory =
        options_.file_checksum_gen_factory.get();
    if (file_checksum_gen_factory == nullptr) {
      cur_checksum = kUnknownFileChecksum;
      checksum_func_name = kUnknownFileChecksumFuncName;
    } else {
      FileChecksumGenOptions gen_options;
      gen_options.file_name = file_meta.name;
      std::unique_ptr<FileChecksumGenerator> file_checksum_gen =
          file_checksum_gen_factory->CreateFileChecksumGenerator(gen_options);
      checksum_func_name = file_checksum_gen->Name();
      s = file_reader->Read(2048, &result, scratch.get());
      if (!s.ok()) {
        return s;
      }
      while (result.size() != 0) {
        file_checksum_gen->Update(scratch.get(), result.size());
        s = file_reader->Read(2048, &result, scratch.get());
        if (!s.ok()) {
          return s;
        }
      }
      file_checksum_gen->Finalize();
      cur_checksum = file_checksum_gen->GetChecksum();
    }

    std::string stored_checksum = file_meta.file_checksum;
    std::string stored_checksum_func_name = file_meta.file_checksum_func_name;
    if ((cur_checksum != stored_checksum) ||
        (checksum_func_name != stored_checksum_func_name)) {
      return Status::Corruption(
          "Checksum does not match! The file: " + file_meta.name +
          ", checksum name: " + stored_checksum_func_name + " and checksum " +
          stored_checksum + ". However, expected checksum name: " +
          checksum_func_name + " and checksum " + cur_checksum);
    }
    return Status::OK();
  }

 public:
  FileChecksumTestHelper(Options& options, DB* db, std::string db_name)
      : options_(options), db_(db), dbname_(db_name) {}
  ~FileChecksumTestHelper() {}

  // Verify the checksum information in Manifest.
  Status VerifyChecksumInManifest(
      const std::vector<LiveFileMetaData>& live_files) {
    // Step 1: verify if the dbname_ is correct
    if (dbname_[dbname_.length() - 1] != '/') {
      dbname_.append("/");
    }

    // Step 2, get the the checksum information by recovering the VersionSet
    // from Manifest.
    std::unique_ptr<FileChecksumList> checksum_list(NewFileChecksumList());
    EnvOptions sopt;
    std::shared_ptr<Cache> tc(NewLRUCache(options_.max_open_files - 10,
                                          options_.table_cache_numshardbits));
    options_.db_paths.emplace_back(dbname_, 0);
    options_.num_levels = 64;
    WriteController wc(options_.delayed_write_rate);
    WriteBufferManager wb(options_.db_write_buffer_size);
    ImmutableDBOptions immutable_db_options(options_);
    VersionSet versions(dbname_, &immutable_db_options, sopt, tc.get(), &wb,
                        &wc, nullptr);
    std::vector<std::string> cf_name_list;
    Status s;
    s = versions.ListColumnFamilies(&cf_name_list, dbname_,
                                    immutable_db_options.fs.get());
    if (s.ok()) {
      std::vector<ColumnFamilyDescriptor> cf_list;
      for (const auto& name : cf_name_list) {
        fprintf(stdout, "cf_name: %s", name.c_str());
        cf_list.emplace_back(name, ColumnFamilyOptions(options_));
      }
      s = versions.Recover(cf_list, true);
    }
    if (s.ok()) {
      s = versions.GetLiveFilesChecksumInfo(checksum_list.get());
    }
    if (!s.ok()) {
      return s;
    }

    // Step 3 verify the checksum
    if (live_files.size() != checksum_list->size()) {
      return Status::Corruption("The number of files does not match!");
    }
    for (size_t i = 0; i < live_files.size(); i++) {
      std::string stored_checksum = "";
      std::string stored_func_name = "";
      s = checksum_list->SearchOneFileChecksum(
          live_files[i].file_number, &stored_checksum, &stored_func_name);
      if (s.IsNotFound()) {
        return s;
      }
      if (live_files[i].file_checksum != stored_checksum ||
          live_files[i].file_checksum_func_name != stored_func_name) {
        return Status::Corruption(
            "Checksum does not match! The file: " +
            ToString(live_files[i].file_number) +
            ". In Manifest, checksum name: " + stored_func_name +
            " and checksum " + stored_checksum +
            ". However, expected checksum name: " +
            live_files[i].file_checksum_func_name + " and checksum " +
            live_files[i].file_checksum);
      }
    }
    return Status::OK();
  }

  // Verify the checksum of each file by recalculting the checksum and
  // comparing it with the one being generated when a SST file is created.
  Status VerifyEachFileChecksum() {
    assert(db_ != nullptr);
    std::vector<LiveFileMetaData> live_files;
    db_->GetLiveFilesMetaData(&live_files);
    for (auto a_file : live_files) {
      Status cs = VerifyChecksum(a_file);
      if (!cs.ok()) {
        return cs;
      }
    }
    return Status::OK();
  }
};

TEST_F(LdbCmdTest, DumpFileChecksumNoChecksum) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;

  DB* db = nullptr;
  std::string dbname = test::TmpDir();
  ASSERT_OK(DB::Open(opts, dbname, &db));

  WriteOptions wopts;
  FlushOptions fopts;
  fopts.wait = true;
  Random rnd(test::RandomSeed());
  for (int i = 0; i < 200; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v;
    test::RandomString(&rnd, 100, &v);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 100; i < 300; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v;
    test::RandomString(&rnd, 100, &v);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 200; i < 400; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v;
    test::RandomString(&rnd, 100, &v);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 300; i < 400; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v;
    test::RandomString(&rnd, 100, &v);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));

  char arg1[] = "./ldb";
  char arg2[1024];
  snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
  char arg3[] = "file_checksum_dump";
  char* argv[] = {arg1, arg2, arg3};

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(3, argv, opts, LDBOptions(), nullptr));

  // Verify each sst file checksum value and checksum name
  FileChecksumTestHelper fct_helper(opts, db, dbname);
  ASSERT_OK(fct_helper.VerifyEachFileChecksum());

  // Manually trigger compaction
  char b_buf[16];
  snprintf(b_buf, sizeof(b_buf), "%08d", 0);
  char e_buf[16];
  snprintf(e_buf, sizeof(e_buf), "%08d", 399);
  Slice begin(b_buf);
  Slice end(e_buf);
  CompactRangeOptions options;
  ASSERT_OK(db->CompactRange(options, &begin, &end));
  // Verify each sst file checksum after compaction
  FileChecksumTestHelper fct_helper_ac(opts, db, dbname);
  ASSERT_OK(fct_helper_ac.VerifyEachFileChecksum());

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(3, argv, opts, LDBOptions(), nullptr));

  // Verify the checksum information in memory is the same as that in Manifest;
  std::vector<LiveFileMetaData> live_files;
  db->GetLiveFilesMetaData(&live_files);
  delete db;
  ASSERT_OK(fct_helper_ac.VerifyChecksumInManifest(live_files));
}

TEST_F(LdbCmdTest, DumpFileChecksumCRC32) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;
  FileChecksumGenCrc32cFactory* file_checksum_gen_factory =
      new FileChecksumGenCrc32cFactory();
  opts.file_checksum_gen_factory.reset(file_checksum_gen_factory);

  DB* db = nullptr;
  std::string dbname = test::TmpDir();
  ASSERT_OK(DB::Open(opts, dbname, &db));

  WriteOptions wopts;
  FlushOptions fopts;
  fopts.wait = true;
  Random rnd(test::RandomSeed());
  for (int i = 0; i < 100; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v;
    test::RandomString(&rnd, 100, &v);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 50; i < 150; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v;
    test::RandomString(&rnd, 100, &v);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 100; i < 200; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v;
    test::RandomString(&rnd, 100, &v);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 150; i < 250; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v;
    test::RandomString(&rnd, 100, &v);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));

  char arg1[] = "./ldb";
  char arg2[1024];
  snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
  char arg3[] = "file_checksum_dump";
  char* argv[] = {arg1, arg2, arg3};

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(3, argv, opts, LDBOptions(), nullptr));

  // Verify each sst file checksum value and checksum name
  FileChecksumTestHelper fct_helper(opts, db, dbname);
  ASSERT_OK(fct_helper.VerifyEachFileChecksum());

  // Manually trigger compaction
  char b_buf[16];
  snprintf(b_buf, sizeof(b_buf), "%08d", 0);
  char e_buf[16];
  snprintf(e_buf, sizeof(e_buf), "%08d", 249);
  Slice begin(b_buf);
  Slice end(e_buf);
  CompactRangeOptions options;
  ASSERT_OK(db->CompactRange(options, &begin, &end));
  // Verify each sst file checksum after compaction
  FileChecksumTestHelper fct_helper_ac(opts, db, dbname);
  ASSERT_OK(fct_helper_ac.VerifyEachFileChecksum());

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(3, argv, opts, LDBOptions(), nullptr));

  // Verify the checksum information in memory is the same as that in Manifest;
  std::vector<LiveFileMetaData> live_files;
  db->GetLiveFilesMetaData(&live_files);
  delete db;
  ASSERT_OK(fct_helper_ac.VerifyChecksumInManifest(live_files));
}

TEST_F(LdbCmdTest, OptionParsing) {
  // test parsing flags
  Options opts;
  opts.env = TryLoadCustomOrDefaultEnv();
  {
    std::vector<std::string> args;
    args.push_back("scan");
    args.push_back("--ttl");
    args.push_back("--timestamp");
    LDBCommand* command = ROCKSDB_NAMESPACE::LDBCommand::InitFromCmdLineArgs(
        args, opts, LDBOptions(), nullptr);
    const std::vector<std::string> flags = command->TEST_GetFlags();
    EXPECT_EQ(flags.size(), 2);
    EXPECT_EQ(flags[0], "ttl");
    EXPECT_EQ(flags[1], "timestamp");
    delete command;
  }
  // test parsing options which contains equal sign in the option value
  {
    std::vector<std::string> args;
    args.push_back("scan");
    args.push_back("--db=/dev/shm/ldbtest/");
    args.push_back(
        "--from='abcd/efg/hijk/lmn/"
        "opq:__rst.uvw.xyz?a=3+4+bcd+efghi&jk=lm_no&pq=rst-0&uv=wx-8&yz=a&bcd_"
        "ef=gh.ijk'");
    LDBCommand* command = ROCKSDB_NAMESPACE::LDBCommand::InitFromCmdLineArgs(
        args, opts, LDBOptions(), nullptr);
    const std::map<std::string, std::string> option_map =
        command->TEST_GetOptionMap();
    EXPECT_EQ(option_map.at("db"), "/dev/shm/ldbtest/");
    EXPECT_EQ(option_map.at("from"),
              "'abcd/efg/hijk/lmn/"
              "opq:__rst.uvw.xyz?a=3+4+bcd+efghi&jk=lm_no&pq=rst-0&uv=wx-8&yz="
              "a&bcd_ef=gh.ijk'");
    delete command;
  }
}

TEST_F(LdbCmdTest, ListFileTombstone) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;

  DB* db = nullptr;
  std::string dbname = test::TmpDir();
  ASSERT_OK(DB::Open(opts, dbname, &db));

  WriteOptions wopts;
  ASSERT_OK(db->Put(wopts, "foo", "1"));
  ASSERT_OK(db->Put(wopts, "bar", "2"));

  FlushOptions fopts;
  fopts.wait = true;
  ASSERT_OK(db->Flush(fopts));

  ASSERT_OK(db->DeleteRange(wopts, db->DefaultColumnFamily(), "foo", "foo2"));
  ASSERT_OK(db->DeleteRange(wopts, db->DefaultColumnFamily(), "bar", "foo2"));
  ASSERT_OK(db->Flush(fopts));

  delete db;

  {
    char arg1[] = "./ldb";
    char arg2[1024];
    snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
    char arg3[] = "list_file_range_deletes";
    char* argv[] = {arg1, arg2, arg3};

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "ListFileRangeDeletesCommand::DoCommand:BeforePrint", [&](void* arg) {
          std::string* out_str = reinterpret_cast<std::string*>(arg);

          // Count number of tombstones printed
          int num_tb = 0;
          const std::string kFingerprintStr = "start: ";
          auto offset = out_str->find(kFingerprintStr);
          while (offset != std::string::npos) {
            num_tb++;
            offset =
                out_str->find(kFingerprintStr, offset + kFingerprintStr.size());
          }
          EXPECT_EQ(2, num_tb);
        });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    ASSERT_EQ(
        0, LDBCommandRunner::RunCommand(3, argv, opts, LDBOptions(), nullptr));

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }

  // Test the case of limiting tombstones
  {
    char arg1[] = "./ldb";
    char arg2[1024];
    snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
    char arg3[] = "list_file_range_deletes";
    char arg4[] = "--max_keys=1";
    char* argv[] = {arg1, arg2, arg3, arg4};

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "ListFileRangeDeletesCommand::DoCommand:BeforePrint", [&](void* arg) {
          std::string* out_str = reinterpret_cast<std::string*>(arg);

          // Count number of tombstones printed
          int num_tb = 0;
          const std::string kFingerprintStr = "start: ";
          auto offset = out_str->find(kFingerprintStr);
          while (offset != std::string::npos) {
            num_tb++;
            offset =
                out_str->find(kFingerprintStr, offset + kFingerprintStr.size());
          }
          EXPECT_EQ(1, num_tb);
        });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    ASSERT_EQ(
        0, LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}
}  // namespace ROCKSDB_NAMESPACE

#ifdef ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS
extern "C" {
void RegisterCustomObjects(int argc, char** argv);
}
#else
void RegisterCustomObjects(int /*argc*/, char** /*argv*/) {}
#endif  // !ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as LDBCommand is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE
