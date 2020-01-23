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
#include "rocksdb/sst_file_checksum.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/sst_file_checksum_helper.h"

using std::string;
using std::vector;
using std::map;

namespace rocksdb {

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
    auto actual = rocksdb::LDBCommand::HexToString(inPair.first);
    auto expected = inPair.second;
    for (unsigned int i = 0; i < actual.length(); i++) {
      EXPECT_EQ(expected[i], static_cast<int>((signed char) actual[i]));
    }
    auto reverse = rocksdb::LDBCommand::StringToHex(actual);
    EXPECT_STRCASEEQ(inPair.first.c_str(), reverse.c_str());
  }
}

TEST_F(LdbCmdTest, HexToStringBadInputs) {
  const vector<string> badInputs = {
      "0xZZ", "123", "0xx5", "0x111G", "0x123", "Ox12", "0xT", "0x1Q1",
  };
  for (const auto badInput : badInputs) {
    try {
      rocksdb::LDBCommand::HexToString(badInput);
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

  opts.file_system.reset(new LegacyFileSystemWrapper(opts.env));

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

class FileChecksumTestHelper {
 private:
  Options options_;
  DB* db_;
  std::string dbname_;

  Status VerifyChecksum(LiveFileMetaData& file_meta,
                        FileChecksumList* checksum_list) {
    uint32_t cur_checksum = 0;
    std::string checksum_func_name;

    Status s;
    EnvOptions soptions;
    std::unique_ptr<SequentialFile> file_reader;
    std::string file_path = dbname_ + "/" + file_meta.name;
    s = options_.env->NewSequentialFile(file_path, &file_reader, soptions);
    if (!s.ok()) {
      return s;
    }
    char* scratch = nullptr;
    scratch = new char[2048];
    bool first_read = true;
    Slice result;
    SstFileChecksumFunc* file_checksum_func =
        options_.sst_file_checksum_func.get();
    if (file_checksum_func == nullptr) {
      cur_checksum = 0;
      checksum_func_name = kUnknownFileChecksumFuncName;
    } else {
      checksum_func_name = file_checksum_func->Name();
      s = file_reader->Read(2048, &result, scratch);
      if (!s.ok()) {
        delete[] scratch;
        return s;
      }
      while (result.size() != 0) {
        if (first_read) {
          first_read = false;
          cur_checksum = file_checksum_func->Value(scratch, result.size());
        } else {
          cur_checksum =
              file_checksum_func->Extend(cur_checksum, scratch, result.size());
        }
        s = file_reader->Read(2048, &result, scratch);
        if (!s.ok()) {
          delete[] scratch;
          return s;
        }
      }
    }
    delete[] scratch;

    uint32_t stored_checksum = 0;
    std::string stored_checksum_func_name = "";
    s = checksum_list->SearchOneFileChecksum(
        file_meta.file_number, &stored_checksum, &stored_checksum_func_name);
    if (!s.ok()) {
      return Status::Corruption("the file: " + file_meta.name +
                                " is not in the checksum map!");
    }
    if ((cur_checksum != stored_checksum) ||
        (checksum_func_name != stored_checksum_func_name)) {
      return Status::Corruption(
          "Checksum does not match! The file: " + file_meta.name +
          ", checksum name: " + stored_checksum_func_name + " and checksum " +
          ToString(stored_checksum) + ". However, expected checksum name: " +
          checksum_func_name + " and checksum " + ToString(cur_checksum));
    }
    return Status::OK();
  }

 public:
  FileChecksumTestHelper(Options& options, DB* db, std::string db_name)
      : options_(options), db_(db), dbname_(db_name) {}
  ~FileChecksumTestHelper() {}

  Status VerifyEachFileChecksum() {
    std::string manifestfile;
    if (db_ == nullptr) {
      return Status::Corruption("db is nullptr!");
    }

    // Step 1: verify if MANIFEST is there and dbname_ is correct
    // get the absolute path of MANIFEST
    std::vector<std::string> files;
    Status s = options_.env->GetChildren(dbname_, &files);
    if (!s.ok()) {
      return s;
    }
    const std::string kManifestNamePrefix = "MANIFEST-";
    std::string matched_file;

#ifdef OS_WIN
    const char kPathDelim = '\\';
#else
    const char kPathDelim = '/';
#endif
    for (const auto& file_path : files) {
      size_t pos = file_path.find_last_of(kPathDelim);
      if (pos == file_path.size() - 1) {
        continue;
      }
      std::string fname;
      if (pos != std::string::npos) {
        fname.assign(file_path, pos + 1, file_path.size() - pos - 1);
      } else {
        fname = file_path;
      }
      uint64_t file_num = 0;
      FileType file_type = kLogFile;  // Just for initialization
      if (ParseFileName(fname, &file_num, &file_type) &&
          file_type == kDescriptorFile) {
        if (!matched_file.empty()) {
          return Status::Corruption("Multiple MANIFEST files found");
        } else {
          matched_file.swap(fname);
        }
      }
    }
    if (matched_file.empty()) {
      return Status::Corruption("No MANIFEST found in " + dbname_);
    }
    if (dbname_[dbname_.length() - 1] != '/') {
      dbname_.append("/");
    }
    manifestfile = dbname_ + matched_file;

    // Step 2, get the list of sst file checksum in checksum_info
    std::unique_ptr<FileChecksumList> checksum_list(NewFileChecksumList());

    EnvOptions sopt;
    std::shared_ptr<Cache> tc(NewLRUCache(options_.max_open_files - 10,
                                          options_.table_cache_numshardbits));
    WriteController wc(options_.delayed_write_rate);
    WriteBufferManager wb(options_.db_write_buffer_size);
    ImmutableDBOptions immutable_db_options(options_);
    VersionSet versions(dbname_, &immutable_db_options, sopt, tc.get(), &wb,
                        &wc,
                        /*block_cache_tracer=*/nullptr);
    s = versions.GetAllFileCheckSumInfo(options_, manifestfile,
                                        checksum_list.get());
    if (!s.ok()) {
      return s;
    }

    // Step 3, get the live file list and verify the checksum
    if (checksum_list == nullptr) {
      return Status::Corruption(
          "Checksum_list construction failed or being deleted!");
    }
    std::vector<LiveFileMetaData> live_file;
    db_->GetLiveFilesMetaData(&live_file);
    if (live_file.size() != checksum_list->size()) {
      return Status::Corruption(
          "Live file number does not match checksum list file number!");
    }
    for (auto a_file : live_file) {
      Status cs = VerifyChecksum(a_file, checksum_list.get());
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
  opts.file_system.reset(new LegacyFileSystemWrapper(opts.env));

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

  delete db;
}

TEST_F(LdbCmdTest, DumpFileChecksumCRC32) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;
  opts.sst_file_checksum_func = std::make_shared<SstFileChecksumCrc32c>();
  opts.file_system.reset(new LegacyFileSystemWrapper(opts.env));

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

  delete db;
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
    LDBCommand* command = rocksdb::LDBCommand::InitFromCmdLineArgs(
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
    LDBCommand* command = rocksdb::LDBCommand::InitFromCmdLineArgs(
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

    rocksdb::SyncPoint::GetInstance()->SetCallBack(
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
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();

    ASSERT_EQ(
        0, LDBCommandRunner::RunCommand(3, argv, opts, LDBOptions(), nullptr));

    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  }

  // Test the case of limiting tombstones
  {
    char arg1[] = "./ldb";
    char arg2[1024];
    snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
    char arg3[] = "list_file_range_deletes";
    char arg4[] = "--max_keys=1";
    char* argv[] = {arg1, arg2, arg3, arg4};

    rocksdb::SyncPoint::GetInstance()->SetCallBack(
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
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();

    ASSERT_EQ(
        0, LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));

    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  }
}
} // namespace rocksdb

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
  fprintf(stderr, "SKIPPED as LDBCommand is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE
