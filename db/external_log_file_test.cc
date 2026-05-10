//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/external_log_file.h"

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "db/db_test_util.h"
#include "db/external_log_file_edit.h"
#include "file/sst_file_manager_impl.h"
#include "port/stack_trace.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/checkpoint.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/file_checksum_helper.h"

namespace ROCKSDB_NAMESPACE {

class ExternalLogFileTest : public DBTestBase {
 public:
  ExternalLogFileTest()
      : DBTestBase("external_log_file_test", /*env_do_fsync=*/true) {}

  ~ExternalLogFileTest() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->LoadDependency({});
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  ExternalLogFileTest(const ExternalLogFileTest&) = delete;
  ExternalLogFileTest& operator=(const ExternalLogFileTest&) = delete;
  ExternalLogFileTest(ExternalLogFileTest&&) = delete;
  ExternalLogFileTest& operator=(ExternalLogFileTest&&) = delete;

  Options TestOptions() {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
    return options;
  }

  void Open() { DestroyAndReopen(TestOptions()); }

  void NewManager(std::unique_ptr<ExternalLogFileManager>* manager) {
    ASSERT_OK(db_->NewExternalLogFileManager(manager));
  }

  void CreateExternalLog(ExternalLogFileManager* manager,
                         const std::string& name,
                         std::unique_ptr<ExternalLogFileWriter>* writer) {
    ExternalLogFileOptions create_options;
    create_options.name = name;
    ASSERT_OK(manager->CreateExternalLogFile(create_options, writer));
  }

  std::string ReadAll(ExternalLogFileReader* reader) {
    const uint64_t visible_size = reader->VisibleSize();
    std::string scratch(static_cast<size_t>(visible_size), '\0');
    Slice result;
    EXPECT_OK(reader->Read(ReadOptions(), 0, scratch.size(), &result,
                           scratch.data()));
    return result.ToString();
  }

  void ChecksumForString(const std::string& data, std::string* checksum,
                         std::string* checksum_func_name) {
    FileChecksumGenContext context;
    context.file_name = "external_log_file_test";
    std::unique_ptr<FileChecksumGenerator> generator =
        GetFileChecksumGenCrc32cFactory()->CreateFileChecksumGenerator(context);
    ASSERT_NE(generator, nullptr);
    generator->Update(data.data(), data.size());
    generator->Finalize();
    *checksum = generator->GetChecksum();
    *checksum_func_name = generator->Name();
  }
};

TEST_F(ExternalLogFileTest, CreateAppendRecoverSealAndVerify) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "app-log", &writer);

  uint64_t offset = 0;
  ASSERT_OK(writer->Append(WriteOptions(), "hello", &offset));
  ASSERT_EQ(0, offset);

  ExternalLogFileReaderOptions follow_options;
  follow_options.visibility = ExternalLogFileReaderVisibility::kFollowWriter;
  std::unique_ptr<ExternalLogFileReader> follow_reader;
  ASSERT_OK(writer->NewReader(follow_options, &follow_reader));
  ASSERT_EQ("hello", ReadAll(follow_reader.get()));

  ASSERT_OK(writer->Append(WriteOptions(), " world", &offset));
  ASSERT_EQ(5, offset);
  ASSERT_EQ("hello world", ReadAll(follow_reader.get()));

  ExternalLogFileInfo synced_info;
  ASSERT_OK(writer->Sync(WriteOptions(), &synced_info));
  ASSERT_EQ(ExternalLogFileState::kUnsealed, synced_info.state);
  ASSERT_EQ(0, synced_info.durable_size);
  ASSERT_EQ(11, synced_info.physical_size);
  ASSERT_EQ(kUnknownFileChecksumFuncName, synced_info.file_checksum_func_name);
  ASSERT_EQ("app-log", synced_info.relative_filename);
  ASSERT_OK(env_->FileExists(dbname_ + "/app-log"));
  ASSERT_OK(writer->Close(WriteOptions()));
  writer.reset();

  std::vector<std::string> live_files;
  uint64_t manifest_size = 0;
  ASSERT_OK(db_->GetLiveFiles(live_files, &manifest_size,
                              /*flush_memtable=*/false));
  ASSERT_NE(live_files.end(), std::find(live_files.begin(), live_files.end(),
                                        "/" + synced_info.relative_filename));
  follow_reader.reset();

  Reopen(TestOptions());
  NewManager(&manager);

  ListExternalLogFilesOptions list_options;
  list_options.get_physical_size = true;
  std::vector<ExternalLogFileInfo> files;
  ASSERT_OK(manager->ListExternalLogFiles(list_options, &files));
  ASSERT_EQ(1, files.size());
  ASSERT_EQ("app-log", files[0].name);
  ASSERT_EQ(ExternalLogFileState::kUnsealed, files[0].state);
  ASSERT_EQ(0, files[0].durable_size);
  ASSERT_EQ(11, files[0].physical_size);

  ReopenExternalLogFileOptions reopen_options;
  reopen_options.has_recovered_size = true;
  reopen_options.recovered_size = files[0].physical_size;
  reopen_options.recompute_checksum = true;
  ASSERT_OK(manager->ReopenExternalLogFile("app-log", reopen_options, &writer));
  ASSERT_OK(writer->Append(WriteOptions(), "!", &offset));
  ASSERT_EQ(11, offset);

  ExternalLogFileInfo sealed_info;
  ASSERT_OK(writer->Seal(WriteOptions(), &sealed_info));
  ASSERT_EQ(ExternalLogFileState::kSealed, sealed_info.state);
  ASSERT_EQ(12, sealed_info.durable_size);
  writer.reset();

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(
      manager->OpenExternalLogFileForRead(ReadOptions(), "app-log", &reader));
  ASSERT_EQ("hello world!", ReadAll(reader.get()));

  ExternalLogFileChecksumInfo checksum_info;
  ASSERT_OK(manager->VerifyExternalLogFileChecksum(
      ReadOptions(), "app-log", VerifyExternalLogFileChecksumOptions(),
      &checksum_info));
  ASSERT_EQ(12, checksum_info.verified_size);

  ASSERT_TRUE(manager
                  ->ReopenExternalLogFile(
                      "app-log", ReopenExternalLogFileOptions(), &writer)
                  .IsInvalidArgument());
}

TEST_F(ExternalLogFileTest, UsesApplicationProvidedDBRelativePath) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "external_logs/app-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "subdir-data"));

  ExternalLogFileInfo info;
  ASSERT_OK(writer->Seal(WriteOptions(), &info));
  writer.reset();

  ASSERT_EQ("external_logs/app-log", info.name);
  ASSERT_EQ("external_logs/app-log", info.relative_filename);

  std::string data;
  ASSERT_OK(ReadFileToString(env_, dbname_ + "/external_logs/app-log", &data));
  ASSERT_EQ("subdir-data", data);

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(
      ReadOptions(), "external_logs/app-log", &reader));
  ASSERT_EQ("subdir-data", ReadAll(reader.get()));
}

TEST_F(ExternalLogFileTest, UsesExplicitExternalPath) {
  Open();

  const std::string external_dir = dbname_ + "_external";
  ASSERT_OK(DestroyDir(env_, external_dir));
  ASSERT_OK(env_->CreateDir(external_dir));
  const std::string db_basename = dbname_.substr(dbname_.find_last_of('/') + 1);
  const std::string external_path =
      dbname_ + "/../" + db_basename + "_external/app-log-file";
  const std::string external_parent =
      dbname_ + "/../" + db_basename + "_external";

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  ExternalLogFileOptions create_options;
  create_options.name = external_path;
  create_options.path_type = ExternalLogFilePathType::kExternalPath;

  std::unique_ptr<ExternalLogFileWriter> writer;
  ASSERT_OK(manager->CreateExternalLogFile(create_options, &writer));
  ASSERT_OK(writer->Append(WriteOptions(), "external-data"));

  ExternalLogFileInfo info;
  ASSERT_OK(writer->Seal(WriteOptions(), &info));
  writer.reset();

  ASSERT_EQ(external_path, info.name);
  ASSERT_EQ(ExternalLogFilePathType::kExternalPath, info.path_type);
  ASSERT_EQ(external_parent, info.directory);
  ASSERT_EQ("app-log-file", info.relative_filename);
  ASSERT_OK(env_->FileExists(external_path));

  ListExternalLogFilesOptions list_options;
  list_options.get_physical_size = true;
  std::vector<ExternalLogFileInfo> files;
  ASSERT_OK(manager->ListExternalLogFiles(list_options, &files));
  ASSERT_EQ(1, files.size());
  ASSERT_EQ(external_path, files[0].name);
  ASSERT_EQ(external_parent, files[0].directory);
  ASSERT_EQ("app-log-file", files[0].relative_filename);
  ASSERT_EQ(13, files[0].physical_size);

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(), external_path,
                                                &reader));
  ASSERT_EQ("external-data", ReadAll(reader.get()));
  reader.reset();

  manager.reset();
  Reopen(TestOptions());
  NewManager(&manager);

  files.clear();
  ASSERT_OK(manager->ListExternalLogFiles(list_options, &files));
  ASSERT_EQ(1, files.size());
  ASSERT_EQ(external_path, files[0].name);
  ASSERT_EQ(ExternalLogFilePathType::kExternalPath, files[0].path_type);
  ASSERT_EQ(external_parent, files[0].directory);
  ASSERT_EQ("app-log-file", files[0].relative_filename);

  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(), external_path,
                                                &reader));
  ASSERT_EQ("external-data", ReadAll(reader.get()));
  reader.reset();

  ASSERT_OK(manager->DeleteExternalLogFile(external_path));
  ASSERT_TRUE(env_->FileExists(external_path).IsNotFound());
  ASSERT_OK(env_->DeleteDir(external_dir));
}

TEST_F(ExternalLogFileTest, RejectsInvalidDBRelativePathNames) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  const std::string invalid_names[] = {
      "",
      "/absolute-log",
      "external_logs//empty-component",
      "external_logs/../escape",
      "CURRENT",
      "CURRENT/nested-log",
      "000123.sst",
      "000123.sst/nested-log",
  };
  for (const auto& name : invalid_names) {
    SCOPED_TRACE(name);
    ExternalLogFileOptions options;
    options.name = name;
    std::unique_ptr<ExternalLogFileWriter> writer;
    ASSERT_TRUE(
        manager->CreateExternalLogFile(options, &writer).IsInvalidArgument());
  }
}

TEST_F(ExternalLogFileTest, RejectsInvalidExternalPathOptions) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;

  ExternalLogFileOptions empty_external_path_options;
  empty_external_path_options.name = "";
  empty_external_path_options.path_type =
      ExternalLogFilePathType::kExternalPath;
  ASSERT_TRUE(
      manager->CreateExternalLogFile(empty_external_path_options, &writer)
          .IsInvalidArgument());

  ExternalLogFileOptions dot_external_path_options;
  dot_external_path_options.name = "relative/./external-log";
  dot_external_path_options.path_type = ExternalLogFilePathType::kExternalPath;
  ASSERT_TRUE(manager->CreateExternalLogFile(dot_external_path_options, &writer)
                  .IsInvalidArgument());

  ExternalLogFileOptions directory_external_path_options;
  directory_external_path_options.name = dbname_ + "_external/";
  directory_external_path_options.path_type =
      ExternalLogFilePathType::kExternalPath;
  ASSERT_TRUE(
      manager->CreateExternalLogFile(directory_external_path_options, &writer)
          .IsInvalidArgument());
}

TEST_F(ExternalLogFileTest, SealRetryAfterManifestFailureKeepsChecksumState) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "seal-retry-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "retry-data"));

  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        *static_cast<IOStatus*>(arg) =
            IOStatus::NotSupported("injected manifest write failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(writer->Seal(WriteOptions()).IsNotSupported());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ExternalLogFileInfo sealed_info;
  ASSERT_OK(writer->Seal(WriteOptions(), &sealed_info));
  ASSERT_EQ(ExternalLogFileState::kSealed, sealed_info.state);
  ASSERT_EQ(10, sealed_info.durable_size);
  writer.reset();

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(), "seal-retry-log",
                                                &reader));
  ASSERT_EQ("retry-data", ReadAll(reader.get()));

  ExternalLogFileChecksumInfo checksum_info;
  ASSERT_OK(manager->VerifyExternalLogFileChecksum(
      ReadOptions(), "seal-retry-log", VerifyExternalLogFileChecksumOptions(),
      &checksum_info));
  ASSERT_TRUE(checksum_info.verified);
  ASSERT_EQ(10, checksum_info.verified_size);
}

TEST_F(ExternalLogFileTest, IngestSealedFileWithChecksum) {
  Open();

  const std::string source_path = dbname_ + "/external_log_source";
  const std::string data = "bulk-load-external-log";
  ASSERT_OK(WriteStringToFile(env_, data, source_path, /*should_sync=*/true));

  std::string checksum;
  std::string checksum_func_name;
  ChecksumForString(data, &checksum, &checksum_func_name);

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  IngestExternalLogFileOptions ingest_options;
  ingest_options.name = "bulk-log";
  ingest_options.source_path = source_path;
  ingest_options.logical_size = data.size();
  ingest_options.file_checksum = checksum;
  ingest_options.file_checksum_func_name = checksum_func_name;
  ingest_options.verify_file_checksum = true;

  ExternalLogFileInfo info;
  ASSERT_OK(manager->IngestExternalLogFile(ingest_options, &info));
  ASSERT_EQ(ExternalLogFileState::kSealed, info.state);
  ASSERT_EQ(data.size(), info.durable_size);
  ASSERT_EQ("bulk-log", info.relative_filename);
  ASSERT_OK(env_->FileExists(source_path));
  ASSERT_OK(env_->FileExists(dbname_ + "/bulk-log"));

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(
      manager->OpenExternalLogFileForRead(ReadOptions(), "bulk-log", &reader));
  ASSERT_EQ(data, ReadAll(reader.get()));

  std::unique_ptr<ExternalLogFileWriter> writer;
  ASSERT_TRUE(manager
                  ->ReopenExternalLogFile(
                      "bulk-log", ReopenExternalLogFileOptions(), &writer)
                  .IsInvalidArgument());
  reader.reset();

  Reopen(TestOptions());
  NewManager(&manager);
  ASSERT_OK(manager->VerifyExternalLogFileChecksum(
      ReadOptions(), "bulk-log", VerifyExternalLogFileChecksumOptions()));
}

TEST_F(ExternalLogFileTest, IngestRegistersFileAlreadyAtDBRelativePath) {
  Open();

  const std::string data = "already-in-place";
  const std::string source_path = dbname_ + "/registered-log";
  ASSERT_OK(WriteStringToFile(env_, data, source_path, /*should_sync=*/true));

  std::string checksum;
  std::string checksum_func_name;
  ChecksumForString(data, &checksum, &checksum_func_name);

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  IngestExternalLogFileOptions ingest_options;
  ingest_options.name = "registered-log";
  ingest_options.source_path = source_path;
  ingest_options.logical_size = data.size();
  ingest_options.file_checksum = checksum;
  ingest_options.file_checksum_func_name = checksum_func_name;
  ingest_options.verify_file_checksum = true;
  ingest_options.ingestion_mode = ExternalLogFileIngestionMode::kMove;

  ExternalLogFileInfo info;
  ASSERT_OK(manager->IngestExternalLogFile(ingest_options, &info));
  ASSERT_EQ("registered-log", info.relative_filename);
  ASSERT_OK(env_->FileExists(source_path));

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(), "registered-log",
                                                &reader));
  ASSERT_EQ(data, ReadAll(reader.get()));
}

TEST_F(ExternalLogFileTest, IngestRegistersFileAlreadyAtExplicitExternalPath) {
  Open();

  const std::string external_dir = dbname_ + "_ingest_external";
  ASSERT_OK(DestroyDir(env_, external_dir));
  ASSERT_OK(env_->CreateDir(external_dir));
  const std::string external_path = external_dir + "/registered-file";
  const std::string data = "already-in-external-place";
  ASSERT_OK(WriteStringToFile(env_, data, external_path, /*should_sync=*/true));

  std::string checksum;
  std::string checksum_func_name;
  ChecksumForString(data, &checksum, &checksum_func_name);

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  IngestExternalLogFileOptions ingest_options;
  ingest_options.name = external_path;
  ingest_options.path_type = ExternalLogFilePathType::kExternalPath;
  ingest_options.source_path = external_path;
  ingest_options.logical_size = data.size();
  ingest_options.file_checksum = checksum;
  ingest_options.file_checksum_func_name = checksum_func_name;
  ingest_options.verify_file_checksum = true;
  ingest_options.ingestion_mode = ExternalLogFileIngestionMode::kMove;

  ExternalLogFileInfo info;
  ASSERT_OK(manager->IngestExternalLogFile(ingest_options, &info));
  ASSERT_EQ(external_path, info.name);
  ASSERT_EQ(ExternalLogFilePathType::kExternalPath, info.path_type);
  ASSERT_EQ(external_dir, info.directory);
  ASSERT_EQ("registered-file", info.relative_filename);
  ASSERT_OK(env_->FileExists(external_path));

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(), external_path,
                                                &reader));
  ASSERT_EQ(data, ReadAll(reader.get()));
  reader.reset();

  ASSERT_OK(manager->DeleteExternalLogFile(external_path));
  ASSERT_TRUE(env_->FileExists(external_path).IsNotFound());
  ASSERT_OK(env_->DeleteDir(external_dir));
}

TEST_F(ExternalLogFileTest, IngestMultipleFilesAndVerifyAll) {
  Open();

  const std::string source_path1 = dbname_ + "/external_log_source1";
  const std::string source_path2 = dbname_ + "/external_log_source2";
  const std::string data1 = "bulk-load-one";
  const std::string data2 = "bulk-load-two";
  ASSERT_OK(WriteStringToFile(env_, data1, source_path1, /*should_sync=*/true));
  ASSERT_OK(WriteStringToFile(env_, data2, source_path2, /*should_sync=*/true));

  std::string checksum1;
  std::string checksum_func_name1;
  ChecksumForString(data1, &checksum1, &checksum_func_name1);
  std::string checksum2;
  std::string checksum_func_name2;
  ChecksumForString(data2, &checksum2, &checksum_func_name2);

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::vector<IngestExternalLogFileOptions> ingest_options(2);
  ingest_options[0].name = "bulk-log-one";
  ingest_options[0].source_path = source_path1;
  ingest_options[0].logical_size = data1.size();
  ingest_options[0].file_checksum = checksum1;
  ingest_options[0].file_checksum_func_name = checksum_func_name1;
  ingest_options[1].name = "bulk-log-two";
  ingest_options[1].source_path = source_path2;
  ingest_options[1].logical_size = data2.size();
  ingest_options[1].file_checksum = checksum2;
  ingest_options[1].file_checksum_func_name = checksum_func_name2;

  std::vector<ExternalLogFileInfo> infos;
  ASSERT_OK(manager->IngestExternalLogFiles(ingest_options, &infos));
  ASSERT_EQ(2, infos.size());
  ASSERT_EQ(ExternalLogFileState::kSealed, infos[0].state);
  ASSERT_EQ(ExternalLogFileState::kSealed, infos[1].state);

  ListExternalLogFilesOptions list_options;
  list_options.include_unsealed = false;
  std::vector<ExternalLogFileInfo> files;
  ASSERT_OK(manager->ListExternalLogFiles(list_options, &files));
  ASSERT_EQ(2, files.size());

  list_options.include_unsealed = true;
  list_options.include_sealed = false;
  ASSERT_OK(manager->ListExternalLogFiles(list_options, &files));
  ASSERT_TRUE(files.empty());

  std::vector<ExternalLogFileChecksumInfo> checksum_infos;
  ASSERT_OK(manager->VerifyExternalLogFileChecksums(
      ReadOptions(), VerifyExternalLogFileChecksumOptions(), &checksum_infos));
  ASSERT_EQ(2, checksum_infos.size());
  ASSERT_TRUE(checksum_infos[0].verified);
  ASSERT_TRUE(checksum_infos[1].verified);
}

TEST_F(ExternalLogFileTest, MoveIngestRollbackRestoresSourceBeforeUnprotect) {
  Open();

  const std::string source_path = dbname_ + "/external_log_move_source";
  const std::string data = "move-ingest-data";
  ASSERT_OK(WriteStringToFile(env_, data, source_path, /*should_sync=*/true));

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  bool fail_manifest_write = false;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        if (fail_manifest_write) {
          *static_cast<IOStatus*>(arg) =
              IOStatus::NotSupported("injected manifest write failure");
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalLogFileManagerImpl::IngestExternalLogFiles:AfterTransfer",
      [&](void*) { fail_manifest_write = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  IngestExternalLogFileOptions ingest_options;
  ingest_options.name = "move-log";
  ingest_options.source_path = source_path;
  ingest_options.logical_size = data.size();
  ingest_options.verify_file_checksum = false;
  ingest_options.ingestion_mode = ExternalLogFileIngestionMode::kMove;

  ASSERT_TRUE(manager->IngestExternalLogFile(ingest_options).IsNotSupported());

  std::string restored_data;
  ASSERT_OK(ReadFileToString(env_, source_path, &restored_data));
  ASSERT_EQ(data, restored_data);

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_TRUE(
      manager->OpenExternalLogFileForRead(ReadOptions(), "move-log", &reader)
          .IsNotFound());
}

TEST_F(ExternalLogFileTest, BackupInclusionRequestIsNotSupported) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  ExternalLogFileOptions create_options;
  create_options.name = "backup-log";
  create_options.include_in_backup = true;
  std::unique_ptr<ExternalLogFileWriter> writer;
  ASSERT_TRUE(
      manager->CreateExternalLogFile(create_options, &writer).IsNotSupported());

  const std::string source_path = dbname_ + "/backup_source";
  const std::string data = "backup-data";
  ASSERT_OK(WriteStringToFile(env_, data, source_path, /*should_sync=*/true));

  std::string checksum;
  std::string checksum_func_name;
  ChecksumForString(data, &checksum, &checksum_func_name);

  IngestExternalLogFileOptions ingest_options;
  ingest_options.name = "backup-ingest-log";
  ingest_options.source_path = source_path;
  ingest_options.logical_size = data.size();
  ingest_options.file_checksum = checksum;
  ingest_options.file_checksum_func_name = checksum_func_name;
  ingest_options.include_in_backup = true;
  ASSERT_TRUE(manager->IngestExternalLogFile(ingest_options).IsNotSupported());
}

TEST_F(ExternalLogFileTest, BackupWithExternalLogFilesIsNotSupported) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "backup-existing-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "backup-existing-data"));
  ASSERT_OK(writer->Seal(WriteOptions()));
  writer.reset();

  const std::string backup_dir = dbname_ + "_backup";
  ASSERT_OK(DestroyDir(env_, backup_dir));
  BackupEngineOptions backup_options(backup_dir);
  BackupEngine* raw_backup = nullptr;
  ASSERT_OK(BackupEngine::Open(backup_options, env_, &raw_backup));
  std::unique_ptr<BackupEngine> backup(raw_backup);

  ASSERT_TRUE(backup->CreateNewBackup(db_.get()).IsNotSupported());
  backup.reset();
  ASSERT_OK(DestroyDir(env_, backup_dir));
}

TEST_F(ExternalLogFileTest, CheckpointWithExternalLogFilesIsNotSupported) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "checkpoint-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "checkpoint-data"));
  ASSERT_OK(writer->Seal(WriteOptions()));
  writer.reset();

  Checkpoint* raw_checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_.get(), &raw_checkpoint));
  std::unique_ptr<Checkpoint> checkpoint(raw_checkpoint);

  const std::string checkpoint_dir = dbname_ + "_checkpoint";
  ASSERT_OK(DestroyDir(env_, checkpoint_dir));
  ASSERT_TRUE(checkpoint->CreateCheckpoint(checkpoint_dir).IsNotSupported());
}

TEST_F(ExternalLogFileTest, DestroyDBDoesNotDeleteExternalLogFileOutsideDB) {
  Options options = TestOptions();
  DestroyAndReopen(options);

  const std::string external_dir = dbname_ + "_destroy_external";
  ASSERT_OK(DestroyDir(env_, external_dir));
  ASSERT_OK(env_->CreateDir(external_dir));
  const std::string external_path = external_dir + "/destroy-log-file";

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  ExternalLogFileOptions create_options;
  create_options.name = external_path;
  create_options.path_type = ExternalLogFilePathType::kExternalPath;

  std::unique_ptr<ExternalLogFileWriter> writer;
  ASSERT_OK(manager->CreateExternalLogFile(create_options, &writer));
  ASSERT_OK(writer->Append(WriteOptions(), "persistent"));

  ASSERT_OK(writer->Seal(WriteOptions()));
  writer.reset();
  manager.reset();

  ASSERT_OK(env_->FileExists(external_path));

  Close();
  ASSERT_OK(DestroyDB(dbname_, options));
  ASSERT_OK(env_->FileExists(external_path));

  ASSERT_OK(env_->DeleteFile(external_path));
  ASSERT_OK(env_->DeleteDir(external_dir));
}

TEST_F(ExternalLogFileTest, ReadOnlyManagerAllowsReadsAndRejectsMutations) {
  Options options = TestOptions();
  DestroyAndReopen(options);

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "readonly-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "readonly-data"));
  ASSERT_OK(writer->Seal(WriteOptions()));
  writer.reset();
  manager.reset();

  const std::string source_path = dbname_ + "/readonly_source";
  ASSERT_OK(WriteStringToFile(env_, "ingest-data", source_path,
                              /*should_sync=*/true));

  ASSERT_OK(ReadOnlyReopen(options));
  NewManager(&manager);

  std::vector<ExternalLogFileInfo> files;
  ASSERT_OK(
      manager->ListExternalLogFiles(ListExternalLogFilesOptions(), &files));
  ASSERT_EQ(1, files.size());
  ASSERT_EQ("readonly-log", files[0].name);

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(), "readonly-log",
                                                &reader));
  ASSERT_EQ("readonly-data", ReadAll(reader.get()));
  ASSERT_OK(manager->VerifyExternalLogFileChecksum(
      ReadOptions(), "readonly-log", VerifyExternalLogFileChecksumOptions()));

  ExternalLogFileOptions create_options;
  create_options.name = "readonly-create-log";
  ASSERT_TRUE(
      manager->CreateExternalLogFile(create_options, &writer).IsNotSupported());

  IngestExternalLogFileOptions ingest_options;
  ingest_options.name = "readonly-ingest-log";
  ingest_options.source_path = source_path;
  ingest_options.logical_size = 0;
  ingest_options.verify_file_checksum = false;
  ASSERT_TRUE(manager->IngestExternalLogFile(ingest_options).IsNotSupported());

  ASSERT_TRUE(manager
                  ->ReopenExternalLogFile(
                      "readonly-log", ReopenExternalLogFileOptions(), &writer)
                  .IsNotSupported());
  ASSERT_TRUE(manager->DeleteExternalLogFile(WriteOptions(), "readonly-log")
                  .IsNotSupported());
  ASSERT_TRUE(manager->DeleteExternalLogFiles(WriteOptions(), {"readonly-log"})
                  .IsNotSupported());
}

TEST_F(ExternalLogFileTest, ManagerFollowReaderObservesActiveWriter) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "follow-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "one"));

  ExternalLogFileReaderOptions reader_options;
  reader_options.visibility = ExternalLogFileReaderVisibility::kFollowWriter;
  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(), "follow-log",
                                                reader_options, &reader));
  ASSERT_EQ("one", ReadAll(reader.get()));

  ASSERT_OK(writer->Append(WriteOptions(), "two"));
  ASSERT_EQ("onetwo", ReadAll(reader.get()));
}

TEST_F(ExternalLogFileTest, SnapshotReaderCapturesActiveWriterVisibleSize) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "snapshot-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "abc"));

  std::unique_ptr<ExternalLogFileReader> snapshot_reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(), "snapshot-log",
                                                &snapshot_reader));
  ASSERT_EQ("abc", ReadAll(snapshot_reader.get()));

  ASSERT_OK(writer->Append(WriteOptions(), "def"));
  ASSERT_EQ("abc", ReadAll(snapshot_reader.get()));

  ExternalLogFileReaderOptions follow_options;
  follow_options.visibility = ExternalLogFileReaderVisibility::kFollowWriter;
  std::unique_ptr<ExternalLogFileReader> follow_reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(
      ReadOptions(), "snapshot-log", follow_options, &follow_reader));
  ASSERT_EQ("abcdef", ReadAll(follow_reader.get()));
}

TEST_F(ExternalLogFileTest, ConcurrentAppendOnSameWriterUsesDistinctOffsets) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "concurrent-append-log", &writer);

  constexpr size_t kNumThreads = 8;
  constexpr size_t kChunkSize = 4;
  std::vector<Status> statuses(kNumThreads);
  std::vector<uint64_t> offsets(kNumThreads, 0);
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      const std::string chunk(kChunkSize, static_cast<char>('a' + i));
      statuses[i] = writer->Append(WriteOptions(), chunk, &offsets[i]);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  for (const auto& status : statuses) {
    ASSERT_OK(status);
  }

  std::sort(offsets.begin(), offsets.end());
  for (size_t i = 0; i < kNumThreads; ++i) {
    ASSERT_EQ(i * kChunkSize, offsets[i]);
  }

  ASSERT_OK(writer->Seal(WriteOptions()));
  writer.reset();

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(
      ReadOptions(), "concurrent-append-log", &reader));
  ASSERT_EQ(kNumThreads * kChunkSize, reader->VisibleSize());
  ASSERT_OK(manager->VerifyExternalLogFileChecksum(
      ReadOptions(), "concurrent-append-log",
      VerifyExternalLogFileChecksumOptions()));
}

TEST_F(ExternalLogFileTest, MultipleUnsealedFilesCanHaveActiveWriters) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer1;
  std::unique_ptr<ExternalLogFileWriter> writer2;
  CreateExternalLog(manager.get(), "active-writer-log-one", &writer1);
  CreateExternalLog(manager.get(), "active-writer-log-two", &writer2);

  uint64_t offset1 = 0;
  uint64_t offset2 = 0;
  ASSERT_OK(writer1->Append(WriteOptions(), "one", &offset1));
  ASSERT_OK(writer2->Append(WriteOptions(), "two", &offset2));
  ASSERT_EQ(0, offset1);
  ASSERT_EQ(0, offset2);

  std::unique_ptr<ExternalLogFileReader> reader1;
  std::unique_ptr<ExternalLogFileReader> reader2;
  ASSERT_OK(writer1->NewReader(&reader1));
  ASSERT_OK(writer2->NewReader(&reader2));
  ASSERT_EQ("one", ReadAll(reader1.get()));
  ASSERT_EQ("two", ReadAll(reader2.get()));

  reader1.reset();
  reader2.reset();
  ASSERT_OK(writer1->Seal(WriteOptions()));
  ASSERT_OK(writer2->Seal(WriteOptions()));
}

TEST_F(ExternalLogFileTest, CreateRegistersActiveWriterBeforeManifestVisible) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  Status reopen_status;
  std::unique_ptr<ExternalLogFileWriter> reopened_writer;
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalLogFileManagerImpl::CreateExternalLogFile:AfterManifest",
      [&](void*) {
        reopen_status = manager->ReopenExternalLogFile(
            "create-race-log", ReopenExternalLogFileOptions(),
            &reopened_writer);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "create-race-log", &writer);
  ASSERT_TRUE(reopen_status.IsBusy()) << reopen_status.ToString();
  ASSERT_EQ(nullptr, reopened_writer);
  ASSERT_OK(writer->Close(WriteOptions()));
}

TEST_F(ExternalLogFileTest, DeleteRejectsActiveWriterAndActiveReader) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "delete-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "delete-data"));
  ASSERT_TRUE(
      manager->DeleteExternalLogFile(WriteOptions(), "delete-log").IsBusy());

  ASSERT_OK(writer->Seal(WriteOptions()));
  writer.reset();

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(), "delete-log",
                                                &reader));
  ASSERT_TRUE(
      manager->DeleteExternalLogFile(WriteOptions(), "delete-log").IsBusy());
  ASSERT_EQ("delete-data", ReadAll(reader.get()));
  reader.reset();

  ASSERT_OK(manager->DeleteExternalLogFile(WriteOptions(), "delete-log"));

  std::unique_ptr<ExternalLogFileReader> deleted_reader;
  ASSERT_TRUE(manager
                  ->OpenExternalLogFileForRead(ReadOptions(), "delete-log",
                                               &deleted_reader)
                  .IsNotFound());
}

TEST_F(ExternalLogFileTest, DeleteUnlinksPhysicalFileWithoutActiveRefs) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "delete-physical-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "delete-physical-data"));
  ExternalLogFileInfo info;
  ASSERT_OK(writer->Seal(WriteOptions(), &info));
  writer.reset();

  const std::string path = dbname_ + "/" + info.relative_filename;
  ASSERT_OK(env_->FileExists(path));
  ASSERT_OK(manager->DeleteExternalLogFile("delete-physical-log"));
  ASSERT_TRUE(env_->FileExists(path).IsNotFound());
}

TEST_F(ExternalLogFileTest, DeleteMultipleUsesOneManifestEdit) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer1;
  CreateExternalLog(manager.get(), "bulk-delete-log-one", &writer1);
  ASSERT_OK(writer1->Append(WriteOptions(), "delete-one"));
  ExternalLogFileInfo info1;
  ASSERT_OK(writer1->Seal(WriteOptions(), &info1));
  writer1.reset();

  std::unique_ptr<ExternalLogFileWriter> writer2;
  CreateExternalLog(manager.get(), "bulk-delete-log-two", &writer2);
  ASSERT_OK(writer2->Append(WriteOptions(), "delete-two"));
  ExternalLogFileInfo info2;
  ASSERT_OK(writer2->Seal(WriteOptions(), &info2));
  writer2.reset();

  const std::string path1 = dbname_ + "/" + info1.relative_filename;
  const std::string path2 = dbname_ + "/" + info2.relative_filename;
  ASSERT_OK(env_->FileExists(path1));
  ASSERT_OK(env_->FileExists(path2));

  int manifest_writes = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest",
      [&](void*) { ++manifest_writes; });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(manager->DeleteExternalLogFiles(
      WriteOptions(), {"bulk-delete-log-one", "bulk-delete-log-two"}));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(1, manifest_writes);
  ASSERT_TRUE(env_->FileExists(path1).IsNotFound());
  ASSERT_TRUE(env_->FileExists(path2).IsNotFound());

  std::vector<ExternalLogFileInfo> files;
  ASSERT_OK(
      manager->ListExternalLogFiles(ListExternalLogFilesOptions(), &files));
  ASSERT_TRUE(files.empty());
}

TEST_F(ExternalLogFileTest,
       DeleteMultipleRejectsActiveFileWithoutPartialManifestChange) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> sealed_writer;
  CreateExternalLog(manager.get(), "bulk-delete-sealed-log", &sealed_writer);
  ASSERT_OK(sealed_writer->Append(WriteOptions(), "sealed"));
  ExternalLogFileInfo sealed_info;
  ASSERT_OK(sealed_writer->Seal(WriteOptions(), &sealed_info));
  sealed_writer.reset();

  std::unique_ptr<ExternalLogFileWriter> active_writer;
  CreateExternalLog(manager.get(), "bulk-delete-active-log", &active_writer);
  ASSERT_OK(active_writer->Append(WriteOptions(), "active"));

  ASSERT_TRUE(manager
                  ->DeleteExternalLogFiles(
                      WriteOptions(),
                      {"bulk-delete-sealed-log", "bulk-delete-active-log"})
                  .IsBusy());

  std::vector<ExternalLogFileInfo> files;
  ASSERT_OK(
      manager->ListExternalLogFiles(ListExternalLogFilesOptions(), &files));
  ASSERT_EQ(2, files.size());
  ASSERT_OK(env_->FileExists(dbname_ + "/" + sealed_info.relative_filename));

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(
      ReadOptions(), "bulk-delete-sealed-log", &reader));
  ASSERT_EQ("sealed", ReadAll(reader.get()));
  reader.reset();

  ASSERT_OK(active_writer->Close(WriteOptions()));
  active_writer.reset();
  ASSERT_OK(manager->DeleteExternalLogFiles(
      {"bulk-delete-sealed-log", "bulk-delete-active-log"}));
}

TEST_F(ExternalLogFileTest, DeleteUsesRateLimitedUnaccountedPurge) {
  Options options = TestOptions();
  Status s;
  options.sst_file_manager.reset(
      NewSstFileManager(env_, nullptr, "", 0, false, &s, 0));
  ASSERT_OK(s);
  options.sst_file_manager->SetDeleteRateBytesPerSecond(1024 * 1024 * 1024);
  DestroyAndReopen(options);

  const std::string external_dir = dbname_ + "_delete_external";
  ASSERT_OK(DestroyDir(env_, external_dir));
  ASSERT_OK(env_->CreateDir(external_dir));
  const std::string external_path =
      external_dir + "/delete-rate-limited-log-file";

  std::vector<uint64_t> penalties;
  int unaccounted_deletions = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleUnaccountedFileDeletion", [&](void* arg) {
        const std::string* path = static_cast<const std::string*>(arg);
        if (path->find("delete-rate-limited-log") != std::string::npos) {
          ++unaccounted_deletions;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::BackgroundEmptyTrash:Wait",
      [&](void* arg) { penalties.push_back(*static_cast<uint64_t*>(arg)); });
  SyncPoint::GetInstance()->EnableProcessing();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  ExternalLogFileOptions create_options;
  create_options.name = external_path;
  create_options.path_type = ExternalLogFilePathType::kExternalPath;

  std::unique_ptr<ExternalLogFileWriter> writer;
  ASSERT_OK(manager->CreateExternalLogFile(create_options, &writer));
  ASSERT_OK(writer->Append(WriteOptions(), DummyString(1024 * 1024, 'x')));
  ASSERT_OK(writer->Seal(WriteOptions()));
  writer.reset();

  ASSERT_OK(manager->DeleteExternalLogFile(external_path));
  static_cast<SstFileManagerImpl*>(options.sst_file_manager.get())
      ->WaitForEmptyTrash();

  ASSERT_EQ(1, unaccounted_deletions);
  ASSERT_EQ(1, penalties.size());
  ASSERT_GT(penalties[0], 0);
  ASSERT_TRUE(env_->FileExists(external_path).IsNotFound());
  ASSERT_OK(DestroyDir(env_, external_dir));
}

TEST_F(ExternalLogFileTest, OpenReaderBlocksDeleteUntilUnregistered) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "open-delete-race-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "race-data"));
  ASSERT_OK(writer->Seal(WriteOptions()));
  writer.reset();

  Status delete_status = Status::NotFound("delete callback was not called");
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalLogFileManagerImpl::OpenExternalLogFileForRead:"
      "AfterRegisterReader",
      [&](void*) {
        delete_status = manager->DeleteExternalLogFile(WriteOptions(),
                                                       "open-delete-race-log");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(
      ReadOptions(), "open-delete-race-log", &reader));
  ASSERT_TRUE(delete_status.IsBusy()) << delete_status.ToString();
  ASSERT_EQ("race-data", ReadAll(reader.get()));
  reader.reset();
  ASSERT_OK(
      manager->DeleteExternalLogFile(WriteOptions(), "open-delete-race-log"));

  std::unique_ptr<ExternalLogFileReader> deleted_reader;
  ASSERT_TRUE(manager
                  ->OpenExternalLogFileForRead(
                      ReadOptions(), "open-delete-race-log", &deleted_reader)
                  .IsNotFound());
}

TEST_F(ExternalLogFileTest, VerifyBlocksDeleteUntilUnregistered) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "verify-delete-race-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "verify-data"));
  ASSERT_OK(writer->Seal(WriteOptions()));
  writer.reset();

  Status delete_status = Status::NotFound("delete callback was not called");
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalLogFileManagerImpl::VerifyExternalLogFileChecksum:"
      "AfterRegisterReader",
      [&](void*) {
        delete_status = manager->DeleteExternalLogFile(
            WriteOptions(), "verify-delete-race-log");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(manager->VerifyExternalLogFileChecksum(
      ReadOptions(), "verify-delete-race-log",
      VerifyExternalLogFileChecksumOptions()));
  ASSERT_TRUE(delete_status.IsBusy()) << delete_status.ToString();
  ASSERT_OK(
      manager->DeleteExternalLogFile(WriteOptions(), "verify-delete-race-log"));

  std::unique_ptr<ExternalLogFileReader> deleted_reader;
  ASSERT_TRUE(manager
                  ->OpenExternalLogFileForRead(
                      ReadOptions(), "verify-delete-race-log", &deleted_reader)
                  .IsNotFound());
}

TEST_F(ExternalLogFileTest, ReopenRejectsActiveWriterWithoutTruncating) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "active-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "abc"));

  std::unique_ptr<ExternalLogFileWriter> second_writer;
  ASSERT_TRUE(manager
                  ->ReopenExternalLogFile("active-log",
                                          ReopenExternalLogFileOptions(),
                                          &second_writer)
                  .IsBusy());

  ExternalLogFileReaderOptions reader_options;
  reader_options.visibility = ExternalLogFileReaderVisibility::kFollowWriter;
  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(), "active-log",
                                                reader_options, &reader));
  ASSERT_EQ("abc", ReadAll(reader.get()));
}

TEST_F(ExternalLogFileTest, ReopenBlocksDeleteUntilWriterClosed) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "reopen-delete-race-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "reopen-race-data"));

  ExternalLogFileInfo sync_info;
  ASSERT_OK(writer->Sync(WriteOptions(), &sync_info));
  ASSERT_OK(writer->Close(WriteOptions()));
  writer.reset();

  Status delete_status = Status::NotFound("delete callback was not called");
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalLogFileManagerImpl::ReopenExternalLogFile:"
      "AfterRegisterActiveWriter",
      [&](void*) {
        delete_status = manager->DeleteExternalLogFile(
            WriteOptions(), "reopen-delete-race-log");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ReopenExternalLogFileOptions reopen_options;
  reopen_options.has_recovered_size = true;
  reopen_options.recovered_size = sync_info.physical_size;
  reopen_options.recompute_checksum = true;
  ASSERT_OK(manager->ReopenExternalLogFile("reopen-delete-race-log",
                                           reopen_options, &writer));
  ASSERT_TRUE(delete_status.IsBusy());

  ASSERT_OK(writer->Close(WriteOptions()));
  writer.reset();
  ASSERT_OK(
      manager->DeleteExternalLogFile(WriteOptions(), "reopen-delete-race-log"));
}

TEST_F(ExternalLogFileTest, ReopenCannotTruncateActiveReaderSnapshot) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "reader-truncate-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "abcdef"));

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(writer->NewReader(&reader));
  ASSERT_OK(writer->Close(WriteOptions()));
  writer.reset();

  ReopenExternalLogFileOptions reopen_options;
  reopen_options.has_recovered_size = true;
  reopen_options.recovered_size = 3;
  reopen_options.recompute_checksum = true;
  ASSERT_TRUE(manager
                  ->ReopenExternalLogFile("reader-truncate-log", reopen_options,
                                          &writer)
                  .IsBusy());
  ASSERT_EQ("abcdef", ReadAll(reader.get()));

  reader.reset();
  ASSERT_OK(manager->ReopenExternalLogFile("reader-truncate-log",
                                           reopen_options, &writer));

  std::unique_ptr<ExternalLogFileReader> truncated_reader;
  ASSERT_OK(writer->NewReader(&truncated_reader));
  ASSERT_EQ("abc", ReadAll(truncated_reader.get()));

  ASSERT_OK(writer->Close(WriteOptions()));
  writer.reset();
  truncated_reader.reset();

  std::unique_ptr<ExternalLogFileReader> closed_unsealed_reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(
      ReadOptions(), "reader-truncate-log", &closed_unsealed_reader));
  ASSERT_EQ("", ReadAll(closed_unsealed_reader.get()));
}

TEST_F(ExternalLogFileTest, ClosedWriterCannotOpenNewReader) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "closed-writer-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "abc"));
  ASSERT_OK(writer->Close(WriteOptions()));

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_TRUE(writer->NewReader(&reader).IsInvalidArgument());
}

TEST_F(ExternalLogFileTest, SoftTruncateRequiresAvailableChecksumState) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "soft-truncate-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "abcXYZ"));

  SealExternalLogFileOptions seal_options;
  seal_options.has_logical_size = true;
  seal_options.logical_size = 3;
  ASSERT_TRUE(writer->Seal(WriteOptions(), seal_options).IsInvalidArgument());

  seal_options.recompute_checksum = true;
  ASSERT_OK(writer->Seal(WriteOptions(), seal_options));

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(),
                                                "soft-truncate-log", &reader));
  ASSERT_EQ("abc", ReadAll(reader.get()));
}

TEST_F(ExternalLogFileTest, SyncDoesNotUpdateManifestMetadata) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "sync-manifest-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "abc"));

  ExternalLogFileInfo sync_info;
  ASSERT_OK(writer->Sync(WriteOptions(), &sync_info));
  ASSERT_EQ(0, sync_info.durable_size);
  ASSERT_EQ(3, sync_info.physical_size);
  ASSERT_EQ(kUnknownFileChecksumFuncName, sync_info.file_checksum_func_name);
  ASSERT_OK(writer->Close(WriteOptions()));
  writer.reset();

  Reopen(TestOptions());
  NewManager(&manager);

  ListExternalLogFilesOptions list_options;
  list_options.get_physical_size = true;
  std::vector<ExternalLogFileInfo> files;
  ASSERT_OK(manager->ListExternalLogFiles(list_options, &files));
  ASSERT_EQ(1, files.size());
  ASSERT_EQ("sync-manifest-log", files[0].name);
  ASSERT_EQ(ExternalLogFileState::kUnsealed, files[0].state);
  ASSERT_EQ(0, files[0].durable_size);
  ASSERT_EQ(3, files[0].physical_size);

  ReopenExternalLogFileOptions reopen_options;
  reopen_options.has_recovered_size = true;
  reopen_options.recovered_size = files[0].physical_size;
  reopen_options.recompute_checksum = true;
  ASSERT_OK(manager->ReopenExternalLogFile("sync-manifest-log", reopen_options,
                                           &writer));

  uint64_t offset = 0;
  ASSERT_OK(writer->Append(WriteOptions(), "XYZ", &offset));
  ASSERT_EQ(3, offset);

  ExternalLogFileInfo sealed_info;
  ASSERT_OK(writer->Seal(WriteOptions(), &sealed_info));
  ASSERT_EQ(6, sealed_info.durable_size);

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(),
                                                "sync-manifest-log", &reader));
  ASSERT_EQ("abcXYZ", ReadAll(reader.get()));
}

TEST_F(ExternalLogFileTest, ChecksumOperationsWithoutFactoryReportUnavailable) {
  Options checksum_options = TestOptions();
  DestroyAndReopen(checksum_options);

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "checksum-factory-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "abc"));
  ASSERT_OK(writer->Seal(WriteOptions()));
  writer.reset();

  CreateExternalLog(manager.get(), "checksum-recompute-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "abc"));
  ASSERT_OK(writer->Close(WriteOptions()));
  writer.reset();
  manager.reset();

  Options no_checksum_options = checksum_options;
  no_checksum_options.file_checksum_gen_factory.reset();
  Reopen(no_checksum_options);
  NewManager(&manager);

  ASSERT_TRUE(manager
                  ->VerifyExternalLogFileChecksum(
                      ReadOptions(), "checksum-factory-log",
                      VerifyExternalLogFileChecksumOptions())
                  .IsInvalidArgument());

  VerifyExternalLogFileChecksumOptions skip_options;
  skip_options.fail_if_checksum_unavailable = false;
  ExternalLogFileChecksumInfo checksum_info;
  ASSERT_OK(manager->VerifyExternalLogFileChecksum(
      ReadOptions(), "checksum-factory-log", skip_options, &checksum_info));
  ASSERT_FALSE(checksum_info.verified);
  ASSERT_EQ("checksum-factory-log", checksum_info.file_info.name);

  ReopenExternalLogFileOptions recompute_options;
  recompute_options.has_recovered_size = true;
  recompute_options.recovered_size = 3;
  recompute_options.recompute_checksum = true;
  ASSERT_TRUE(manager
                  ->ReopenExternalLogFile("checksum-recompute-log",
                                          recompute_options, &writer)
                  .IsInvalidArgument());
  manager.reset();

  Reopen(checksum_options);
  NewManager(&manager);
  ASSERT_OK(manager->VerifyExternalLogFileChecksum(
      ReadOptions(), "checksum-factory-log",
      VerifyExternalLogFileChecksumOptions(), &checksum_info));
  ASSERT_TRUE(checksum_info.verified);
  ASSERT_EQ(3, checksum_info.verified_size);
}

TEST_F(ExternalLogFileTest, RecoveredSizeWithoutChecksumRecomputeCannotSeal) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "stale-checksum-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "abc"));
  ASSERT_OK(writer->Close(WriteOptions()));
  writer.reset();

  Reopen(TestOptions());
  NewManager(&manager);

  ListExternalLogFilesOptions list_options;
  list_options.get_physical_size = true;
  std::vector<ExternalLogFileInfo> files;
  ASSERT_OK(manager->ListExternalLogFiles(list_options, &files));
  ASSERT_EQ(1, files.size());
  ASSERT_EQ(0, files[0].durable_size);
  ASSERT_EQ(3, files[0].physical_size);

  ReopenExternalLogFileOptions stale_checksum_options;
  stale_checksum_options.has_recovered_size = true;
  stale_checksum_options.recovered_size = files[0].physical_size;
  ASSERT_OK(manager->ReopenExternalLogFile("stale-checksum-log",
                                           stale_checksum_options, &writer));
  ASSERT_OK(writer->Append(WriteOptions(), "def"));
  ASSERT_TRUE(writer->Seal(WriteOptions()).IsInvalidArgument());
  ASSERT_OK(writer->Close(WriteOptions()));
}

TEST_F(ExternalLogFileTest, ReopenRecoveryDoesNotUpdateManifestMetadata) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "reopen-manifest-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "abc"));
  ASSERT_OK(writer->Close(WriteOptions()));
  writer.reset();

  Reopen(TestOptions());
  NewManager(&manager);

  ReopenExternalLogFileOptions reopen_options;
  reopen_options.has_recovered_size = true;
  reopen_options.recovered_size = 3;
  reopen_options.recompute_checksum = true;
  ASSERT_OK(manager->ReopenExternalLogFile("reopen-manifest-log",
                                           reopen_options, &writer));
  ASSERT_OK(writer->Close(WriteOptions()));
  writer.reset();

  Reopen(TestOptions());
  NewManager(&manager);

  ListExternalLogFilesOptions list_options;
  list_options.get_physical_size = true;
  std::vector<ExternalLogFileInfo> files;
  ASSERT_OK(manager->ListExternalLogFiles(list_options, &files));
  ASSERT_EQ(1, files.size());
  ASSERT_EQ("reopen-manifest-log", files[0].name);
  ASSERT_EQ(ExternalLogFileState::kUnsealed, files[0].state);
  ASSERT_EQ(0, files[0].durable_size);
  ASSERT_EQ(3, files[0].physical_size);

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(
      ReadOptions(), "reopen-manifest-log", &reader));
  ASSERT_EQ("", ReadAll(reader.get()));

  ASSERT_OK(manager->ReopenExternalLogFile("reopen-manifest-log",
                                           reopen_options, &writer));
  ExternalLogFileInfo sealed_info;
  ASSERT_OK(writer->Seal(WriteOptions(), &sealed_info));
  ASSERT_EQ(ExternalLogFileState::kSealed, sealed_info.state);
  ASSERT_EQ(3, sealed_info.durable_size);

  reader.reset();
  ASSERT_OK(manager->OpenExternalLogFileForRead(
      ReadOptions(), "reopen-manifest-log", &reader));
  ASSERT_EQ("abc", ReadAll(reader.get()));
}

TEST_F(ExternalLogFileTest, RecoveredWriterCanSealSoftTruncatedPrefix) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "seal-recovered-prefix-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "abcdef"));
  ASSERT_OK(writer->Close(WriteOptions()));
  writer.reset();

  Reopen(TestOptions());
  NewManager(&manager);

  ReopenExternalLogFileOptions reopen_options;
  reopen_options.has_recovered_size = true;
  reopen_options.recovered_size = 6;
  reopen_options.recompute_checksum = true;
  ASSERT_OK(manager->ReopenExternalLogFile("seal-recovered-prefix-log",
                                           reopen_options, &writer));

  SealExternalLogFileOptions shrink_options;
  shrink_options.has_logical_size = true;
  shrink_options.logical_size = 3;
  shrink_options.recompute_checksum = true;
  ASSERT_OK(writer->Seal(WriteOptions(), shrink_options));
  writer.reset();

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(
      ReadOptions(), "seal-recovered-prefix-log", &reader));
  ASSERT_EQ("abc", ReadAll(reader.get()));
}

TEST_F(ExternalLogFileTest, SyncSerializesAppendWithoutManifestUpdate) {
  Open();

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "sync-append-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "abc"));

  Status append_status;
  uint64_t append_offset = 0;
  std::thread append_thread;
  // The callback runs after Sync() has taken the writer mutex but before it
  // syncs the file, so the append thread must wait and receive the next offset
  // after the synced prefix.
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalLogFileWriterImpl::Sync:BeforeWritableFileSync", [&](void*) {
        append_thread = std::thread([&]() {
          append_status = writer->Append(WriteOptions(), "DEF", &append_offset);
        });
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ExternalLogFileInfo sync_info;
  Status sync_status = writer->Sync(WriteOptions(), &sync_info);
  ASSERT_TRUE(append_thread.joinable());
  append_thread.join();
  ASSERT_OK(sync_status);
  ASSERT_OK(append_status);
  ASSERT_EQ(3, append_offset);
  ASSERT_EQ(0, sync_info.durable_size);
  ASSERT_EQ(3, sync_info.physical_size);

  ExternalLogFileInfo sealed_info;
  ASSERT_OK(writer->Seal(WriteOptions(), &sealed_info));
  ASSERT_EQ(6, sealed_info.durable_size);
  ASSERT_OK(manager->VerifyExternalLogFileChecksum(
      ReadOptions(), "sync-append-log",
      VerifyExternalLogFileChecksumOptions()));

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(),
                                                "sync-append-log", &reader));
  ASSERT_EQ("abcDEF", ReadAll(reader.get()));
}

TEST_F(ExternalLogFileTest, TrustedIngestValidatesChecksumFunctionName) {
  Open();

  const std::string source_path = dbname_ + "/bad_checksum_func_source";
  const std::string data = "trusted-checksum-data";
  ASSERT_OK(WriteStringToFile(env_, data, source_path, /*should_sync=*/true));

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  IngestExternalLogFileOptions ingest_options;
  ingest_options.name = "bad-checksum-func-log";
  ingest_options.source_path = source_path;
  ingest_options.logical_size = data.size();
  ingest_options.file_checksum = "trusted";
  ingest_options.file_checksum_func_name = "NotARealChecksum";
  ingest_options.verify_file_checksum = false;

  ASSERT_TRUE(
      manager->IngestExternalLogFile(ingest_options).IsInvalidArgument());

  ingest_options.name = "missing-checksum-func-log";
  ingest_options.file_checksum = "trusted";
  ingest_options.file_checksum_func_name.clear();
  ASSERT_TRUE(
      manager->IngestExternalLogFile(ingest_options).IsInvalidArgument());

  ingest_options.name = "missing-checksum-value-log";
  ingest_options.file_checksum.clear();
  ingest_options.file_checksum_func_name = kStandardDbFileChecksumFuncName;
  ASSERT_TRUE(
      manager->IngestExternalLogFile(ingest_options).IsInvalidArgument());

  ingest_options.name = "unknown-checksum-log";
  ingest_options.file_checksum_func_name = kUnknownFileChecksumFuncName;
  ExternalLogFileInfo info;
  ASSERT_OK(manager->IngestExternalLogFile(ingest_options, &info));
  ASSERT_EQ(kUnknownFileChecksum, info.file_checksum);
  ASSERT_EQ(kUnknownFileChecksumFuncName, info.file_checksum_func_name);
}

TEST_F(ExternalLogFileTest, ManifestDecodeRejectsInvalidExternalLogMetadata) {
  auto put_varint32_field = [](std::string* dst, ExternalLogFileAdditionTag tag,
                               uint32_t value) {
    PutVarint32(dst, static_cast<uint32_t>(tag));
    std::string field;
    PutVarint32(&field, value);
    PutLengthPrefixedSlice(dst, field);
  };
  auto put_varint64_field = [](std::string* dst, ExternalLogFileAdditionTag tag,
                               uint64_t value) {
    PutVarint32(dst, static_cast<uint32_t>(tag));
    std::string field;
    PutVarint64(&field, value);
    PutLengthPrefixedSlice(dst, field);
  };
  auto put_string_field = [](std::string* dst, ExternalLogFileAdditionTag tag,
                             const std::string& value) {
    PutVarint32(dst, static_cast<uint32_t>(tag));
    PutLengthPrefixedSlice(dst, value);
  };
  auto build_record = [&](const std::string& checksum,
                          const std::string& checksum_func_name,
                          uint32_t temperature) {
    std::string record;
    PutVarint64(&record, 100);
    put_string_field(&record, ExternalLogFileAdditionTag::kName, "bad-log");
    put_varint32_field(&record, ExternalLogFileAdditionTag::kState,
                       static_cast<uint32_t>(ExternalLogFileState::kUnsealed));
    put_varint64_field(&record, ExternalLogFileAdditionTag::kDurableSize, 0);
    put_string_field(&record, ExternalLogFileAdditionTag::kFileChecksum,
                     checksum);
    put_string_field(&record, ExternalLogFileAdditionTag::kFileChecksumFuncName,
                     checksum_func_name);
    put_varint32_field(&record, ExternalLogFileAdditionTag::kTemperature,
                       temperature);
    PutVarint32(&record,
                static_cast<uint32_t>(ExternalLogFileAdditionTag::kTerminate));
    return record;
  };

  std::string record =
      build_record("checksum", kUnknownFileChecksumFuncName,
                   static_cast<uint32_t>(Temperature::kUnknown));
  Slice input(record);
  ExternalLogFileAddition addition;
  ASSERT_TRUE(addition.DecodeFrom(&input).IsCorruption());

  record = build_record(kUnknownFileChecksum, kUnknownFileChecksumFuncName,
                        static_cast<uint32_t>(Temperature::kLastTemperature));
  input = Slice(record);
  ASSERT_TRUE(addition.DecodeFrom(&input).IsCorruption());
}

TEST_F(ExternalLogFileTest, ManifestRollPreservesExternalLogWithWalTracking) {
  Options options = TestOptions();
  options.max_manifest_file_size = 1;
  options.max_manifest_space_amp_pct = 0;
  options.track_and_verify_wals_in_manifest = true;
  DestroyAndReopen(options);

  std::unique_ptr<ExternalLogFileManager> manager;
  NewManager(&manager);

  std::unique_ptr<ExternalLogFileWriter> writer;
  CreateExternalLog(manager.get(), "manifest-roll-log", &writer);
  ASSERT_OK(writer->Append(WriteOptions(), "manifest-roll-data"));

  ExternalLogFileInfo sealed_info;
  ASSERT_OK(writer->Seal(WriteOptions(), &sealed_info));
  writer.reset();

  const uint64_t pre_sync_wal_manifest_no =
      dbfull()->TEST_Current_Manifest_FileNo();
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  const uint64_t post_sync_wal_manifest_no =
      dbfull()->TEST_Current_Manifest_FileNo();
  ASSERT_GT(post_sync_wal_manifest_no, pre_sync_wal_manifest_no);

  Reopen(options);
  NewManager(&manager);

  std::vector<ExternalLogFileInfo> files;
  ASSERT_OK(
      manager->ListExternalLogFiles(ListExternalLogFilesOptions(), &files));
  ASSERT_EQ(1, files.size());
  ASSERT_EQ("manifest-roll-log", files[0].name);
  ASSERT_EQ(ExternalLogFileState::kSealed, files[0].state);

  std::unique_ptr<ExternalLogFileReader> reader;
  ASSERT_OK(manager->OpenExternalLogFileForRead(ReadOptions(),
                                                "manifest-roll-log", &reader));
  ASSERT_EQ("manifest-roll-data", ReadAll(reader.get()));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
