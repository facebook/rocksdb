// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "cloud/aws/aws_env.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/manifest.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "util/file_reader_writer.h"
#include "db/version_set.h"

namespace rocksdb {

CloudManifest::CloudManifest(std::shared_ptr<Logger> info_log,
                             CloudEnv* cenv,
                             const std::string& bucket_prefix) :
    info_log_(info_log),
    cenv_(cenv),
    bucket_prefix_(bucket_prefix) {
}

CloudManifest::~CloudManifest() {
}

//
// Extract all the live files needed by this MANIFEST file
//
Status CloudManifest::GetLiveFiles(const std::string manifest_path,
                                   std::set<uint64_t>* list) {
  // Open the specified manifest file.
  unique_ptr<SequentialFileReader> file_reader;
  Status s;
  {
    unique_ptr<SequentialFile> file;
    s = cenv_->NewSequentialFileCloud(bucket_prefix_, manifest_path, &file,
                                      EnvOptions());
    if (!s.ok()) {
      return s;
    }
    file_reader.reset(new SequentialFileReader(std::move(file)));
  }

  // create a callback that gets invoked whil looping through the log records
  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(NULL, std::move(file_reader), &reporter,
                     true /*checksum*/, 0 /*initial_offset*/, 0);

  Slice record;
  std::string scratch;
  int count = 0;

  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      break;
    }
    count++;

    // add the files that are added by this transaction
    std::vector<std::pair<int, FileMetaData>> new_files = edit.GetNewFiles();
    for (auto& one: new_files) {
      uint64_t num = one.second.fd.GetNumber();
      list->insert(num);
    }
    // delete the files that are removed by this transaction
    std::set<std::pair<int, uint64_t>> deleted_files = edit.GetDeletedFiles();
    for (auto& one: deleted_files) {
      uint64_t num = one.second;
      list->erase(num);
    }
  }
  file_reader.reset();
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[mn] manifest path %s has %d entries %s",
      manifest_path.c_str(), count, s.ToString().c_str());
  return s;
}

}
#endif /* ROCKSDB_LITE */
