// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/options_util.h"

#include "db/filename.h"
#include "rocksdb/options.h"
#include "util/options_parser.h"

namespace rocksdb {
Status LoadOptionsFromFile(const std::string& file_name, Env* env,
                           DBOptions* db_options,
                           std::vector<ColumnFamilyDescriptor>* cf_descs) {
  RocksDBOptionsParser parser;
  Status s = parser.Parse(file_name, env);
  if (!s.ok()) {
    return s;
  }

  *db_options = *parser.db_opt();

  const std::vector<std::string>& cf_names = *parser.cf_names();
  const std::vector<ColumnFamilyOptions>& cf_opts = *parser.cf_opts();
  cf_descs->clear();
  for (size_t i = 0; i < cf_opts.size(); ++i) {
    cf_descs->push_back({cf_names[i], cf_opts[i]});
  }
  return Status::OK();
}

Status GetLatestOptionsFileName(const std::string& dbpath,
                                Env* env, std::string* options_file_name) {
  Status s;
  std::string latest_file_name;
  uint64_t latest_time_stamp = 0;
  std::vector<std::string> file_names;
  s = env->GetChildren(dbpath, &file_names);
  if (!s.ok()) {
    return s;
  }
  for (auto& file_name : file_names) {
    uint64_t time_stamp;
    FileType type;
    if (ParseFileName(file_name, &time_stamp, &type) && type == kOptionsFile) {
      if (time_stamp > latest_time_stamp) {
        latest_time_stamp = time_stamp;
        latest_file_name = file_name;
      }
    }
  }
  if (latest_file_name.size() == 0) {
    return Status::NotFound("No options files found in the DB directory.");
  }
  *options_file_name = latest_file_name;
  return Status::OK();
}

Status LoadLatestOptions(const std::string& dbpath, Env* env,
                         DBOptions* db_options,
                         std::vector<ColumnFamilyDescriptor>* cf_descs) {
  std::string options_file_name;
  Status s = GetLatestOptionsFileName(dbpath, env, &options_file_name);
  if (!s.ok()) {
    return s;
  }

  return LoadOptionsFromFile(dbpath + "/" + options_file_name, env,
                             db_options, cf_descs);
}

Status CheckOptionsCompatibility(
    const std::string& dbpath, Env* env, const DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& cf_descs) {
  std::string options_file_name;
  Status s = GetLatestOptionsFileName(dbpath, env, &options_file_name);
  if (!s.ok()) {
    return s;
  }

  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_opts;
  for (const auto& cf_desc : cf_descs) {
    cf_names.push_back(cf_desc.name);
    cf_opts.push_back(cf_desc.options);
  }

  const OptionsSanityCheckLevel kDefaultLevel = kSanityLevelLooselyCompatible;

  return RocksDBOptionsParser::VerifyRocksDBOptionsFromFile(
      db_options, cf_names, cf_opts, dbpath + "/" + options_file_name, env,
      kDefaultLevel);
}

}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
