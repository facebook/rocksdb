// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/options_util.h"

#include "file/filename.h"
#include "options/options_parser.h"
#include "rocksdb/convenience.h"
#include "rocksdb/options.h"
#include "table/block_based/block_based_table_factory.h"

namespace ROCKSDB_NAMESPACE {
Status LoadOptionsFromFile(const std::string& file_name, Env* env,
                           DBOptions* db_options,
                           std::vector<ColumnFamilyDescriptor>* cf_descs,
                           bool ignore_unknown_options,
                           std::shared_ptr<Cache>* cache) {
  ConfigOptions config_options;
  config_options.ignore_unknown_options = ignore_unknown_options;
  config_options.input_strings_escaped = true;
  config_options.env = env;

  return LoadOptionsFromFile(config_options, file_name, db_options, cf_descs,
                             cache);
}

Status LoadOptionsFromFile(const ConfigOptions& config_options,
                           const std::string& file_name, DBOptions* db_options,
                           std::vector<ColumnFamilyDescriptor>* cf_descs,
                           std::shared_ptr<Cache>* cache) {
  RocksDBOptionsParser parser;
  const auto& fs = config_options.env->GetFileSystem();
  Status s = parser.Parse(config_options, file_name, fs.get());
  if (!s.ok()) {
    return s;
  }
  *db_options = *parser.db_opt();
  const std::vector<std::string>& cf_names = *parser.cf_names();
  const std::vector<ColumnFamilyOptions>& cf_opts = *parser.cf_opts();
  cf_descs->clear();
  for (size_t i = 0; i < cf_opts.size(); ++i) {
    cf_descs->push_back({cf_names[i], cf_opts[i]});
    if (cache != nullptr) {
      TableFactory* tf = cf_opts[i].table_factory.get();
      if (tf != nullptr) {
        auto* opts = tf->GetOptions<BlockBasedTableOptions>();
        if (opts != nullptr) {
          opts->block_cache = *cache;
        }
      }
    }
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
  if (s.IsNotFound()) {
    return Status::NotFound(Status::kPathNotFound,
                            "No options files found in the DB directory.",
                            dbpath);
  } else if (!s.ok()) {
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
    return Status::NotFound(Status::kPathNotFound,
                            "No options files found in the DB directory.",
                            dbpath);
  }
  *options_file_name = latest_file_name;
  return Status::OK();
}

Status LoadLatestOptions(const std::string& dbpath, Env* env,
                         DBOptions* db_options,
                         std::vector<ColumnFamilyDescriptor>* cf_descs,
                         bool ignore_unknown_options,
                         std::shared_ptr<Cache>* cache) {
  ConfigOptions config_options;
  config_options.ignore_unknown_options = ignore_unknown_options;
  config_options.input_strings_escaped = true;
  config_options.env = env;

  return LoadLatestOptions(config_options, dbpath, db_options, cf_descs, cache);
}

Status LoadLatestOptions(const ConfigOptions& config_options,
                         const std::string& dbpath, DBOptions* db_options,
                         std::vector<ColumnFamilyDescriptor>* cf_descs,
                         std::shared_ptr<Cache>* cache) {
  std::string options_file_name;
  Status s =
      GetLatestOptionsFileName(dbpath, config_options.env, &options_file_name);
  if (!s.ok()) {
    return s;
  }
  return LoadOptionsFromFile(config_options, dbpath + "/" + options_file_name,
                             db_options, cf_descs, cache);
}

Status CheckOptionsCompatibility(
    const std::string& dbpath, Env* env, const DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& cf_descs,
    bool ignore_unknown_options) {
  ConfigOptions config_options(db_options);
  config_options.sanity_level = ConfigOptions::kSanityLevelLooselyCompatible;
  config_options.ignore_unknown_options = ignore_unknown_options;
  config_options.input_strings_escaped = true;
  config_options.env = env;
  return CheckOptionsCompatibility(config_options, dbpath, db_options,
                                   cf_descs);
}

Status CheckOptionsCompatibility(
    const ConfigOptions& config_options, const std::string& dbpath,
    const DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& cf_descs) {
  std::string options_file_name;
  Status s =
      GetLatestOptionsFileName(dbpath, config_options.env, &options_file_name);
  if (!s.ok()) {
    return s;
  }

  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_opts;
  for (const auto& cf_desc : cf_descs) {
    cf_names.push_back(cf_desc.name);
    cf_opts.push_back(cf_desc.options);
  }

  const auto& fs = config_options.env->GetFileSystem();

  return RocksDBOptionsParser::VerifyRocksDBOptionsFromFile(
      config_options, db_options, cf_names, cf_opts,
      dbpath + "/" + options_file_name, fs.get());
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
