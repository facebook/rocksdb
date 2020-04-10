// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/options_util.h"

#include "env/composite_env_wrapper.h"
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
  ConfigOptions options;
  options.ignore_unknown_options = ignore_unknown_options;
  options.input_strings_escaped = true;
  return LoadOptionsFromFile(file_name, env, options, db_options, cf_descs,
                             cache);
}

Status LoadOptionsFromFile(const std::string& file_name, Env* env,
                           const ConfigOptions& options, DBOptions* db_options,
                           std::vector<ColumnFamilyDescriptor>* cf_descs,
                           std::shared_ptr<Cache>* cache) {
  RocksDBOptionsParser parser;
  LegacyFileSystemWrapper fs(env);
  Status s = parser.Parse(file_name, &fs, options);
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
        auto* opts = tf->GetOptions<BlockBasedTableOptions>(
            TableFactory::kBlockBasedTableOpts);
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
                         std::vector<ColumnFamilyDescriptor>* cf_descs,
                         bool ignore_unknown_options,
                         std::shared_ptr<Cache>* cache) {
  ConfigOptions cfg_options;
  cfg_options.ignore_unknown_options = ignore_unknown_options;
  cfg_options.input_strings_escaped = true;

  return LoadLatestOptions(dbpath, env, cfg_options, db_options, cf_descs,
                           cache);
}

Status LoadLatestOptions(const std::string& dbpath, Env* env,
                         const ConfigOptions& cfg_options,
                         DBOptions* db_options,
                         std::vector<ColumnFamilyDescriptor>* cf_descs,
                         std::shared_ptr<Cache>* cache) {
  std::string options_file_name;
  Status s = GetLatestOptionsFileName(dbpath, env, &options_file_name);
  if (!s.ok()) {
    return s;
  }
  return LoadOptionsFromFile(dbpath + "/" + options_file_name, env, cfg_options,
                             db_options, cf_descs, cache);
}

Status CheckOptionsCompatibility(
    const std::string& dbpath, Env* env, const DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& cf_descs,
    bool ignore_unknown_options) {
  ConfigOptions options(db_options);
  options.sanity_level = ConfigOptions::kSanityLevelLooselyCompatible;
  options.ignore_unknown_options = ignore_unknown_options;
  options.input_strings_escaped = true;
  return CheckOptionsCompatibility(dbpath, env, db_options, cf_descs, options);
}

Status CheckOptionsCompatibility(
    const std::string& dbpath, Env* env, const DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& cf_descs,
    const ConfigOptions& cfg_options) {
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

  LegacyFileSystemWrapper fs(env);

  return RocksDBOptionsParser::VerifyRocksDBOptionsFromFile(
      db_options, cf_names, cf_opts, dbpath + "/" + options_file_name, &fs,
      cfg_options);
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
