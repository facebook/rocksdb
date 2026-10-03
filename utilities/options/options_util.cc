// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/options_util.h"

#include "db/version_set.h"
#include "file/filename.h"
#include "options/options_parser.h"
#include "rocksdb/convenience.h"
#include "rocksdb/options.h"
#include "table/block_based/block_based_table_factory.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
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

Status GetLatestOptionsFileName(const std::string& dbpath, Env* env,
                                std::string* options_file_name) {
  assert(env != nullptr);
  assert(options_file_name != nullptr);
  Status s;
  auto select_effective_options_file = [&](uint64_t file_number) {
    const std::string full_name = OptionsFileName(dbpath, file_number);
    Status exists_s = env->FileExists(full_name);
    if (!exists_s.ok()) {
      if (exists_s.IsNotFound() || exists_s.IsPathNotFound()) {
        return Status::Corruption(
            "MANIFEST references a missing effective OPTIONS file", full_name);
      }
      return exists_s;
    }
    *options_file_name = OptionsFileName(file_number);
    return Status::OK();
  };
  // A writer can prepare or commit while the directory is being scanned.
  // Replay the complete protocol state after the scan before selecting a file.
  TEST_SYNC_POINT("GetLatestOptionsFileName:AfterPointerAbsentManifestReplay");
  std::vector<std::string> file_names;
  s = env->GetChildren(dbpath, &file_names);
  if (s.IsNotFound()) {
    return Status::NotFound(Status::kPathNotFound,
                            "No options files found in the DB directory.",
                            dbpath);
  } else if (!s.ok()) {
    return s;
  }
  OptionsFileProtocolState protocol_state;
  s = VersionSet::GetOptionsFileProtocolState(
      dbpath, env->GetFileSystem().get(), &protocol_state);
  if (!s.ok()) {
    return s;
  }

  bool selected_by_manifest = false;
  const uint64_t selected_options_file_number =
      VersionSet::ResolveOptionsFileNumber(protocol_state, file_names,
                                           &selected_by_manifest);
  if (selected_by_manifest) {
    return select_effective_options_file(selected_options_file_number);
  }
  if (selected_options_file_number == 0) {
    return Status::NotFound(Status::kPathNotFound,
                            "No options files found in the DB directory.",
                            dbpath);
  }
  // Preserve the directory entry's spelling for legacy files. Historically
  // ParseFileName accepted non-canonical zero padding such as OPTIONS-0001.
  for (const std::string& file_name : file_names) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(file_name, &number, &type) && type == kOptionsFile &&
        number == selected_options_file_number) {
      *options_file_name = file_name;
      return Status::OK();
    }
  }
  *options_file_name = OptionsFileName(selected_options_file_number);
  return Status::OK();
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
