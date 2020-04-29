// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "options/options_parser.h"

#include <cmath>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "table/block_based/block_based_table_factory.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

static const std::string option_file_header =
    "# This is a RocksDB option file.\n"
    "#\n"
    "# For detailed file format spec, please refer to the example file\n"
    "# in examples/rocksdb_option_file_example.ini\n"
    "#\n"
    "\n";

Status PersistRocksDBOptions(const DBOptions& db_opt,
                             const std::vector<std::string>& cf_names,
                             const std::vector<ColumnFamilyOptions>& cf_opts,
                             const std::string& file_name, FileSystem* fs) {
  ConfigOptions
      config_options;  // Use default for escaped(true) and check (exact)
  config_options.delimiter = "\n  ";
  // If a readahead size was set in the input options, use it
  if (db_opt.log_readahead_size > 0) {
    config_options.file_readahead_size = db_opt.log_readahead_size;
  }
  return PersistRocksDBOptions(config_options, db_opt, cf_names, cf_opts,
                               file_name, fs);
}

Status PersistRocksDBOptions(const ConfigOptions& config_options_in,
                             const DBOptions& db_opt,
                             const std::vector<std::string>& cf_names,
                             const std::vector<ColumnFamilyOptions>& cf_opts,
                             const std::string& file_name, FileSystem* fs) {
  ConfigOptions config_options = config_options_in;
  config_options.delimiter = "\n  ";  // Override the default to nl

  TEST_SYNC_POINT("PersistRocksDBOptions:start");
  if (cf_names.size() != cf_opts.size()) {
    return Status::InvalidArgument(
        "cf_names.size() and cf_opts.size() must be the same");
  }
  std::unique_ptr<FSWritableFile> wf;

  Status s =
      fs->NewWritableFile(file_name, FileOptions(), &wf, nullptr);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<WritableFileWriter> writable;
  writable.reset(new WritableFileWriter(std::move(wf), file_name, EnvOptions(),
                                        nullptr /* statistics */));

  std::string options_file_content;

  writable->Append(option_file_header + "[" +
                   opt_section_titles[kOptionSectionVersion] +
                   "]\n"
                   "  rocksdb_version=" +
                   ToString(ROCKSDB_MAJOR) + "." + ToString(ROCKSDB_MINOR) +
                   "." + ToString(ROCKSDB_PATCH) + "\n");
  writable->Append("  options_file_version=" +
                   ToString(ROCKSDB_OPTION_FILE_MAJOR) + "." +
                   ToString(ROCKSDB_OPTION_FILE_MINOR) + "\n");
  writable->Append("\n[" + opt_section_titles[kOptionSectionDBOptions] +
                   "]\n  ");

  s = GetStringFromDBOptions(config_options, db_opt, &options_file_content);
  if (!s.ok()) {
    writable->Close();
    return s;
  }
  writable->Append(options_file_content + "\n");

  for (size_t i = 0; i < cf_opts.size(); ++i) {
    // CFOptions section
    writable->Append("\n[" + opt_section_titles[kOptionSectionCFOptions] +
                     " \"" + EscapeOptionString(cf_names[i]) + "\"]\n  ");
    s = GetStringFromColumnFamilyOptions(config_options, cf_opts[i],
                                         &options_file_content);
    if (!s.ok()) {
      writable->Close();
      return s;
    }
    writable->Append(options_file_content + "\n");
    // TableOptions section
    auto* tf = cf_opts[i].table_factory.get();
    if (tf != nullptr) {
      writable->Append("[" + opt_section_titles[kOptionSectionTableOptions] +
                       tf->Name() + " \"" + EscapeOptionString(cf_names[i]) +
                       "\"]\n  ");
      options_file_content.clear();
      s = tf->GetOptionString(config_options, &options_file_content);
      if (!s.ok()) {
        return s;
      }
      writable->Append(options_file_content + "\n");
    }
  }
  writable->Sync(true /* use_fsync */);
  writable->Close();

  return RocksDBOptionsParser::VerifyRocksDBOptionsFromFile(
      config_options, db_opt, cf_names, cf_opts, file_name, fs);
}

RocksDBOptionsParser::RocksDBOptionsParser() { Reset(); }

void RocksDBOptionsParser::Reset() {
  db_opt_ = DBOptions();
  db_opt_map_.clear();
  cf_names_.clear();
  cf_opts_.clear();
  cf_opt_maps_.clear();
  has_version_section_ = false;
  has_db_options_ = false;
  has_default_cf_options_ = false;
  for (int i = 0; i < 3; ++i) {
    db_version[i] = 0;
    opt_file_version[i] = 0;
  }
}

bool RocksDBOptionsParser::IsSection(const std::string& line) {
  if (line.size() < 2) {
    return false;
  }
  if (line[0] != '[' || line[line.size() - 1] != ']') {
    return false;
  }
  return true;
}

Status RocksDBOptionsParser::ParseSection(OptionSection* section,
                                          std::string* title,
                                          std::string* argument,
                                          const std::string& line,
                                          const int line_num) {
  *section = kOptionSectionUnknown;
  // A section is of the form [<SectionName> "<SectionArg>"], where
  // "<SectionArg>" is optional.
  size_t arg_start_pos = line.find("\"");
  size_t arg_end_pos = line.rfind("\"");
  // The following if-then check tries to identify whether the input
  // section has the optional section argument.
  if (arg_start_pos != std::string::npos && arg_start_pos != arg_end_pos) {
    *title = TrimAndRemoveComment(line.substr(1, arg_start_pos - 1), true);
    *argument = UnescapeOptionString(
        line.substr(arg_start_pos + 1, arg_end_pos - arg_start_pos - 1));
  } else {
    *title = TrimAndRemoveComment(line.substr(1, line.size() - 2), true);
    *argument = "";
  }
  for (int i = 0; i < kOptionSectionUnknown; ++i) {
    if (title->find(opt_section_titles[i]) == 0) {
      if (i == kOptionSectionVersion || i == kOptionSectionDBOptions ||
          i == kOptionSectionCFOptions) {
        if (title->size() == opt_section_titles[i].size()) {
          // if true, then it indicats equal
          *section = static_cast<OptionSection>(i);
          return CheckSection(*section, *argument, line_num);
        }
      } else if (i == kOptionSectionTableOptions) {
        // This type of sections has a sufffix at the end of the
        // section title
        if (title->size() > opt_section_titles[i].size()) {
          *section = static_cast<OptionSection>(i);
          return CheckSection(*section, *argument, line_num);
        }
      }
    }
  }
  return Status::InvalidArgument(std::string("Unknown section ") + line);
}

Status RocksDBOptionsParser::InvalidArgument(const int line_num,
                                             const std::string& message) {
  return Status::InvalidArgument(
      "[RocksDBOptionsParser Error] ",
      message + " (at line " + ToString(line_num) + ")");
}

Status RocksDBOptionsParser::ParseStatement(std::string* name,
                                            std::string* value,
                                            const std::string& line,
                                            const int line_num) {
  size_t eq_pos = line.find("=");
  if (eq_pos == std::string::npos) {
    return InvalidArgument(line_num, "A valid statement must have a '='.");
  }

  *name = TrimAndRemoveComment(line.substr(0, eq_pos), true);
  *value =
      TrimAndRemoveComment(line.substr(eq_pos + 1, line.size() - eq_pos - 1));
  if (name->empty()) {
    return InvalidArgument(line_num,
                           "A valid statement must have a variable name.");
  }
  return Status::OK();
}

Status RocksDBOptionsParser::Parse(const std::string& file_name, FileSystem* fs,
                                   bool ignore_unknown_options,
                                   size_t file_readahead_size) {
  ConfigOptions
      config_options;  // Use default for escaped(true) and check (exact)
  config_options.ignore_unknown_options = ignore_unknown_options;
  if (file_readahead_size > 0) {
    config_options.file_readahead_size = file_readahead_size;
  }
  return Parse(config_options, file_name, fs);
}

Status RocksDBOptionsParser::Parse(const ConfigOptions& config_options_in,
                                   const std::string& file_name,
                                   FileSystem* fs) {
  Reset();
  ConfigOptions config_options = config_options_in;

  std::unique_ptr<FSSequentialFile> seq_file;
  Status s = fs->NewSequentialFile(file_name, FileOptions(), &seq_file,
                                   nullptr);
  if (!s.ok()) {
    return s;
  }
  SequentialFileReader sf_reader(std::move(seq_file), file_name,
                                 config_options.file_readahead_size);

  OptionSection section = kOptionSectionUnknown;
  std::string title;
  std::string argument;
  std::unordered_map<std::string, std::string> opt_map;
  std::istringstream iss;
  std::string line;
  bool has_data = true;
  // we only support single-lined statement.
  for (int line_num = 1; ReadOneLine(&iss, &sf_reader, &line, &has_data, &s);
       ++line_num) {
    if (!s.ok()) {
      return s;
    }
    line = TrimAndRemoveComment(line);
    if (line.empty()) {
      continue;
    }
    if (IsSection(line)) {
      s = EndSection(config_options, section, title, argument, opt_map);
      opt_map.clear();
      if (!s.ok()) {
        return s;
      }

      // If the option file is not generated by a higher minor version,
      // there shouldn't be any unknown option.
      if (config_options.ignore_unknown_options &&
          section == kOptionSectionVersion) {
        if (db_version[0] < ROCKSDB_MAJOR || (db_version[0] == ROCKSDB_MAJOR &&
                                              db_version[1] <= ROCKSDB_MINOR)) {
          config_options.ignore_unknown_options = false;
        }
      }

      s = ParseSection(&section, &title, &argument, line, line_num);
      if (!s.ok()) {
        return s;
      }
    } else {
      std::string name;
      std::string value;
      s = ParseStatement(&name, &value, line, line_num);
      if (!s.ok()) {
        return s;
      }
      opt_map.insert({name, value});
    }
  }

  s = EndSection(config_options, section, title, argument, opt_map);
  opt_map.clear();
  if (!s.ok()) {
    return s;
  }
  return ValidityCheck();
}

Status RocksDBOptionsParser::CheckSection(const OptionSection section,
                                          const std::string& section_arg,
                                          const int line_num) {
  if (section == kOptionSectionDBOptions) {
    if (has_db_options_) {
      return InvalidArgument(
          line_num,
          "More than one DBOption section found in the option config file");
    }
    has_db_options_ = true;
  } else if (section == kOptionSectionCFOptions) {
    bool is_default_cf = (section_arg == kDefaultColumnFamilyName);
    if (cf_opts_.size() == 0 && !is_default_cf) {
      return InvalidArgument(
          line_num,
          "Default column family must be the first CFOptions section "
          "in the option config file");
    } else if (cf_opts_.size() != 0 && is_default_cf) {
      return InvalidArgument(
          line_num,
          "Default column family must be the first CFOptions section "
          "in the optio/n config file");
    } else if (GetCFOptions(section_arg) != nullptr) {
      return InvalidArgument(
          line_num,
          "Two identical column families found in option config file");
    }
    has_default_cf_options_ |= is_default_cf;
  } else if (section == kOptionSectionTableOptions) {
    if (GetCFOptions(section_arg) == nullptr) {
      return InvalidArgument(
          line_num, std::string(
                        "Does not find a matched column family name in "
                        "TableOptions section.  Column Family Name:") +
                        section_arg);
    }
  } else if (section == kOptionSectionVersion) {
    if (has_version_section_) {
      return InvalidArgument(
          line_num,
          "More than one Version section found in the option config file.");
    }
    has_version_section_ = true;
  }
  return Status::OK();
}

Status RocksDBOptionsParser::ParseVersionNumber(const std::string& ver_name,
                                                const std::string& ver_string,
                                                const int max_count,
                                                int* version) {
  int version_index = 0;
  int current_number = 0;
  int current_digit_count = 0;
  bool has_dot = false;
  for (int i = 0; i < max_count; ++i) {
    version[i] = 0;
  }
  constexpr int kBufferSize = 200;
  char buffer[kBufferSize];
  for (size_t i = 0; i < ver_string.size(); ++i) {
    if (ver_string[i] == '.') {
      if (version_index >= max_count - 1) {
        snprintf(buffer, sizeof(buffer) - 1,
                 "A valid %s can only contains at most %d dots.",
                 ver_name.c_str(), max_count - 1);
        return Status::InvalidArgument(buffer);
      }
      if (current_digit_count == 0) {
        snprintf(buffer, sizeof(buffer) - 1,
                 "A valid %s must have at least one digit before each dot.",
                 ver_name.c_str());
        return Status::InvalidArgument(buffer);
      }
      version[version_index++] = current_number;
      current_number = 0;
      current_digit_count = 0;
      has_dot = true;
    } else if (isdigit(ver_string[i])) {
      current_number = current_number * 10 + (ver_string[i] - '0');
      current_digit_count++;
    } else {
      snprintf(buffer, sizeof(buffer) - 1,
               "A valid %s can only contains dots and numbers.",
               ver_name.c_str());
      return Status::InvalidArgument(buffer);
    }
  }
  version[version_index] = current_number;
  if (has_dot && current_digit_count == 0) {
    snprintf(buffer, sizeof(buffer) - 1,
             "A valid %s must have at least one digit after each dot.",
             ver_name.c_str());
    return Status::InvalidArgument(buffer);
  }
  return Status::OK();
}

Status RocksDBOptionsParser::EndSection(
    const ConfigOptions& config_options, const OptionSection section,
    const std::string& section_title, const std::string& section_arg,
    const std::unordered_map<std::string, std::string>& opt_map) {
  Status s;
  if (section == kOptionSectionDBOptions) {
    s = GetDBOptionsFromMap(config_options, DBOptions(), opt_map, &db_opt_);
    if (!s.ok()) {
      return s;
    }
    db_opt_map_ = opt_map;
  } else if (section == kOptionSectionCFOptions) {
    // This condition should be ensured earlier in ParseSection
    // so we make an assertion here.
    assert(GetCFOptions(section_arg) == nullptr);
    cf_names_.emplace_back(section_arg);
    cf_opts_.emplace_back();
    s = GetColumnFamilyOptionsFromMap(config_options, ColumnFamilyOptions(),
                                      opt_map, &cf_opts_.back());
    if (!s.ok()) {
      return s;
    }
    // keep the parsed string.
    cf_opt_maps_.emplace_back(opt_map);
  } else if (section == kOptionSectionTableOptions) {
    assert(GetCFOptions(section_arg) != nullptr);
    auto* cf_opt = GetCFOptionsImpl(section_arg);
    if (cf_opt == nullptr) {
      return Status::InvalidArgument(
          "The specified column family must be defined before the "
          "TableOptions section:",
          section_arg);
    }
    // Ignore error as table factory deserialization is optional
    s = GetTableFactoryFromMap(
        config_options,
        section_title.substr(
            opt_section_titles[kOptionSectionTableOptions].size()),
        opt_map, &(cf_opt->table_factory));
    if (!s.ok()) {
      return s;
    }
  } else if (section == kOptionSectionVersion) {
    for (const auto& pair : opt_map) {
      if (pair.first == "rocksdb_version") {
        s = ParseVersionNumber(pair.first, pair.second, 3, db_version);
        if (!s.ok()) {
          return s;
        }
      } else if (pair.first == "options_file_version") {
        s = ParseVersionNumber(pair.first, pair.second, 2, opt_file_version);
        if (!s.ok()) {
          return s;
        }
        if (opt_file_version[0] < 1) {
          return Status::InvalidArgument(
              "A valid options_file_version must be at least 1.");
        }
      }
    }
  }
  return Status::OK();
}

Status RocksDBOptionsParser::ValidityCheck() {
  if (!has_db_options_) {
    return Status::Corruption(
        "A RocksDB Option file must have a single DBOptions section");
  }
  if (!has_default_cf_options_) {
    return Status::Corruption(
        "A RocksDB Option file must have a single CFOptions:default section");
  }

  return Status::OK();
}

std::string RocksDBOptionsParser::TrimAndRemoveComment(const std::string& line,
                                                       bool trim_only) {
  size_t start = 0;
  size_t end = line.size();

  // we only support "#" style comment
  if (!trim_only) {
    size_t search_pos = 0;
    while (search_pos < line.size()) {
      size_t comment_pos = line.find('#', search_pos);
      if (comment_pos == std::string::npos) {
        break;
      }
      if (comment_pos == 0 || line[comment_pos - 1] != '\\') {
        end = comment_pos;
        break;
      }
      search_pos = comment_pos + 1;
    }
  }

  while (start < end && isspace(line[start]) != 0) {
    ++start;
  }

  // start < end implies end > 0.
  while (start < end && isspace(line[end - 1]) != 0) {
    --end;
  }

  if (start < end) {
    return line.substr(start, end - start);
  }

  return "";
}

Status RocksDBOptionsParser::VerifyRocksDBOptionsFromFile(
    const ConfigOptions& config_options, const DBOptions& db_opt,
    const std::vector<std::string>& cf_names,
    const std::vector<ColumnFamilyOptions>& cf_opts,
    const std::string& file_name, FileSystem* fs) {
  RocksDBOptionsParser parser;
  Status s = parser.Parse(config_options, file_name, fs);
  if (!s.ok()) {
    return s;
  }

  // Verify DBOptions
  s = VerifyDBOptions(config_options, db_opt, *parser.db_opt(),
                      parser.db_opt_map());
  if (!s.ok()) {
    return s;
  }

  // Verify ColumnFamily Name
  if (cf_names.size() != parser.cf_names()->size()) {
    if (config_options.sanity_level >=
        ConfigOptions::kSanityLevelLooselyCompatible) {
      return Status::InvalidArgument(
          "[RocksDBOptionParser Error] The persisted options does not have "
          "the same number of column family names as the db instance.");
    } else if (cf_opts.size() > parser.cf_opts()->size()) {
      return Status::InvalidArgument(
          "[RocksDBOptionsParser Error]",
          "The persisted options file has less number of column family "
          "names than that of the specified one.");
    }
  }
  for (size_t i = 0; i < cf_names.size(); ++i) {
    if (cf_names[i] != parser.cf_names()->at(i)) {
      return Status::InvalidArgument(
          "[RocksDBOptionParser Error] The persisted options and the db"
          "instance does not have the same name for column family ",
          ToString(i));
    }
  }

  // Verify Column Family Options
  if (cf_opts.size() != parser.cf_opts()->size()) {
    if (config_options.sanity_level >=
        ConfigOptions::kSanityLevelLooselyCompatible) {
      return Status::InvalidArgument(
          "[RocksDBOptionsParser Error]",
          "The persisted options does not have the same number of "
          "column families as the db instance.");
    } else if (cf_opts.size() > parser.cf_opts()->size()) {
      return Status::InvalidArgument(
          "[RocksDBOptionsParser Error]",
          "The persisted options file has less number of column families "
          "than that of the specified number.");
    }
  }
  for (size_t i = 0; i < cf_opts.size(); ++i) {
    s = VerifyCFOptions(config_options, cf_opts[i], parser.cf_opts()->at(i),
                        &(parser.cf_opt_maps()->at(i)));
    if (!s.ok()) {
      return s;
    }
    s = VerifyTableFactory(config_options, cf_opts[i].table_factory.get(),
                           parser.cf_opts()->at(i).table_factory.get());
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status RocksDBOptionsParser::VerifyDBOptions(
    const ConfigOptions& config_options, const DBOptions& base_opt,
    const DBOptions& file_opt,
    const std::unordered_map<std::string, std::string>* /*opt_map*/) {
  for (const auto& pair : db_options_type_info) {
    const auto& opt_info = pair.second;
    if (config_options.IsCheckEnabled(opt_info.GetSanityLevel())) {
      const char* base_addr =
          reinterpret_cast<const char*>(&base_opt) + opt_info.offset;
      const char* file_addr =
          reinterpret_cast<const char*>(&file_opt) + opt_info.offset;
      std::string mismatch;
      if (!opt_info.MatchesOption(config_options, pair.first, base_addr,
                                  file_addr, &mismatch) &&
          !opt_info.MatchesByName(config_options, pair.first, base_addr,
                                  file_addr)) {
        const size_t kBufferSize = 2048;
        char buffer[kBufferSize];
        std::string base_value;
        std::string file_value;
        opt_info.SerializeOption(config_options, pair.first, base_addr,
                                 &base_value);
        opt_info.SerializeOption(config_options, pair.first, file_addr,
                                 &file_value);
        snprintf(buffer, sizeof(buffer),
                 "[RocksDBOptionsParser]: "
                 "failed the verification on DBOptions::%s --- "
                 "The specified one is %s while the persisted one is %s.\n",
                 pair.first.c_str(), base_value.c_str(), file_value.c_str());
        return Status::InvalidArgument(Slice(buffer, strlen(buffer)));
      }
    }
  }
  return Status::OK();
}

Status RocksDBOptionsParser::VerifyCFOptions(
    const ConfigOptions& config_options, const ColumnFamilyOptions& base_opt,
    const ColumnFamilyOptions& file_opt,
    const std::unordered_map<std::string, std::string>* opt_map) {
  for (const auto& pair : cf_options_type_info) {
    const auto& opt_info = pair.second;

    if (config_options.IsCheckEnabled(opt_info.GetSanityLevel())) {
      std::string mismatch;
      const char* base_addr =
          reinterpret_cast<const char*>(&base_opt) + opt_info.offset;
      const char* file_addr =
          reinterpret_cast<const char*>(&file_opt) + opt_info.offset;
      bool matches = opt_info.MatchesOption(config_options, pair.first,
                                            base_addr, file_addr, &mismatch);
      if (!matches && opt_info.IsByName()) {
        if (opt_map == nullptr) {
          matches = true;
        } else {
          auto iter = opt_map->find(pair.first);
          if (iter == opt_map->end()) {
            matches = true;
          } else {
            matches = opt_info.MatchesByName(config_options, pair.first,
                                             base_addr, iter->second);
          }
        }
      }
      if (!matches) {
        // The options do not match
        const size_t kBufferSize = 2048;
        char buffer[kBufferSize];
        std::string base_value;
        std::string file_value;
        opt_info.SerializeOption(config_options, pair.first, base_addr,
                                 &base_value);
        opt_info.SerializeOption(config_options, pair.first, file_addr,
                                 &file_value);
        snprintf(buffer, sizeof(buffer),
                 "[RocksDBOptionsParser]: "
                 "failed the verification on ColumnFamilyOptions::%s --- "
                 "The specified one is %s while the persisted one is %s.\n",
                 pair.first.c_str(), base_value.c_str(), file_value.c_str());
        return Status::InvalidArgument(Slice(buffer, sizeof(buffer)));
      }  // if (! matches)
    }    // CheckSanityLevel
  }      // For each option
  return Status::OK();
}

Status RocksDBOptionsParser::VerifyTableFactory(
    const ConfigOptions& config_options, const TableFactory* base_tf,
    const TableFactory* file_tf) {
  if (base_tf && file_tf) {
    if (config_options.sanity_level > ConfigOptions::kSanityLevelNone &&
        std::string(base_tf->Name()) != std::string(file_tf->Name())) {
      return Status::Corruption(
          "[RocksDBOptionsParser]: "
          "failed the verification on TableFactory->Name()");
    }
    if (base_tf->Name() == BlockBasedTableFactory::kName) {
      return VerifyBlockBasedTableFactory(
          config_options,
          static_cast_with_check<const BlockBasedTableFactory,
                                 const TableFactory>(base_tf),
          static_cast_with_check<const BlockBasedTableFactory,
                                 const TableFactory>(file_tf));
    }
    // TODO(yhchiang): add checks for other table factory types
  } else {
    // TODO(yhchiang): further support sanity check here
  }
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE
