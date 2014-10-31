//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <map>
#include <vector>
#include <utility>

#include "db/version_set.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/ldb_tool.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/db_ttl.h"
#include "util/logging.h"
#include "util/ldb_cmd_execute_result.h"
#include "util/string_util.h"
#include "utilities/ttl/db_ttl_impl.h"

namespace rocksdb {

class LDBCommand {
 public:

  // Command-line arguments
  static const std::string ARG_DB;
  static const std::string ARG_HEX;
  static const std::string ARG_KEY_HEX;
  static const std::string ARG_VALUE_HEX;
  static const std::string ARG_TTL;
  static const std::string ARG_TTL_START;
  static const std::string ARG_TTL_END;
  static const std::string ARG_TIMESTAMP;
  static const std::string ARG_FROM;
  static const std::string ARG_TO;
  static const std::string ARG_MAX_KEYS;
  static const std::string ARG_BLOOM_BITS;
  static const std::string ARG_FIX_PREFIX_LEN;
  static const std::string ARG_COMPRESSION_TYPE;
  static const std::string ARG_BLOCK_SIZE;
  static const std::string ARG_AUTO_COMPACTION;
  static const std::string ARG_WRITE_BUFFER_SIZE;
  static const std::string ARG_FILE_SIZE;
  static const std::string ARG_CREATE_IF_MISSING;

  static LDBCommand* InitFromCmdLineArgs(
    const std::vector<std::string>& args,
    const Options& options,
    const LDBOptions& ldb_options
  );

  static LDBCommand* InitFromCmdLineArgs(
    int argc,
    char** argv,
    const Options& options,
    const LDBOptions& ldb_options
  );

  bool ValidateCmdLineOptions();

  virtual Options PrepareOptionsForOpenDB();

  virtual void SetDBOptions(Options options) {
    options_ = options;
  }

  void SetLDBOptions(const LDBOptions& ldb_options) {
    ldb_options_ = ldb_options;
  }

  virtual bool NoDBOpen() {
    return false;
  }

  virtual ~LDBCommand() {
    if (db_ != nullptr) {
      delete db_;
      db_ = nullptr;
    }
  }

  /* Run the command, and return the execute result. */
  void Run() {
    if (!exec_state_.IsNotStarted()) {
      return;
    }

    if (db_ == nullptr && !NoDBOpen()) {
      OpenDB();
      if (!exec_state_.IsNotStarted()) {
        return;
      }
    }

    DoCommand();
    if (exec_state_.IsNotStarted()) {
      exec_state_ = LDBCommandExecuteResult::SUCCEED("");
    }

    if (db_ != nullptr) {
      CloseDB ();
    }
  }

  virtual void DoCommand() = 0;

  LDBCommandExecuteResult GetExecuteState() {
    return exec_state_;
  }

  void ClearPreviousRunState() {
    exec_state_.Reset();
  }

  static std::string HexToString(const std::string& str) {
    std::string parsed;
    if (str[0] != '0' || str[1] != 'x') {
      fprintf(stderr, "Invalid hex input %s.  Must start with 0x\n",
              str.c_str());
      throw "Invalid hex input";
    }

    for (unsigned int i = 2; i < str.length();) {
      int c;
      sscanf(str.c_str() + i, "%2X", &c);
      parsed.push_back(c);
      i += 2;
    }
    return parsed;
  }

  static std::string StringToHex(const std::string& str) {
    std::string result = "0x";
    char buf[10];
    for (size_t i = 0; i < str.length(); i++) {
      snprintf(buf, 10, "%02X", (unsigned char)str[i]);
      result += buf;
    }
    return result;
  }

  static const char* DELIM;

protected:

  LDBCommandExecuteResult exec_state_;
  std::string db_path_;
  DB* db_;
  DBWithTTL* db_ttl_;

  /**
   * true implies that this command can work if the db is opened in read-only
   * mode.
   */
  bool is_read_only_;

  /** If true, the key is input/output as hex in get/put/scan/delete etc. */
  bool is_key_hex_;

  /** If true, the value is input/output as hex in get/put/scan/delete etc. */
  bool is_value_hex_;

  /** If true, the value is treated as timestamp suffixed */
  bool is_db_ttl_;

  // If true, the kvs are output with their insert/modify timestamp in a ttl db
  bool timestamp_;

  /**
   * Map of options passed on the command-line.
   */
  const std::map<std::string, std::string> option_map_;

  /**
   * Flags passed on the command-line.
   */
  const std::vector<std::string> flags_;

  /** List of command-line options valid for this command */
  const std::vector<std::string> valid_cmd_line_options_;

  bool ParseKeyValue(const std::string& line,
                     std::string* key, std::string* value,
                     bool is_key_hex, bool is_value_hex);

  LDBCommand(const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags,
             bool is_read_only,
             const std::vector<std::string>& valid_cmd_line_options) :
      db_(nullptr),
      is_read_only_(is_read_only),
      is_key_hex_(false),
      is_value_hex_(false),
      is_db_ttl_(false),
      timestamp_(false),
      option_map_(options),
      flags_(flags),
      valid_cmd_line_options_(valid_cmd_line_options) {

    auto itr = options.find(ARG_DB);
    if (itr != options.end()) {
      db_path_ = itr->second;
    }

    is_key_hex_ = IsKeyHex(options, flags);
    is_value_hex_ = IsValueHex(options, flags);
    is_db_ttl_ = IsFlagPresent(flags, ARG_TTL);
    timestamp_ = IsFlagPresent(flags, ARG_TIMESTAMP);
  }

  void OpenDB() {
    Options opt = PrepareOptionsForOpenDB();
    if (!exec_state_.IsNotStarted()) {
      return;
    }
    // Open the DB.
    Status st;
    if (is_db_ttl_) {
      if (is_read_only_) {
        st = DBWithTTL::Open(opt, db_path_, &db_ttl_, 0, true);
      } else {
        st = DBWithTTL::Open(opt, db_path_, &db_ttl_);
      }
      db_ = db_ttl_;
    } else if (is_read_only_) {
      st = DB::OpenForReadOnly(opt, db_path_, &db_);
    } else {
      st = DB::Open(opt, db_path_, &db_);
    }
    if (!st.ok()) {
      std::string msg = st.ToString();
      exec_state_ = LDBCommandExecuteResult::FAILED(msg);
    }

    options_ = opt;
  }

  void CloseDB () {
    if (db_ != nullptr) {
      delete db_;
      db_ = nullptr;
    }
  }

  static std::string PrintKeyValue(
      const std::string& key, const std::string& value,
      bool is_key_hex, bool is_value_hex) {
    std::string result;
    result.append(is_key_hex ? StringToHex(key) : key);
    result.append(DELIM);
    result.append(is_value_hex ? StringToHex(value) : value);
    return result;
  }

  static std::string PrintKeyValue(
      const std::string& key, const std::string& value,
      bool is_hex) {
    return PrintKeyValue(key, value, is_hex, is_hex);
  }

  /**
   * Return true if the specified flag is present in the specified
   * flags vector
   */
  static bool IsFlagPresent(
      const std::vector<std::string>& flags, const std::string& flag) {
    return (std::find(flags.begin(), flags.end(), flag) != flags.end());
  }

  static std::string HelpRangeCmdArgs() {
    std::ostringstream str_stream;
    str_stream << " ";
    str_stream << "[--" << ARG_FROM << "] ";
    str_stream << "[--" << ARG_TO << "] ";
    return str_stream.str();
  }

  /**
   * A helper function that returns a list of command line options
   * used by this command.  It includes the common options and the ones
   * passed in.
   */
  std::vector<std::string> BuildCmdLineOptions(
      std::vector<std::string> options) {
    std::vector<std::string> ret = {
        ARG_DB,               ARG_BLOOM_BITS,
        ARG_BLOCK_SIZE,       ARG_AUTO_COMPACTION,
        ARG_COMPRESSION_TYPE, ARG_WRITE_BUFFER_SIZE,
        ARG_FILE_SIZE,        ARG_FIX_PREFIX_LEN};
    ret.insert(ret.end(), options.begin(), options.end());
    return ret;
  }

  bool ParseIntOption(const std::map<std::string, std::string>& options,
                      const std::string& option,
                      int* value, LDBCommandExecuteResult* exec_state);

  bool ParseStringOption(const std::map<std::string, std::string>& options,
                         const std::string& option, std::string* value);

  Options options_;
  LDBOptions ldb_options_;

 private:

  /**
   * Interpret command line options and flags to determine if the key
   * should be input/output in hex.
   */
  bool IsKeyHex(const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags) {
    return (IsFlagPresent(flags, ARG_HEX) ||
        IsFlagPresent(flags, ARG_KEY_HEX) ||
        ParseBooleanOption(options, ARG_HEX, false) ||
        ParseBooleanOption(options, ARG_KEY_HEX, false));
  }

  /**
   * Interpret command line options and flags to determine if the value
   * should be input/output in hex.
   */
  bool IsValueHex(const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags) {
    return (IsFlagPresent(flags, ARG_HEX) ||
          IsFlagPresent(flags, ARG_VALUE_HEX) ||
          ParseBooleanOption(options, ARG_HEX, false) ||
          ParseBooleanOption(options, ARG_VALUE_HEX, false));
  }

  /**
   * Returns the value of the specified option as a boolean.
   * default_val is used if the option is not found in options.
   * Throws an exception if the value of the option is not
   * "true" or "false" (case insensitive).
   */
  bool ParseBooleanOption(
      const std::map<std::string, std::string>& options,
      const std::string& option, bool default_val) {

    auto itr = options.find(option);
    if (itr != options.end()) {
      std::string option_val = itr->second;
      return StringToBool(itr->second);
    }
    return default_val;
  }

  /**
   * Converts val to a boolean.
   * val must be either true or false (case insensitive).
   * Otherwise an exception is thrown.
   */
  bool StringToBool(std::string val) {
    std::transform(val.begin(), val.end(), val.begin(), ::tolower);
    if (val == "true") {
      return true;
    } else if (val == "false") {
      return false;
    } else {
      throw "Invalid value for boolean argument";
    }
  }

  static LDBCommand* SelectCommand(
    const std::string& cmd,
    const std::vector<std::string>& cmdParams,
    const std::map<std::string, std::string>& option_map,
    const std::vector<std::string>& flags
  );

};

class CompactorCommand: public LDBCommand {
 public:
  static std::string Name() { return "compact"; }

  CompactorCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  static void Help(std::string* ret);

  virtual void DoCommand();

 private:
  bool null_from_;
  std::string from_;
  bool null_to_;
  std::string to_;
};

class DBDumperCommand: public LDBCommand {
 public:
  static std::string Name() { return "dump"; }

  DBDumperCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  static void Help(std::string* ret);

  virtual void DoCommand();

 private:
  bool null_from_;
  std::string from_;
  bool null_to_;
  std::string to_;
  uint64_t max_keys_;
  std::string delim_;
  bool count_only_;
  bool count_delim_;
  bool print_stats_;

  static const std::string ARG_COUNT_ONLY;
  static const std::string ARG_COUNT_DELIM;
  static const std::string ARG_STATS;
  static const std::string ARG_TTL_BUCKET;
};

class InternalDumpCommand: public LDBCommand {
 public:
  static std::string Name() { return "idump"; }

  InternalDumpCommand(const std::vector<std::string>& params,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::string>& flags);

  static void Help(std::string* ret);

  virtual void DoCommand();

 private:
  bool has_from_;
  std::string from_;
  bool has_to_;
  std::string to_;
  int max_keys_;
  std::string delim_;
  bool count_only_;
  bool count_delim_;
  bool print_stats_;
  bool is_input_key_hex_;

  static const std::string ARG_DELIM;
  static const std::string ARG_COUNT_ONLY;
  static const std::string ARG_COUNT_DELIM;
  static const std::string ARG_STATS;
  static const std::string ARG_INPUT_KEY_HEX;
};

class DBLoaderCommand: public LDBCommand {
 public:
  static std::string Name() { return "load"; }

  DBLoaderCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  static void Help(std::string* ret);
  virtual void DoCommand();

  virtual Options PrepareOptionsForOpenDB();

 private:
  bool create_if_missing_;
  bool disable_wal_;
  bool bulk_load_;
  bool compact_;

  static const std::string ARG_DISABLE_WAL;
  static const std::string ARG_BULK_LOAD;
  static const std::string ARG_COMPACT;
};

class ManifestDumpCommand: public LDBCommand {
 public:
  static std::string Name() { return "manifest_dump"; }

  ManifestDumpCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  static void Help(std::string* ret);
  virtual void DoCommand();

  virtual bool NoDBOpen() {
    return true;
  }

 private:
  bool verbose_;
  std::string path_;

  static const std::string ARG_VERBOSE;
  static const std::string ARG_PATH;
};

class ListColumnFamiliesCommand : public LDBCommand {
 public:
  static std::string Name() { return "list_column_families"; }

  ListColumnFamiliesCommand(
      const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  static void Help(std::string* ret);
  virtual void DoCommand();

  virtual bool NoDBOpen() { return true; }

 private:
  std::string dbname_;
};

class ReduceDBLevelsCommand : public LDBCommand {
 public:
  static std::string Name() { return "reduce_levels"; }

  ReduceDBLevelsCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual Options PrepareOptionsForOpenDB();

  virtual void DoCommand();

  virtual bool NoDBOpen() {
    return true;
  }

  static void Help(std::string* msg);

  static std::vector<std::string> PrepareArgs(
      const std::string& db_path,
      int new_levels,
      bool print_old_level = false);

 private:
  int old_levels_;
  int new_levels_;
  bool print_old_levels_;

  static const std::string ARG_NEW_LEVELS;
  static const std::string ARG_PRINT_OLD_LEVELS;

  Status GetOldNumOfLevels(Options& opt, int* levels);
};

class ChangeCompactionStyleCommand : public LDBCommand {
 public:
  static std::string Name() { return "change_compaction_style"; }

  ChangeCompactionStyleCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual Options PrepareOptionsForOpenDB();

  virtual void DoCommand();

  static void Help(std::string* msg);

 private:
  int old_compaction_style_;
  int new_compaction_style_;

  static const std::string ARG_OLD_COMPACTION_STYLE;
  static const std::string ARG_NEW_COMPACTION_STYLE;
};

class WALDumperCommand : public LDBCommand {
 public:
  static std::string Name() { return "dump_wal"; }

  WALDumperCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual bool  NoDBOpen() {
    return true;
  }

  static void Help(std::string* ret);
  virtual void DoCommand();

 private:
  bool print_header_;
  std::string wal_file_;
  bool print_values_;

  static const std::string ARG_WAL_FILE;
  static const std::string ARG_PRINT_HEADER;
  static const std::string ARG_PRINT_VALUE;
};


class GetCommand : public LDBCommand {
 public:
  static std::string Name() { return "get"; }

  GetCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual void DoCommand();

  static void Help(std::string* ret);

 private:
  std::string key_;
};

class ApproxSizeCommand : public LDBCommand {
 public:
  static std::string Name() { return "approxsize"; }

  ApproxSizeCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual void DoCommand();

  static void Help(std::string* ret);

 private:
  std::string start_key_;
  std::string end_key_;
};

class BatchPutCommand : public LDBCommand {
 public:
  static std::string Name() { return "batchput"; }

  BatchPutCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual void DoCommand();

  static void Help(std::string* ret);

  virtual Options PrepareOptionsForOpenDB();

 private:
  /**
   * The key-values to be inserted.
   */
  std::vector<std::pair<std::string, std::string>> key_values_;
};

class ScanCommand : public LDBCommand {
 public:
  static std::string Name() { return "scan"; }

  ScanCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual void DoCommand();

  static void Help(std::string* ret);

 private:
  std::string start_key_;
  std::string end_key_;
  bool start_key_specified_;
  bool end_key_specified_;
  int max_keys_scanned_;
};

class DeleteCommand : public LDBCommand {
 public:
  static std::string Name() { return "delete"; }

  DeleteCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual void DoCommand();

  static void Help(std::string* ret);

 private:
  std::string key_;
};

class PutCommand : public LDBCommand {
 public:
  static std::string Name() { return "put"; }

  PutCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual void DoCommand();

  static void Help(std::string* ret);

  virtual Options PrepareOptionsForOpenDB();

 private:
  std::string key_;
  std::string value_;
};

/**
 * Command that starts up a REPL shell that allows
 * get/put/delete.
 */
class DBQuerierCommand: public LDBCommand {
 public:
  static std::string Name() { return "query"; }

  DBQuerierCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  static void Help(std::string* ret);

  virtual void DoCommand();

 private:
  static const char* HELP_CMD;
  static const char* GET_CMD;
  static const char* PUT_CMD;
  static const char* DELETE_CMD;
};

class CheckConsistencyCommand : public LDBCommand {
 public:
  static std::string Name() { return "checkconsistency"; }

  CheckConsistencyCommand(const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual void DoCommand();

  virtual bool NoDBOpen() {
    return true;
  }

  static void Help(std::string* ret);
};

} // namespace rocksdb
