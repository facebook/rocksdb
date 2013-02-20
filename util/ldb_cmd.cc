// Copyright (c) 2012 Facebook. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "util/ldb_cmd.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "leveldb/write_batch.h"
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/write_batch_internal.h"

namespace leveldb {

const string LDBCommand::ARG_DB = "db";
const string LDBCommand::ARG_HEX = "hex";
const string LDBCommand::ARG_KEY_HEX = "key_hex";
const string LDBCommand::ARG_VALUE_HEX = "value_hex";
const string LDBCommand::ARG_FROM = "from";
const string LDBCommand::ARG_TO = "to";
const string LDBCommand::ARG_MAX_KEYS = "max_keys";
const string LDBCommand::ARG_BLOOM_BITS = "bloom_bits";
const string LDBCommand::ARG_COMPRESSION_TYPE = "compression_type";
const string LDBCommand::ARG_BLOCK_SIZE = "block_size";
const string LDBCommand::ARG_AUTO_COMPACTION = "auto_compaction";
const string LDBCommand::ARG_WRITE_BUFFER_SIZE = "write_buffer_size";
const string LDBCommand::ARG_FILE_SIZE = "file_size";
const string LDBCommand::ARG_CREATE_IF_MISSING = "create_if_missing";

const char* LDBCommand::DELIM = " ==> ";

LDBCommand* LDBCommand::InitFromCmdLineArgs(int argc, char** argv) {
  vector<string> args;
  for (int i = 1; i < argc; i++) {
    args.push_back(argv[i]);
  }
  return InitFromCmdLineArgs(args);
}

/**
 * Parse the command-line arguments and create the appropriate LDBCommand2
 * instance.
 * The command line arguments must be in the following format:
 * ./ldb --db=PATH_TO_DB [--commonOpt1=commonOpt1Val] ..
 *        COMMAND <PARAM1> <PARAM2> ... [-cmdSpecificOpt1=cmdSpecificOpt1Val] ..
 * This is similar to the command line format used by HBaseClientTool.
 * Command name is not included in args.
 * Returns nullptr if the command-line cannot be parsed.
 */
LDBCommand* LDBCommand::InitFromCmdLineArgs(const vector<string>& args) {
  // --x=y command line arguments are added as x->y map entries.
  map<string, string> options;

  // Command-line arguments of the form --hex end up in this array as hex
  vector<string> flags;

  // Everything other than options and flags. Represents commands
  // and their parameters.  For eg: put key1 value1 go into this vector.
  vector<string> cmdTokens;

  const string OPTION_PREFIX = "--";

  for (vector<string>::const_iterator itr = args.begin();
      itr != args.end(); itr++) {
    string arg = *itr;
    if (boost::starts_with(arg, OPTION_PREFIX)){
      vector<string> splits;
      boost::split(splits, arg, boost::is_any_of("="));
      if (splits.size() == 2) {
        string optionKey = splits[0].substr(OPTION_PREFIX.size());
        options[optionKey] = splits[1];
      } else {
        string optionKey = splits[0].substr(OPTION_PREFIX.size());
        flags.push_back(optionKey);
      }
    } else {
      cmdTokens.push_back(string(arg));
    }
  }

  if (cmdTokens.size() < 1) {
    fprintf(stderr, "Command not specified!");
    return nullptr;
  }

  string cmd = cmdTokens[0];
  vector<string> cmdParams(cmdTokens.begin()+1, cmdTokens.end());

  if (cmd == GetCommand::Name()) {
    return new GetCommand(cmdParams, options, flags);
  } else if (cmd == PutCommand::Name()) {
    return new PutCommand(cmdParams, options, flags);
  } else if (cmd == BatchPutCommand::Name()) {
    return new BatchPutCommand(cmdParams, options, flags);
  } else if (cmd == ScanCommand::Name()) {
    return new ScanCommand(cmdParams, options, flags);
  } else if (cmd == DeleteCommand::Name()) {
    return new DeleteCommand(cmdParams, options, flags);
  } else if (cmd == ApproxSizeCommand::Name()) {
    return new ApproxSizeCommand(cmdParams, options, flags);
  } else if (cmd == DBQuerierCommand::Name()) {
    return new DBQuerierCommand(cmdParams, options, flags);
  } else if (cmd == CompactorCommand::Name()) {
    return new CompactorCommand(cmdParams, options, flags);
  } else if (cmd == WALDumperCommand::Name()) {
    return new WALDumperCommand(cmdParams, options, flags);
  } else if (cmd == ReduceDBLevelsCommand::Name()) {
    return new ReduceDBLevelsCommand(cmdParams, options, flags);
  } else if (cmd == DBDumperCommand::Name()) {
    return new DBDumperCommand(cmdParams, options, flags);
  } else if (cmd == DBLoaderCommand::Name()) {
    return new DBLoaderCommand(cmdParams, options, flags);
  }

  return nullptr;
}

/**
 * Parses the specific integer option and fills in the value.
 * Returns true if the option is found.
 * Returns false if the option is not found or if there is an error parsing the
 * value.  If there is an error, the specified exec_state is also
 * updated.
 */
bool LDBCommand::ParseIntOption(const map<string, string>& options,
    string option, int& value, LDBCommandExecuteResult& exec_state) {

  map<string, string>::const_iterator itr = options_.find(option);
  if (itr != options_.end()) {
    try {
      value = boost::lexical_cast<int>(itr->second);
      return true;
    } catch( const boost::bad_lexical_cast & ) {
      exec_state = LDBCommandExecuteResult::FAILED(option +
                      " has an invalid value.");
    }
  }
  return false;
}

leveldb::Options LDBCommand::PrepareOptionsForOpenDB() {

  leveldb::Options opt;
  opt.create_if_missing = false;

  map<string, string>::const_iterator itr;

  int bits;
  if (ParseIntOption(options_, ARG_BLOOM_BITS, bits, exec_state_)) {
    if (bits > 0) {
      opt.filter_policy = leveldb::NewBloomFilterPolicy(bits);
    } else {
      exec_state_ = LDBCommandExecuteResult::FAILED(ARG_BLOOM_BITS +
                      " must be > 0.");
    }
  }

  int block_size;
  if (ParseIntOption(options_, ARG_BLOCK_SIZE, block_size, exec_state_)) {
    if (block_size > 0) {
      opt.block_size = block_size;
    } else {
      exec_state_ = LDBCommandExecuteResult::FAILED(ARG_BLOCK_SIZE +
                      " must be > 0.");
    }
  }

  itr = options_.find(ARG_AUTO_COMPACTION);
  if (itr != options_.end()) {
    opt.disable_auto_compactions = ! StringToBool(itr->second);
  }

  itr = options_.find(ARG_COMPRESSION_TYPE);
  if (itr != options_.end()) {
    string comp = itr->second;
    if (comp == "no") {
      opt.compression = leveldb::kNoCompression;
    } else if (comp == "snappy") {
      opt.compression = leveldb::kSnappyCompression;
    } else if (comp == "zlib") {
      opt.compression = leveldb::kZlibCompression;
    } else if (comp == "bzip2") {
      opt.compression = leveldb::kBZip2Compression;
    } else {
      // Unknown compression.
      exec_state_ = LDBCommandExecuteResult::FAILED(
                      "Unknown compression level: " + comp);
    }
  }

  int write_buffer_size;
  if (ParseIntOption(options_, ARG_WRITE_BUFFER_SIZE, write_buffer_size,
        exec_state_)) {
    if (write_buffer_size > 0) {
      opt.write_buffer_size = write_buffer_size;
    } else {
      exec_state_ = LDBCommandExecuteResult::FAILED(ARG_WRITE_BUFFER_SIZE +
                      " must be > 0.");
    }
  }

  int file_size;
  if (ParseIntOption(options_, ARG_FILE_SIZE, file_size, exec_state_)) {
    if (file_size > 0) {
      opt.target_file_size_base = file_size;
    } else {
      exec_state_ = LDBCommandExecuteResult::FAILED(ARG_FILE_SIZE +
                      " must be > 0.");
    }
  }

  return opt;
}

bool LDBCommand::ParseKeyValue(const string& line, string* key, string* value,
                              bool is_key_hex, bool is_value_hex) {
  size_t pos = line.find(DELIM);
  if (pos != std::string::npos) {
    *key = line.substr(0, pos);
    *value = line.substr(pos + strlen(DELIM));
    if (is_key_hex) {
      *key = HexToString(*key);
    }
    if (is_value_hex) {
      *value = HexToString(*value);
    }
    return true;
  } else {
    return false;
  }
}

/**
 * Make sure that ONLY the command-line options and flags expected by this
 * command are specified on the command-line.  Extraneous options are usually
 * the result of user error.
 * Returns true if all checks pass.  Else returns false, and prints an
 * appropriate error msg to stderr.
 */
bool LDBCommand::ValidateCmdLineOptions() {

  for (map<string, string>::const_iterator itr = options_.begin();
        itr != options_.end(); itr++) {
    if (std::find(valid_cmd_line_options_.begin(),
          valid_cmd_line_options_.end(), itr->first) ==
          valid_cmd_line_options_.end()) {
      fprintf(stderr, "Invalid command-line option %s\n", itr->first.c_str());
      return false;
    }
  }

  for (vector<string>::const_iterator itr = flags_.begin();
        itr != flags_.end(); itr++) {
    if (std::find(valid_cmd_line_options_.begin(),
          valid_cmd_line_options_.end(), *itr) ==
          valid_cmd_line_options_.end()) {
      fprintf(stderr, "Invalid command-line flag %s\n", itr->c_str());
      return false;
    }
  }

  if (options_.find(ARG_DB) == options_.end()) {
    fprintf(stderr, "%s must be specified\n", ARG_DB.c_str());
    return false;
  }

  return true;
}

CompactorCommand::CompactorCommand(const vector<string>& params,
      const map<string, string>& options, const vector<string>& flags) :
    LDBCommand(options, flags, false,
               BuildCmdLineOptions({ARG_FROM, ARG_TO, ARG_HEX, ARG_KEY_HEX,
                                   ARG_VALUE_HEX})),
    null_from_(true), null_to_(true) {

  map<string, string>::const_iterator itr = options.find(ARG_FROM);
  if (itr != options.end()) {
    null_from_ = false;
    from_ = itr->second;
  }

  itr = options.find(ARG_TO);
  if (itr != options.end()) {
    null_to_ = false;
    to_ = itr->second;
  }

  if (is_key_hex_) {
    if (!null_from_) {
      from_ = HexToString(from_);
    }
    if (!null_to_) {
      to_ = HexToString(to_);
    }
  }
}

void CompactorCommand::Help(string& ret) {
  ret.append("  ");
  ret.append(CompactorCommand::Name());
  ret.append(HelpRangeCmdArgs());
  ret.append("\n");
}

void CompactorCommand::DoCommand() {

  leveldb::Slice* begin = nullptr;
  leveldb::Slice* end = nullptr;
  if (!null_from_) {
    begin = new leveldb::Slice(from_);
  }
  if (!null_to_) {
    end = new leveldb::Slice(to_);
  }

  db_->CompactRange(begin, end);
  exec_state_ = LDBCommandExecuteResult::SUCCEED("");

  delete begin;
  delete end;
}

const string DBLoaderCommand::ARG_DISABLE_WAL = "disable_wal";

DBLoaderCommand::DBLoaderCommand(const vector<string>& params,
      const map<string, string>& options, const vector<string>& flags) :
    LDBCommand(options, flags, false,
               BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX,
                                    ARG_FROM, ARG_TO, ARG_CREATE_IF_MISSING,
                                    ARG_DISABLE_WAL})),
    create_if_missing_(false) {

  create_if_missing_ = IsFlagPresent(flags, ARG_CREATE_IF_MISSING);
  disable_wal_ = IsFlagPresent(flags, ARG_DISABLE_WAL);
}

void DBLoaderCommand::Help(string& ret) {
  ret.append("  ");
  ret.append(DBLoaderCommand::Name());
  ret.append(" [--" + ARG_CREATE_IF_MISSING + "]");
  ret.append(" [--" + ARG_DISABLE_WAL + "]");
  ret.append("\n");
}

leveldb::Options DBLoaderCommand::PrepareOptionsForOpenDB() {
  leveldb::Options opt = LDBCommand::PrepareOptionsForOpenDB();
  opt.create_if_missing = create_if_missing_;
  return opt;
}

void DBLoaderCommand::DoCommand() {
  if (!db_) {
    return;
  }

  WriteOptions write_options;
  if (disable_wal_) {
    write_options.disableWAL = true;
  }

  int bad_lines = 0;
  string line;
  while (std::getline(std::cin, line, '\n')) {
    string key;
    string value;
    if (ParseKeyValue(line, &key, &value, is_key_hex_, is_value_hex_)) {
      db_->Put(write_options, Slice(key), Slice(value));
    } else if (0 == line.find("Keys in range:")) {
      // ignore this line
    } else if (0 == line.find("Created bg thread 0x")) {
      // ignore this line
    } else {
      bad_lines ++;
    }
  }

  if (bad_lines > 0) {
    std::cout << "Warning: " << bad_lines << " bad lines ignored." << std::endl;
  }
}

const string DBDumperCommand::ARG_COUNT_ONLY = "count_only";
const string DBDumperCommand::ARG_STATS = "stats";

DBDumperCommand::DBDumperCommand(const vector<string>& params,
      const map<string, string>& options, const vector<string>& flags) :
    LDBCommand(options, flags, true,
               BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX,
                                    ARG_FROM, ARG_TO, ARG_MAX_KEYS,
                                    ARG_COUNT_ONLY, ARG_STATS})),
    null_from_(true),
    null_to_(true),
    max_keys_(-1),
    count_only_(false),
    print_stats_(false) {

  map<string, string>::const_iterator itr = options.find(ARG_FROM);
  if (itr != options.end()) {
    null_from_ = false;
    from_ = itr->second;
  }

  itr = options.find(ARG_TO);
  if (itr != options.end()) {
    null_to_ = false;
    to_ = itr->second;
  }

  itr = options.find(ARG_MAX_KEYS);
  if (itr != options.end()) {
    try {
      max_keys_ = boost::lexical_cast<int>(itr->second);
    } catch( const boost::bad_lexical_cast & ) {
      exec_state_ = LDBCommandExecuteResult::FAILED(ARG_MAX_KEYS +
                        " has an invalid value");
    }
  }

  print_stats_ = IsFlagPresent(flags, ARG_STATS);
  count_only_ = IsFlagPresent(flags, ARG_COUNT_ONLY);

  if (is_key_hex_) {
    if (!null_from_) {
      from_ = HexToString(from_);
    }
    if (!null_to_) {
      to_ = HexToString(to_);
    }
  }
}

void DBDumperCommand::Help(string& ret) {
  ret.append("  ");
  ret.append(DBDumperCommand::Name());
  ret.append(HelpRangeCmdArgs());
  ret.append(" [--" + ARG_MAX_KEYS + "=<N>]");
  ret.append(" [--" + ARG_COUNT_ONLY + "]");
  ret.append(" [--" + ARG_STATS + "]");
  ret.append("\n");
}

void DBDumperCommand::DoCommand() {
  if (!db_) {
    return;
  }
  // Parse command line args
  uint64_t count = 0;
  if (print_stats_) {
    string stats;
    if (db_->GetProperty("leveldb.stats", &stats)) {
      fprintf(stdout, "%s\n", stats.c_str());
    }
  }

  // Setup key iterator
  leveldb::Iterator* iter = db_->NewIterator(leveldb::ReadOptions());
  leveldb::Status st = iter->status();
  if (!st.ok()) {
    exec_state_ = LDBCommandExecuteResult::FAILED("Iterator error."
        + st.ToString());
  }

  if (!null_from_) {
    iter->Seek(from_);
  } else {
    iter->SeekToFirst();
  }

  int max_keys = max_keys_;
  for (; iter->Valid(); iter->Next()) {
    // If end marker was specified, we stop before it
    if (!null_to_ && (iter->key().ToString() >= to_))
      break;
    // Terminate if maximum number of keys have been dumped
    if (max_keys == 0)
      break;
    if (max_keys > 0) {
      --max_keys;
    }
    ++count;
    if (!count_only_) {
      string str = PrintKeyValue(iter->key().ToString(),
                                      iter->value().ToString(),
                                      is_key_hex_, is_value_hex_);
      fprintf(stdout, "%s\n", str.c_str());
    }
  }
  fprintf(stdout, "Keys in range: %lld\n", (long long) count);
  // Clean up
  delete iter;
}

const string ReduceDBLevelsCommand::ARG_NEW_LEVELS = "new_levels";
const string  ReduceDBLevelsCommand::ARG_PRINT_OLD_LEVELS = "print_old_levels";

ReduceDBLevelsCommand::ReduceDBLevelsCommand(const vector<string>& params,
      const map<string, string>& options, const vector<string>& flags) :
    LDBCommand(options, flags, false,
               BuildCmdLineOptions({ARG_NEW_LEVELS, ARG_PRINT_OLD_LEVELS})),
    old_levels_(1 << 16),
    new_levels_(-1),
    print_old_levels_(false) {


  ParseIntOption(options_, ARG_NEW_LEVELS, new_levels_, exec_state_);
  print_old_levels_ = IsFlagPresent(flags, ARG_PRINT_OLD_LEVELS);

  if(new_levels_ <= 0) {
    exec_state_ = LDBCommandExecuteResult::FAILED(
           " Use --" + ARG_NEW_LEVELS + " to specify a new level number\n");
  }
}

vector<string> ReduceDBLevelsCommand::PrepareArgs(const string& db_path,
    int new_levels, bool print_old_level) {
  vector<string> ret;
  ret.push_back("reduce_levels");
  ret.push_back("--" + ARG_DB + "=" + db_path);
  ret.push_back("--" + ARG_NEW_LEVELS + "=" + std::to_string(new_levels));
  if(print_old_level) {
    ret.push_back("--" + ARG_PRINT_OLD_LEVELS);
  }
  return ret;
}

void ReduceDBLevelsCommand::Help(string& ret) {
  ret.append("  ");
  ret.append(ReduceDBLevelsCommand::Name());
  ret.append(" --" + ARG_NEW_LEVELS + "=<New number of levels>");
  ret.append(" [--" + ARG_PRINT_OLD_LEVELS + "]");
  ret.append("\n");
}

leveldb::Options ReduceDBLevelsCommand::PrepareOptionsForOpenDB() {
  leveldb::Options opt = LDBCommand::PrepareOptionsForOpenDB();
  opt.num_levels = old_levels_;
  // Disable size compaction
  opt.max_bytes_for_level_base = 1UL << 50;
  opt.max_bytes_for_level_multiplier = 1;
  opt.max_mem_compaction_level = 0;
  return opt;
}

Status ReduceDBLevelsCommand::GetOldNumOfLevels(leveldb::Options& opt,
    int* levels) {
  TableCache* tc = new TableCache(db_path_, &opt, 10);
  const InternalKeyComparator* cmp = new InternalKeyComparator(
      opt.comparator);
  VersionSet* versions = new VersionSet(db_path_, &opt,
                                   tc, cmp);
  // We rely the VersionSet::Recover to tell us the internal data structures
  // in the db. And the Recover() should never do any change
  // (like LogAndApply) to the manifest file.
  Status st = versions->Recover();
  if (!st.ok()) {
    return st;
  }
  int max = -1;
  for (int i = 0; i < versions->NumberLevels(); i++) {
    if (versions->NumLevelFiles(i)) {
      max = i;
    }
  }

  *levels = max + 1;
  delete versions;
  return st;
}

void ReduceDBLevelsCommand::DoCommand() {
  if (new_levels_ <= 1) {
    exec_state_ = LDBCommandExecuteResult::FAILED(
        "Invalid number of levels.\n");
    return;
  }

  leveldb::Status st;
  leveldb::Options opt = PrepareOptionsForOpenDB();
  int old_level_num = -1;
  st = GetOldNumOfLevels(opt, &old_level_num);
  if (!st.ok()) {
    exec_state_ = LDBCommandExecuteResult::FAILED(st.ToString());
    return;
  }

  if (print_old_levels_) {
    fprintf(stdout, "The old number of levels in use is %d\n", old_level_num);
  }

  if (old_level_num <= new_levels_) {
    return;
  }

  old_levels_ = old_level_num;

  OpenDB();
  if (!db_) {
    return;
  }
  // Compact the whole DB to put all files to the highest level.
  fprintf(stdout, "Compacting the db...\n");
  db_->CompactRange(nullptr, nullptr);
  CloseDB();

  TableCache* tc = new TableCache(db_path_, &opt, 10);
  const InternalKeyComparator* cmp = new InternalKeyComparator(
      opt.comparator);
  VersionSet* versions = new VersionSet(db_path_, &opt,
                                   tc, cmp);
  // We rely the VersionSet::Recover to tell us the internal data structures
  // in the db. And the Recover() should never do any change (like LogAndApply)
  // to the manifest file.
  st = versions->Recover();
  if (!st.ok()) {
    exec_state_ = LDBCommandExecuteResult::FAILED(st.ToString());
    return;
  }

  port::Mutex mu;
  mu.Lock();
  st = versions->ReduceNumberOfLevels(new_levels_, &mu);
  mu.Unlock();

  if (!st.ok()) {
    exec_state_ = LDBCommandExecuteResult::FAILED(st.ToString());
    return;
  }
}

class InMemoryHandler : public WriteBatch::Handler {
 public:

  virtual void Put(const Slice& key, const Slice& value) {
    putMap_[key.ToString()] = value.ToString();
  }
  virtual void Delete(const Slice& key) {
    deleteList_.push_back(key.ToString(true));
  }
  virtual ~InMemoryHandler() { };

  map<string, string> PutMap() {
    return putMap_;
  }
  vector<string> DeleteList() {
    return deleteList_;
  }

 private:
  std::map<string, string> putMap_;
  std::vector<string> deleteList_;
};

const string WALDumperCommand::ARG_WAL_FILE = "walfile";
const string WALDumperCommand::ARG_PRINT_VALUE = "print_value";
const string WALDumperCommand::ARG_PRINT_HEADER = "header";

WALDumperCommand::WALDumperCommand(const vector<string>& params,
      const map<string, string>& options, const vector<string>& flags) :
    LDBCommand(options, flags, true,
               BuildCmdLineOptions(
                {ARG_WAL_FILE, ARG_PRINT_HEADER, ARG_PRINT_VALUE})),
    print_header_(false), print_values_(false) {

  wal_file_.clear();

  map<string, string>::const_iterator itr = options.find(ARG_WAL_FILE);
  if (itr != options.end()) {
    wal_file_ = itr->second;
  }


  print_header_ = IsFlagPresent(flags, ARG_PRINT_HEADER);
  print_values_ = IsFlagPresent(flags, ARG_PRINT_VALUE);
  if (wal_file_.empty()) {
    exec_state_ = LDBCommandExecuteResult::FAILED(
                    "Argument " + ARG_WAL_FILE + " must be specified.");
  }
}

void WALDumperCommand::Help(string& ret) {
  ret.append("  ");
  ret.append(WALDumperCommand::Name());
  ret.append(" --" + ARG_WAL_FILE + "=<write_ahead_log_file_path>");
  ret.append(" --[" + ARG_PRINT_HEADER + "] ");
  ret.append(" --[ " + ARG_PRINT_VALUE + "] ");
  ret.append("\n");
}

void WALDumperCommand::DoCommand() {
  struct StdErrReporter : public log::Reader::Reporter {
    virtual void Corruption(size_t bytes, const Status& s) {
      std::cerr<<"Corruption detected in log file "<<s.ToString()<<"\n";
    }
  };

  unique_ptr<SequentialFile> file;
  Env* env_ = Env::Default();
  Status status = env_->NewSequentialFile(wal_file_, &file);
  if (!status.ok()) {
    exec_state_ = LDBCommandExecuteResult::FAILED("Failed to open WAL file " +
      status.ToString());
  } else {
    StdErrReporter reporter;
    log::Reader reader(std::move(file), &reporter, true, 0);
    string scratch;
    WriteBatch batch;
    Slice record;
    std::stringstream row;
    if (print_header_) {
      std::cout<<"Sequence,Count,ByteSize,Physical Offset,Key(s)";
      if (print_values_) {
        std::cout << " : value ";
      }
      std::cout << "\n";
    }
    while(reader.ReadRecord(&record, &scratch)) {
      row.str("");
      if (record.size() < 12) {
        reporter.Corruption(
            record.size(), Status::Corruption("log record too small"));
      } else {
        WriteBatchInternal::SetContents(&batch, record);
        row<<WriteBatchInternal::Sequence(&batch)<<",";
        row<<WriteBatchInternal::Count(&batch)<<",";
        row<<WriteBatchInternal::ByteSize(&batch)<<",";
        row<<reader.LastRecordOffset()<<",";
        InMemoryHandler handler;
        batch.Iterate(&handler);
        row << "PUT : ";
        if (print_values_) {
          for (auto& kv : handler.PutMap()) {
            std::string k = StringToHex(kv.first);
            std::string v = StringToHex(kv.second);
            row << k << " : ";
            row << v << " ";
          }
        }
        else {
          for(auto& kv : handler.PutMap()) {
            row << StringToHex(kv.first) << " ";
          }
        }
        row<<",DELETE : ";
        for(string& s : handler.DeleteList()) {
          row << StringToHex(s) << " ";
        }
        row<<"\n";
      }
      std::cout<<row.str();
    }
  }
}


GetCommand::GetCommand(const vector<string>& params,
      const map<string, string>& options, const vector<string>& flags) :
  LDBCommand(options, flags, true,
             BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX})) {

  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::FAILED(
                    "<key> must be specified for the get command");
  } else {
    key_ = params.at(0);
  }

  if (is_key_hex_) {
    key_ = HexToString(key_);
  }
}

void GetCommand::Help(string& ret) {
  ret.append("  ");
  ret.append(GetCommand::Name());
  ret.append(" <key>");
  ret.append("\n");
}

void GetCommand::DoCommand() {
  string value;
  leveldb::Status st = db_->Get(leveldb::ReadOptions(), key_, &value);
  if (st.ok()) {
    fprintf(stdout, "%s\n",
              (is_value_hex_ ? StringToHex(value) : value).c_str());
  } else {
    exec_state_ = LDBCommandExecuteResult::FAILED(st.ToString());
  }
}


ApproxSizeCommand::ApproxSizeCommand(const vector<string>& params,
      const map<string, string>& options, const vector<string>& flags) :
  LDBCommand(options, flags, true,
             BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX,
                                  ARG_FROM, ARG_TO})) {

  if (options.find(ARG_FROM) != options.end()) {
    start_key_ = options.find(ARG_FROM)->second;
  } else {
    exec_state_ = LDBCommandExecuteResult::FAILED(ARG_FROM +
                    " must be specified for approxsize command");
    return;
  }

  if (options.find(ARG_TO) != options.end()) {
    end_key_ = options.find(ARG_TO)->second;
  } else {
    exec_state_ = LDBCommandExecuteResult::FAILED(ARG_TO +
                    " must be specified for approxsize command");
    return;
  }

  if (is_key_hex_) {
    start_key_ = HexToString(start_key_);
    end_key_ = HexToString(end_key_);
  }
}

void ApproxSizeCommand::Help(string& ret) {
  ret.append("  ");
  ret.append(ApproxSizeCommand::Name());
  ret.append(HelpRangeCmdArgs());
  ret.append("\n");
}

void ApproxSizeCommand::DoCommand() {

  leveldb::Range ranges[1];
  ranges[0] = leveldb::Range(start_key_, end_key_);
  uint64_t sizes[1];
  db_->GetApproximateSizes(ranges, 1, sizes);
  fprintf(stdout, "%ld\n", sizes[0]);
  /* Wierd that GetApproximateSizes() returns void, although documentation
   * says that it returns a Status object.
  if (!st.ok()) {
    exec_state_ = LDBCommandExecuteResult::FAILED(st.ToString());
  }
  */
}


BatchPutCommand::BatchPutCommand(const vector<string>& params,
      const map<string, string>& options, const vector<string>& flags) :
  LDBCommand(options, flags, false,
             BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX,
                                  ARG_CREATE_IF_MISSING})) {

  if (params.size() < 2) {
    exec_state_ = LDBCommandExecuteResult::FAILED(
        "At least one <key> <value> pair must be specified batchput.");
  } else if (params.size() % 2 != 0) {
    exec_state_ = LDBCommandExecuteResult::FAILED(
        "Equal number of <key>s and <value>s must be specified for batchput.");
  } else {
    for (size_t i = 0; i < params.size(); i += 2) {
      string key = params.at(i);
      string value = params.at(i+1);
      key_values_.push_back(std::pair<string, string>(
                    is_key_hex_ ? HexToString(key) : key,
                    is_value_hex_ ? HexToString(value) : value));
    }
  }
}

void BatchPutCommand::Help(string& ret) {
  ret.append("  ");
  ret.append(BatchPutCommand::Name());
  ret.append(" <key> <value> [<key> <value>] [..]");
  ret.append("\n");
}

void BatchPutCommand::DoCommand() {
  leveldb::WriteBatch batch;

  for (vector<std::pair<string, string>>::const_iterator itr
        = key_values_.begin(); itr != key_values_.end(); itr++) {
      batch.Put(itr->first, itr->second);
  }
  leveldb::Status st = db_->Write(leveldb::WriteOptions(), &batch);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::FAILED(st.ToString());
  }
}

leveldb::Options BatchPutCommand::PrepareOptionsForOpenDB() {
  leveldb::Options opt = LDBCommand::PrepareOptionsForOpenDB();
  opt.create_if_missing = IsFlagPresent(flags_, ARG_CREATE_IF_MISSING);
  return opt;
}


ScanCommand::ScanCommand(const vector<string>& params,
      const map<string, string>& options, const vector<string>& flags) :
    LDBCommand(options, flags, true,
               BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX,
                                    ARG_FROM, ARG_TO, ARG_MAX_KEYS})),
    start_key_specified_(false),
    end_key_specified_(false),
    max_keys_scanned_(-1) {

  map<string, string>::const_iterator itr = options.find(ARG_FROM);
  if (itr != options.end()) {
    start_key_ = itr->second;
    if (is_key_hex_) {
      start_key_ = HexToString(start_key_);
    }
    start_key_specified_ = true;
  }
  itr = options.find(ARG_TO);
  if (itr != options.end()) {
    end_key_ = itr->second;
    if (is_key_hex_) {
      end_key_ = HexToString(end_key_);
    }
    end_key_specified_ = true;
  }

  itr = options.find(ARG_MAX_KEYS);
  if (itr != options.end()) {
    try {
      max_keys_scanned_ = boost::lexical_cast< int >(itr->second);
    } catch( const boost::bad_lexical_cast & ) {
      exec_state_ = LDBCommandExecuteResult::FAILED(ARG_MAX_KEYS +
                        " has an invalid value");
    }
  }
}

void ScanCommand::Help(string& ret) {
  ret.append("  ");
  ret.append(ScanCommand::Name());
  ret.append(HelpRangeCmdArgs());
  ret.append("--" + ARG_MAX_KEYS + "=N] ");
  ret.append("\n");
}

void ScanCommand::DoCommand() {

  int num_keys_scanned = 0;
  Iterator* it = db_->NewIterator(leveldb::ReadOptions());
  if (start_key_specified_) {
    it->Seek(start_key_);
  } else {
    it->SeekToFirst();
  }
  for ( ;
        it->Valid() && (!end_key_specified_ || it->key().ToString() < end_key_);
        it->Next()) {
    string key = it->key().ToString();
    string value = it->value().ToString();
    fprintf(stdout, "%s : %s\n",
          (is_key_hex_ ? StringToHex(key) : key).c_str(),
          (is_value_hex_ ? StringToHex(value) : value).c_str()
        );
    num_keys_scanned++;
    if (max_keys_scanned_ >= 0 && num_keys_scanned >= max_keys_scanned_) {
      break;
    }
  }
  if (!it->status().ok()) {  // Check for any errors found during the scan
    exec_state_ = LDBCommandExecuteResult::FAILED(it->status().ToString());
  }
  delete it;
}


DeleteCommand::DeleteCommand(const vector<string>& params,
      const map<string, string>& options, const vector<string>& flags) :
  LDBCommand(options, flags, false,
             BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX})) {

  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::FAILED(
                    "KEY must be specified for the delete command");
  } else {
    key_ = params.at(0);
    if (is_key_hex_) {
      key_ = HexToString(key_);
    }
  }
}

void DeleteCommand::Help(string& ret) {
  ret.append("  ");
  ret.append(DeleteCommand::Name() + " <key>");
  ret.append("\n");
}

void DeleteCommand::DoCommand() {
  leveldb::Status st = db_->Delete(leveldb::WriteOptions(), key_);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::FAILED(st.ToString());
  }
}


PutCommand::PutCommand(const vector<string>& params,
      const map<string, string>& options, const vector<string>& flags) :
  LDBCommand(options, flags, false,
             BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX,
                                  ARG_CREATE_IF_MISSING})) {

  if (params.size() != 2) {
    exec_state_ = LDBCommandExecuteResult::FAILED(
                    "<key> and <value> must be specified for the put command");
  } else {
    key_ = params.at(0);
    value_ = params.at(1);
  }

  if (is_key_hex_) {
    key_ = HexToString(key_);
  }

  if (is_value_hex_) {
    value_ = HexToString(value_);
  }
}

void PutCommand::Help(string& ret) {
  ret.append("  ");
  ret.append(PutCommand::Name());
  ret.append(" <key> <value> ");
  ret.append("\n");
}

void PutCommand::DoCommand() {
  leveldb::Status st = db_->Put(leveldb::WriteOptions(), key_, value_);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::FAILED(st.ToString());
  }
}

leveldb::Options PutCommand::PrepareOptionsForOpenDB() {
  leveldb::Options opt = LDBCommand::PrepareOptionsForOpenDB();
  opt.create_if_missing = IsFlagPresent(flags_, ARG_CREATE_IF_MISSING);
  return opt;
}


const char* DBQuerierCommand::HELP_CMD = "help";
const char* DBQuerierCommand::GET_CMD = "get";
const char* DBQuerierCommand::PUT_CMD = "put";
const char* DBQuerierCommand::DELETE_CMD = "delete";

DBQuerierCommand::DBQuerierCommand(const vector<string>& params,
    const map<string, string>& options, const vector<string>& flags) :
  LDBCommand(options, flags, false,
             BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX})) {

}

void DBQuerierCommand::Help(string& ret) {
  ret.append("  ");
  ret.append(DBQuerierCommand::Name());
  ret.append("\n");
  ret.append("    Starts a REPL shell.  Type help for list of available "
             "commands.");
  ret.append("\n");
}

void DBQuerierCommand::DoCommand() {
  if (!db_) {
    return;
  }

  leveldb::ReadOptions read_options;
  leveldb::WriteOptions write_options;

  string line;
  string key;
  string value;
  while (getline(std::cin, line, '\n')) {

    // Parse line into vector<string>
    vector<string> tokens;
    size_t pos = 0;
    while (true) {
      size_t pos2 = line.find(' ', pos);
      if (pos2 == string::npos) {
        break;
      }
      tokens.push_back(line.substr(pos, pos2-pos));
      pos = pos2 + 1;
    }
    tokens.push_back(line.substr(pos));

    const string& cmd = tokens[0];

    if (cmd == HELP_CMD) {
      fprintf(stdout,
              "get <key>\n"
              "put <key> <value>\n"
              "delete <key>\n");
    } else if (cmd == DELETE_CMD && tokens.size() == 2) {
      key = (is_key_hex_ ? HexToString(tokens[1]) : tokens[1]);
      db_->Delete(write_options, Slice(key));
      fprintf(stdout, "Successfully deleted %s\n", tokens[1].c_str());
    } else if (cmd == PUT_CMD && tokens.size() == 3) {
      key = (is_key_hex_ ? HexToString(tokens[1]) : tokens[1]);
      value = (is_value_hex_ ? HexToString(tokens[2]) : tokens[2]);
      db_->Put(write_options, Slice(key), Slice(value));
      fprintf(stdout, "Successfully put %s %s\n",
              tokens[1].c_str(), tokens[2].c_str());
    } else if (cmd == GET_CMD && tokens.size() == 2) {
      key = (is_key_hex_ ? HexToString(tokens[1]) : tokens[1]);
      if (db_->Get(read_options, Slice(key), &value).ok()) {
        fprintf(stdout, "%s\n", PrintKeyValue(key, value,
              is_key_hex_, is_value_hex_).c_str());
      } else {
        fprintf(stdout, "Not found %s\n", tokens[1].c_str());
      }
    } else {
      fprintf(stdout, "Unknown command %s\n", line.c_str());
    }
  }
}


}
