// Copyright (c) 2012 Facebook. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.


#include "leveldb/write_batch.h"
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/write_batch_internal.h"
#include "util/ldb_cmd.h"

namespace leveldb {

const char* LDBCommand::BLOOM_ARG = "--bloom_bits=";
const char* LDBCommand::COMPRESSION_TYPE_ARG = "--compression_type=";
const char* LDBCommand::BLOCK_SIZE = "--block_size=";
const char* LDBCommand::AUTO_COMPACTION = "--auto_compaction=";
const char* LDBCommand::WRITE_BUFFER_SIZE_ARG = "--write_buffer_size=";
const char* LDBCommand::FILE_SIZE_ARG = "--file_size=";

void LDBCommand::parse_open_args(std::vector<std::string>& args) {
  std::vector<std::string> rest_of_args;
  for (unsigned int i = 0; i < args.size(); i++) {
    std::string& arg = args.at(i);
    if (arg.find(BLOOM_ARG) == 0
        || arg.find(COMPRESSION_TYPE_ARG) == 0
        || arg.find(BLOCK_SIZE) == 0
        || arg.find(AUTO_COMPACTION) == 0
        || arg.find(WRITE_BUFFER_SIZE_ARG) == 0
        || arg.find(FILE_SIZE_ARG) == 0) {
      open_args_.push_back(arg);
    } else {
      rest_of_args.push_back(arg);
    }
  }
  swap(args, rest_of_args);
}

leveldb::Options LDBCommand::PrepareOptionsForOpenDB() {
  leveldb::Options opt;
  opt.create_if_missing = false;
  for (unsigned int i = 0; i < open_args_.size(); i++) {
    std::string& arg = open_args_.at(i);
    if (arg.find(BLOOM_ARG) == 0) {
      std::string bits_string = arg.substr(strlen(BLOOM_ARG));
      int bits = atoi(bits_string.c_str());
      if (bits == 0) {
        // Badly-formatted bits.
        exec_state_ = LDBCommandExecuteResult::FAILED(
          std::string("Badly-formatted bits: ") + bits_string);
      }
      opt.filter_policy = leveldb::NewBloomFilterPolicy(bits);
    } else if (arg.find(BLOCK_SIZE) == 0) {
      std::string block_size_string = arg.substr(strlen(BLOCK_SIZE));
      int block_size = atoi(block_size_string.c_str());
      if (block_size == 0) {
        // Badly-formatted bits.
        exec_state_ = LDBCommandExecuteResult::FAILED(
          std::string("Badly-formatted block size: ") + block_size_string);
      }
      opt.block_size = block_size;
    } else if (arg.find(AUTO_COMPACTION) == 0) {
      std::string value = arg.substr(strlen(AUTO_COMPACTION));
      if (value == "false") {
        opt.disable_auto_compactions = true;
      } else if (value == "true") {
        opt.disable_auto_compactions = false;
      } else {
        // Unknown compression.
        exec_state_ = LDBCommandExecuteResult::FAILED(
          "Unknown auto_compaction value: " + value);
      }
    } else if (arg.find(COMPRESSION_TYPE_ARG) == 0) {
      std::string comp = arg.substr(strlen(COMPRESSION_TYPE_ARG));
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
    } else if (arg.find(WRITE_BUFFER_SIZE_ARG) == 0) {
      std::string write_buffer_str = arg.substr(strlen(WRITE_BUFFER_SIZE_ARG));
      int write_buffer_size = atoi(write_buffer_str.c_str());
      if (write_buffer_size == 0) {
        exec_state_ = LDBCommandExecuteResult::FAILED(
          std::string("Badly-formatted buffer size: ") + write_buffer_str);
      }
      opt.write_buffer_size = write_buffer_size;
    } else if (arg.find(FILE_SIZE_ARG) == 0) {
      std::string file_size_str = arg.substr(strlen(FILE_SIZE_ARG));
      int file_size = atoi(file_size_str.c_str());
      if (file_size == 0) {
        exec_state_ = LDBCommandExecuteResult::FAILED(
          std::string("Badly-formatted file size: ") + file_size_str);
      }
      opt.target_file_size_base = file_size;
    } else {
      exec_state_ = LDBCommandExecuteResult::FAILED(
        "Unknown option: " + arg);
    }
  }

  return opt;
}


const char* LDBCommand::FROM_ARG = "--from=";
const char* LDBCommand::END_ARG = "--to=";
const char* LDBCommand::HEX_ARG = "--hex";

Compactor::Compactor(std::string& db_name, std::vector<std::string>& args) :
  LDBCommand(db_name, args), null_from_(true), null_to_(true), hex_(false) {
  for (unsigned int i = 0; i < args.size(); i++) {
    std::string& arg = args.at(i);
    if (arg.find(FROM_ARG) == 0) {
      null_from_ = false;
      from_ = arg.substr(strlen(FROM_ARG));
    } else if (arg.find(END_ARG) == 0) {
      null_to_ = false;
      to_ = arg.substr(strlen(END_ARG));
    } else if (arg == HEX_ARG) {
      hex_ = true;
    } else {
      exec_state_ = LDBCommandExecuteResult::FAILED("Unknown argument." + arg);
    }
  }

  if (hex_) {
    if (!null_from_) {
      from_ = HexToString(from_);
    }
    if (!null_to_) {
      to_ = HexToString(to_);
    }
  }
}

void Compactor::Help(std::string& ret) {
  LDBCommand::Help(ret);
  ret.append("[--from=START KEY] ");
  ret.append("[--to=START KEY] ");
  ret.append("[--hex] ");
}

void Compactor::DoCommand() {

  leveldb::Slice* begin = NULL;
  leveldb::Slice* end = NULL;
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

const char* DBLoader::HEX_INPUT_ARG = "--input_hex";
const char* DBLoader::CREATE_IF_MISSING_ARG = "--create_if_missing";
const char* DBLoader::DISABLE_WAL_ARG = "--disable_wal";
static const char* delim = " ==> ";

DBLoader::DBLoader(std::string& db_name, std::vector<std::string>& args) :
    LDBCommand(db_name, args),
    hex_input_(false),
    create_if_missing_(false) {
  for (unsigned int i = 0; i < args.size(); i++) {
    std::string& arg = args.at(i);
    if (arg == HEX_INPUT_ARG) {
      hex_input_ = true;
    } else if (arg == CREATE_IF_MISSING_ARG) {
      create_if_missing_ = true;
    } else if (arg == DISABLE_WAL_ARG) {
      disable_wal_ = true;
    } else {
      exec_state_ = LDBCommandExecuteResult::FAILED("Unknown argument:" + arg);
    }
  }
}

void DBLoader::Help(std::string& ret) {
  LDBCommand::Help(ret);
  ret.append("[");
  ret.append(HEX_INPUT_ARG);
  ret.append("] [");
  ret.append(CREATE_IF_MISSING_ARG);
  ret.append("] [");
  ret.append(DISABLE_WAL_ARG);
  ret.append("]");
}

leveldb::Options DBLoader::PrepareOptionsForOpenDB() {
  leveldb::Options opt = LDBCommand::PrepareOptionsForOpenDB();
  opt.create_if_missing = create_if_missing_;
  return opt;
}

void DBLoader::DoCommand() {
  if (!db_) {
    return;
  }

  WriteOptions write_options;
  if (disable_wal_) {
    write_options.disableWAL = true;
  }

  int bad_lines = 0;
  std::string line;
  while (std::getline(std::cin, line, '\n')) {
    size_t pos = line.find(delim);
    if (pos != std::string::npos) {
      std::string key = line.substr(0, pos);
      std::string value = line.substr(pos + strlen(delim));

      if (hex_input_) {
        key = HexToString(key);
        value = HexToString(value);
      }

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

const char* DBDumper::MAX_KEYS_ARG = "--max_keys=";
const char* DBDumper::COUNT_ONLY_ARG = "--count_only";
const char* DBDumper::STATS_ARG = "--stats";
const char* DBDumper::HEX_OUTPUT_ARG = "--output_hex";

DBDumper::DBDumper(std::string& db_name, std::vector<std::string>& args) :
    LDBCommand(db_name, args),
    null_from_(true),
    null_to_(true),
    max_keys_(-1),
    count_only_(false),
    print_stats_(false),
    hex_(false),
    hex_output_(false) {
  for (unsigned int i = 0; i < args.size(); i++) {
    std::string& arg = args.at(i);
    if (arg.find(FROM_ARG) == 0) {
      null_from_ = false;
      from_ = arg.substr(strlen(FROM_ARG));
    } else if (arg.find(END_ARG) == 0) {
      null_to_ = false;
      to_ = arg.substr(strlen(END_ARG));
    } else if (arg == HEX_ARG) {
      hex_ = true;
    } else if (arg.find(MAX_KEYS_ARG) == 0) {
      max_keys_ = atoi(arg.substr(strlen(MAX_KEYS_ARG)).c_str());
    } else if (arg == STATS_ARG) {
      print_stats_ = true;
    } else if (arg == COUNT_ONLY_ARG) {
      count_only_ = true;
    } else if (arg == HEX_OUTPUT_ARG) {
      hex_output_ = true;
    } else {
      exec_state_ = LDBCommandExecuteResult::FAILED("Unknown argument:" + arg);
    }
  }

  if (hex_) {
    if (!null_from_) {
      from_ = HexToString(from_);
    }
    if (!null_to_) {
      to_ = HexToString(to_);
    }
  }
}

void DBDumper::Help(std::string& ret) {
  LDBCommand::Help(ret);
  ret.append("[--from=START KEY] ");
  ret.append("[--to=END Key] ");
  ret.append("[--hex] ");
  ret.append("[--output_hex] ");
  ret.append("[--max_keys=NUM] ");
  ret.append("[--count_only] ");
  ret.append("[--stats] ");
}

void DBDumper::DoCommand() {
  if (!db_) {
    return;
  }
  // Parse command line args
  uint64_t count = 0;
  if (print_stats_) {
    std::string stats;
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
      if (hex_output_) {
        std::string str = iter->key().ToString();
        for (unsigned int i = 0; i < str.length(); ++i) {
          fprintf(stdout, "%02X", (unsigned char)str[i]);
        }
        fprintf(stdout, delim);
        str = iter->value().ToString();
        for (unsigned int i = 0; i < str.length(); ++i) {
          fprintf(stdout, "%02X", (unsigned char)str[i]);
        }
        fprintf(stdout, "\n");
      } else {
        fprintf(stdout, "%s%s%s\n", iter->key().ToString().c_str(),
            delim,
            iter->value().ToString().c_str());
      }
    }
  }
  fprintf(stdout, "Keys in range: %lld\n", (long long) count);
  // Clean up
  delete iter;
}


const char* ReduceDBLevels::NEW_LEVLES_ARG = "--new_levels=";
const char* ReduceDBLevels::PRINT_OLD_LEVELS_ARG = "--print_old_levels";

ReduceDBLevels::ReduceDBLevels(std::string& db_name,
    std::vector<std::string>& args)
: LDBCommand(db_name, args),
  old_levels_(1 << 16),
  new_levels_(-1),
  print_old_levels_(false) {

  for (unsigned int i = 0; i < args.size(); i++) {
    std::string& arg = args.at(i);
    if (arg.find(NEW_LEVLES_ARG) == 0) {
      new_levels_ = atoi(arg.substr(strlen(NEW_LEVLES_ARG)).c_str());
    } else if (arg == PRINT_OLD_LEVELS_ARG) {
      print_old_levels_ = true;
    } else {
      exec_state_ = LDBCommandExecuteResult::FAILED(
          "Unknown argument." + arg);
    }
  }

  if(new_levels_ <= 0) {
    exec_state_ = LDBCommandExecuteResult::FAILED(
           " Use --new_levels to specify a new level number\n");
  }
}

std::vector<std::string> ReduceDBLevels::PrepareArgs(int new_levels,
    bool print_old_level) {
  std::vector<std::string> ret;
  char arg[100];
  sprintf(arg, "%s%d", NEW_LEVLES_ARG, new_levels);
  ret.push_back(arg);
  if(print_old_level) {
    sprintf(arg, "%s", PRINT_OLD_LEVELS_ARG);
    ret.push_back(arg);
  }
  return ret;
}

void ReduceDBLevels::Help(std::string& msg) {
    LDBCommand::Help(msg);
    msg.append("[--new_levels=New number of levels] ");
    msg.append("[--print_old_levels] ");
    msg.append("[--compression=none|snappy|zlib|bzip2] ");
    msg.append("[--file_size= per-file size] ");
}

leveldb::Options ReduceDBLevels::PrepareOptionsForOpenDB() {
  leveldb::Options opt = LDBCommand::PrepareOptionsForOpenDB();
  opt.num_levels = old_levels_;
  // Disable size compaction
  opt.max_bytes_for_level_base = 1UL << 50;
  opt.max_bytes_for_level_multiplier = 1;
  opt.max_mem_compaction_level = 0;
  return opt;
}

Status ReduceDBLevels::GetOldNumOfLevels(leveldb::Options& opt, int* levels) {
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

void ReduceDBLevels::DoCommand() {
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
  db_->CompactRange(NULL, NULL);
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

const char* WALDumper::WAL_FILE_ARG = "--walfile=";
WALDumper::WALDumper(std::vector<std::string>& args) :
  LDBCommand(args), print_header_(false) {
  wal_file_.clear();
  for (unsigned int i = 0; i < args.size(); i++) {
    std::string& arg = args.at(i);
    if (arg == "--header") {
      print_header_ = true;
    } else if (arg.find(WAL_FILE_ARG) == 0) {
      wal_file_ = arg.substr(strlen(WAL_FILE_ARG));
    } else {
      exec_state_ = LDBCommandExecuteResult::FAILED("Unknown argument " + arg);
    }
  }
  if (wal_file_.empty()) {
    exec_state_ = LDBCommandExecuteResult::FAILED("Argument --walfile reqd.");
  }
}

void WALDumper::Help(std::string& ret) {
  ret.append("--walfile write_ahead_log ");
  ret.append("[--header print's a header] ");
}

void WALDumper::DoCommand() {
  struct StdErrReporter : public log::Reader::Reporter {
    virtual void Corruption(size_t bytes, const Status& s) {
      std::cerr<<"Corruption detected in log file "<<s.ToString()<<"\n";
    }
  };

  SequentialFile* file;
  Env* env_ = Env::Default();
  Status status = env_->NewSequentialFile(wal_file_, &file);
  if (!status.ok()) {
    exec_state_ = LDBCommandExecuteResult::FAILED("Failed to open WAL file " +
      status.ToString());
  } else {
    StdErrReporter reporter;
    log::Reader reader(file, &reporter, true, 0);
    std::string scratch;
    WriteBatch batch;
    Slice record;
    std::stringstream row;
    if (print_header_) {
      std::cout<<"Sequence,Count,ByteSize,Physical Offset\n";
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
        row<<reader.LastRecordOffset()<<"\n";
      }
      std::cout<<row.str();
    }
  }
}

}
