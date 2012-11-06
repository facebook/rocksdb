// Copyright (c) 2012 Facebook. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "util/ldb_cmd.h"

namespace leveldb {

const char* LDBCommand::FROM_ARG = "--from=";
const char* LDBCommand::END_ARG = "--to=";
const char* LDBCommand::HEX_ARG = "--hex";

Compactor::Compactor(std::string& db_name, std::vector<std::string>& args) :
  LDBCommand(db_name, args), null_from_(true), null_to_(true), hex_(false) {
  for (int i = 0; i < args.size(); i++) {
    std::string& arg = args.at(i);
    if (arg.find(FROM_ARG) == 0) {
      null_from_ = false;
      from_ = arg.substr(strlen(FROM_ARG));
    } else if (arg.find(END_ARG) == 0) {
      null_to_ = false;
      to_ = arg.substr(strlen(END_ARG));
    } else if (arg.find(HEX_ARG) == 0) {
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

const char* DBDumper::MAX_KEYS_ARG = "--max_keys=";
const char* DBDumper::COUNT_ONLY_ARG = "--count_only";
const char* DBDumper::STATS_ARG = "--stats";
const char* DBDumper::HEX_OUTPUT_ARG = "--output_hex";

DBDumper::DBDumper(std::string& db_name, std::vector<std::string>& args) :
  LDBCommand(db_name, args), null_from_(true), null_to_(true), hex_(false),
      count_only_(false), print_stats_(false), max_keys_(-1),
      hex_output_(false) {
  for (int i = 0; i < args.size(); i++) {
    std::string& arg = args.at(i);
    if (arg.find(FROM_ARG) == 0) {
      null_from_ = false;
      from_ = arg.substr(strlen(FROM_ARG));
    } else if (arg.find(END_ARG) == 0) {
      null_to_ = false;
      to_ = arg.substr(strlen(END_ARG));
    } else if (arg.find(HEX_ARG) == 0) {
      hex_ = true;
    } else if (arg.find(MAX_KEYS_ARG) == 0) {
      max_keys_ = atoi(arg.substr(strlen(MAX_KEYS_ARG)).c_str());
    } else if (arg.find(STATS_ARG) == 0) {
      print_stats_ = true;
    } else if (arg.find(COUNT_ONLY_ARG) == 0) {
      count_only_ = true;
    } else if (arg.find(HEX_OUTPUT_ARG) == 0) {
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
        for (int i = 0; i < str.length(); ++i) {
          fprintf(stdout, "%X", str[i]);
        }
        fprintf(stdout, " ==> ");
        str = iter->value().ToString();
        for (int i = 0; i < str.length(); ++i) {
          fprintf(stdout, "%X", str[i]);
        }
        fprintf(stdout, "\n");
      } else {
        fprintf(stdout, "%s ==> %s\n", iter->key().ToString().c_str(),
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
  new_levels_(-1),
  print_old_levels_(false) {
  for (int i = 0; i < args.size(); i++) {
    std::string& arg = args.at(i);
    if (arg.find(NEW_LEVLES_ARG) == 0) {
      new_levels_ = atoi(arg.substr(strlen(NEW_LEVLES_ARG)).c_str());
    } else if (arg.find(PRINT_OLD_LEVELS_ARG) == 0) {
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
}

leveldb::Options ReduceDBLevels::PrepareOptionsForOpenDB() {
  leveldb::Options opt = LDBCommand::PrepareOptionsForOpenDB();
  // Set to a big value to make sure we can open the db
  opt.num_levels = 1 << 16;
  return opt;
}

void ReduceDBLevels::DoCommand() {
  if (new_levels_ <= 1) {
    exec_state_ = LDBCommandExecuteResult::FAILED(
        "Invalid number of levels.\n");
    return;
  }

  leveldb::Status st;

  leveldb::Options opt = PrepareOptionsForOpenDB();
  if (print_old_levels_) {
    TableCache* tc = new TableCache(db_path_, &opt, 10);
    const InternalKeyComparator* cmp = new InternalKeyComparator(
        opt.comparator);
    VersionSet* versions = new VersionSet(db_path_, &opt,
                                     tc, cmp);
    // We rely the VersionSet::Recover to tell us the internal data structures
    // in the db. And the Recover() should never do any change
    // (like LogAndApply) to the manifest file.
    st = versions->Recover();
    int max = -1;
    for(int i = 0; i<versions->NumberLevels(); i++) {
      if (versions->NumLevelFiles(i)) {
        max = i;
      }
    }
    fprintf(stdout, "The old number of levels in use is %d\n", max + 1);
    delete versions;

    if (!st.ok()) {
      exec_state_ = LDBCommandExecuteResult::FAILED(st.ToString());
      return;
    }
  }

  // Compact the whole DB to put all files to the highest level.
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

}
