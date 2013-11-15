//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <map>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/table.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "util/ldb_cmd.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class SstFileReader {
 public:
  explicit SstFileReader(const std::string& file_name,
                         bool verify_checksum,
                         bool output_hex);

  Status ReadSequential(bool print_kv,
                        uint64_t read_num,
                        bool has_from,
                        const std::string& from_key,
                        bool has_to,
                        const std::string& to_key);

  uint64_t GetReadNumber() { return read_num_; }

private:
  std::string file_name_;
  uint64_t read_num_;
  bool verify_checksum_;
  bool output_hex_;
  EnvOptions soptions_;
};

SstFileReader::SstFileReader(const std::string& file_path,
                             bool verify_checksum,
                             bool output_hex)
 :file_name_(file_path), read_num_(0), verify_checksum_(verify_checksum),
  output_hex_(output_hex) {
  std::cout << "Process " << file_path << "\n";
}

Status SstFileReader::ReadSequential(bool print_kv,
                                     uint64_t read_num,
                                     bool has_from,
                                     const std::string& from_key,
                                     bool has_to,
                                     const std::string& to_key)
{
  unique_ptr<TableReader> table_reader;
  InternalKeyComparator internal_comparator_(BytewiseComparator());
  Options table_options;
  table_options.comparator = &internal_comparator_;
  unique_ptr<RandomAccessFile> file;
  Status s = table_options.env->NewRandomAccessFile(file_name_, &file,
                                                    soptions_);
  if(!s.ok()) {
   return s;
  }
  uint64_t file_size;
  table_options.env->GetFileSize(file_name_, &file_size);
  unique_ptr<TableFactory> table_factory;
  s = table_options.table_factory->GetTableReader(table_options, soptions_,
                                                  std::move(file), file_size,
                                                  &table_reader);
  if(!s.ok()) {
   return s;
  }

  Iterator* iter = table_reader->NewIterator(ReadOptions(verify_checksum_,
                                                         false));
  uint64_t i = 0;
  if (has_from) {
    InternalKey ikey(from_key, kMaxSequenceNumber, kValueTypeForSeek);
    iter->Seek(ikey.Encode());
  } else {
    iter->SeekToFirst();
  }
  for (; iter->Valid(); iter->Next()) {
    Slice key = iter->key();
    Slice value = iter->value();
    ++i;
    if (read_num > 0 && i > read_num)
      break;

    ParsedInternalKey ikey;
    if (!ParseInternalKey(key, &ikey)) {
      std::cerr << "Internal Key ["
                << key.ToString(true /* in hex*/)
                << "] parse error!\n";
      continue;
    }

    // If end marker was specified, we stop before it
    if (has_to && BytewiseComparator()->Compare(ikey.user_key, to_key) >= 0) {
      break;
    }

    if (print_kv) {
      std::cout << ikey.DebugString(output_hex_)
                << " => "
                << value.ToString(output_hex_) << "\n";
    }

   }

   read_num_ += i;

   Status ret = iter->status();
   delete iter;
   return ret;
}

} // namespace rocksdb

static void print_help() {
  fprintf(stderr,
      "sst_dump [--command=check|scan] [--verify_checksum] "
      "--file=data_dir_OR_sst_file"
      " [--output_hex]"
      " [--input_key_hex]"
      " [--from=<user_key>]"
      " [--to=<user_key>]"
      " [--read_num=NUM]\n");
}

string HexToString(const string& str) {
  string parsed;
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

int main(int argc, char** argv) {

  const char* dir_or_file = nullptr;
  uint64_t read_num = -1;
  std::string command;

  char junk;
  uint64_t n;
  bool verify_checksum = false;
  bool output_hex = false;
  bool input_key_hex = false;
  bool has_from = false;
  bool has_to = false;
  std::string from_key;
  std::string to_key;
  for (int i = 1; i < argc; i++)
  {
    if (strncmp(argv[i], "--file=", 7) == 0) {
      dir_or_file = argv[i] + 7;
    } else if (strcmp(argv[i], "--output_hex") == 0) {
      output_hex = true;
    } else if (strcmp(argv[i], "--input_key_hex") == 0) {
      input_key_hex = true;
    } else if (sscanf(argv[i],
               "--read_num=%lu%c",
               (unsigned long*)&n, &junk) == 1) {
      read_num = n;
    } else if (strcmp(argv[i], "--verify_checksum") == 0) {
      verify_checksum = true;
    } else if (strncmp(argv[i], "--command=", 10) == 0) {
      command = argv[i] + 10;
    } else if (strncmp(argv[i], "--from=", 7) == 0) {
      from_key = argv[i] + 7;
      has_from = true;
    } else if (strncmp(argv[i], "--to=", 5) == 0) {
      to_key = argv[i] + 5;
      has_to = true;
    }else {
      print_help();
      exit(1);
    }
  }


  if (input_key_hex) {
    if (has_from) {
      from_key = HexToString(from_key);
    }
    if (has_to) {
      to_key = HexToString(to_key);
    }
  }

  if(dir_or_file == nullptr) {
    print_help();
    exit(1);
  }

  std::vector<std::string> filenames;
  rocksdb::Env* env = rocksdb::Env::Default();
  rocksdb::Status st = env->GetChildren(dir_or_file, &filenames);
  bool dir = true;
  if (!st.ok()) {
    filenames.clear();
    filenames.push_back(dir_or_file);
    dir = false;
  }

  std::cout << "from [" << rocksdb::Slice(from_key).ToString(true)
            << "] to [" << rocksdb::Slice(to_key).ToString(true) << "]\n";

  uint64_t total_read = 0;
  for (size_t i = 0; i < filenames.size(); i++) {
    std::string filename = filenames.at(i);
    if (filename.length() <= 4 ||
        filename.rfind(".sst") != filename.length() - 4) {
      //ignore
      continue;
    }
    if(dir) {
      filename = std::string(dir_or_file) + "/" + filename;
    }
    rocksdb::SstFileReader reader(filename, verify_checksum,
                                  output_hex);
    rocksdb::Status st;
    // scan all files in give file path.
    if (command == "" || command == "scan" || command == "check") {
      st = reader.ReadSequential(command != "check",
                                 read_num > 0 ? (read_num - total_read) :
                                                read_num,
                                 has_from, from_key, has_to, to_key);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(),
            st.ToString().c_str());
      }
      total_read += reader.GetReadNumber();
      if (read_num > 0 && total_read > read_num) {
        break;
      }
    }
  }
}
