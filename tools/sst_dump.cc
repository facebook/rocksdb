#include "leveldb/table.h"

#include <map>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/table_builder.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace leveldb {

class SstFileReader {
public:
  SstFileReader(std::string file_name);
  Status ReadSequential(bool print_kv, uint64_t read_num = -1);

  uint64_t GetReadNumber() { return read_num_; }

private:
  std::string file_name_;
  uint64_t read_num_;
};

SstFileReader::SstFileReader(std::string file_path)
:file_name_(file_path), read_num_(0) {
}

Status SstFileReader::ReadSequential(bool print_kv, uint64_t read_num)
{
  Table* table;
  Options table_options;
  RandomAccessFile* file = NULL;
  Status s = table_options.env->NewRandomAccessFile(file_name_, &file);
  if(!s.ok()) {
   return s;
  }
  uint64_t file_size;
  table_options.env->GetFileSize(file_name_, &file_size);
  s = Table::Open(table_options, file, file_size, &table);
  if(!s.ok()) {
   return s;
  }

  Iterator* iter = table->NewIterator(ReadOptions());
  long i = 0;
  int64_t bytes = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    Slice key = iter->key();
    Slice value = iter->value();
    ++i;
    if (i > read_num)
      break;
    if (print_kv) {
      fprintf(stdout, "%s : %s\n",
          key.ToString().c_str(), value.ToString().c_str());
    }
   }

   read_num_ += i;

   Status ret = iter->status();
   delete iter;
   return ret;
}

} // namespace leveldb

static void print_help() {
  fprintf(stderr,
      "sst_dump [--command=check|scan] --file=data_dir_OR_sst_file"
      " [--read_num=NUM]\n");
}

int main(int argc, char** argv) {

  const char* dir_or_file;
  uint64_t read_num = -1;
  std::string command;

  char junk;
  uint64_t n;
  for (int i = 1; i < argc; i++)
  {
    if (strncmp(argv[i], "--file=", 7) == 0) {
      dir_or_file = argv[i] + 7;
    } else if (sscanf(argv[i], "--read_num=%ld%c", &n, &junk) == 1) {
      read_num = n;
    } else if (strncmp(argv[i], "--command=", 10) == 0) {
      command = argv[i] + 10;
    } else {
      print_help();
      exit(1);
    }
  }

  if(dir_or_file == NULL) {
    print_help();
    exit(1);
  }

  std::vector<std::string> filenames;
  leveldb::Env* env = leveldb::Env::Default();
  leveldb::Status st = env->GetChildren(dir_or_file, &filenames);
  bool dir = true;
  if (!st.ok()) {
    filenames.clear();
    filenames.push_back(dir_or_file);
    dir = false;
  }

  int total_read = 0;
  for (int i = 0; i < filenames.size(); i++) {
    std::string filename = filenames.at(i);
    if (filename.length() <= 4 ||
        filename.rfind(".sst") != filename.length() - 4) {
      //ignore
      continue;
    }
    if(dir) {
      filename = dir_or_file + filename;
    }
    leveldb::SstFileReader reader(filename);
    leveldb::Status st;
    // scan all files in give file path.
    if (command == "" || command == "scan" || command == "check") {
      st = reader.ReadSequential(command != "check");
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(),
            st.ToString().c_str());
      }
      total_read += reader.GetReadNumber();
      if (read_num >= 0 && total_read > read_num) {
        break;
      }
    }
  }
}
