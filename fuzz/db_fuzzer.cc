#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <filesystem>

#include "rocksdb/db.h"

// Helper function to getting strings based on the random data.
std::string get_string(const uint8_t** data, size_t* remaining) {
  if (*remaining == 0) {
    return std::string("");
  }
  uint8_t s_size = (*data)[0];

  // if s_size is more than we have available, do not use s_size
  if (s_size > *remaining) {
    std::string s1(reinterpret_cast<const char*>(*data), *remaining);
    *data += *remaining;
    *remaining = 0;
    return s1;
  }

  std::string s1(reinterpret_cast<const char*>(*data), s_size);
  *remaining -= s_size;
  return s1;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/testdb", &db);

  // for random string generation
  const uint8_t* curr_offset = data;
  size_t curr_size = size;

  std::string value;

  // perform a sequence of calls on our db instance
  int max_iter = (int)data[0];
  for (int i = 0; i < max_iter && i < size; i++) {
#define SIZE_OF_FUNCS 10
    char c = data[i] % SIZE_OF_FUNCS;

    if (c == 0) {  // PUT
      std::string tmp1 = get_string(&curr_offset, &curr_size);
      std::string tmp2 = get_string(&curr_offset, &curr_size);
      db->Put(rocksdb::WriteOptions(), tmp1, tmp2);
    } else if (c == 1) {  // Get
      std::string tmp1 = get_string(&curr_offset, &curr_size);
      db->Get(rocksdb::ReadOptions(), tmp1, &value);
    } else if (c == 2) {  // Delete
      std::string tmp1 = get_string(&curr_offset, &curr_size);
      db->Delete(rocksdb::WriteOptions(), tmp1);
    } else if (c == 3) {  // GetProperty
      std::string prop;
      std::string tmp1 = get_string(&curr_offset, &curr_size);
      db->GetProperty(tmp1, &prop);
    } else if (c == 4) {  // Iterator
      rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        continue;
      }
      delete it;
    } else if (c == 5) {  // GetSnapshot and Release Snapshot
      rocksdb::ReadOptions snapshot_options;
      snapshot_options.snapshot = db->GetSnapshot();
      rocksdb::Iterator* it = db->NewIterator(snapshot_options);
      db->ReleaseSnapshot(snapshot_options.snapshot);
      delete it;
    } else if (c == 6) {  // Open and close DB
      delete db;
      status = rocksdb::DB::Open(options, "/tmp/testdb", &db);
    } else if (c == 7) {  // Create column family
      rocksdb::ColumnFamilyHandle* cf;
      rocksdb::Status s;
      s = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "new_cf", &cf);
      s = db->DestroyColumnFamilyHandle(cf);
      delete db;

      // open DB with two column families
      std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
      // have to open default column family
      column_families.push_back(rocksdb::ColumnFamilyDescriptor(
          rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
      // open the new one, too
      column_families.push_back(rocksdb::ColumnFamilyDescriptor(
          "new_cf", rocksdb::ColumnFamilyOptions()));
      std::vector<rocksdb::ColumnFamilyHandle*> handles;
      s = rocksdb::DB::Open(rocksdb::DBOptions(), "/tmp/testdb",
                            column_families, &handles, &db);

      if (s.ok()) {
        std::string tmp1 = get_string(&curr_offset, &curr_size);
        std::string tmp2 = get_string(&curr_offset, &curr_size);
        std::string tmp3 = get_string(&curr_offset, &curr_size);
        s = db->Put(rocksdb::WriteOptions(), handles[1], tmp1, tmp2);
        std::string value;
        s = db->Get(rocksdb::ReadOptions(), handles[1], tmp3, &value);
        s = db->DropColumnFamily(handles[1]);
        for (auto handle : handles) {
          s = db->DestroyColumnFamilyHandle(handle);
        }
      } else {
        status = rocksdb::DB::Open(options, "/tmp/testdb", &db);
        if (!status.ok()) {
          // At this point there is no saving to do. So we exit
          std::__fs::filesystem::remove_all("/tmp/testdb");
          return 0;
        }
      }
    } else if (c == 8) {  // CompactRange
      std::string tmp1 = get_string(&curr_offset, &curr_size);
      std::string tmp2 = get_string(&curr_offset, &curr_size);

      rocksdb::Slice begin(tmp1);
      rocksdb::Slice end(tmp2);
      rocksdb::CompactRangeOptions options;
      rocksdb::Status s = db->CompactRange(options, &begin, &end);
    } else if (c == 9) {  // SeekForPrev
      std::string tmp1 = get_string(&curr_offset, &curr_size);
      auto iter = db->NewIterator(rocksdb::ReadOptions());
      iter->SeekForPrev(tmp1);
      delete iter;
    }
  }

  // Cleanup DB
  delete db;
  std::__fs::filesystem::remove_all("/tmp/testdb");
  return 0;
}
