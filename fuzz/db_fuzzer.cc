#include <fuzzer/FuzzedDataProvider.h>

#include "rocksdb/db.h"

enum OperationType {
  kPut,
  kGet,
  kDelete,
  kGetProperty,
  kIterator,
  kSnapshot,
  kOpenClose,
  kColumn,
  kCompactRange,
  kSeekForPrev,
  OP_COUNT
};

constexpr char db_path[] = "/tmp/testdb";

// Fuzzes DB operations by doing interpretations on the data. Both the
// sequence of API calls to be called on the DB as well as the arguments
// to each of these APIs are interpreted by way of the data buffer.
// The operations that the fuzzer supports are given by the OperationType
// enum. The goal is to capture sanitizer bugs, so the code should be
// compiled with a given sanitizer (ASan, UBSan, MSan).
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);
  if (!status.ok()) {
    return 0;
  }
  FuzzedDataProvider fuzzed_data(data, size);

  // perform a sequence of calls on our db instance
  int max_iter = static_cast<int>(data[0]);
  for (int i = 0; i < max_iter && i < size; i++) {
    OperationType op = static_cast<OperationType>(data[i] % OP_COUNT);

    switch (op) {
      case kPut: {
        std::string key = fuzzed_data.ConsumeRandomLengthString();
        std::string val = fuzzed_data.ConsumeRandomLengthString();
        db->Put(rocksdb::WriteOptions(), key, val);
        break;
      }
      case kGet: {
        std::string key = fuzzed_data.ConsumeRandomLengthString();
        std::string value;
        db->Get(rocksdb::ReadOptions(), key, &value);
        break;
      }
      case kDelete: {
        std::string key = fuzzed_data.ConsumeRandomLengthString();
        db->Delete(rocksdb::WriteOptions(), key);
        break;
      }
      case kGetProperty: {
        std::string prop;
        std::string property_name = fuzzed_data.ConsumeRandomLengthString();
        db->GetProperty(property_name, &prop);
        break;
      }
      case kIterator: {
        rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
        }
        delete it;
        break;
      }
      case kSnapshot: {
        rocksdb::ReadOptions snapshot_options;
        snapshot_options.snapshot = db->GetSnapshot();
        rocksdb::Iterator* it = db->NewIterator(snapshot_options);
        db->ReleaseSnapshot(snapshot_options.snapshot);
        delete it;
        break;
      }
      case kOpenClose: {
        db->Close();
        delete db;
        status = rocksdb::DB::Open(options, db_path, &db);
        if (!status.ok()) {
          rocksdb::DestroyDB(db_path, options);
          return 0;
        }

        break;
      }
      case kColumn: {
        rocksdb::ColumnFamilyHandle* cf;
        rocksdb::Status s;
        s = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "new_cf",
                                   &cf);
        s = db->DestroyColumnFamilyHandle(cf);
        db->Close();
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
        s = rocksdb::DB::Open(rocksdb::DBOptions(), db_path, column_families,
                              &handles, &db);

        if (s.ok()) {
          std::string key1 = fuzzed_data.ConsumeRandomLengthString();
          std::string val1 = fuzzed_data.ConsumeRandomLengthString();
          std::string key2 = fuzzed_data.ConsumeRandomLengthString();
          s = db->Put(rocksdb::WriteOptions(), handles[1], key1, val1);
          std::string value;
          s = db->Get(rocksdb::ReadOptions(), handles[1], key2, &value);
          s = db->DropColumnFamily(handles[1]);
          for (auto handle : handles) {
            s = db->DestroyColumnFamilyHandle(handle);
          }
        } else {
          status = rocksdb::DB::Open(options, db_path, &db);
          if (!status.ok()) {
            // At this point there is no saving to do. So we exit
            rocksdb::DestroyDB(db_path, rocksdb::Options());
            return 0;
          }
        }
        break;
      }
      case kCompactRange: {
        std::string slice_start = fuzzed_data.ConsumeRandomLengthString();
        std::string slice_end = fuzzed_data.ConsumeRandomLengthString();

        rocksdb::Slice begin(slice_start);
        rocksdb::Slice end(slice_end);
        rocksdb::CompactRangeOptions options;
        rocksdb::Status s = db->CompactRange(options, &begin, &end);
        break;
      }
      case kSeekForPrev: {
        std::string key = fuzzed_data.ConsumeRandomLengthString();
        auto iter = db->NewIterator(rocksdb::ReadOptions());
        iter->SeekForPrev(key);
        delete iter;
        break;
      }
    }
  }

  // Cleanup DB
  db->Close();
  delete db;
  rocksdb::DestroyDB(db_path, options);
  return 0;
}
