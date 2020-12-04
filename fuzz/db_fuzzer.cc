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

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/testdb", &db);
  if (!status.ok()) {
    return 0;
  }
  FuzzedDataProvider fuzzed_data(data, size);

  // for random string generation
  const uint8_t* curr_offset = data;
  size_t curr_size = size;

  std::string value;

  // perform a sequence of calls on our db instance
  int max_iter = static_cast<int>(data[0]);
  for (int i = 0; i < max_iter && i < size; i++) {
    OperationType c = static_cast<OperationType>(data[i] % OP_COUNT);

    switch (c) {  // PUT
      case kPut: {
        std::string tmp1 = fuzzed_data.ConsumeRandomLengthString();
        std::string tmp2 = fuzzed_data.ConsumeRandomLengthString();
        db->Put(rocksdb::WriteOptions(), tmp1, tmp2);
        break;
      }
      case kGet: {
        std::string tmp1 = fuzzed_data.ConsumeRandomLengthString();
        db->Get(rocksdb::ReadOptions(), tmp1, &value);
        break;
      }
      case kDelete: {
        std::string tmp1 = fuzzed_data.ConsumeRandomLengthString();
        db->Delete(rocksdb::WriteOptions(), tmp1);
        break;
      }
      case kGetProperty: {
        std::string prop;
        std::string tmp1 = fuzzed_data.ConsumeRandomLengthString();
        db->GetProperty(tmp1, &prop);
        break;
      }
      case kIterator: {
        rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
          continue;
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
        delete db;
        status = rocksdb::DB::Open(options, "/tmp/testdb", &db);
        break;
      }
      case kColumn: {
        rocksdb::ColumnFamilyHandle* cf;
        rocksdb::Status s;
        s = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "new_cf",
                                   &cf);
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
          std::string tmp1 = fuzzed_data.ConsumeRandomLengthString();
          std::string tmp2 = fuzzed_data.ConsumeRandomLengthString();
          std::string tmp3 = fuzzed_data.ConsumeRandomLengthString();
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
            rocksdb::DestroyDB("/tmp/testdb", rocksdb::Options());
            return 0;
          }
        }
        break;
      }
      case kCompactRange: {
        std::string tmp1 = fuzzed_data.ConsumeRandomLengthString();
        std::string tmp2 = fuzzed_data.ConsumeRandomLengthString();

        rocksdb::Slice begin(tmp1);
        rocksdb::Slice end(tmp2);
        rocksdb::CompactRangeOptions options;
        rocksdb::Status s = db->CompactRange(options, &begin, &end);
        break;
      }
      case kSeekForPrev: {
        std::string tmp1 = fuzzed_data.ConsumeRandomLengthString();
        auto iter = db->NewIterator(rocksdb::ReadOptions());
        iter->SeekForPrev(tmp1);
        delete iter;
        break;
      }
    }
  }

  // Cleanup DB
  db->Close();
  delete db;
  rocksdb::DestroyDB("/tmp/testdb", rocksdb::Options());
  return 0;
}
