#pragma once

#include "rocksdb/utilities/stackable_db.h"
#include "utilities/titandb/options.h"

namespace rocksdb {
namespace titandb {

struct TitanCFDescriptor {
  std::string name;
  TitanCFOptions options;
  TitanCFDescriptor()
      : name(kDefaultColumnFamilyName), options(TitanCFOptions()) {}
  TitanCFDescriptor(const std::string& _name, const TitanCFOptions& _options)
      : name(_name), options(_options) {}
};

class TitanDB : public StackableDB {
 public:
  static Status Open(const TitanOptions& options, const std::string& dbname,
                     TitanDB** db);

  static Status Open(const TitanDBOptions& db_options,
                     const std::string& dbname,
                     const std::vector<TitanCFDescriptor>& descs,
                     std::vector<ColumnFamilyHandle*>* handles, TitanDB** db);

  TitanDB() : StackableDB(nullptr) {}

  using StackableDB::CreateColumnFamily;
  Status CreateColumnFamily(const ColumnFamilyOptions& options,
                            const std::string& name,
                            ColumnFamilyHandle** handle) override {
    TitanCFDescriptor desc(name, TitanCFOptions(options));
    return CreateColumnFamily(desc, handle);
  }
  Status CreateColumnFamily(const TitanCFDescriptor& desc,
                            ColumnFamilyHandle** handle) {
    std::vector<ColumnFamilyHandle*> handles;
    Status s = CreateColumnFamilies({desc}, &handles);
    if (s.ok()) {
      *handle = handles[0];
    }
    return s;
  }

  using StackableDB::CreateColumnFamilies;
  Status CreateColumnFamilies(
      const ColumnFamilyOptions& options,
      const std::vector<std::string>& names,
      std::vector<ColumnFamilyHandle*>* handles) override {
    std::vector<TitanCFDescriptor> descs;
    for (auto& name : names) {
      descs.emplace_back(name, TitanCFOptions(options));
    }
    return CreateColumnFamilies(descs, handles);
  }
  Status CreateColumnFamilies(
      const std::vector<ColumnFamilyDescriptor>& base_descs,
      std::vector<ColumnFamilyHandle*>* handles) override {
    std::vector<TitanCFDescriptor> descs;
    for (auto& desc : base_descs) {
      descs.emplace_back(desc.name, TitanCFOptions(desc.options));
    }
    return CreateColumnFamilies(descs, handles);
  }
  virtual Status CreateColumnFamilies(
      const std::vector<TitanCFDescriptor>& descs,
      std::vector<ColumnFamilyHandle*>* handles) = 0;

  Status DropColumnFamily(ColumnFamilyHandle* handle) override {
    return DropColumnFamilies({handle});
  }

  virtual Status DropColumnFamilies(
      const std::vector<ColumnFamilyHandle*>& handles) override = 0;

  using StackableDB::Merge;
  Status Merge(const WriteOptions&, ColumnFamilyHandle*, const Slice& /*key*/,
               const Slice& /*value*/) override {
    return Status::NotSupported("TitanDB doesn't support this operation");
  }

  using rocksdb::StackableDB::SingleDelete;
  virtual Status SingleDelete(const WriteOptions& /*wopts*/,
                              ColumnFamilyHandle* /*column_family*/,
                              const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in titan db.");
  }

  using rocksdb::StackableDB::CompactFiles;
  virtual Status CompactFiles(
      const CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr,
      CompactionJobInfo* compaction_job_info = nullptr) override = 0;
};

}  // namespace titandb
}  // namespace rocksdb
