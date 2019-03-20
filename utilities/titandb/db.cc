#include "rocksdb/utilities/titandb/db.h"

#include "utilities/titandb/db_impl.h"

namespace rocksdb {
namespace titandb {

Status TitanDB::Open(const TitanOptions& options, const std::string& dbname,
                     TitanDB** db) {
  TitanDBOptions db_options(options);
  TitanCFOptions cf_options(options);
  std::vector<TitanCFDescriptor> descs;
  descs.emplace_back(kDefaultColumnFamilyName, cf_options);
  std::vector<ColumnFamilyHandle*> handles;
  Status s = TitanDB::Open(db_options, dbname, descs, &handles, db);
  if (s.ok()) {
    assert(handles.size() == 1);
    // DBImpl is always holding the default handle.
    delete handles[0];
  }
  return s;
}

Status TitanDB::Open(const TitanDBOptions& db_options,
                     const std::string& dbname,
                     const std::vector<TitanCFDescriptor>& descs,
                     std::vector<ColumnFamilyHandle*>* handles, TitanDB** db) {
  auto impl = new TitanDBImpl(db_options, dbname);
  auto s = impl->Open(descs, handles);
  if (s.ok()) {
    *db = impl;
  } else {
    *db = nullptr;
    delete impl;
  }
  return s;
}

}  // namespace titandb
}  // namespace rocksdb
