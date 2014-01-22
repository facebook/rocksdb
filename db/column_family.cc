#include "db/column_family.h"
#include "db/version_set.h"

namespace rocksdb {

ColumnFamilyData::ColumnFamilyData(uint32_t id, const std::string& name,
                                   Version* dummy_versions,
                                   const ColumnFamilyOptions& options)
    : id(id),
      name(name),
      dummy_versions(dummy_versions),
      current(nullptr),
      options(options) {}

ColumnFamilyData::~ColumnFamilyData() {
  // List must be empty
  assert(dummy_versions->next_ == dummy_versions);
  delete dummy_versions;
}

ColumnFamilySet::ColumnFamilySet() : max_column_family_(0) {}

ColumnFamilySet::~ColumnFamilySet() {
  for (auto& cfd : column_family_data_) {
    delete cfd.second;
  }
  for (auto& cfd : droppped_column_families_) {
    delete cfd;
  }
}

ColumnFamilyData* ColumnFamilySet::GetDefault() const {
  auto ret = GetColumnFamily(0);
  assert(ret != nullptr); // default column family should always exist
  return ret;
}

ColumnFamilyData* ColumnFamilySet::GetColumnFamily(uint32_t id) const {
  auto cfd_iter = column_family_data_.find(id);
  if (cfd_iter != column_family_data_.end()) {
    return cfd_iter->second;
  } else {
    return nullptr;
  }
}

bool ColumnFamilySet::Exists(uint32_t id) {
  return column_family_data_.find(id) != column_family_data_.end();
}

bool ColumnFamilySet::Exists(const std::string& name) {
  return column_families_.find(name) != column_families_.end();
}

uint32_t ColumnFamilySet::GetID(const std::string& name) {
  auto cfd_iter = column_families_.find(name);
  assert(cfd_iter != column_families_.end());
  return cfd_iter->second;
}

uint32_t ColumnFamilySet::GetNextColumnFamilyID() {
  return ++max_column_family_;
}

ColumnFamilyData* ColumnFamilySet::CreateColumnFamily(
    const std::string& name, uint32_t id, Version* dummy_versions,
    const ColumnFamilyOptions& options) {
  assert(column_families_.find(name) == column_families_.end());
  column_families_.insert({name, id});
  ColumnFamilyData* new_cfd =
      new ColumnFamilyData(id, name, dummy_versions, options);
  column_family_data_.insert({id, new_cfd});
  max_column_family_ = std::max(max_column_family_, id);
  return new_cfd;
}

void ColumnFamilySet::DropColumnFamily(uint32_t id) {
  auto cfd = column_family_data_.find(id);
  assert(cfd != column_family_data_.end());
  column_families_.erase(cfd->second->name);
  cfd->second->current->Unref();
  droppped_column_families_.push_back(cfd->second);
  column_family_data_.erase(cfd);
}

}  // namespace rocksdb
