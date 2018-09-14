#include <rocksdb/memtablerep.h>
#include "util/string_util.h"

namespace rocksdb {

static std::unordered_map<std::string, MemTableRegister::FactoryCreator>
memtable_factory_map;

MemTableRegister::MemTableRegister(const char* name, FactoryCreator fc) {
  auto ib = memtable_factory_map.insert(std::make_pair(name, fc));
  if (!ib.second) {
    fprintf(stderr, "ERROR: duplicate MemTable name: %s\n", name);
  }
}

MemTableRepFactory* CreateMemTableRepFactory(
    const std::string& name,
    const std::unordered_map<std::string, std::string>& options, Status* s) {
  auto f = memtable_factory_map.find(name);
  if (memtable_factory_map.end() != f) {
    return f->second(options, s);
  }
  *s = Status::NotFound("CreateMemTableRepFactory", name);
  return NULL;
}

} // namespace rocksdb

