#include <rocksdb/memtablerep.h>
#include "util/string_util.h"

namespace rocksdb {

static std::unordered_map<std::string, MemTableRegister::FactoryCreator>
memtable_factory_map;

MemTableRegister::MemTableRegister(const char* factoryName, FactoryCreator fc) {
  auto ib = memtable_factory_map.insert(std::make_pair(factoryName, fc));
  if (!ib.second) {
    fprintf(stderr, "ERROR: duplicate MemTable name: %s\n", factoryName);
  }
}

MemTableRepFactory* CreateMemTableRepFactory(
    const std::string& factoryName,
    const std::unordered_map<std::string, std::string>& options, Status* s) {
  auto f = memtable_factory_map.find(factoryName);
  if (memtable_factory_map.end() != f) {
    return f->second(options, s);
  }
  *s = Status::NotFound();
  return NULL;
}

} // namespace rocksdb

