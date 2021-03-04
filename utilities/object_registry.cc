// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/object_registry.h"

#include "logging/logging.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
#ifndef ROCKSDB_LITE
static const std::string kDefaultLibName = "Default";

// Looks through the "type" factories for one that matches "name".
// If found, returns the pointer to the Entry matching this name.
// Otherwise, nullptr is returned
const ObjectLibrary::Entry *ObjectLibrary::FindEntry(
    const std::string &type, const std::string &name) const {
  auto entries = entries_.find(type);
  if (entries != entries_.end()) {
    for (const auto &entry : entries->second) {
      if (entry->matches(name)) {
        return entry.get();
      }
    }
  }
  return nullptr;
}

size_t ObjectLibrary::GetFactoryCount(size_t *types) const {
  *types = entries_.size();
  size_t factories = 0;
  for (const auto &e : entries_) {
    factories += e.second.size();
  }
  return factories;
}

void ObjectLibrary::AddEntry(const std::string &type,
                             std::unique_ptr<Entry> &entry) {
  auto &entries = entries_[type];
  entries.emplace_back(std::move(entry));
}

void ObjectLibrary::Dump(Logger *logger) const {
  for (const auto &iter : entries_) {
    ROCKS_LOG_HEADER(logger, "    Registered factories for type[%s] ",
                     iter.first.c_str());
    bool printed_one = false;
    for (const auto &e : iter.second) {
      ROCKS_LOG_HEADER(logger, "%c %s", (printed_one) ? ',' : ':',
                       e->Name().c_str());
      printed_one = true;
    }
  }
  ROCKS_LOG_HEADER(logger, "\n");
}

size_t ObjectLibrary::GetRegisteredTypes(
    std::unordered_set<std::string> *types) const {
  size_t count = 0;
  for (const auto &iter : entries_) {
    if (types != nullptr) {
      types->insert(iter.first);
    }
    count++;
  }
  return count;
}

size_t ObjectLibrary::GetNamesForType(const std::string &type,
                                      std::vector<std::string> *names) const {
  size_t count = 0;
  auto iter = entries_.find(type);
  if (iter == entries_.end()) {
    count = 0;  // No entry, no count
  } else {
    count = iter->second.size();
    if (names != nullptr) {
      for (const auto &f : iter->second) {
        names->push_back(f->Name());
      }
    }
  }
  return count;
}

// Returns the Default singleton instance of the ObjectLibrary
// This instance will contain most of the "standard" registered objects
std::shared_ptr<ObjectLibrary> &ObjectLibrary::Default() {
  static std::shared_ptr<ObjectLibrary> instance =
      std::make_shared<ObjectLibrary>(kDefaultLibName);
  return instance;
}

class ObjectRegistryImpl : public ObjectRegistry {
 public:
  ObjectRegistryImpl() { libraries_.push_back(ObjectLibrary::Default()); }

  ObjectRegistryImpl(const std::shared_ptr<ObjectRegistry> &parent)
      : parent_(parent) {}

  std::shared_ptr<ObjectLibrary> AddProgramLibrary(
      const std::string &id, const RegistrarFunc &registrar,
      const std::string &arg) override;

  void Dump(Logger *logger) const override;

  // Returns the number of registered types for this registry.
  // If specified (not-null), types is updated to include the names of the
  // registered types.
  size_t GetRegisteredTypes(
      std::unordered_set<std::string> *types) const override;

  // Returns the number of registered names for the input type
  // If specified (not-null), names is updated to include the names for the type
  size_t GetNamesForType(
      const std::string &type,
      std::vector<std::string> *names = nullptr) const override;

  // Returns the total number of factories registered for this library.
  // This method returns the sum of all factories registered for all types.
  // @param num_types returns how many unique types are registered.
  size_t GetFactoryCount(size_t *num_types) const override;

 protected:
  const ObjectLibrary::Entry *FindEntry(const std::string &type,
                                        const std::string &name) const override;

 private:
  // The set of libraries to search for factories for this registry.
  // The libraries are searched in reverse order (back to front) when
  // searching for entries.
  std::vector<std::shared_ptr<ObjectLibrary>> libraries_;
  std::shared_ptr<ObjectRegistry> parent_;
};

std::shared_ptr<ObjectRegistry> &ObjectRegistry::Default() {
  static std::shared_ptr<ObjectRegistry> instance =
      std::make_shared<ObjectRegistryImpl>();
  return instance;
}

std::shared_ptr<ObjectRegistry> ObjectRegistry::NewInstance() {
  return NewInstance(Default());
}

std::shared_ptr<ObjectRegistry> ObjectRegistry::NewInstance(
    const std::shared_ptr<ObjectRegistry> &parent) {
  return std::make_shared<ObjectRegistryImpl>(parent);
}

void ObjectRegistryImpl::Dump(Logger *logger) const {
  for (auto iter = libraries_.crbegin(); iter != libraries_.crend(); ++iter) {
    iter->get()->Dump(logger);
  }
  if (parent_ != nullptr) {
    parent_->Dump(logger);
  }
}

std::shared_ptr<ObjectLibrary> ObjectRegistryImpl::AddProgramLibrary(
    const std::string &id, const RegistrarFunc &registrar,
    const std::string &arg) {
  auto library = std::make_shared<ObjectLibrary>(id);
  registrar(*(library.get()), arg);
  libraries_.push_back(library);
  return library;
}

// Searches (from back to front) the libraries looking for
// an entry that matches this pattern.
// Returns the entry if it is found, and nullptr otherwise
const ObjectLibrary::Entry *ObjectRegistryImpl::FindEntry(
    const std::string &type, const std::string &name) const {
  for (auto iter = libraries_.crbegin(); iter != libraries_.crend(); ++iter) {
    const auto *entry = iter->get()->FindEntry(type, name);
    if (entry != nullptr) {
      return entry;
    }
  }
  if (parent_ != nullptr) {
    return parent_->FindEntry(type, name);
  } else {
    return nullptr;
  }
}

// Returns the number of registered types for this registry.
// If specified (not-null), types is updated to include the names of the
// registered types.
size_t ObjectRegistryImpl::GetRegisteredTypes(
    std::unordered_set<std::string> *types) const {
  assert(types);
  for (const auto &library : libraries_) {
    library->GetRegisteredTypes(types);
  }
  if (parent_ != nullptr) {
    parent_->GetRegisteredTypes(types);
  }
  return types->size();
}

size_t ObjectRegistryImpl::GetNamesForType(
    const std::string &type, std::vector<std::string> *names) const {
  size_t count = 0;
  for (const auto &library : libraries_) {
    count += library->GetNamesForType(type, names);
  }
  if (parent_ != nullptr) {
    count += parent_->GetNamesForType(type, names);
  }
  return count;
}

// Returns the total number of factories registered for this library.
// This method returns the sum of all factories registered for all types.
// @param num_types returns how many unique types are registered.
size_t ObjectRegistryImpl::GetFactoryCount(size_t *num_types) const {
  std::unordered_set<std::string> types;
  *num_types = GetRegisteredTypes(&types);
  size_t count = 0;
  for (const auto &type : types) {
    count += GetNamesForType(type);
  }
  if (parent_ != nullptr) {
    count += parent_->GetFactoryCount(num_types);
  }
  return count;
}

#endif  // ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE
