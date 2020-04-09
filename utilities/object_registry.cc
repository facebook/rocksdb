// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/object_registry.h"

#include <memory>
#include <unordered_map>

#include "logging/logging.h"
#include "options/customizable_helper.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace rocksdb {
#ifndef ROCKSDB_LITE
static const std::string kDefaultLibraryName = "default";
static const std::string kLocalLibraryName = "local";
static const std::string kDynamicLibraryName = "dynamic";
static const std::string kClassPropName = "id";

std::string ObjectLibrary::AsPrintableOptions(const std::string &name) const {
  size_t types, factories;
  factories = GetFactoryCount(&types);
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  snprintf(buffer, kBufferSize, "%s: %" ROCKSDB_PRIszt "/%" ROCKSDB_PRIszt,
           name.c_str(), types, factories);
  std::string result = buffer;
  return result;
}

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

size_t ObjectLibrary::GetRegisteredTypes(
    std::unordered_set<std::string> *types) const {
  size_t count = 0;
  for (const auto &iter : entries_) {
    types->insert(iter.first);
    count++;
  }
  return count;
}

size_t ObjectLibrary::GetRegisteredNames(
    const std::string &type, std::vector<std::string> *names) const {
  size_t count = 0;
  auto iter = entries_.find(type);
  if (iter == entries_.end()) {
    count = 0;  // No entry, no count
  } else if (names != nullptr) {
    for (count = 0; count < iter->second.size(); count++) {
      names->push_back(iter->second[count]->Name());
    }
  } else {
    count = iter->second.size();
  }
  return count;
}

class DefaultObjectLibrary : public ObjectLibrary {
 public:
  const char *Name() const override { return kDefaultLibraryName.c_str(); }

  virtual std::string GetPrintableOptions() const override {
    return AsPrintableOptions("default_library");
  }
};

// Returns the Default singleton instance of the ObjectLibrary
// This instance will contain most of the "standard" registered objects
std::shared_ptr<ObjectLibrary> &ObjectLibrary::Default() {
  static std::shared_ptr<ObjectLibrary> instance =
      std::make_shared<DefaultObjectLibrary>();
  return instance;
}

std::shared_ptr<ObjectRegistry> ObjectRegistry::NewInstance() {
  std::shared_ptr<ObjectRegistry> instance = std::make_shared<ObjectRegistry>();
  return instance;
}

std::shared_ptr<ObjectRegistry> ObjectRegistry::Clone() const {
  std::shared_ptr<ObjectRegistry> instance = NewInstance();
  for (const auto &library : libraries_) {
    if (library != ObjectLibrary::Default()) {
      instance->libraries_.push_back(library);
    }
  }
  return instance;
}
static std::unordered_map<std::string, OptionTypeInfo>
    object_registry_library_type_info = {
        {"libraries",
         OptionTypeInfo::Vector<std::shared_ptr<ObjectLibrary> >(
             0, OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever,
             OptionTypeInfo::AsCustomS<ObjectLibrary>(
                 0, OptionVerificationType::kNormal,
                 OptionTypeFlags::kShared | OptionTypeFlags::kCompareNever))},
};

ObjectRegistry::ObjectRegistry() {
  libraries_.push_back(ObjectLibrary::Default());
  RegisterOptions("Libraries", &libraries_, &object_registry_library_type_info);
}

// Searches (from back to front) the libraries looking for the
// an entry that matches this pattern.
// Returns the entry if it is found, and nullptr otherwise
const ObjectLibrary::Entry *ObjectRegistry::FindEntry(
    const std::string &type, const std::string &name) const {
  for (auto iter = libraries_.crbegin(); iter != libraries_.crend(); ++iter) {
    const auto *entry = iter->get()->FindEntry(type, name);
    if (entry != nullptr) {
      return entry;
    }
  }
  return nullptr;
}

size_t ObjectRegistry::GetRegisteredTypes(
    std::unordered_set<std::string> *types) const {
  assert(types);
  for (const auto library : libraries_) {
    library->GetRegisteredTypes(types);
  }
  return types->size();
}

size_t ObjectRegistry::GetRegisteredNames(
    const std::string &type, std::vector<std::string> *names) const {
  size_t count = 0;
  for (const auto library : libraries_) {
    count += library->GetRegisteredNames(type, names);
  }
  return count;
}

size_t ObjectRegistry::GetFactoryCount(size_t *type_count) const {
  std::unordered_set<std::string> types;
  *type_count = GetRegisteredTypes(&types);
  size_t count = 0;
  for (const auto type : types) {
    count += GetRegisteredNames(type, nullptr);
  }
  return count;
}

std::string ObjectRegistry::GetPrintableOptions() const {
  size_t types, factories;
  factories = GetFactoryCount(&types);
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  snprintf(buffer, kBufferSize, "%" ROCKSDB_PRIszt "/%" ROCKSDB_PRIszt "\n",
           types, factories);
  std::string result = buffer;

  for (const auto library : libraries_) {
    result.append(library->GetPrintableOptions() + "\n");
  }
  return result;
}

struct LocalLibraryOptions {
  std::string method;
  std::string arg;
  std::string library;
};

static std::unordered_map<std::string, OptionTypeInfo> local_library_type_info =
    {
        {"method",
         {offsetof(struct LocalLibraryOptions, method), OptionType::kString}},
        {"arg",
         {offsetof(struct LocalLibraryOptions, arg), OptionType::kString}},
};

class LocalObjectLibrary : public ObjectLibrary {
 public:
  LocalObjectLibrary() {
    RegisterOptions("LocalOptions", &options, &local_library_type_info);
  }

  LocalObjectLibrary(const std::string &method, const std::string &arg) {
    options.method = method;
    options.arg = arg;
    RegisterOptions("LocalOptions", &options, &local_library_type_info);
  }

  const char *Name() const override { return kLocalLibraryName.c_str(); }

  std::string GetPrintableOptions() const override {
    return AsPrintableOptions("::" + options.method + "(" + options.arg + ")");
  }

  Status RegisterLibrary(const std::shared_ptr<DynamicLibrary> &dll) {
    Status status;
    if (options.method.empty()) {
      status = Status::InvalidArgument("Missing method argument");
    } else {
      RegistrarFunc registrar;
      status = dll->LoadFunction(options.method, &registrar);
      if (status.ok()) {
        Register(registrar, options.arg);
      }
    }
    return status;
  }

  Status PrepareOptions(const ConfigOptions &opts) override {
    Status status;
    if (entries_.empty()) {
      std::shared_ptr<DynamicLibrary> dll;
      status = opts.env->LoadLibrary(options.library, "", &dll);
      if (status.ok()) {
        status = RegisterLibrary(dll);
      }
    }
    return status;
  }

  LocalLibraryOptions options;
};

void ObjectRegistry::AddLocalLibrary(const RegistrarFunc &registrar,
                                     const std::string &method,
                                     const std::string &arg) {
  std::shared_ptr<ObjectLibrary> lib =
      std::make_shared<LocalObjectLibrary>(method, arg);
  lib->Register(registrar, arg);
  libraries_.emplace_back(lib);
}

Status ObjectRegistry::AddLocalLibrary(Env *env, const std::string &method,
                                       const std::string &arg) {
  std::shared_ptr<DynamicLibrary> dll;
  Status status = env->LoadLibrary("", "", &dll);
  if (status.ok()) {
    std::shared_ptr<LocalObjectLibrary> lib =
        std::make_shared<LocalObjectLibrary>(method, arg);
    status = lib->RegisterLibrary(dll);
    if (status.ok()) {
      libraries_.emplace_back(lib);
    }
  }
  return status;
}

static std::unordered_map<std::string, OptionTypeInfo>
    dynamic_library_type_info = {
        {"library",
         {offsetof(struct LocalLibraryOptions, library), OptionType::kString}},
};

class DynamicObjectLibrary : public LocalObjectLibrary {
 public:
  DynamicObjectLibrary() : LocalObjectLibrary() {
    RegisterOptions("DynamicOptions", &options, &dynamic_library_type_info);
  }

  DynamicObjectLibrary(const std::shared_ptr<DynamicLibrary> &dll,
                       const std::string &method, const std::string &arg)
      : LocalObjectLibrary(method, arg), dll_(dll) {
    options.library = dll_->Name();
    RegisterOptions("DynamicOptions", &options, &dynamic_library_type_info);
  }

  ~DynamicObjectLibrary() override {
    // Force the entries to be cleared before the library is closed...
    entries_.clear();
  }

  std::string GetPrintableOptions() const override {
    return options.library + LocalObjectLibrary::GetPrintableOptions();
  }
  const char *Name() const override { return kDynamicLibraryName.c_str(); }

  Status PrepareOptions(const ConfigOptions &opts) override {
    Status status = opts.env->LoadLibrary(options.library, "", &dll_);
    if (status.ok()) {
      status = RegisterLibrary(dll_);
    }
    return status;
  }

 private:
  std::shared_ptr<DynamicLibrary> dll_;
};

static bool LoadObjectLibrary(const std::string &id,
                              std::shared_ptr<ObjectLibrary> *library) {
  bool success = true;
  if (id == kDynamicLibraryName) {
    library->reset(new DynamicObjectLibrary());
  } else if (id == kLocalLibraryName) {
    library->reset(new LocalObjectLibrary());
  } else if (id == kDefaultLibraryName) {
    *library = ObjectLibrary::Default();
  } else {
    success = false;
  }
  return success;
}

Status ObjectLibrary::CreateFromString(
    const std::string &value, const ConfigOptions &opts,
    std::shared_ptr<ObjectLibrary> *library) {
  Status s =
      LoadSharedObject<ObjectLibrary>(value, LoadObjectLibrary, opts, library);
  if (s.ok()) {
    s = library->get()->PrepareOptions(opts);
  }
  return s;
}

Status ObjectRegistry::AddDynamicLibrary(Env *env, const std::string &name,
                                         const std::string &method,
                                         const std::string &arg) {
  // Load a dynamic library and add it to the list
  std::shared_ptr<DynamicLibrary> library;
  Status s = env->LoadLibrary(name, "", &library);
  if (s.ok()) {
    s = AddDynamicLibrary(library, method, arg);
  }

  return s;
}

Status ObjectRegistry::AddDynamicLibrary(
    const std::shared_ptr<DynamicLibrary> &dyn_lib, const std::string &method,
    const std::string &arg) {
  std::function<void *(void *, const char *)> function;
  RegistrarFunc registrar;
  Status s = dyn_lib->LoadFunction(method, &registrar);
  if (s.ok()) {
    std::shared_ptr<ObjectLibrary> obj_lib;
    obj_lib.reset(new DynamicObjectLibrary(dyn_lib, method, arg));
    obj_lib->Register(registrar, arg);
    libraries_.emplace_back(obj_lib);
  }
  return s;
}

#endif  // ROCKSDB_LITE
}  // namespace rocksdb
