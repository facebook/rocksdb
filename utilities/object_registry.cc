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

namespace ROCKSDB_NAMESPACE {
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

class DefaultObjectLibrary : public ObjectLibrary {
 public:
  const char *Name() const override { return kDefaultLibraryName.c_str(); }

  virtual std::string GetPrintableOptions() const override {
    return AsPrintableOptions("default_library");
  }
};

struct ObjectRegistryOptions {
  // The set of libraries to search for factories for this registry.
  // The libraries are searched in reverse order (back to front) when
  // searching for entries.
  std::vector<std::shared_ptr<ObjectLibrary>> libraries;

  // An optional list of paths in which to search for the dynamic libraries in.
  std::string paths;
};

static std::unordered_map<std::string, OptionTypeInfo>
    object_registry_library_type_info = {
        {"libraries",
         OptionTypeInfo::Vector<std::shared_ptr<ObjectLibrary>>(
             offsetof(struct ObjectRegistryOptions, libraries),
             OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever,
             OptionTypeInfo::AsCustomS<ObjectLibrary>(
                 0, OptionVerificationType::kNormal,
                 OptionTypeFlags::kShared | OptionTypeFlags::kCompareNever))},
        {"paths",
         {offsetof(struct ObjectRegistryOptions, paths), OptionType::kString,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone,
          /* Use the default parser */ nullptr,
          [](const ConfigOptions & /*opts*/, const std::string & /*name*/,
             const char *addr, std::string *value) {
            // If the paths contain ";", we need to wrap in {} to prevent
            // them from being treated as individual options (since ":"
            // is the default separator).
            auto *paths = reinterpret_cast<const std::string *>(addr);
            if (paths->empty()) {
              *value = "";
            } else if (paths->find(";") != std::string::npos) {
              *value = "{" + *paths + "}";
            } else {
              *value = *paths;
            }
            return Status::OK();
          },
          /* Use the default comparison*/ nullptr}},
};

// Returns the Default singleton instance of the ObjectLibrary
// This instance will contain most of the "standard" registered objects
std::shared_ptr<ObjectLibrary> &ObjectLibrary::Default() {
  static std::shared_ptr<ObjectLibrary> instance =
      std::make_shared<DefaultObjectLibrary>();
  return instance;
}

class ObjectRegistryImpl : public ObjectRegistry {
 public:
  constexpr static const char *kPathOpts = "LibraryPaths";

  ObjectRegistryImpl() {
    options.libraries.push_back(ObjectLibrary::Default());
    RegisterOptions("Libraries", &options, &object_registry_library_type_info);
  }

  ObjectRegistryImpl(const ObjectRegistryImpl &that) {
    for (const auto &library : that.options.libraries) {
      options.libraries.push_back(library);
    }
    RegisterOptions("Libraries", &options.libraries,
                    &object_registry_library_type_info);
  }

  // Makes a copy of this registry.
  std::shared_ptr<ObjectRegistry> Clone() const override {
    return std::make_shared<ObjectRegistryImpl>(*this);
  }

  // Returns the registry contents as a printable string, suitable for Dump
  std::string GetPrintableOptions() const override {
    size_t types, factories;
    factories = GetFactoryCount(&types);
    const int kBufferSize = 200;
    char buffer[kBufferSize];
    snprintf(buffer, kBufferSize, "%" ROCKSDB_PRIszt "/%" ROCKSDB_PRIszt "\n",
             types, factories);
    std::string result = buffer;

    for (const auto library : options.libraries) {
      result.append(library->GetPrintableOptions() + "\n");
    }
    return result;
  }

  void AddLocalLibrary(const RegistrarFunc &registrar,
                       const std::string &method,
                       const std::string &arg) override;

  // Creates a new local library, registering the factories in method
  Status AddLocalLibrary(Env *env, const std::string &method,
                         const std::string &arg) override;

  // Loads the types from the named library and method into a new dynamic object
  Status AddDynamicLibrary(Env *env, const std::string &library,
                           const std::string &method,
                           const std::string &arg) override;

  // Loads the method from the named library and method into a new dynamic
  // object library.
  Status AddDynamicLibrary(const std::shared_ptr<DynamicLibrary> &library,
                           const std::string &method,
                           const std::string &arg) override;

  // Returns the number of registered types for this registry.
  // If specified (not-null), types is updated to include the names of the
  // registered types.
  size_t GetRegisteredTypes(
      std::unordered_set<std::string> *types) const override {
    assert(types);
    for (const auto library : options.libraries) {
      library->GetRegisteredTypes(types);
    }
    return types->size();
  }

  // Returns the number of registered names for the input type
  // If specified (not-null), names is updated to include the names for the type
  size_t GetRegisteredNames(const std::string &type) const override {
    return GetRegisteredNames(type, nullptr);
  }

  size_t GetRegisteredNames(const std::string &type,
                            std::vector<std::string> *names) const override {
    size_t count = 0;
    for (const auto library : options.libraries) {
      count += library->GetRegisteredNames(type, names);
    }
    return count;
  }

  // Returns the total number of factories registered for this library.
  // This method returns the sum of all factories registered for all types.
  // @param num_types returns how many unique types are registered.
  size_t GetFactoryCount(size_t *num_types) const override {
    std::unordered_set<std::string> types;
    *num_types = GetRegisteredTypes(&types);
    size_t count = 0;
    for (const auto type : types) {
      count += GetRegisteredNames(type, nullptr);
    }
    return count;
  }

 protected:
  const void *GetOptionsPtr(const std::string &name) const override {
    if (name == kPathOpts) {
      return &options.paths;
    } else {
      return Configurable::GetOptionsPtr(name);
    }
  }

  // Searches (from back to front) the libraries looking for the
  // an entry that matches this pattern.
  // Returns the entry if it is found, and nullptr otherwise
  const ObjectLibrary::Entry *FindEntry(
      const std::string &type, const std::string &name) const override {
    for (auto iter = options.libraries.crbegin();
         iter != options.libraries.crend(); ++iter) {
      const auto *entry = iter->get()->FindEntry(type, name);
      if (entry != nullptr) {
        return entry;
      }
    }
    return nullptr;
  }

  Status DoConfigureFromMap(
      const ConfigOptions &config_options,
      const std::unordered_map<std::string, std::string> &opt_map,
      std::unordered_map<std::string, std::string> *unused) override {
    // We want to control when any libraries are initialized.
    // Turn off prepare while we are configuring the registry.
    // Once the registry is completely configured, we can go back and configure
    // the libraries.
    ConfigOptions copy = config_options;
    copy.invoke_prepare_options = false;
    copy.ignore_unknown_objects = false;
    Status s =
        Configurable::DoConfigureFromMap(config_options, opt_map, unused);
    if (s.ok()) {
      // The registry was successfully configured.  Now prepare the libraries
      // We should do the prepare even if one was not requested, as there is
      // the potential that everything will break later if the libraries are
      // not loaded...
      s = PrepareOptions(config_options);
    }
    return s;
  }

 private:
  ObjectRegistryOptions options;
};

std::shared_ptr<ObjectRegistry> ObjectRegistry::NewInstance() {
  std::shared_ptr<ObjectRegistry> instance =
      std::make_shared<ObjectRegistryImpl>();
  return instance;
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

  Status PrepareOptions(const ConfigOptions &config_options) override {
    Status status;
    if (entries_.empty()) {
      std::shared_ptr<DynamicLibrary> dll;
      status = config_options.env->LoadLibrary(options.library, "", &dll);
      if (status.ok()) {
        status = RegisterLibrary(dll);
      }
    }
    return status;
  }

  LocalLibraryOptions options;
};

void ObjectRegistryImpl::AddLocalLibrary(const RegistrarFunc &registrar,
                                         const std::string &method,
                                         const std::string &arg) {
  std::shared_ptr<ObjectLibrary> lib =
      std::make_shared<LocalObjectLibrary>(method, arg);
  lib->Register(registrar, arg);
  options.libraries.emplace_back(lib);
}

Status ObjectRegistryImpl::AddLocalLibrary(Env *env, const std::string &method,
                                           const std::string &arg) {
  std::shared_ptr<DynamicLibrary> dll;
  Status status = env->LoadLibrary("", "", &dll);
  if (status.ok()) {
    std::shared_ptr<LocalObjectLibrary> lib =
        std::make_shared<LocalObjectLibrary>(method, arg);
    status = lib->RegisterLibrary(dll);
    if (status.ok()) {
      options.libraries.emplace_back(lib);
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

  Status PrepareOptions(const ConfigOptions &config_options) override {
    Status s;
    if (dll_ == nullptr) {  // Check if we have already loaded the library
      const auto *paths = config_options.registry->GetOptions<std::string>(
          ObjectRegistryImpl::kPathOpts);
      s = config_options.env->LoadLibrary(
          options.library, ((paths != nullptr) ? *paths : ""), &dll_);
      if (s.ok()) {
        s = RegisterLibrary(dll_);
      }
    }
    return s;
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
    const ConfigOptions &config_options, const std::string &value,
    std::shared_ptr<ObjectLibrary> *library) {
  return LoadSharedObject<ObjectLibrary>(config_options, value,
                                         LoadObjectLibrary, library);
}

Status ObjectRegistryImpl::AddDynamicLibrary(Env *env, const std::string &name,
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

Status ObjectRegistryImpl::AddDynamicLibrary(
    const std::shared_ptr<DynamicLibrary> &dyn_lib, const std::string &method,
    const std::string &arg) {
  std::function<void *(void *, const char *)> function;
  RegistrarFunc registrar;
  Status s = dyn_lib->LoadFunction(method, &registrar);
  if (s.ok()) {
    std::shared_ptr<ObjectLibrary> obj_lib;
    obj_lib.reset(new DynamicObjectLibrary(dyn_lib, method, arg));
    obj_lib->Register(registrar, arg);
    options.libraries.emplace_back(obj_lib);
  }
  return s;
}

#endif  // ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE
