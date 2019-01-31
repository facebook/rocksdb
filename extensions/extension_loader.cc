// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include <functional>
#include <memory>
#include "rocksdb/extension_loader.h"
#include "rocksdb/env.h"
#include "rocksdb/extensions.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace rocksdb {
std::shared_ptr<ExtensionLoader> ExtensionLoader::Get() {
  std::shared_ptr<ExtensionLoader> factory = std::make_shared<ExtensionLoader>();
  factory->parent = Default();
  return factory;
}

std::shared_ptr<ExtensionLoader> ExtensionLoader::Default() {
  static std::shared_ptr<ExtensionLoader>  instance  =
                       std::make_shared<ExtensionLoader>();
  return instance;
}
				 
const ExtensionLoader::FactoryFunction &
ExtensionLoader::RegisterFactory(const std::string & type,
				 const std::string & name,
				 const FactoryFunction & factory) {
  std::vector<std::pair<std::regex, FactoryFunction> > & vector = factories[type];
  std::regex pattern(name);
  
  vector.emplace_back(std::move(pattern), factory);
  return factory;
}
  
Status ExtensionLoader::RegisterLibrary(
				 const std::shared_ptr<DynamicLibrary> & library,
				 const std::string & method,
				 const std::string & arg) {
  RegistrarFunction registrar;
  library->LoadFunction(method, &registrar);
  if (registrar) {
    libraries.insert(library);
    registrar(*this, arg);
    return Status::OK();
  } else {
    return Status::NotFound("No such method", method);
  }
}
  
Status ExtensionLoader::RegisterLibraryFactory(
				  const std::shared_ptr<DynamicLibrary> & library,
				  const std::string & type,
				  const std::string & name,
				  const std::string & method) {
  FactoryFunction factory;
  library->LoadFunction(method, &factory);
  if (factory) {
    libraries.insert(library);
    RegisterFactory(type, name, factory);
    return Status::OK();
  } else {
    return Status::NotFound("No such method", method);
  }
}

ExtensionLoader::FactoryFunction ExtensionLoader::FindFactory(
						  const std::string & type, 
						  const std::string & name) {
  auto entries = factories.find(type);
  
  if (entries != factories.end()) {
    for (const auto& entry : entries->second) {
      if (std::regex_match(name, entry.first)) {
	return (entry.second);
      }
    }
  }
  if (parent) {
    return parent->FindFactory(type, name);
  } else {
    return nullptr;
  }
}

Extension *ExtensionLoader::CreateUniqueExtension(const std::string & type,
						  const std::string & name,
						  const DBOptions & dbOpts,
						  const ColumnFamilyOptions * cfOpts,
						  std::unique_ptr<Extension> *guard) {
  guard->reset();
  FactoryFunction factory = FindFactory(type, name);
  if (factory) {
    Extension *ext = factory(name, dbOpts, cfOpts, guard);
    return ext;
  } else {
    return nullptr;
  }
}

Status ExtensionLoader::CreateSharedExtension(const std::string & type,
					      const std::string & name,
					      const DBOptions & dbOpts,
					      const ColumnFamilyOptions * cfOpts,
					      std::shared_ptr<Extension> *result) {
  std::unique_ptr<Extension> guard;
  result->reset();
  Extension *extension = CreateUniqueExtension(type, name, dbOpts, cfOpts, &guard);
  if (extension == nullptr) {
    return Status::InvalidArgument("Could not load extension", name);
  } else if (guard) {
    result->reset(guard.release());
    return Status::OK();
  } else {
    return Status::NotSupported("Cannot share extension", name);
  }
}
  
void ExtensionLoader::Dump(Logger*) const {
} 

} // Namespace rocksdb
#endif  // ROCKSDB_LITE
