// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "rocksdb/env.h"
#include "rocksdb/extensions.h"
#include "rocksdb/status.h"
#include "options/options_helper.h"

namespace rocksdb {
const std::string ExtensionTypes::kTypeEventListener = "event-listener";
const std::string ExtensionTypes::kTypeTableFactory = "table-factory";
const std::string ExtensionTypes::kTypeEnvironment = "environment";

Status Extension::ConfigureFromMap(
          const std::unordered_map<std::string, std::string> & opts_map,
	  const DBOptions & dbOpts,
	  const ColumnFamilyOptions * cfOpts,
	  bool input_strings_escaped,
	  bool ignore_unknown_options) {
  Status s;
  for (const auto& o : opts_map) {
    s = SetOption(o.first, o.second, input_strings_escaped, ignore_unknown_options);
    if (! s.ok()) {
      return s;
    }
  }
  if (cfOpts != nullptr) {
    return SanitizeOptions(dbOpts, *cfOpts);
  } else {
    return Status::OK();
  }
}
  
Status Extension::ConfigureFromString(
	  const std::string & opt_str,
	  const DBOptions & dbOpts,
	  const ColumnFamilyOptions * cfOpts,
	  bool input_strings_escaped,
	  bool ignore_unknown_options) {
  std::unordered_map<std::string, std::string> opt_map;
  Status s = StringToMap(opt_str, &opt_map);
  if (s.ok()) {
    s = ConfigureFromMap(opt_map, dbOpts, cfOpts,
			 input_strings_escaped,
			 ignore_unknown_options);
  }
  return s;
}
  
Status Extension::SetOption(const std::string & name,
			    const std::string &,
			    bool,
			    bool ignore_unknown_options) {
  
  if (! ignore_unknown_options) {
    return Status::InvalidArgument("Unrecognized option: ", name);
  } else {
    return Status::OK();
  }
}

 template <typename T>
Status CreateExtensionObject(const std::string & type,
			     const std::string & name,
			     const DBOptions * dbOptions,
			     const ColumnFamilyOptions * ,
			     std::shared_ptr<T> * result) {
  Extension *extension;
  for (auto rit = dbOptions->extension_factories.crbegin();
       rit != dbOptions->extension_factories.crend(); ++rit) {
    extension = (*rit)->CreateExtensionObject(type, name);
    if (extension != nullptr) {
      T * resultptr = reinterpret_cast<T *>(extension);
      if (resultptr != nullptr) {
	result->reset(resultptr);
	return Status::OK();
      }
    }
  }
  return Status::NotFound("Could not create", name);
}

  class DynamicExtensionFactory : public ExtensionFactory {
  private:
    std::shared_ptr<DynamicLibrary> library_;
    ExtensionFactoryFunction        function_;
  public:
    DynamicExtensionFactory(const std::shared_ptr<DynamicLibrary> & library,
			    ExtensionFactoryFunction function)
      : library_(library), function_(function) {
    }
    virtual const char *Name() const override {
      return library_->Name();
    }
    
    virtual Extension * CreateExtensionObject(const std::string & type,
					      const std::string & name) const override {
      Extension * extension = (function_)(type, name);
      return extension;
    }
  };

  class DefaultExtensionFactory : public ExtensionFactory {
    virtual const char *Name() const override {
      return "default";
    }
    
    virtual Extension * CreateExtensionObject(const std::string &,
					      const std::string &) const override {
      return nullptr;
    }
  };
  

  Status ExtensionFactory::LoadDynamicFactory(const std::shared_ptr<DynamicLibrary> & library,
					    const std::string & method,
					    std::shared_ptr<ExtensionFactory> * factory) {
    ExtensionFactoryFunction function = (ExtensionFactoryFunction) library->LoadSymbol(method);
  if (function != nullptr) {
    factory->reset(new DynamicExtensionFactory(library, function));
    return Status::OK();
  } else {
    return Status::NotFound("No factory method", method);
  }
}

Status ExtensionFactory::LoadDefaultFactory(std::shared_ptr<ExtensionFactory> * factory) {
  ExtensionFactory *f = new DefaultExtensionFactory();
  factory->reset(f);
  return Status::OK();
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
