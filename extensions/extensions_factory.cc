// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "rocksdb/env.h"
#include "rocksdb/extensions.h"

namespace rocksdb {

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
    
    virtual Extension * CreateExtensionObject(const std::string & name,
					      ExtensionType type) override {
      Extension * extension = (function_)(name, type);
      return extension;
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

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
