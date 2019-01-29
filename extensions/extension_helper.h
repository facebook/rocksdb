// Template methods for creating and loading extensions

#pragma once

#include <functional>
#include <memory>
#include <regex>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "rocksdb/options.h"
#include "rocksdb/extensions.h"
#include "rocksdb/extension_loader.h"
#include "rocksdb/status.h"

namespace rocksdb {
class DynamicLibrary;
  
using std::shared_ptr;
using std::unique_ptr;

/**
 * Converts the base shared Extension from to the Derived Extension to.
 * If the cast fails, returns NotSupported.  Otherwise, returns OK
 */
template<typename Derived>  
Status CastSharedExtension(const std::shared_ptr<Extension> & from,
			   std::shared_ptr<Derived> * to) {
  *to = std::dynamic_pointer_cast<Derived>(from);
  if (!to && from) {
    return Status::NotSupported("Cannot cast extension: ", from->Name());
  } else {
    return Status::OK();
  }
}

/**
 * Creates and returns a new Extension of type T
 * @param name     The name of the returned extension
 * @param dbOpts   Database options for creating this extension
 * @param cfOpts   Optional column family options for creating this extension
 * @param result   The resuting new T extension 
 * @return         OK if the new extension was successfully created
 *                 InvalidArgument if the named extension of type T could not
 *                                 be found.
 *                 NotSupported if the class types do not match
 */
template<typename T>
Status NewSharedExtension(const std::string & name,
			  const DBOptions & dbOpts, 
			  const ColumnFamilyOptions * cfOpts,
			  std::shared_ptr<T> * result)  {
  std::shared_ptr<Extension> extension;
  Status s =  dbOpts.extensions->CreateSharedExtension(T::Type(), name,
						       dbOpts, cfOpts, 
						       &extension);
  if (! s.ok()) {
    return s;
  } else if (extension) {
    return CastSharedExtension(extension, result);
  } else {
    return Status::NotSupported("Cannot share extension: ", name);
  }
}
/**
 * Converts the base shared Extension from to the Derived Extension to.
 * If the cast fails, returns NotSupported.  Otherwise, returns OK
 */
template<typename T>
Status CastUniqueExtension(Extension *from,
			   std::unique_ptr<Extension> & from_guard,
			   T **to,
			   std::unique_ptr<T> * to_guard) {
  to_guard->reset();
  *to = nullptr;
  if (from != nullptr) {
    *to = dynamic_cast<T *>(from);
    if (*to == nullptr) {
      return Status::NotSupported("Cannot cast extension: ", from->Name());
    } else if (from_guard.release() != nullptr) {
      to_guard->reset(*to);
    }
  } 
  return Status::OK();
}

/**
 * Creates a new unique extension of type T of the appropriate name
 * and returns the result. 
 * If guard is set on return, the guard controls the lifetime of the returned result.
 */
template<typename T>
Status NewUniqueExtension(const std::string & name,
			  const DBOptions & dbOpts, 
			  const ColumnFamilyOptions * cfOpts,
			  T ** result,
			  std::unique_ptr<T> * guard)  {
  std::unique_ptr<Extension> ext_guard;
  Extension *extension;
  guard->reset();
  *result = nullptr;
  extension = dbOpts.extensions->CreateUniqueExtension(T::Type(), name,
						       dbOpts, cfOpts, 
						       &ext_guard);
  if (extension != nullptr) {
    Status status = CastUniqueExtension(extension, ext_guard, result, guard);
    return status;
  } else {
    return Status::InvalidArgument("Could not find extension: ", name);
  }
}
   
/**
 * Creates a new Extension of type T if the current one is not appropriate (either
 * the extension is null or the wrong name and stores it in a shared pointer.
 * @param name     The name of the returned extension
 * @param dbOpts   Database options for creating this extension
 * @param cfOpts   Optional column family options for creating this extension
 * @param result   The resuting new T extension 
 * @return         OK if the new extension was successfully created
 *                 InvalidArgument if the named extension of type T could not
 *                                 be found.
 *                 NotSupported if the class types do not match
 */
template<typename T>
Status GetSharedExtension(const DBOptions & dbOpts, 
			  const ColumnFamilyOptions * cfOpts,
			  const std::string & name,
			  std::shared_ptr<T> * result) {
  if (! result->get() || result->get()->Name() != name) {
    return NewSharedExtension(name, dbOpts, cfOpts, result);
  } else {
    return Status::OK();
  }
}
  
/**
 * Checks if the input property/name are for an extension.  If so, the name/value are parsed and
 * a new extension may (depending on the values) be created and/or configured.
 * @param property    The name of the configuration property for this extension
 * @param dbOpts      Parameter for creating and configuring the result
 * @param cfOpts      Parameter for creating and configuring the result
 * @param name        The name of the supplied property for this extension.  This name is compared
 *                    to the input property to see if the name represents this extension
 * @param value       The settings/value of the property being configured
 * @param result      The returned extension.  Depending on the property/name/value parameters, 
 *                    the extension may be created and/or configured
 * @param status      If the property represents this extension, the returned status for this operation.
 * @result            True if the name represents this property, false otherwise.
 */
template<typename T>
bool ConfigureSharedExtension(const std::string & property,
			      const DBOptions & dbOpts, 
			      const ColumnFamilyOptions * cfOpts,
			      const std::string & name,
			      const std::string & value, 
			      std::shared_ptr<T> * result,
			      Status *status) {
  std::string extName;
  std::unordered_map<std::string, std::string> extOpts;
  if (Extension::IsExtensionOption(property, name, value, &extName, &extOpts)) {
    *status = Status::OK();
    if (! extName.empty()) {
      *status = GetSharedExtension(dbOpts, cfOpts, extName, result);
    } 
    if (status->ok() && ! extOpts.empty()) {
      if (result->get() == nullptr) {
	*status = Status::NotFound("Cannot configure null extension");
      } else {
	*status = result->get()->ConfigureFromMap(dbOpts, cfOpts, extOpts);
      }
    }
    return true;
  } else {
    return false;
  }
}


/**
 * Creates a new Extension of type T if the current one is not appropriate (either
 * the extension is null or the wrong name and stores it in a unique pointer.
 * @param name     The name of the returned extension
 * @param dbOpts   Database options for creating this extension
 * @param cfOpts   Optional column family options for creating this extension
 * @param result   The resuting new T extension 
 * @return         OK if the new extension was successfully created
 *                 InvalidArgument if the named extension of type T could not
 *                                 be found.
 *                 NotSupported if the class types do not match
 */
template<typename T>
Status GetUniqueExtension(const DBOptions & dbOpts, 
			  const ColumnFamilyOptions * cfOpts,
			  const std::string & name,
			  T **extension,
			  std::unique_ptr<T> * guard) {
  if (*extension == nullptr || (*extension)->Name() != name) {
    return NewUniqueExtension(name, dbOpts, cfOpts, extension, guard);
  } else {
    return Status::OK();
  }
}
}  // namespace rocksdb
