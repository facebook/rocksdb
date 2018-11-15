// An Extension is an abstract class used by the rocksdb to provide
// extensions to the base functionality provided by rocksdb.
// Extensions can be built into rocksdb or loaded and configured by
// dynamically loaded libraries.  This functionality allows users to
// extend the core rocksdb functionality by changing configuration
// options, rather than requiring changes to the base source code.
// Extensions can be added to rocksdb by adding the appopriate
// ExtensionFactory to the options and registering the corresponding
// named Extension  (at either compile- or run-time)


#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include "rocksdb/status.h"

namespace rocksdb {
struct ColumnFamilyOptions;
struct DBOptions;
  
using std::unique_ptr;
using std::shared_ptr;
  
enum ExtensionType : char {
  kExtensionEventListener = 0,
  kExtensionTableFactory,
  kExtensionEnvironment,
  kExtensionCompactionFilter,
  kExtensionMergeOperator,
  kExtensionUnknown
};

  
class Extension {
  public:
  virtual ~Extension() {}
  // Names starting with "rocksdb." are reserved and should not be used
  // by any clients of this package.
  virtual const char* Name() const = 0;

  // Configures the options for this extension based on the input parameters.
  // Returns an OK status if configuration was successful.
  // If a non-OK status is returned, the options should be left in their original
  // state.
  virtual Status ConfigureFromMap(
		     const std::unordered_map<std::string, std::string> &,
		     const DBOptions &,
		     const ColumnFamilyOptions * cfOpts = nullptr,
		     bool input_strings_escaped = true,
		     bool ignore_unknown_options = false);
  
  // Configures the options for this extension based on the input parameters.
  // Returns an OK status if configuration was successful.
  // If a non-OK status is returned, the options should be left in their original
  // state.
  virtual Status ConfigureFromString(
		     const std::string &,
		     const DBOptions &,
		     const ColumnFamilyOptions * cfOpts = nullptr,
		     bool input_strings_escaped = true,
		     bool ignore_unknown_options = false);
  
  // Updateas the named option to the input value, returning OK if successful.
  virtual Status SetOption(const std::string & name,
			   const std::string & value,
			   bool input_strings_escaped = false,
			   bool ignore_unknown_options = false);

  // Updateas the named option to the input value, returning OK if successful.
  virtual Status SetOption(const std::string & name,
			   const std::string & value,
			   const DBOptions &,
			   const ColumnFamilyOptions *,
			   bool input_strings_escaped = false,
			   bool ignore_unknown_options = false) {
    return SetOption(name, value, input_strings_escaped, ignore_unknown_options);
  }

  // Sanitizes the specified DB Options and ColumnFamilyOptions.
  // If the function cannot find a way to sanitize the input DB Options,
  // a non-ok Status will be returned.
  virtual Status SanitizeOptions(const DBOptions &,
				   const ColumnFamilyOptions &) const {
    return Status::OK();
  }
};

  class DynamicLibrary;
  
  class ExtensionFactory {
  public:
    virtual const char *Name() const = 0;
    typedef Extension *(*ExtensionFactoryFunction)(const std::string & name, ExtensionType type);
    static Status LoadDynamicFactory(const std::shared_ptr<DynamicLibrary> & library, const std::string & method, std::shared_ptr<ExtensionFactory> * factory);
    static Status LoadDefaultFactory(std::shared_ptr<ExtensionFactory> * factory);
  public:
    virtual ~ExtensionFactory() { }
    virtual Extension *CreateExtensionObject(const std::string &,
					     ExtensionType) const = 0;
  };

#ifndef ROCKSDB_LITE
template<typename T>
Status GetExtension(const std::vector<std::shared_ptr<ExtensionFactory> > & factories,
		    const std::string   & name,
		    std::shared_ptr<T> *result) {
  for (auto rit = factories.crbegin();
       rit != factories.crend(); ++rit) {
    Extension *extension = (*rit)->CreateExtensionObject(name, T::GetType());
    if (extension != nullptr) {
      T *resultptr = dynamic_cast<T *>(extension);
      result->reset(resultptr);
      return Status::OK();
    }
  }
  return Status::NotFound("Could not load extension", name);
}
#endif

}  // namespace rocksd

