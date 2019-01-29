// An Extension is an abstract class used by the rocksdb to provide
// extensions to the base functionality provided by rocksdb.
// Extensions can be built into rocksdb or loaded and configured by
// dynamically loaded libraries.  This functionality allows users to
// extend the core rocksdb functionality by changing configuration
// options, rather than requiring changes to the base source code.

#pragma once

#include <functional>
#include <memory>
#include <regex>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "rocksdb/configurable.h"
#include "rocksdb/status.h"

namespace rocksdb {
struct ColumnFamilyOptions;
struct DBOptions;
  
using std::unique_ptr;
using std::shared_ptr;

class Extension : public Configurable {
public:
  using Configurable::ConfigureFromMap;
  using Configurable::ConfigureFromString;
  using Configurable::SanitizeOptions;
  using Configurable::ConfigureOption;
protected:
  using Configurable::ParseUnknown;
  using Configurable::ParseExtension;
  using Configurable::SetOption;
  using Configurable::SetOptions;
protected:
  Extension() : Configurable() { } 
  Extension(const std::string & prefix, const OptionTypeMap *map = nullptr) : Configurable(prefix, map) { }
public:
  static Status ConfigureExtension(Extension * extension,
				   const std::string & name,
				   const std::unordered_map<std::string, std::string> & options);
  static bool ConfigureExtension(const std::string & property,
				 Extension * extension,
				 const std::string & name,
				 const std::string & value,
				 Status * status);
  // Returns true if the input option matches the input extension property.
  // If "property.options="options", then extName will be empty and extOpts will be the name-value paris in value
  // Ir "property.name"=options", then extName will be set to value and the extOpts map will be emprt
  // If "property" == "options", then "value" is treated as a map of named value pairs.
  //    If a name property exists, it is removed from the map and extName is set to its value
  static bool IsExtensionOption(const std::string & property,
				const std::string & option,
				const std::string & value,
				std::string * extNamme,
				std::unordered_map<std::string, std::string> * extOpts);
public:
  virtual ~Extension() {}
  // Names starting with "rocksdb." are reserved and should not be used
  // by any clients of this package.
  virtual const char* Name() const = 0;

  // Configures the options for this extension based on the input parameters.
  // Returns an OK status if configuration was successful.
  // Parameters:
  //   dbOpts                  The database options for this extension 
  //   cfOpts                  The (optional) column family options
  //   opt_map                 The name/value pair map of the options to set
  //   opt_str                 String of name/value pairs of the options to set
  //   input_strings_escaped   True if the strings are escaped (old-style?)
  //   ignore_unusued_options  If true and there are any unused options,
  //                           they will be ignored and OK will be returne
  //   unused_opts             The set of any unused option names from the map
  Status ConfigureFromMap(const DBOptions & dbOpts,
			  const ColumnFamilyOptions * cfOpts,
			  const std::unordered_map<std::string, std::string> &,
			  bool input_strings_escaped,
			  std::unordered_set<std::string> *unused);  
  Status ConfigureFromMap(const DBOptions & dbOpts,
			  const ColumnFamilyOptions * cfOpts,
			  const std::unordered_map<std::string, std::string> & map,
			  bool input_strings_escaped,
			  bool ignore_unused_options);
  Status ConfigureFromMap(const DBOptions & dbOpts,
			  const ColumnFamilyOptions * cfOpts,
			  const std::unordered_map<std::string, std::string> & map,
			  bool input_strings_escaped);
  Status ConfigureFromMap(const DBOptions & dbOpts,
			  const ColumnFamilyOptions * cfOpts,
			  const std::unordered_map<std::string, std::string> & map);
  Status ConfigureFromMap(const DBOptions & dbOpts,
			  const std::unordered_map<std::string, std::string> & map,
			  bool input_strings_escaped);
  Status ConfigureFromMap(const DBOptions & dbOpts,
			  const std::unordered_map<std::string, std::string> & map);
  Status ConfigureFromString(const DBOptions & dbOpts,
			     const ColumnFamilyOptions * cfOpts,
			     const std::string & opt_str,
			     bool input_strings_escaped,
			     std::unordered_set<std::string> *unused_opts);
  Status ConfigureFromString(const DBOptions & dbOpts,
			     const ColumnFamilyOptions * cfOpts,
			     const std::string & opts_str,
			     bool input_strings_escaped,
			     bool ignore_unused_options);
  Status ConfigureFromString(const DBOptions & dbOpts,
			     const ColumnFamilyOptions * cfOpts,
			     const std::string & opts_str,
			     bool input_strings_escaped);
  Status ConfigureFromString(const DBOptions & dbOpts,
			     const ColumnFamilyOptions * cfOpts,
			     const std::string & opt_str);
  Status ConfigureFromString(const DBOptions & dbOpts,
			     const std::string & opt_str,
			     bool input_strings_escaped);
  Status ConfigureFromString(const DBOptions & dbOpts,
			     const std::string & opt_str);
  Status ConfigureOption(const DBOptions &,
			 const ColumnFamilyOptions *,
			 const std::string & name,
			 const std::string & value,
			 bool input_strings_escaped);
  
  Status ConfigureOption(const DBOptions & dbOpts,
			 const std::string & name,
			 const std::string & value,
			 bool input_strings_escaped)  {
    return ConfigureOption(dbOpts, nullptr, name, value, input_strings_escaped);
  }
  Status ConfigureOption(const DBOptions & dbOpts,
		   const std::string & name,
		   const std::string & value) {
    return ConfigureOption(dbOpts, name, value, Configurable::kInputStringsEscaped);
  }
  
  // Sanitizes the specified DB Options and ColumnFamilyOptions.
  // If the function cannot find a way to sanitize the input DB Options,
  // a non-ok Status will be returned.
  virtual Status SanitizeOptions(const DBOptions &) const {
    return SanitizeOptions();
  }
  virtual Status SanitizeOptions(const DBOptions & dbOpts,
				 const ColumnFamilyOptions &) const {
    return SanitizeOptions(dbOpts);
  }
protected:
  // Sets the properties named in the input map to their corresponding value.
  // unused contains a collection of named properties that were specified in the input map
  // but for which no property existed
  virtual Status SetOptions(const DBOptions & dbOpts,
			    const ColumnFamilyOptions * cfOpts,
			    const std::unordered_map<std::string, std::string> &,
			    bool input_strings_escaped,
			    std::unordered_set<std::string> *unused);  
  // Sets the named option to the input value, where th optType is the type of the option,
  // and the optAddr is the offset of this named property in this object
  virtual Status SetOption(const DBOptions & dbOpts,
			   const ColumnFamilyOptions * cfOpts,
			   const OptionType & optType,
			   char *optAddr,
			   const std::string & name,
			   const std::string & value);
  // Parses the name and value as an Extension object.
  virtual Status ParseExtension(const DBOptions & dbOpts, const ColumnFamilyOptions *cfOpts,
				const std::string & name, const std::string & value);
  // Parses the name and value as aobject of unknown type.
  virtual Status ParseUnknown(const DBOptions & dbOpts, const ColumnFamilyOptions *cfOpts,
			      const std::string & name, const std::string & value);
};
}  // namespace rocksdb

