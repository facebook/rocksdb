// An Env is an interface used by the rocksdb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.

#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "rocksdb/configurable.h"
#include "rocksdb/status.h"

namespace rocksdb {
struct DBOptions;
struct ColumnFamilyOptions;
  
using std::unique_ptr;
using std::shared_ptr;

class Extension : public Configurable {
public:
  using Configurable::ConfigureFromMap;
  using Configurable::ConfigureFromString;
  using Configurable::SanitizeOptions;
  using Configurable::SetOption;
public:
  virtual ~Extension() {}
  // Names starting with "rocksdb." are reserved and should not be used
  // by any clients of this package.
  virtual const char *Name() const = 0;
  virtual const std::string & Type() const = 0;

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
  virtual Status ConfigureFromMap(const DBOptions & dbOpts,
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
  
  virtual Status SetOption(const DBOptions &,
			   const ColumnFamilyOptions *,
			   const std::string & name,
			   const std::string & value,
			   bool input_strings_escaped) {
    return SetOption(name, value, input_strings_escaped);
  }

  Status SetOption(const DBOptions & dbOpts,
		   const std::string & name,
		   const std::string & value,
		   bool input_strings_escaped)  {
    return SetOption(dbOpts, nullptr, name, value, input_strings_escaped);
  }
  Status SetOption(const DBOptions & dbOpts,
		   const std::string & name,
		   const std::string & value) {
    return SetOption(dbOpts, name, value, Configurable::kInputStringsEscaped);
  }
  
  // Sanitizes the specified DB Options and ColumnFamilyOptions.
  //
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
  // Updates the parameters for the input extension
  // Parameters:
  //     extension             The extension to update
  //     prefix                The prefix representing the name of this extension
  //     name                  The name of the option to update
  //     value                 The value to set for the named option
  // If the extension exists and the named option exists in the extension,
  //    the extension is updated
  // If the prefix represents *this* extension (see PrefixMatchesOption), then
  //    the extension with the properties from the value
  // If the prefix matches this extension type but not this extension,
  //    InvalidArgument is returned.
  Status SetExtensionOption(Extension * extension,
			    const std::string & prefix,
			    const std::string & name,
			    const std::string & value,
			    bool input_strings_escaped);
  Status ConfigureOption(const OptionTypeMap *type_map,
			 char *opt_base,
			 const DBOptions & dbOpts,
			 const ColumnFamilyOptions * cfOpts,
			 const std::string & name,
			 const std::string & value,
			 bool input_strings_escaped);
};
}  // namespace rocksdb

