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

#include "rocksdb/status.h"

namespace rocksdb {
struct ColumnFamilyOptions;
struct DBOptions;
  
using std::unique_ptr;
using std::shared_ptr;


struct ExtensionConsts {
  static const std::string kPropNameValue;
  static const std::string kPropOptValue;
  static bool kIgnoreUnknownOptions;
  static bool kInputStringsEscaped;
};

class Extension {
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
  virtual Status ConfigureFromMap(const std::unordered_map<std::string, std::string> &,
				  bool input_strings_escaped,
				  std::unordered_set<std::string> *unused);
  virtual Status ConfigureFromMap(const DBOptions & dbOpts,
				  const ColumnFamilyOptions * cfOpts,
				  const std::unordered_map<std::string, std::string> &,
				  bool input_strings_escaped,
				  std::unordered_set<std::string> *unused);  
  Status ConfigureFromMap(const std::unordered_map<std::string, std::string> &,
			  bool input_strings_escaped,
			  bool ignore_unused_options);
  Status ConfigureFromMap(const std::unordered_map<std::string, std::string> &,
			  bool input_strings_escaped);
  Status ConfigureFromMap(const std::unordered_map<std::string, std::string> & map);
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
  Status ConfigureFromString(const std::string & opts,
			     bool input_strings_escaped,
			     std::unordered_set<std::string> *unused);
  Status ConfigureFromString(const std::string & opts,
			     bool input_strings_escaped,
			     bool ignore_unused_options);
  Status ConfigureFromString(const std::string & opts,
			     bool input_strings_escaped);
  Status ConfigureFromString(const std::string & opts);

  
  // Updates the named option to the input value, returning OK if successful.
  // Parameters:
  //   dbOpts                  The database options
  //   cfOpts                  The (optional) column family options
  //     name                  The name of the option to update
  //     value                 The value to set for the named option
  // Returns:  OK              on success
  //           NotFound        if the name is not a valid option
  //           InvalidArgument if the value is valid for the named option
  //           NotSupported    If the name/value is not supported
  virtual Status SetOption(const std::string & name,
				const std::string & value,
				bool input_strings_escaped);
  Status SetOption(const std::string & name,
			   const std::string & value) {
    return SetOption(name, value, ExtensionConsts::kInputStringsEscaped);
  }
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
    return SetOption(dbOpts, name, value, ExtensionConsts::kInputStringsEscaped);
  }
  
  // Sanitizes the specified DB Options and ColumnFamilyOptions.
  // If the function cannot find a way to sanitize the input DB Options,
  // a non-ok Status will be returned.
  virtual Status SanitizeOptions(const DBOptions &) const {
    return Status::OK();
  }
  virtual Status SanitizeOptions(const DBOptions & dbOpts,
				 const ColumnFamilyOptions &) const {
    return SanitizeOptions(dbOpts);
  }
public:
  // Checks to see if the named "option" matches "prefix" or "prefix.name"
  // This method is used to separate the "value" into its pieces
  // If "prefix.name=option", then:
  //     1) name is set to value
  //     2) props is empty
  //     3) OK is returned
  // If "prefix=name", then, value should be of the form "name=x[;options={a=b;c=d}]"
  //     1) name is set to "x"
  //     2) props is set to "a=b;c=d"
  //     3) If "name" is missing, InvalidArgument is returned
  //     4) If values other than "name" and "options" are found, InvalidArgument
  //     5) Otherwise, OK is returned
  //  If the prefix=name" conditions do not match, NotFound is returned
  static Status PrefixMatchesOption(const std::string & prefix,
				    const std::string & option,
				    const std::string & value,
				    std::string * name,
				    std::string * props);
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
};
}  // namespace rocksdb

