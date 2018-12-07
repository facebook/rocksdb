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
  // If a non-OK status is returned, the options should be left in their original
  // state.
  virtual Status ConfigureFromMap(
		     const std::unordered_map<std::string, std::string> &,
		     bool input_strings_escaped,
		     const DBOptions & dbOpts,
		     const ColumnFamilyOptions * cfOpts,
		     std::unordered_set<std::string> *unused);
    virtual Status ConfigureFromString(
		     const std::string & opt_str,
		     bool input_strings_escaped,
		     const DBOptions & dbOpts,
		     const ColumnFamilyOptions * cfOpts,
		     std::unordered_set<std::string> *unused_opts);


  Status ConfigureFromMap(
		    const std::unordered_map<std::string, std::string> & map,
		    bool input_strings_escaped,
		    const DBOptions & dbOpts,
		    const ColumnFamilyOptions * cfOpts,
		    bool ignore_unused_options);
  Status ConfigureFromMap(
		     const std::unordered_map<std::string, std::string> & map,
		     const DBOptions & dbOpts,
		     const ColumnFamilyOptions * cfOpts = nullptr) {
    return ConfigureFromMap(map, ExtensionConsts::kInputStringsEscaped,
			    dbOpts, cfOpts);
  }
  
  Status ConfigureFromMap(
		     const std::unordered_map<std::string, std::string> & map,
		     bool input_strings_escaped,
		     const DBOptions & dbOpts,
		     const ColumnFamilyOptions * cfOpts = nullptr) {
    return ConfigureFromMap(map, input_strings_escaped, dbOpts, cfOpts,
			    ExtensionConsts::kIgnoreUnknownOptions);
  }
  
  
  // Configures the options for this extension based on the input parameters.
  // Returns an OK status if configuration was successful.
  // If a non-OK status is returned, the options should be left in their original
  // state.
  Status ConfigureFromString(
		    const std::string & opts_str,
		    bool input_strings_escaped,
		    const DBOptions & dbOpts,
		    const ColumnFamilyOptions * cfOpts,
		    bool ignore_unused_options);
  Status ConfigureFromString(
		     const std::string & opt_str,
		     const DBOptions & dbOpts,
		     const ColumnFamilyOptions * cfOpts = nullptr) {
    return ConfigureFromString(opt_str, ExtensionConsts::kInputStringsEscaped,
			       dbOpts, cfOpts);
  }
  Status ConfigureFromString(
		     const std::string & opt_str,
		     bool input_strings_escaped,
		     const DBOptions & dbOpts,
		     const ColumnFamilyOptions * cfOpts = nullptr) {
    return ConfigureFromString(opt_str, input_strings_escaped, dbOpts, cfOpts,
				ExtensionConsts::kIgnoreUnknownOptions);
  }

  
  // Updateas the named option to the input value, returning OK if successful.
  virtual Status SetOption(const std::string & name,
				const std::string & value,
				bool input_strings_escaped);
  virtual Status SetOption(const std::string & name,
			   const std::string & value) {
    return SetOption(name, value, ExtensionConsts::kInputStringsEscaped);
  }
  
  // Updates the named option to the input value, returning OK if successful.
  virtual Status SetOption(const std::string & name,
				const std::string & value,
				bool input_strings_escaped,
				const DBOptions &,
				const ColumnFamilyOptions *,
				bool ignore_unknown_options) {
    Status s = SetOption(name, value, input_strings_escaped);
    if (ignore_unknown_options && s.IsNotFound()) {
      return Status::OK();
    } else {
      return s;
    }
  }

  Status SetOption(const std::string & name,
		   const std::string & value,
		   bool input_strings_escaped,
		   const DBOptions & dbOpts,
		   const ColumnFamilyOptions *cfOpts = nullptr) {
    return SetOption(name, value, input_strings_escaped, dbOpts, cfOpts,
			  ExtensionConsts::kIgnoreUnknownOptions);
  }

  // Updates the named option to the input value, returning OK if successful.
  Status SetOption(const std::string & name,
		   const std::string & value,
		   const DBOptions & dbOpts,
		   const ColumnFamilyOptions *cfOpts = nullptr) {
    return SetOption(name, value, ExtensionConsts::kInputStringsEscaped,
		     dbOpts, cfOpts);
		     
  }
  
  // Sanitizes the specified DB Options and ColumnFamilyOptions.
  // If the function cannot find a way to sanitize the input DB Options,
  // a non-ok Status will be returned.
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
protected:
  Status SetExtensionOption(const std::string & prefix,
			    const std::string & name,
			    const std::string & value,
			    bool input_strings_escaped,
			    Extension * extension);
};
}  // namespace rocksdb

