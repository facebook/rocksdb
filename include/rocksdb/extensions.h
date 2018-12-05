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
  // Returns true if propName == prefix+suffix
  static bool MatchesProperty(const std::string & propName,
			      const std::string & prefix,
			      const std::string & suffix);
  static Status ExtractPropertyNameValue(const std::string & prop,
					 std::string * name,
					 std::string * value,
					 bool ignore_unknown_options,
					 bool input_strings_escaped);
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
		     const ColumnFamilyOptions * cfOpts,
		     bool ignore_unknown_options,
		     bool input_strings_escaped);
  Status ConfigureFromMap(
		     const std::unordered_map<std::string, std::string> &,
		     const DBOptions &,
		     const ColumnFamilyOptions * cfOpts,
		     bool input_strings_escaped,
		     std::unordered_set<std::string> *unused);
  Status ConfigureFromMap(
		     const std::unordered_map<std::string, std::string> & map,
		     const DBOptions & dbOpts,
		     const ColumnFamilyOptions * cfOpts = nullptr) {
    return ConfigureFromMap(map, dbOpts, cfOpts,
			    ExtensionConsts::kIgnoreUnknownOptions,
			    ExtensionConsts::kInputStringsEscaped);
  }
  
  // Configures the options for this extension based on the input parameters.
  // Returns an OK status if configuration was successful.
  // If a non-OK status is returned, the options should be left in their original
  // state.
  virtual Status ConfigureFromString(
		     const std::string &,
		     const DBOptions &,
		     const ColumnFamilyOptions * cfOpts,
		     bool ignore_unknown_options,
		     bool input_strings_escaped);

  Status ConfigureFromString(
		     const std::string & opt_str,
		     const DBOptions & dbOpts,
		     const ColumnFamilyOptions * cfOpts = nullptr) {
    return ConfigureFromString(opt_str, dbOpts, cfOpts,
			       ExtensionConsts::kIgnoreUnknownOptions,
			       ExtensionConsts::kInputStringsEscaped);
  }

  
  // Updateas the named option to the input value, returning OK if successful.
  virtual Status SetOption(const std::string & name,
			   const std::string & value,
			   bool ignore_unknown_options,
			   bool input_strings_escaped);

  
  Status SetOption(const std::string & name, const std::string & value) {
    return SetOption(name, value,
		     ExtensionConsts::kIgnoreUnknownOptions,
		     ExtensionConsts::kInputStringsEscaped);
  }
  
  // Updates the named option to the input value, returning OK if successful.
  virtual Status SetOption(const std::string & name,
			   const std::string & value,
			   const DBOptions &,
			   const ColumnFamilyOptions *,
			   bool ignore_unknown_options,
			   bool input_strings_escaped) {
    return SetOption(name, value, ignore_unknown_options, input_strings_escaped);
  }

  Status SetOption(const std::string & name,
		   const std::string & value,
		   const DBOptions & dbOpts,
		   bool ignore_unknown_options,
		   bool input_strings_escaped) {
    return SetOption(name, value, dbOpts, nullptr,
		     ignore_unknown_options, input_strings_escaped);
  }

  // Updates the named option to the input value, returning OK if successful.
  Status SetOption(const std::string & name,
		   const std::string & value,
		   const DBOptions & dbOpts,
		   const ColumnFamilyOptions *cfOpts = nullptr) {
    return SetOption(name, value, dbOpts, cfOpts,
		     ExtensionConsts::kIgnoreUnknownOptions,
		     ExtensionConsts::kInputStringsEscaped);
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

public:
  template<typename T> T *CastTo(std::unique_ptr<Extension> * from_guard,
				 std::unique_ptr<T> * to_guard) {
    T *result = dynamic_cast<T *>(this);
    if (result != nullptr && from_guard->release()) {
      to_guard->reset(result);
    } else {
      to_guard->reset();
    }
    return result;
  }
};
  
}  // namespace rocksd

