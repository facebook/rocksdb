// A Configurable is an is an abstract class used by the rocksdb that

#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>

#include "rocksdb/status.h"
namespace rocksdb {

enum class OptionType {
  kBoolean,
  kInt,
  kVectorInt,
  kUInt,
  kUInt32T,
  kUInt64T,
  kSizeT,
  kString,
  kDouble,
  kCompactionStyle,
  kCompactionPri,
  kSliceTransform,
  kCompressionType,
  kVectorCompressionType,
  kTableFactory,
  kComparator,
  kCompactionFilter,
  kCompactionFilterFactory,
  kCompactionOptionsFIFO,
  kCompactionOptionsUniversal,
  kCompactionStopStyle,
  kMergeOperator,
  kMemTableRepFactory,
  kBlockBasedTableIndexType,
  kBlockBasedTableDataBlockIndexType,
  kFilterPolicy,
  kFlushBlockPolicyFactory,
  kChecksumType,
  kEncodingType,
  kWALRecoveryMode,
  kAccessHint,
  kInfoLogLevel,
  kLRUCacheOptions,
  kEnum,
  kExtension,
  kUnknown
};

enum class OptionVerificationType {
  kNormal,
  kByName,               // The option is pointer typed so we can only verify
                         // based on it's name.
  kByNameAllowNull,      // Same as kByName, but it also allows the case
                         // where one of them is a nullptr.
  kByNameAllowFromNull,  // Same as kByName, but it also allows the case
                         // where the old option is nullptr.
  kDeprecated            // The option is no longer used in rocksdb. The RocksDB
                         // OptionsParser will still accept this option if it
                         // happen to exists in some Options file.  However,
                         // the parser will not include it in serialization
                         // and verification processes.
};

// A struct for storing constant option information such as option name,
// option type, and offset.
struct OptionTypeInfo {
  int offset;
  OptionType type;
  OptionVerificationType verification;
  bool is_mutable;
  int mutable_offset;
};

typedef std::unordered_map<std::string, OptionTypeInfo> OptionTypeMap;

class Configurable {
 public:
  static const bool kIgnoreUnknownOptions /* = false */;
  static const bool kInputStringsEscaped /* = false */;
  static const std::string kPropNameValue  /* = "name" */;
  static const std::string kPropOptValue /* = "options" */;
  static const std::string kOptionsPrefix /* = "rocksdb." */;
private:
  const OptionTypeMap *optionsMap_;

protected:
  Configurable(const OptionTypeMap *map = nullptr) : optionsMap_(map) { }
public:
  virtual ~Configurable() {}

  // Configures the options for this extension based on the input parameters.
  // Returns an OK status if configuration was successful.
  // Parameters:
  //   opt_map                 The name/value pair map of the options to set
  //   opt_str                 String of name/value pairs of the options to set
  //   input_strings_escaped   True if the strings are escaped (old-style?)
  //   ignore_unusued_options  If true and there are any unused options,
  //                           they will be ignored and OK will be returne
  //   unused_opts             The set of any unused option names from the map
  virtual Status ConfigureFromMap(const std::unordered_map<std::string, std::string> &,
				  bool input_strings_escaped,
				  std::unordered_set<std::string> *unused);
  Status ConfigureFromMap(const std::unordered_set<std::string> &, 
			  const std::unordered_map<std::string, std::string> &,
			  bool,
			  std::unordered_set<std::string> *);
  Status ConfigureFromMap(const std::unordered_map<std::string, std::string> &,
			  bool input_strings_escaped,
			  bool ignore_unused_options);
  Status ConfigureFromMap(const std::unordered_map<std::string, std::string> &,
			  bool input_strings_escaped);
  Status ConfigureFromMap(const std::unordered_map<std::string, std::string> & map);

  Status ConfigureFromString(const std::string & opts,
			     bool input_strings_escaped,
			     std::unordered_set<std::string> *unused);
  Status ConfigureFromString(const std::string & opts,
			     bool input_strings_escaped,
			     bool ignore_unused_options);
  Status ConfigureFromString(const std::string & opts,
			     bool input_strings_escaped);
  Status ConfigureFromString(const std::string & opts);

  bool OptionMatchesName(const std::string & option, 
			 const std::string & name) const;

  const OptionTypeInfo *FindOption(const std::string & option) const;
  virtual const std::string & GetOptionPrefix() const {
    return kOptionsPrefix;
  }

  virtual void *GetOptionsPtr() { return nullptr; }

  // Updates the named option to the input value, returning OK if successful.
  // Parameters:
  //     name                  The name of the option to update
  //     value                 The value to set for the named option
  // Returns:  OK              on success
  //           NotFound        if the name is not a valid option
  //           InvalidArgument if the value is valid for the named option
  //           NotSupported    If the name/value is not supported
  virtual Status ConfigureOption(const std::string & name,
				 const std::string & value,
				 bool input_strings_escaped);
  Status ConfigureOption(const std::string & name,
			 const std::string & value) {
    return ConfigureOption(name, value, kInputStringsEscaped);
  }
  
  // Sanitizes the specified DB Options and ColumnFamilyOptions.
  // If the function cannot find a way to sanitize the input DB Options,
  // a non-ok Status will be returned.
  virtual Status SanitizeOptions() const {
    return Status::OK();
  }
 protected:
  Status ParseOption(const OptionType & optType, char *optAddr,
		     const std::string & name, const std::string & value);
  Status ParseOption(const OptionTypeInfo *opt_info, void *opt_base,
		     const std::string & name, const std::string & value);
  virtual Status ParseEnum(const std::string & name,
			   const std::string & value,
			   char * addr);
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
};
}  // namespace rocksdb

