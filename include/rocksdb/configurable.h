

#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>

#include "rocksdb/status.h"
#include "rocksdb/utilities/options_type.h"

namespace rocksdb {
struct DBOptions;
struct ColumnFamilyOptions;
class Logger;
struct OptionTypeInfo;
typedef std::unordered_map<std::string, OptionTypeInfo> OptionTypeMap;

/**
 * Configurable is an is an base class used by the rocksdb that describes a
 * standard way of configuring objects.  A Configurable object can:
 *   -> Populate itself given:
 *        - One or more "name/value" pair strings
 *        - A string repesenting the set of name=value properties
 *        - A map of name/value properties.
 *   -> Convert itself into its string representation
 *   -> Dump itself to a Logger
 *   -> Compare itself to another Configurable object to see if the two objects
 * are equivalent
 *
 * If a derived class calls RegisterOptionsMap to register (by name) how its
 * options objects are to be processed, this functionality can typically be
 * handled by this class without additional overrides. Otherwise, the derived
 * class will need to implement the methods for handling the corresponding
 * functionality.
 *
 * The Configurable class also provides hooks to validate the option values
 * associated with it
 *   - The SanitizeOptions method can be used to check and change any options
 * required for validity
 *   - The ValidateOptions method can be used to check (but not change the
 * values) of the options. These two methods must be overridden by derived
 * classes to implement class-specific validation.
 */
class Configurable {
 protected:
  static const std::string kDefaultPrefix /*  = "rocksdb." */;

 private:
#ifndef ROCKSDB_LITE
  std::unordered_map<std::string, std::pair<void*, OptionTypeMap>> options_;
#else
  std::unordered_map<std::string, void*> options_;
#endif  // ROCKSDB_LITE
  bool is_mutable_;

 protected:
#ifndef ROCKSDB_LITE
  Status DoConfigureFromMap(const DBOptions&,
                            const std::unordered_map<std::string, std::string>&,
                            bool input_strings_escaped,
                            bool ignore_used_options,
                            std::unordered_set<std::string>* unused);
#endif  // ROCKSDB_LITE
  /**
   * To disambiguate options from different Configurable objects,configurations
   * may specify a prefix. It is strongly recommended that derived classes
   * override this method to some unique representative value.
   */
  virtual const std::string& GetOptionsPrefix() const { return kDefaultPrefix; }

 public:
  Configurable(bool is_mutable = false) : is_mutable_(is_mutable) {}
  Configurable(const std::string& name, void* options,
               const OptionTypeMap& opt_map, bool is_mutable = false)
      : is_mutable_(is_mutable) {
    RegisterOptionsMap(name, options, opt_map);
  }
  virtual ~Configurable() {}

 public:  // Public API methods exposed by Configurable
  /**
   *  Attempts to make the local options valid for the specified DB Options
   * and ColumnFamilyOptions. If necessary, this method can change values of
   * itself or the DBOptions or the ColumnFamilyOptions (unlike
   * ValidateOptions). After calling this method, the
   * DBOptions/ColumnFamilyOptions should be valid. a non-ok Status will be
   * returned.
   */
  Status SanitizeOptions();
  Status SanitizeOptions(DBOptions& db_opts);
  Status SanitizeOptions(DBOptions& db_opts, ColumnFamilyOptions& cf_opts);

  /**
   *  Validates the local options are valid for the specified DB Options
   * and ColumnFamilyOptions. If the options are not valid,
   * a non-ok Status will be returned.  This method cannot change its input
   * arguments or the object itself.
   */
  Status ValidateOptions() const;
  Status ValidateOptions(const DBOptions&) const;
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const;

  /**
   *  Prints the configuration object settings to the input Logger
   */
  void Dump(Logger* log, const std::string& indent,
            uint32_t mode = OptionStringMode::kOptionNone) const {
    DumpOptions(log, indent, mode);
  }
  void Dump(Logger* log, uint32_t mode = OptionStringMode::kOptionNone) const {
    Dump(log, "              ", mode);
  }

  /**
   *  Returns the raw pointer of the named options that is used by this
   * object, or nullptr if this function is not supported.
   * Since the return value is a raw pointer, the object owns the
   * pointer and the caller should not delete the pointer.
   *
   * Note that changing the underlying options while the object
   * is currently used by any open DB is undefined behavior.
   * Developers should use DB::SetOption() instead to dynamically change
   * options while the DB is open.
   */
  template <typename T>
  const T* GetOptions(const std::string& name) const {
    return reinterpret_cast<const T*>(GetOptionsPtr(name));
  }
  template <typename T>
  T* GetOptions(const std::string& name) {
    return reinterpret_cast<T*>(const_cast<void*>(GetOptionsPtr(name)));
  }

#ifndef ROCKSDB_LITE
  /**
   * Configures the options for this extension based on the input parameters.
   * Returns an OK status if configuration was successful.
   * Parameters:
   *   opt_map                 The name/value pair map of the options to set
   *   opt_str                 String of name/value pairs of the options to set
   *   input_strings_escaped   True if the strings are escaped (old-style?)
   *   ignore_unusued_options  If true and there are any unused options,
   *                           they will be ignored and OK will be returned
   *   unused_opts             The set of any unused option names from the map
   */
  Status ConfigureFromMap(const DBOptions&,
                          const std::unordered_map<std::string, std::string>&,
                          bool input_strings_escaped = false,
                          bool ignore_unused_options = false);
  Status ConfigureFromMap(const DBOptions&,
                          const std::unordered_map<std::string, std::string>&,
                          bool input_strings_escaped,
                          std::unordered_set<std::string>* unused);

  Status ConfigureFromString(const DBOptions&, const std::string& opts,
                             bool input_strings_escaped,
                             std::unordered_set<std::string>* unused);
#endif  // ROCKSDB_LITE

  Status ConfigureFromString(const DBOptions&, const std::string& opts,
                             bool input_strings_escaped = false,
                             bool ignore_unused_options = false);

  /**
   *  Updates the named option to the input value, returning OK if successful.
   * Parameters:
   *     name                  The name of the option to update
   *     value                 The value to set for the named option
   * Returns:  OK              on success
   *           NotFound        if the name is not a valid option
   *           InvalidArgument if the value is valid for the named option
   *           NotSupported    If the name/value is not supported
   */
  Status ConfigureOption(const DBOptions& db_opts, const std::string& name,
                         const std::string& value,
                         bool input_strings_escaped = false);

#ifndef ROCKSDB_LITE
  /**
   * Fills in result with the string representation of the configuration options
   * Parameters:
   *   result:                The returned string representation of the options
   *   delimiter:             Separator between options in the string
   *   mode:                  OR-ed together string mode options controlling how
   * the options are returned
   */
  Status GetOptionString(std::string* result,
                         const std::string& delimiter) const {
    return GetOptionString(OptionStringMode::kOptionNone, result, delimiter);
  }
  Status GetOptionString(uint32_t mode, std::string* result,
                         const std::string& delimiter) const;

  // Returns the list of option names associated with this configurable
  Status GetOptionNames(std::unordered_set<std::string>* result) const {
    return GetOptionNames(OptionStringMode::kOptionShallow, result);
  }

  Status GetOptionNames(uint32_t mode,
                        std::unordered_set<std::string>* result) const;
  // Converts this object to a delimited-string
  std::string ToString(uint32_t mode = OptionStringMode::kOptionNone,
                       const std::string& delimiter = ";") const {
    return ToString(mode, "", delimiter);
  }

  std::string ToString(const std::string& delimiter) const {
    return ToString(OptionStringMode::kOptionNone, "", delimiter);
  }
  std::string ToString(uint32_t mode, const std::string& prefix,
                       const std::string& delimiter) const;
#endif  // ROCKSDB_LITE
  /**
   * Returns the value of the option associated with the input name
   */
  Status GetOption(const std::string& name, std::string* value) const;
  /**
   * Checks to see if this Configurable is equivalent to other.
   * Level controls how pedantic the comparison must be for equivalency to be
   achieved

   */
  bool Matches(const Configurable* other, OptionsSanityCheckLevel level) const;
  bool Matches(const Configurable* other, OptionsSanityCheckLevel level,
               std::string* name) const;

 protected:  // Virtual functions that should be overridden by derived classes
  virtual Status Validate(const DBOptions&, const ColumnFamilyOptions&) const;

  virtual Status Sanitize(DBOptions& db_opts, ColumnFamilyOptions& cf_opts);
  /**
   * Returns the raw pointer for the associated named option.
   */
  virtual const void* GetOptionsPtr(const std::string& /* name */) const;

  /**
   * Returns a pointer to a map of name-sanity levels for the named option.
   * This method allows a class to better control how the sanity check level of
   * fields within the options are handled.  If no map is returned, the behavior
   * depends on thetype of the option.
   *
   * This method returns a map (with names equivalent to the option map) for the
   * named option. If no map is specified or the option name is not found in the
   * map, the default behavior of the option type is used.  Otherwise, the
   * returned option type will be used when checking for equivalence.
   */
  virtual const std::unordered_map<std::string, OptionsSanityCheckLevel>*
  GetOptionsSanityCheckLevel(const std::string& /* name */) const {
    return nullptr;
  }
  virtual OptionsSanityCheckLevel GetSanityLevelForOption(
      const std::unordered_map<std::string, OptionsSanityCheckLevel>* map,
      const std::string& name) const;

 protected:  // Virtual functions that must be specified by functions not
             // registering option maps.
  /**
   * Matches the input "other" object at the corresponding sanity level.
   */
  virtual bool MatchesOption(const Configurable* other,
                             OptionsSanityCheckLevel level,
                             std::string* name) const;
  virtual Status ParseStringOptions(const DBOptions& /*db_opts */,
                                    const std::string& options,
                                    bool /*ignore_unused_options */,
                                    bool /*input_strings_escaped */) {
    return (options.empty())
               ? Status::OK()
               : Status::InvalidArgument("Cannot parse option: ", options);
  }

#ifndef ROCKSDB_LITE
  /**
   *  Sets the name and value of a configurable object
   * Sets found_option=true if the name matches a valid option for this object
   * (false otherwise) Returns OK if the option was successfully updated or not
   * found Returns a non-OK status if the name was found but the value was not
   * valid
   *
   * Objects that have options that cannot be parsed by the typical means (via
   * Maps and SetOption) should override this method,
   */
  virtual Status SetOption(const DBOptions& db_opts, const std::string& name,
                           const std::string& value, bool input_strins_escaped,
                           bool* found_option);
  virtual Status ParseOption(const OptionTypeInfo& opt_info,
                             const DBOptions& db_opts, void* opt_ptr,
                             const std::string& opt_name,
                             const std::string& opt_value,
                             bool input_strings_escaped);
  // Tests to see if the single option name/info matches for this and that
  virtual bool OptionIsEqual(const std::string& name,
                             const OptionTypeInfo& opt_info,
                             OptionsSanityCheckLevel level,
                             const char* this_option, const char* that_option,
                             std::string* bad_name) const;
  // If this and that do not match, use some other means to see if they
  // might match (like checking the verification type).
  virtual bool VerifyOptionEqual(const std::string& opt_name,
                                 const OptionTypeInfo& opt_info,
                                 const char* this_offset,
                                 const char* that_offset) const;
#endif
  /**
   * Dumps the values associated with this object to the logger.
   */
  virtual void DumpOptions(Logger* log, const std::string& indent,
                           uint32_t mode) const;
#ifndef ROCKSDB_LITE
  virtual std::string AsString(uint32_t mode, const std::string& prefix,
                               const std::string& delimiter) const;
  /**
   * Converts the options associated with this object into the result string.
   * The options are separated by the input delimiter.
   * The mode is a bitset that controls how the various options are printed
   *   - If kOptionPrefix is set, the prefix is prepended to each option string;
   *   - if kOptionShallow is set, nested configuration values are not included;
   *   - if kOptionDeatched is set, nested configuration options are printed
   * individually, otherwise, they are grouped inside "{ options }" Returns OK
   * if the options could be succcessfully serialized, non-OK on failure
   */
  virtual Status SerializeOptions(uint32_t mode, std::string* result,
                                  const std::string& prefix,
                                  const std::string& delimiter) const;
  virtual Status ListOptions(uint32_t mode, const std::string& prefix,
                             std::unordered_set<std::string>* result) const;
#endif  // ROCKSDB_LITE
 protected:
  // Returns printable options for dump
  // If this method returns non-empty string, the result is used for dumping
  // the options. If an empty string is returned, the registered options are
  // used.
  virtual std::string GetPrintableOptions() const { return ""; }
  // Returns true if this configurable represents a mutable object
  // Mutable classes should override this method.
  virtual bool IsMutable() const { return is_mutable_; }
  /**
   *  Given a prefixed name (e.g. rocksdb.my.type.opt), returns the short name
   * ("opt")
   */
  virtual std::string GetOptionName(const std::string& long_name) const;
#ifndef ROCKSDB_LITE

  Status SerializeOption(const std::string& opt_name,
                         const OptionTypeInfo& opt_info, const char* opt_addr,
                         uint32_t mode, std::string* opt_value,
                         const std::string& prefix,
                         const std::string& delimiter) const;

  // Returns the offset of ptrfor the given option
  // If the configurable is mutable, returns the mutable offset (if any).
  // Otherwise returns the standard one
  const char* GetOptAddress(const OptionTypeInfo& opt_info,
                            const void* ptr) const {
    if (!IsMutable()) {
      return reinterpret_cast<const char*>(ptr) + opt_info.offset;
    } else if (opt_info.IsMutable()) {
      return reinterpret_cast<const char*>(ptr) + opt_info.mutable_offset;
    } else {
      return nullptr;
    }
  }

  // Returns the offset of ptrfor the given option
  // If the configurable is mutable, returns the mutable offset (if any).
  // Otherwise returns the standard one
  char* GetOptAddress(const OptionTypeInfo& opt_info, void* ptr) const {
    if (!IsMutable()) {
      return reinterpret_cast<char*>(ptr) + opt_info.offset;
    } else if (opt_info.IsMutable()) {
      return reinterpret_cast<char*>(ptr) + opt_info.mutable_offset;
    } else {
      return nullptr;
    }
  }

  /**
   * Returns the option info for the named option from the input map, or nullptr
   * if not found.
   */
  OptionTypeMap::const_iterator FindOption(
      const std::string& option, const OptionTypeMap& options_map) const;

  /**
   * Compares the options specified by this_option and that_option for
   * equivalence, returning true if they match. This method looks at the options
   * in the input opt_map and sees if the values in this_option and that_option
   * are equivalent
   *
   * @param opt_map       The set of options to check
   * @param sanity_level  How diligent the comparison must be for equivalence
   * @param opt_level     Optional map specifying a sanity check level for the
   * named options in opt_map.
   * @param this_option   One option to compare
   * @param that_option   The option to compare to
   * @parm opt_name       If the method returns false, opt_name returns the name
   *                      of the first option that failed to match.
   */
  bool OptionsAreEqual(
      const OptionTypeMap& opt_map,
      const std::unordered_map<std::string, OptionsSanityCheckLevel>* opt_level,
      OptionsSanityCheckLevel sanity_check_level, const char* this_option,
      const char* that_option, std::string* opt_name) const;
  Status SerializeSingleOption(uint32_t mode, const std::string& name,
                               std::string* value, bool* found_it) const;
#endif  // ROCKSDB_LITE
  /**
   * Registers the input name with the options and associated map.
   * When classes register their options in this manner, most of the
   * functionality (excluding unknown options and validate/sanitize) is
   * implemented by the base class.
   *
   * @param name    The name of this option (@see GetOptionsPtr)
   * @param opt_ptr Pointer to the options to associate with this name
   * @param opt_map Options map that controls how this option is configured.
   */
  void RegisterOptionsMap(const std::string& name, void* opt_ptr,
                          const OptionTypeMap& opt_map);

  /**
   * Serializes a single option, typically into "name=value" format.
   * The actual representation is controlled by the mode parameter.
   */
  void PrintSingleOption(const std::string& prefix, const std::string& name,
                         const std::string& value, const std::string& delimiter,
                         std::string* result) const;

 protected:
  // Methods for dealing with unknown options, either of type "kUnknown",
  // or not found in the map for some other reason

  /**
   * Sets the name and value of the configurable object of unknown type.
   * Sets found_option=true if the name matches a valid option for this object
   * Returns OK if the option was successfully updated or not found
   * Returns a non-OK status if the name was found but the value was not valid
   * Objects that have options that cannot be parsed by the typical means (via
   * Maps and ParseOption) should override this method,
   */
  virtual Status SetUnknown(const DBOptions& /* db_opts */,
                            const std::string& name,
                            const std::string& /* value */) {
    return Status::NotFound("Could not find option: ", name);
  }

  virtual Status SetEnum(const DBOptions& /* db_opts */,
                         const std::string& name,
                         const std::string& /* value */, char* /* addr */) {
    return Status::NotFound("Could not find option: ", name);
  }

  /**
   * Converts the unknown named option into its string form and returns that
   * value in result.
   */
  virtual Status UnknownToString(uint32_t /* mode */, const std::string& name,
                                 std::string* /*result */) const {
    return Status::NotFound("Could not find option: ", name);
  }

  virtual Status EnumToString(uint32_t /* mode */, const std::string& name,
                              std::string* /*result */) const {
    return Status::NotFound("Could not find option: ", name);
  }

#ifndef ROCKSDB_LITE
  /**
   * Compares to unknown options for equivalence.
   */
  virtual bool IsUnknownEqual(const std::string& /* opt_name */,
                              const OptionTypeInfo& /* type_info */,
                              OptionsSanityCheckLevel /* sanity_check_level */,
                              const char* this_addr,
                              const char* that_addr) const {
    return this_addr == that_addr;
  }

  virtual bool IsEnumEqual(const std::string& /* opt_name */,
                           const OptionTypeInfo& /* type_info */,
                           OptionsSanityCheckLevel /* sanity_check_level */,
                           const char* this_addr, const char* that_addr) const {
    return this_addr == that_addr;
  }

  virtual bool IsConfigEqual(const std::string& opt_name,
                             const OptionTypeInfo& opt_info,
                             OptionsSanityCheckLevel level,
                             const Configurable* this_config,
                             const Configurable* that_config,
                             std::string* mismatch) const;
#endif
};
}  // namespace rocksdb
