// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class Logger;
class ObjectRegistry;
class OptionTypeInfo;
struct ColumnFamilyOptions;
struct ConfigOptions;
struct DBOptions;

/**
 * Configurable is a base class used by the rocksdb that describes a
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
 * If a derived class calls RegisterOptions to register (by name) how its
 * options objects are to be processed, this functionality can typically be
 * handled by this class without additional overrides. Otherwise, the derived
 * class will need to implement the methods for handling the corresponding
 * functionality.
 */
class Configurable {
 protected:
  struct RegisteredOptions {
    // The name of the options being registered
    std::string name;
    // Pointer to the object being registered
    void* opt_ptr;
#ifndef ROCKSDB_LITE
    // The map of options being registered
    const std::unordered_map<std::string, OptionTypeInfo>* type_map;
#endif
  };

 public:
  Configurable() {}
  virtual ~Configurable() {}

  /**
   * Returns the raw pointer of the named options that is used by this
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
  Status ConfigureFromMap(const std::unordered_map<std::string, std::string>&,
                          const ConfigOptions& options);
  Status ConfigureFromMap(const std::unordered_map<std::string, std::string>&,
                          const ConfigOptions& options,
                          std::unordered_map<std::string, std::string>* unused);

#ifndef ROCKSDB_LITE
  Status ConfigureFromString(
      const std::string& opts, const ConfigOptions& options,
      std::unordered_map<std::string, std::string>* unused);
#endif  // ROCKSDB_LITE
  Status ConfigureFromString(const std::string& opts,
                             const ConfigOptions& options);

  /**
   * Updates the named option to the input value, returning OK if successful.
   * Parameters:
   *     name                  The name of the option to update
   *     value                 The value to set for the named option
   * Returns:  OK              on success
   *           NotFound        if the name is not a valid option
   *           InvalidArgument if the value is valid for the named option
   *           NotSupported    If the name/value is not supported
   */
  Status ConfigureOption(const std::string& name, const std::string& value,
                         const ConfigOptions& options);
  /**
   * Fills in result with the string representation of the configuration options
   * Parameters:
   *   options:  Options controlling how the result string appears
   *   result:   The returned string representation of the options
   */
  Status GetOptionString(const ConfigOptions& options,
                         std::string* result) const;

#ifndef ROCKSDB_LITE

  // Returns the list of option names associated with this configurable
  Status GetOptionNames(const ConfigOptions& options,
                        std::unordered_set<std::string>* result) const;

  // Converts this object to a delimited-string

  std::string ToString(const ConfigOptions& options) const {
    return ToString("", options);
  }
  std::string ToString(const std::string& prefix,
                       const ConfigOptions& options) const;
  /**
   * Returns the value of the option associated with the input name
   */
  Status GetOption(const std::string& name, const ConfigOptions& options,
                   std::string* value) const;
#endif  // ROCKSDB_LITE

  /**
   * Checks to see if this Configurable is equivalent to other.
   * Level controls how pedantic the comparison must be for equivalency to be
   * achieved
   */
  bool Matches(const Configurable* other, const ConfigOptions& options) const;
  bool Matches(const Configurable* other, const ConfigOptions& options,
               std::string* name) const;
  // Returns printable options for dump
  // If this method returns non-empty string, the result is used for dumping
  // the options. If an empty string is returned, the registered options are
  // used.
  virtual std::string GetPrintableOptions() const { return ""; }

  // Initializes the options and makes sures the settings are valid/consistent
  virtual Status PrepareOptions(const ConfigOptions& opts);

  // Checks to see if the settings are valid for this set of options
  virtual Status ValidateOptions(const DBOptions& db_opts,
                                 const ColumnFamilyOptions& cf_opts) const;

 protected:
  // True if the options have been successfully validated, false otherwise
  bool validated_;
#ifndef ROCKSDB_LITE
  Status DoConfigureOption(const OptionTypeInfo* opt_info,
                           const std::string& short_name,
                           const std::string& opt_name,
                           const std::string& opt_value,
                           const ConfigOptions& cfg, void* opt_ptr);
  virtual Status DoConfigureOptions(
      std::unordered_map<std::string, std::string>* options,
      const std::unordered_map<std::string, OptionTypeInfo>& type_map,
      const std::string& type_name, void* opt_ptr, const ConfigOptions& cfg);
  virtual Status DoConfigureFromMap(
      const std::unordered_map<std::string, std::string>&,
      const ConfigOptions& options,
      std::unordered_map<std::string, std::string>* unused);
#endif  // ROCKSDB_LITE
  // If this class is a wrapper (has-a), this method should be
  // over-written to return the inner configurable
  virtual Configurable* Inner() const { return nullptr; }

  /**
   * Returns the raw pointer for the associated named option.
   */
  virtual const void* GetOptionsPtr(const std::string& /* name */) const;

  /**
   * Matches the input "other" object at the corresponding sanity level.
   */
  virtual bool DoMatchesOptions(const Configurable* other,
                                const ConfigOptions& options,
                                std::string* name) const;
  virtual Status ParseStringOptions(const std::string& opts_str,
                                    const ConfigOptions& opts);

#ifndef ROCKSDB_LITE
  virtual Status ParseOption(const OptionTypeInfo& opt_info,
                             const std::string& opt_name,
                             const std::string& opt_value,
                             const ConfigOptions& options, void* opt_ptr);
  virtual Status SerializeOption(const OptionTypeInfo& opt_info,
                                 const std::string& opt_name,
                                 const void* const opt_ptr,
                                 const ConfigOptions& options,
                                 std::string* opt_value) const;

  // Tests to see if the single option name/info matches for this and that
  virtual bool MatchesOption(const OptionTypeInfo& opt_info,
                             const std::string& name,
                             const void* const this_ptr,
                             const void* const that_ptr,
                             const ConfigOptions& options,
                             std::string* bad_name) const;
  virtual bool CheckByName(const OptionTypeInfo& opt_info,
                           const std::string& opt_name,
                           const void* const this_ptr,
                           const void* const that_ptr,
                           const ConfigOptions& options) const;
#endif
#ifndef ROCKSDB_LITE
  virtual Status DoGetOption(const std::string& short_name,
                             const ConfigOptions& options,
                             std::string* value) const;
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
  virtual Status DoSerializeOptions(const std::string& prefix,
                                    const ConfigOptions& options,
                                    std::string* result) const;
  virtual std::string AsString(const std::string& header,
                               const ConfigOptions& options) const;
  Status SerializeOptions(
      const std::string& prefix,
      const std::unordered_map<std::string, OptionTypeInfo>& opt_map,
      const void* opt_ptr, const ConfigOptions& options,
      std::string* result) const;

  virtual Status DoListOptions(const std::string& prefix,
                               const ConfigOptions& options,
                               std::unordered_set<std::string>* result) const;

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
      const std::unordered_map<std::string, OptionTypeInfo>& opt_map,
      bool check_mutable, const char* this_option, const Configurable* that,
      const char* that_option, const ConfigOptions& options,
      std::string* opt_name) const;
  Status SerializeSingleOption(
      const std::unordered_map<std::string, OptionTypeInfo>& opt_map,
      const void* opt_ptr, const std::string& name,
      const ConfigOptions& options, std::string* value, bool* found_it) const;
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
  void RegisterOptions(
      const std::string& name, void* opt_ptr,
      const std::unordered_map<std::string, OptionTypeInfo>* opt_map);

  /**
   *  Given a prefixed name (e.g. rocksdb.my.type.opt), returns the short name
   * ("opt")
   */
  virtual std::string GetOptionName(const std::string& long_name) const;

  /**
   * Serializes a single option, typically into "name=value" format.
   * The actual representation is controlled by the mode parameter.
   */
  void PrintSingleOption(const std::string& prefix, const std::string& name,
                         const std::string& value, const std::string& delimiter,
                         std::string* result) const;

  virtual Status SetUnknown(const std::string& opt_name,
                            const std::string& /*opt_value */,
                            const ConfigOptions& /*options*/) {
    return Status::InvalidArgument("Could not find option: ", opt_name);
  }

  static const std::string kIdPropName /* = "id" */;
  static const std::string kIdPropSuffix /* = ".id" */;

 private:
  std::vector<RegisteredOptions> options_;
};
}  // namespace ROCKSDB_NAMESPACE
