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

// Configurable is a base class used by the rocksdb that describes a
// standard way of configuring objects.  A Configurable object can:
//   -> Populate itself given:
//        - One or more "name/value" pair strings
//        - A string repesenting the set of name=value properties
//        - A map of name/value properties.
//   -> Convert itself into its string representation
//   -> Dump itself to a Logger
//   -> Compare itself to another Configurable object to see if the two objects
// are equivalent
//
// If a derived class calls RegisterOptions to register (by name) how its
// options objects are to be processed, this functionality can typically be
// handled by this class without additional overrides. Otherwise, the derived
// class will need to implement the methods for handling the corresponding
// functionality.
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

  constexpr static const char* kIdPropName = "id";
  constexpr static const char* kIdPropSuffix = ".id";

  // Returns the raw pointer of the named options that is used by this
  // object, or nullptr if this function is not supported.
  // Since the return value is a raw pointer, the object owns the
  // pointer and the caller should not delete the pointer.
  //
  // Note that changing the underlying options while the object
  // is currently used by any open DB is undefined behavior.
  // Developers should use DB::SetOption() instead to dynamically change
  // options while the DB is open.
  template <typename T>
  const T* GetOptions(const std::string& name) const {
    return reinterpret_cast<const T*>(GetOptionsPtr(name));
  }
  template <typename T>
  T* GetOptions(const std::string& name) {
    return reinterpret_cast<T*>(const_cast<void*>(GetOptionsPtr(name)));
  }

  // Configures the options for this class based on the input parameters.
  // On successful completion, the object is updated with the settings from
  // the opt_map.  If this method fails, an attempt is made to revert the
  // object to original state.  Note that the revert may not be the original
  // state but may be an equivalent.
  //
  // The acceptable values of the name/value pairs are documented with the
  // specific class/instance.
  //
  // @param config_options Controls how the arguments are processed.
  // @param opt_map Name/value pairs of the options to update
  // @param unused If specified, this value will return the name/value
  //      pairs from opt_map that were NotFound for this object.
  // @return OK If all values in the map were successfully updated
  //      If invoke_prepare_options is true, OK also implies
  //      PrepareOptions ran successfully.
  // @return NotFound If any of the names in the opt_map were not valid
  //      for this object.  If unused is specified, it will contain the
  //      collection of NotFound names.
  // @return NotSupported  If any of the names are valid but the object does
  //       not know how to convert the value.  This can happen if, for example,
  //       there is some nested Configurable that cannot be created.
  // @return InvalidArgument If any of the values cannot be successfully
  //       parsed.  This can also be returned if PrepareOptions encounters an
  //       error.
  // @see ConfigOptions for a description of the controls.
  Status ConfigureFromMap(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opt_map);
  Status ConfigureFromMap(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opt_map,
      std::unordered_map<std::string, std::string>* unused);

#ifndef ROCKSDB_LITE
  // Configures the options for this class based on the input parameters.
  // On successful completion, the object is updated with the settings from
  // the opt_map.  If this method fails, an attempt is made to revert the
  // object to original state.  Note that the revert may not be the original
  // state but may be an equivalent.
  // @see ConfigureFromMap for more details
  // @param config_options Controls how the arguments are processed.
  // @param opt_str string containing the values to update.
  // @param unused If specified, this value will return the name/value
  //      pairs from opt_map that were NotFound for this object.
  // @return OK If all specified values were successfully updated
  //      If invoke_prepare_options is true, OK also implies
  //      PrepareOptions ran successfully.
  // @return NotFound If any of the names were not valid for this object.
  //      If unused is specified, it will contain the collection of NotFound
  //      names.
  // @return NotSupported  If any of the names are valid but the object does
  //       not know how to convert the value.  This can happen if, for example,
  //       there is some nested Configurable that cannot be created.
  // @return InvalidArgument If any of the values cannot be successfully
  //       parsed.  This can also be returned if PrepareOptions encounters an
  //       error.
  Status ConfigureFromString(
      const ConfigOptions& config_options, const std::string& opts_str,
      std::unordered_map<std::string, std::string>* unused);
#endif  // ROCKSDB_LITE
  Status ConfigureFromString(const ConfigOptions& config_options,
                             const std::string& opts);

  // Updates the named option to the input value, returning OK if successful.
  // Note that ConfigureOption does not cause PrepareOptions to be invoked.
  // @param config_options Controls how the name/value is processed.
  // @param name The name of the option to update
  // @param value The value to set for the named option
  // @return OK If the named field was successfully updated to value.
  // @return NotFound If the name is not valid for this object.
  // @return NotSupported  If the name is valid but the object does
  //       not know how to convert the value.  This can happen if, for example,
  //       there is some nested Configurable that cannot be created.
  // @return InvalidArgument If the value cannot be successfully  parsed.
  Status ConfigureOption(const ConfigOptions& config_options,
                         const std::string& name, const std::string& value);

  // Fills in result with the serialized options for this object.
  // This is the inverse of ConfigureFromString.
  // @param config_options Controls how serialization happens.
  // @param result The string representation of this object.
  // @return OK If the options for this object wer successfully serialized.
  // @return InvalidArgument If one or more of the options could not be
  // serialized.
  Status GetOptionString(const ConfigOptions& config_options,
                         std::string* result) const;
#ifndef ROCKSDB_LITE
  // Returns the serialized options for this object.
  // This method is similar to GetOptionString with no errors.
  // @param config_options Controls how serialization happens.
  // @param prefix A string to prepend to every option.
  // @return The serialized representation of the options for this object
  std::string ToString(const ConfigOptions& config_options) const {
    return ToString(config_options, "");
  }
  std::string ToString(const ConfigOptions& config_options,
                       const std::string& prefix) const;

  // Returns the list of option names associated with this configurable
  // @param config_options Controls how the names are returned
  // @param result The set of option names for this object. Note that
  //      options that are deprecated or aliases are not returned.
  // @return OK on success.
  Status GetOptionNames(const ConfigOptions& config_options,
                        std::unordered_set<std::string>* result) const;

  // Returns the value of the option associated with the input name
  // This method is the functional inverse of ConfigureOption
  // @param config_options Controls how the value is returned
  // @param name The name of the option to return a value for.
  // @param value The returned value associated with the named option.
  // @return OK If the named field was successfully updated to value.
  // @return NotFound If the name is not valid for this object.
  // @param InvalidArgument If the name is valid for this object but
  //      its value cannot be serialized.
  Status GetOption(const ConfigOptions& config_options, const std::string& name,
                   std::string* value) const;
#endif  // ROCKSDB_LITE

  // Checks to see if this Configurable is equivalent to other.
  // This method assumes that the two objects are of the same class.
  // @param config_options Controls how the options are compared.
  // @param other The other object to compare to.
  // @param mismatch If the objects do not match, this parameter contains
  //      the name of the option that triggered the match failure.
  // @param True if the objects match, false otherwise.
  bool Matches(const ConfigOptions& config_options,
               const Configurable* other) const;
  bool Matches(const ConfigOptions& config_options, const Configurable* other,
               std::string* name) const;

  // Returns a pretty-printed, human-readable version of the options.
  // This method is typically used to dump the options to a log file.
  // Classes should override this method
  virtual std::string GetPrintableOptions() const { return ""; }

  // Initializes the options and makes sures the settings are valid/consistent
  // This method may be called as part of Configure (if invoke_prepare_options
  // is set), or may be invoked separately. Classes must override this method to
  // provide any implementation-specific initialization, such as opening log
  // files or setting up cache parameters. Implementations should be idempotent
  // (e.g. don't re-open the log file or restart the cache), as there is the
  // potential this method can be called multiple times.
  //
  // By default, this method will also prepare all nested (Inner and
  // OptionType::kConfigurable) objects.
  //
  // @param config_options Controls how the object is prepared.  Also contains
  //      a Logger and Env that can be used to initialize this object.
  // @return OK If the object was successfully initialized.
  // @return InvalidArgument If this object could not be successfull
  // initialized.
  virtual Status PrepareOptions(const ConfigOptions& config_options);

  // Checks to see if the settings are valid for this object.
  // This method checks to see if the input DBOptions and ColumnFamilyOptions
  // are valid for the settings of this object.  For example, an Env might not
  // support certain mmap modes or a TableFactory might require certain
  // settings.
  //
  // By default, this method will also validate all nested (Inner and
  // OptionType::kConfigurable) objects.
  //
  // @param db_opts The DBOptions to validate
  // @param cf_opts The ColumnFamilyOptions to validate
  // @return OK if the options are valid
  // @return InvalidArgument If the arguments are not valid for the options
  //       of the current object.
  virtual Status ValidateOptions(const DBOptions& db_opts,
                                 const ColumnFamilyOptions& cf_opts) const;

 protected:
#ifndef ROCKSDB_LITE
  // Internal method to configure one option for this object.
  // @param config_options Controls how the option is being configured
  // @param opt_info The OptionTypeInfo registered for this option name
  //      that controls what field is updated (offset) and how (type).
  // @param field_name For options with sub-options (like Structs or
  // Configurables),
  //      this value may be the name of the sub-field of the option being
  //      updated. For example, if the option is
  //      "compaction_options_fifo.allow_compaction", then field name would be
  //      "allow_compaction".  For most options, field_name and opt_name will be
  //      equivalent.
  // @param opt_name  The name of the option being updated.  For options with
  // sub-options,
  //      This option may be the name of the option being updated
  //      "compaction_options_fifo", or the full option being updated
  //      ("compaction_options_fifo.allow_compaction".
  // @param opt_value The new value for this option.
  // @param opt_ptr Pointer to the base object registered for this opt_info.
  Status DoConfigureOption(const ConfigOptions& config_options,
                           const OptionTypeInfo* opt_info,
                           const std::string& field_name,
                           const std::string& opt_name,
                           const std::string& opt_value, void* opt_ptr);

  // Internal method to configure a set of options for this object.
  // Classes may override this value to change its behavior.
  // @param config_options Controls how the options are being configured
  // @param type_name The name that was registered for this set of options
  // @param type_map The map of options for this name
  // @param opt_ptr Pointer to the object being configured for this option set.
  // @param options The option name/values being updated.  On return, any
  //    option that was found is removed from the list.
  // @return OK If all of the options were successfully updated.
  // @return InvalidArgument If an option was found but the value could not
  //       be updated.
  // @return NotFound If an option name was not found in type_mape
  // @return NotSupported If the option was found but no rule for converting
  //       the value could be found.
  virtual Status DoConfigureOptions(
      const ConfigOptions& config_options, const std::string& type_name,
      const std::unordered_map<std::string, OptionTypeInfo>& type_map,
      std::unordered_map<std::string, std::string>* options, void* opt_ptr);

  // Internal method to configure the object from opt_map.
  // @see ConfigureFromMap for more details.
  virtual Status DoConfigureFromMap(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opt_map,
      std::unordered_map<std::string, std::string>* unused);
#endif  // ROCKSDB_LITE
  // If this class is a wrapper (has-a), this method should be
  // over-written to return the inner configurable (like an EnvWrapper).
  virtual Configurable* Inner() const { return nullptr; }

  // Returns the raw pointer for the associated named option.
  // Classes may override this method to provide further specialization.
  // The default implemntation looks at the registered options.  If the
  // input name matches that of a registered option, the pointer registered
  // with that name is returned.
  virtual const void* GetOptionsPtr(const std::string& name) const;

  // Internal method to match objects.
  // @see Match for more details.
  virtual bool DoMatchesOptions(const ConfigOptions& config_options,
                                const Configurable* other,
                                std::string* name) const;
  virtual Status ParseStringOptions(const ConfigOptions& config_options,
                                    const std::string& opts_str);

#ifndef ROCKSDB_LITE
  virtual Status ParseOption(const ConfigOptions& config_options,
                             const OptionTypeInfo& opt_info,
                             const std::string& opt_name,
                             const std::string& opt_value, void* opt_ptr);
  virtual Status SerializeOption(const ConfigOptions& config_options,
                                 const OptionTypeInfo& opt_info,
                                 const std::string& opt_name,
                                 const void* const opt_ptr,
                                 std::string* opt_value) const;

  // Internal method to see if the single option name/info matches for this and
  // that Classes may override this value to change its behavior.
  // @param config_options Controls how the options are being matched
  // @param opt_info The OptionTypeInfo registered for this option name
  //      that controls what field is matched (offset) and how (type).
  // @param name The name associated with this opt_info.
  // @param this_ptr The base pointer to compare to.  This is the object
  // registered for
  //      for this OptionTypeInfo.
  // @param that_ptr The other pointer to compare to.  This is the object
  // registered for
  //      for this OptionTypeInfo.
  // @param bad_name  If the match fails, the name of the option that failed to
  // match.
  virtual bool MatchesOption(const ConfigOptions& config_options,
                             const OptionTypeInfo& opt_info,
                             const std::string& name,
                             const void* const this_ptr,
                             const void* const that_ptr,
                             std::string* bad_name) const;

  // Internal method to check if "ByName" options match.
  // Classes may override this value to change its behavior.
  // @param config_options Controls how the options are being matched
  // @param opt_info The OptionTypeInfo registered for this option name
  //      that controls what field is matched (offset) and how (type).
  // @param opt_name The name associated with this opt_info.
  // @param this_ptr The base pointer to compare to.  This is the object
  // registered for
  //      for this OptionTypeInfo.
  // @param that_ptr The other pointer to compare to.  This is the object
  // registered for
  //      for this OptionTypeInfo.
  // @param bad_name  If the serialized name of the two options match.
  virtual bool CheckByName(const ConfigOptions& config_options,
                           const OptionTypeInfo& opt_info,
                           const std::string& opt_name,
                           const void* const this_ptr,
                           const void* const that_ptr) const;
#endif
#ifndef ROCKSDB_LITE
  // Internal method to get the value of a single option
  // Classes may override this value to change its behavior.
  virtual Status DoGetOption(const ConfigOptions& config_options,
                             const std::string& short_name,
                             std::string* value) const;

  // Internal method to serialize options (ToString, GetOptionString)
  // Classes may override this value to change its behavior.
  virtual Status DoSerializeOptions(const ConfigOptions& config_options,
                                    const std::string& prefix,
                                    std::string* result) const;

  // Internal method to serialize options (ToString)
  // Classes may override this value to change its behavior.
  virtual std::string AsString(const ConfigOptions& config_options,
                               const std::string& header) const;

  // Internal method to serialize a set of options for this object.
  // Classes may override this value to change its behavior.
  // @param config_options Controls how the options are being serialized
  // @param prefix String to prepend to every option being serialized
  // @param type_map The map of options for this name
  // @param opt_ptr Pointer to the object being configured for this option set.
  // @param result The serailized result
  // @return OK If all of the options in type_map were successfully serialized
  // @return InvalidArgument If an option could not be serialized.
  Status SerializeOptions(
      const ConfigOptions& config_options, const std::string& prefix,
      const std::unordered_map<std::string, OptionTypeInfo>& type_map,
      const void* opt_ptr, std::string* result) const;

  // Internal method to list the option names for this object.
  // Classes may override this value to change its behavior.
  // @see ListOptions for more details
  virtual Status DoListOptions(const ConfigOptions& config_options,
                               const std::string& prefix,
                               std::unordered_set<std::string>* result) const;
#endif  // ROCKSDB_LITE

  // Registers the input name with the options and associated map.
  // When classes register their options in this manner, most of the
  // functionality (excluding unknown options and validate/sanitize) is
  // implemented by the base class.
  //
  // @param name    The name of this option (@see GetOptionsPtr)
  // @param opt_ptr Pointer to the options to associate with this name
  // @param opt_map Options map that controls how this option is configured.
  void RegisterOptions(
      const std::string& name, void* opt_ptr,
      const std::unordered_map<std::string, OptionTypeInfo>* opt_map);

  //  Given a name (e.g. rocksdb.my.type.opt), returns the short name (opt)
  virtual std::string GetOptionName(const std::string& long_name) const;

  virtual Status SetUnknown(const ConfigOptions& /*config_options*/,
                            const std::string& opt_name,
                            const std::string& /*opt_value */) {
    return Status::InvalidArgument("Could not find option: ", opt_name);
  }

 private:
  std::vector<RegisteredOptions> options_;
};
}  // namespace ROCKSDB_NAMESPACE
