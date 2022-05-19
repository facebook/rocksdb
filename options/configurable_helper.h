// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "rocksdb/configurable.h"
#include "rocksdb/convenience.h"

namespace ROCKSDB_NAMESPACE {
// Helper class defining static methods for supporting the Configurable
// class.  The purpose of this class is to keep the Configurable class
// as tight as possible and provide methods for doing the actual work
// of configuring the objects.
class ConfigurableHelper {
 public:
  // Configures the input Configurable object based on the parameters.
  // On successful completion, the Configurable is updated with the settings
  // from the opt_map.
  //
  // The acceptable values of the name/value pairs are documented with the
  // specific class/instance.
  //
  // @param config_options Controls how the arguments are processed.
  // @param opt_map Name/value pairs of the options to update
  // @param unused If specified, this value will return the name/value
  //      pairs from opt_map that were NotFound for this object.
  // @return OK If all values in the map were successfully updated
  // @return NotFound If any of the names in the opt_map were not valid
  //      for this object.  If unused is specified, it will contain the
  //      collection of NotFound entries
  // @return NotSupported  If any of the names are valid but the object does
  //       not know how to convert the value.  This can happen if, for example,
  //       there is some nested Configurable that cannot be created.
  // @return InvalidArgument If any of the values cannot be successfully
  //       parsed.  This can also be returned if PrepareOptions encounters an
  //       error.
  static Status ConfigureOptions(
      const ConfigOptions& config_options, Configurable& configurable,
      const std::unordered_map<std::string, std::string>& options,
      std::unordered_map<std::string, std::string>* unused);

#ifndef ROCKSDB_LITE
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
  static Status ConfigureSomeOptions(
      const ConfigOptions& config_options, Configurable& configurable,
      const std::unordered_map<std::string, OptionTypeInfo>& type_map,
      std::unordered_map<std::string, std::string>* options, void* opt_ptr);

  // Configures a single option in the input Configurable.
  // This method will look through the set of option names for this
  // Configurable searching for one with the input name.  If such an option
  // is found, it will be configured via the input value.
  //
  // @param config_options Controls how the option is being configured
  // @param configurable The object to configure
  // @param name For options with sub-options (like Structs or
  // Configurables),
  //      this value may be the name of the sub-field of the option being
  //      updated. For example, if the option is
  //      "compaction_options_fifo.allow_compaction", then field name would be
  //      "allow_compaction".  For most options, field_name and opt_name will be
  //      equivalent.
  // @param value The new value for this option.
  // @param See ConfigureOptions for the possible return values
  static Status ConfigureSingleOption(const ConfigOptions& config_options,
                                      Configurable& configurable,
                                      const std::string& name,
                                      const std::string& value);

  // Configures the option referenced by opt_info for this configurable
  // This method configures the option based on opt_info for the input
  // configurable.
  // @param config_options Controls how the option is being configured
  // @param configurable The object to configure
  // @param opt_name The full option name
  // @param name For options with sub-options (like Structs or
  // Configurables),
  //      this value may be the name of the sub-field of the option being
  //      updated. For example, if the option is
  //      "compaction_options_fifo.allow_compaction", then field name would be
  //      "allow_compaction".  For most options, field_name and opt_name will be
  //      equivalent.
  // @param value The new value for this option.
  // @param See ConfigureOptions for the possible return values
  static Status ConfigureOption(const ConfigOptions& config_options,
                                Configurable& configurable,
                                const OptionTypeInfo& opt_info,
                                const std::string& opt_name,
                                const std::string& name,
                                const std::string& value, void* opt_ptr);

  // Returns the value of the option associated with the input name
  // This method is the functional inverse of ConfigureOption
  // @param config_options Controls how the value is returned
  // @param configurable The object from which to get the option.
  // @param name The name of the option to return a value for.
  // @param value The returned value associated with the named option.
  //              Note that value will be only the serialized version
  //              of the option and not "name=value"
  // @return OK If the named field was successfully updated to value.
  // @return NotFound If the name is not valid for this object.
  // @param InvalidArgument If the name is valid for this object but
  //      its value cannot be serialized.
  static Status GetOption(const ConfigOptions& config_options,
                          const Configurable& configurable,
                          const std::string& name, std::string* value);

  // Serializes the input Configurable into the output result.
  // This is the inverse of ConfigureOptions
  // @param config_options Controls how serialization happens.
  // @param configurable The object to serialize
  // @param prefix A prefix to add to the each option as it is serialized.
  // @param result The string representation of the configurable.
  // @return OK If the options for this object wer successfully serialized.
  // @return InvalidArgument If one or more of the options could not be
  // serialized.
  static Status SerializeOptions(const ConfigOptions& config_options,
                                 const Configurable& configurable,
                                 const std::string& prefix,
                                 std::string* result);

  // Internal method to list the option names for this object.
  // Classes may override this value to change its behavior.
  // @see ListOptions for more details
  static Status ListOptions(const ConfigOptions& config_options,
                            const Configurable& configurable,
                            const std::string& prefix,
                            std::unordered_set<std::string>* result);

  // Checks to see if the two configurables are equivalent to one other.
  // This method assumes that the two objects are of the same class.
  // @param config_options Controls how the options are compared.
  // @param this_one The object to compare to.
  // @param that_one The other object being compared.
  // @param mismatch If the objects do not match, this parameter contains
  //      the name of the option that triggered the match failure.
  // @param True if the objects match, false otherwise.
  static bool AreEquivalent(const ConfigOptions& config_options,
                            const Configurable& this_one,
                            const Configurable& that_one,
                            std::string* mismatch);

 private:
  // Looks for the option specified by name in the RegisteredOptions.
  // This method traverses the types in the input options vector.  If an entry
  // matching name is found, that entry, opt_name, and pointer are returned.
  // @param options  The vector of options to search through
  // @param name     The name of the option to search for in the OptionType map
  // @param opt_name If the name was found, this value is set to the option name
  //                 associated with the input name/type.
  // @param opt_ptr  If the name was found, this value is set to the option
  // pointer
  //                 in the RegisteredOptions vector associated with this entry
  // @return         A pointer to the OptionTypeInfo from the options if found,
  //                 nullptr if the name was not found in the input options
  static const OptionTypeInfo* FindOption(
      const std::vector<Configurable::RegisteredOptions>& options,
      const std::string& name, std::string* opt_name, void** opt_ptr);

  static Status ConfigureCustomizableOption(
      const ConfigOptions& config_options, Configurable& configurable,
      const OptionTypeInfo& opt_info, const std::string& opt_name,
      const std::string& name, const std::string& value, void* opt_ptr);
#endif  // ROCKSDB_LITE
};

}  // namespace ROCKSDB_NAMESPACE
