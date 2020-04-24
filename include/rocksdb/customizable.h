// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "rocksdb/configurable.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
/**
 * Customizable a base class used by the rocksdb that describes a
 * standard way of configuring and creating objects.  Customizable objects
 * are configurable objects that can be created from an ObjectRegistry.
 *
 * When a Customizable is being created, the "name" property specifies
 * the name of the instance being created.
 * For custom objects, their configuration and name can be specified by:
 * [prop]={name=X;option 1 = value1[; option2=value2...]}
 *
 * [prop].name=X
 * [prop].option1 = value1
 *
 * [prop].name=X
 * X.option1 =value1
 */
class Customizable : public Configurable {
 public:
  virtual ~Customizable() {}
  // Returns the name of this class of Customizable
  virtual const char* Name() const = 0;
  // Returns an identifier for this Customizable.
  // This could be its name or something more complex (like its URL/pattern).
  // Used for pretty printing.
  virtual std::string GetId() const {
    std::string id = Name();
    return id;
  }

  // Finds the named Customizable in the stack, or nullptr if not found.
  // This method will compare the input name to the name of the instance.
  // If it matches, the instance (e.g this) will be returned.  If they don't
  // match, Inner() is also checked for match.  On no match, nullptr is
  // returned.
  //
  // This method is typically used in conjunction with CastAs to find the
  // derived class instance from its base.  For example, if you have an Env
  // and want the "Default" env, you would FindInstance("Default") to get
  // the default implementation.  This method should be used when you need a
  // specific derivative or implementation of a class.
  //
  // Intermediary caches (such as SharedCache) may wish to override this method
  // to check for the intermediary name (SharedCache).  Classes with multiple
  // potential names (e.g. "PosixEnv", "DefaultEnv") may also wish to override
  // this method.
  //
  // @param name The name of the instance to find.
  // @return The named instance (if found) or nullptr if not.
  virtual const Customizable* FindInstance(const std::string& name) const;

  // Returns the named instance of the Customizable as a T*, or nullptr if not
  // found. This method uses FindInstance to find the appropriate class instance
  // and then casts it to the expected return type.
  template <typename T>
  const T* CastAs(const std::string& name) const {
    const auto c = FindInstance(name);
    return static_cast<const T*>(c);
  }

  template <typename T>
  T* CastAs(const std::string& name) {
    auto c = const_cast<Customizable*>(FindInstance(name));
    return static_cast<T*>(c);
  }

 protected:
  std::string GetOptionName(const std::string& long_name) const override;
#ifndef ROCKSDB_LITE
  Status DoGetOption(const ConfigOptions& config_options,
                     const std::string& short_name,
                     std::string* value) const override;
  std::string AsString(const ConfigOptions& options,
                       const std::string& prefix) const override;
#endif  // ROCKSDB_LITE
  bool DoMatchesOptions(const ConfigOptions& config_options,
                        const Configurable* other,
                        std::string* name) const override;

 public:
  // Helper method for configuring a new customizable object.
  // If base_opts are set, this is the "default" options to use for the new
  // object whereas "new_opts" are overlaid on the base options. Returns OK if
  // the object could be successfully configured
  static Status ConfigureNewObject(
      const ConfigOptions& config_options, Customizable* object,
      const std::string& id, const std::string& base_opts,
      const std::unordered_map<std::string, std::string>& new_opts);

  // Splits the input opt_value into the ID field and the remaining options.
  // The input opt_value can be in the form of "name" or "name=value
  // [;name=value]". The first form uses the "name" as an id with no options The
  // latter form converts the input into a map of name=value pairs and sets "id"
  // to the "id" value from the map.
  // @param opt_value The value to split into id and options
  // @param id The id field from the opt_value
  // @param options The remaining name/value pairs from the opt_value
  // @param default_id If specified and there is no id field in the map, this
  // value
  //      is returned as the ID
  // @return OK if the value was converted to a map succesfully and an ID was
  // found.
  // @return InvalidArgument if the value could not be converted to a map or
  // there was
  //      or there is no id property in the map.
  static Status GetOptionsMap(
      const std::string& opt_value, std::string* id,
      std::unordered_map<std::string, std::string>* options);
  static Status GetOptionsMap(
      const std::string& opt_value, const std::string& default_id,
      std::string* id, std::unordered_map<std::string, std::string>* options);
};

}  // namespace ROCKSDB_NAMESPACE
