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
 * Customizable classes are used when there are multiple potential
 * implementations of a class for use by RocksDB (e.g. Table, Cache,
 * MergeOperator, etc).  The abstract base class is expected to define a method
 * declaring its type and a factory method for creating one of these, such as:
 * static const char *Type() { return "Table"; }
 * static Status CreateFromString(const ConfigOptions& options,
 *                                const std::string& id,
 *                                std::shared_ptr<TableFactory>* result);
 * The "Type" string is expected to be unique (no two base classes are the same
 * type). This factory is expected, based on the options and id, create and
 * return the appropriate derived type of the customizable class (e.g.
 * BlockBasedTableFactory, PlainTableFactory, etc). For extension developers,
 * helper classes and methods are provided for writing this factory.
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

  // This is typically determined by if the input name matches the
  // name of this object.
  // This method is typically used in conjunction with CastAs to find the
  // derived class instance from its base.  For example, if you have an Env
  // and want the "Default" env, you would IsInstanceOf("Default") to get
  // the default implementation.  This method should be used when you need a
  // specific derivative or implementation of a class.
  //
  // Intermediary caches (such as SharedCache) may wish to override this method
  // to check for the intermediary name (SharedCache).  Classes with multiple
  // potential names (e.g. "PosixEnv", "DefaultEnv") may also wish to override
  // this method.
  //
  // @param name The name of the instance to find.
  // Returns true if the class is an instance of the input name.
  virtual bool IsInstanceOf(const std::string& name) const {
    return name == Name();
  }

  // Returns the named instance of the Customizable as a T*, or nullptr if not
  // found. This method uses IsInstanceOf to find the appropriate class instance
  // and then casts it to the expected return type.
  template <typename T>
  const T* CastAs(const std::string& name) const {
    if (IsInstanceOf(name)) {
      return static_cast<const T*>(this);
    } else {
      const auto inner = static_cast<const Customizable*>(Inner());
      if (inner != nullptr) {
        return inner->CastAs<T>(name);
      } else {
        return nullptr;
      }
    }
  }

  template <typename T>
  T* CastAs(const std::string& name) {
    if (IsInstanceOf(name)) {
      return static_cast<T*>(this);
    } else {
      const auto inner = static_cast<const Customizable*>(Inner());
      if (inner != nullptr) {
        return (const_cast<Customizable*>(inner))->CastAs<T>(name);
      } else {
        return nullptr;
      }
    }
  }

  // Checks to see if this Configurable is equivalent to other.
  // This method assumes that the two objects are of the same class.
  // @param config_options Controls how the options are compared.
  // @param other The other object to compare to.
  // @param mismatch If the objects do not match, this parameter contains
  //      the name of the option that triggered the match failure.
  // @param True if the objects match, false otherwise.
  bool AreEquivalent(const ConfigOptions& config_options,
                     const Configurable* other,
                     std::string* name) const override;
#ifndef ROCKSDB_LITE
  Status GetOption(const ConfigOptions& config_options, const std::string& name,
                   std::string* value) const override;

#endif  // ROCKSDB_LITE
 protected:
  std::string GetOptionName(const std::string& long_name) const override;
#ifndef ROCKSDB_LITE
  std::string SerializeOptions(const ConfigOptions& options,
                               const std::string& prefix) const override;
#endif  // ROCKSDB_LITE
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
