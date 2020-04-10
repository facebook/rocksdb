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

  // Finds the named Customizable in the stack, or nullptr if not found
  virtual const Customizable* FindInstance(const std::string& name) const;

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
  Status DoGetOption(const std::string& short_name,
                     const ConfigOptions& options,
                     std::string* value) const override;
  std::string AsString(const std::string& prefix,
                       const ConfigOptions& options) const override;
#endif  // ROCKSDB_LITE
  bool DoMatchesOptions(const Configurable* other, const ConfigOptions& options,
                        std::string* name) const override;

 public:
  // Helper method for configuring a new customizable object.
  // If base_opts are set, this is the "default" options to use for the new
  // object whereas "new_opts" are overlaid on the base options. Returns OK if
  // the object could be successfully configured
  static Status ConfigureNewObject(
      Customizable* object, const std::string& id, const std::string& base_opts,
      const std::unordered_map<std::string, std::string>& new_opts,
      const ConfigOptions& cfg);
  static Status GetOptionsMap(
      const std::string& opt_value, std::string* id,
      std::unordered_map<std::string, std::string>* options);
  static Status GetOptionsMap(
      const std::string& opt_value, const std::string& default_id,
      std::string* id, std::unordered_map<std::string, std::string>* options);
};

}  // namespace ROCKSDB_NAMESPACE
