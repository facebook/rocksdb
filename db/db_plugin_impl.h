// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The DBPlugin class is a means of adding "StackableDBs" to RocksDB without
// creating custom DB constructors.  Plugins are registered with the DBOptions
// and then invoked during the database initialization or destruction phases.
//
// Plugins allow more than one database to be stacked (as opposed to static open
// methods that allow only a "stack over impl" model.
//
// Plugins can also be executed at other times.  Plugins can also be run during
// by the Repair and Destroy methods of a database.
//
// The DBOptions class supports multiple plugins.  During "constructive"
// operations, the plugins are executed in the order in which they are
// registered [0...n] During "destructive" operations, plugins are executed in
// the inverse [n...0] order.

#pragma once

#include <memory>
#include <vector>

#include "rocksdb/db_plugin.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
class ColumnFamilyHandle;
class DB;

struct ColumnFamilyDescriptor;
struct ConfigOptions;
struct DBOptions;
struct Options;

class DBPluginImpl : public DBPlugin {
 public:
  // Finds the plugin by "id" from the input list, returning null if not found.
  static const DBPlugin* Find(
      const std::string& id,
      const std::vector<std::shared_ptr<DBPlugin>>& plugins);

  // Finds the plugin by "id" from the input list, returning null if not found.
  static const DBPlugin* Find(const std::string& id, const DBOptions& db_opts);

  // Finds the plugin by "id" from the list, casting it to the appropriate type.
  template <typename T>
  static const T* FindAs(
      const std::string& id,
      const std::vector<std::shared_ptr<DBPlugin>>& plugins) {
    return static_cast<const T*>(Find(id, plugins));
  }

  // Finds the plugin by "id" from the list, casting it to the appropriate type.
  template <typename T>
  static const T* FindAs(const std::string& id, const DBOptions& db_opts) {
    return static_cast<const T*>(Find(id, db_opts));
  }

  // Returns OK if all of the options support the given options mode
  static Status CheckOpenMode(DBPlugin::OpenMode mode,
                              const DBOptions& db_options);

  Status SanitizeDBOptionsCB(OpenMode mode, const std::string& db_name,
                             DBOptions* db_options) override;
  Status SanitizeCFOptionsCB(
      OpenMode mode, const std::string& db_name, const DBOptions& db_options,
      std::vector<ColumnFamilyDescriptor>* column_families) override;
  Status ValidateCB(OpenMode mode, const std::string& db_name,
                    const DBOptions& db_options,
                    const std::vector<ColumnFamilyDescriptor>& column_families)
      const override;
  Status OpenCB(OpenMode mode, DB* db,
                const std::vector<ColumnFamilyHandle*>& handles,
                DB** wrapped) override;

  // Traverses the list of plugins and sanitizes the options in order.
  // On error, stops the traversal and returns the status.
  // The DBOptions must be specified and are always sanitized
  // If column_families is not null, the ColumnFamilies are are sanitized as
  // well.
  static Status SanitizeOptionsForDB(
      DBPlugin::OpenMode mode, const std::string& db_name,
      DBOptions* db_options,
      std::vector<ColumnFamilyDescriptor>* column_families = nullptr);

  // Traverses the list of plugins and validates the options in order.
  // On error, stops the traversal and returns the status.
  static Status ValidateOptionsForDB(
      DBPlugin::OpenMode mode, const std::string& db_name,
      const DBOptions& db_options,
      const std::vector<ColumnFamilyDescriptor>& column_families);

  // Traverses the plugin list for this database and opens the
  // StackableDBs for the list of plugins.
  // If the operation fails, an error is returned.
  // On success, "wrapped" is updated to point to the StackableDB"
  static Status Open(DBPlugin::OpenMode mode, DB* db,
                     const std::vector<ColumnFamilyHandle*>& handles,
                     DB** wrapped);

  static Status RepairDB(
      const std::string& dbname, const DBOptions& db_options,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      const ColumnFamilyOptions& unknown_cf_opts);

  static Status DestroyDB(
      const std::string& name, const Options& options,
      const std::vector<ColumnFamilyDescriptor>& column_families);

 protected:
  Status NotSupported(OpenMode mode) const;
};

}  // namespace ROCKSDB_NAMESPACE
