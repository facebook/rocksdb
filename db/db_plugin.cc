// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/db_plugin.h"

#include "options/customizable_helper.h"
#include "rocksdb/convenience.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

Status DBPlugin::CreateFromString(const std::string& value,
                                  const ConfigOptions& opts,
                                  std::shared_ptr<DBPlugin>* result) {
  return LoadSharedObject<DBPlugin>(value, nullptr, opts, result);
}

const DBPlugin* DBPlugin::Find(const std::string& id,
                               const DBOptions& db_opts) {
  return Find(id, db_opts.plugins);
}

const DBPlugin* DBPlugin::Find(
    const std::string& id,
    const std::vector<std::shared_ptr<DBPlugin>>& plugins) {
  for (const auto p : plugins) {
    if (p->FindInstance(id) != nullptr) {
      return p.get();
    }
  }
  return nullptr;
}

Status DBPlugin::SanitizeOptions(
    const std::string& db_name, DBOptions* db_options,
    std::vector<ColumnFamilyDescriptor>* column_families) {
  Status s;
  for (auto p : db_options->plugins) {
    s = p->SanitizeCB(db_name, db_options, column_families);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status DBPlugin::ValidateOptions(
    const std::string& db_name, const DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& column_families) {
  Status s;
  for (const auto p : db_options.plugins) {
    s = p->ValidateCB(db_name, db_options, column_families);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status DBPlugin::OpenCB(DB* db,
                        const std::vector<ColumnFamilyHandle*>& /*handles*/,
                        DB** wrapped) {
  *wrapped = db;
  return Status::OK();
}

Status DBPlugin::Open(DB* db, const std::vector<ColumnFamilyHandle*>& handles,
                      DB** wrapped) {
  Status s;

  *wrapped = db;
  DBOptions db_opts = db->GetDBOptions();
  for (const auto p : db_opts.plugins) {
    s = p->OpenCB(*wrapped, handles, wrapped);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status DBPlugin::OpenReadOnlyCB(
    DB* db, const std::vector<ColumnFamilyHandle*>& /*handles*/, DB** wrapped) {
  if (SupportsReadOnly()) {
    *wrapped = db;
    return Status::OK();
  } else {
    return Status::NotSupported("Readonly not supported: ", GetId());
  }
}

Status DBPlugin::OpenReadOnly(DB* db,
                              const std::vector<ColumnFamilyHandle*>& handles,
                              DB** wrapped) {
  Status s;

  *wrapped = db;
  DBOptions db_opts = db->GetDBOptions();
  for (const auto p : db_opts.plugins) {
    s = p->OpenReadOnlyCB(*wrapped, handles, wrapped);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status DBPlugin::OpenSecondaryCB(
    DB* db, const std::vector<ColumnFamilyHandle*>& /*handles*/, DB** wrapped) {
  if (SupportsSecondary()) {
    *wrapped = db;
    return Status::OK();
  } else {
    return Status::NotSupported("Secondary not supported: ", GetId());
  }
}

Status DBPlugin::OpenSecondary(DB* db,
                               const std::vector<ColumnFamilyHandle*>& handles,
                               DB** wrapped) {
  Status s;

  *wrapped = db;
  DBOptions db_opts = db->GetDBOptions();
  for (const auto p : db_opts.plugins) {
    s = p->OpenSecondaryCB(*wrapped, handles, wrapped);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status DBPlugin::RepairDB(const std::string& dbname, const DBOptions& db_options,
                         const std::vector<ColumnFamilyDescriptor>& column_families,
                          const ColumnFamilyOptions& unknown_cf_opts) {
  Status s;
  for (const auto p : db_options.plugins) {
    s = p->RepairCB(dbname, db_options, column_families, unknown_cf_opts);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status DBPlugin::DestroyDB(const std::string& dbname, const Options& options,
                           const std::vector<ColumnFamilyDescriptor>& column_families) {
  Status s;
  for (const auto p : options.plugins) {
    s = p->DestroyCB(dbname, options, column_families);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
