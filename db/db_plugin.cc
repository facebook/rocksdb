// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/db_plugin.h"

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "options/customizable_helper.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db_plugin.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

Status DBPlugin::CreateFromString(const ConfigOptions& config_options,
                                  const std::string& value,
                                  std::shared_ptr<DBPlugin>* result) {
  return LoadSharedObject<DBPlugin>(config_options, value, nullptr, result);
}

static Status UnsupportedMode(DBPlugin::OpenMode /*mode*/, const char* name) {
  return Status::NotSupported("Open mode not supported ", name);
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

static Status CheckOpenMode(
    DBPlugin::OpenMode mode,
    const std::vector<std::shared_ptr<DBPlugin>>& plugins) {
  for (auto p : plugins) {
    if (!p->SupportsOpenMode(mode)) {
      return UnsupportedMode(mode, p->Name());
    }
  }
  return Status::OK();
}

Status DBPlugin::SanitizeCB(OpenMode mode, const std::string& /*db_name*/,
                            DBOptions* /*db_options*/,
                            std::vector<ColumnFamilyDescriptor>* /*cfds*/) {
  if (SupportsOpenMode(mode)) {
    return Status::OK();
  } else {
    return UnsupportedMode(mode, Name());
  }
}

Status DBPlugin::SanitizeOptionsForDB(
    DBPlugin::OpenMode open_mode, const std::string& db_name,
    DBOptions* db_opts, std::vector<ColumnFamilyDescriptor>* cfds) {
  assert(db_opts);
  assert(cfds);

  if (!db_opts->plugins.empty()) {
    Status s = CheckOpenMode(open_mode, db_opts->plugins);
    if (!s.ok()) {
      return s;
    }
    for (auto p : db_opts->plugins) {
      s = p->SanitizeCB(open_mode, db_name, db_opts, cfds);
      if (!s.ok()) {
        return s;
      }
    }
  }
  return Status::OK();
}

Status DBPlugin::ValidateCB(
    OpenMode mode, const std::string& /*db_name*/,
    const DBOptions& /*db_options*/,
    const std::vector<ColumnFamilyDescriptor>& /*cfds*/) const {
  if (SupportsOpenMode(mode)) {
    return Status::OK();
  } else {
    return UnsupportedMode(mode, Name());
  }
}

static Status ValidateOptionsByTable(
    const DBOptions& db_opts,
    const std::vector<ColumnFamilyDescriptor>& column_families) {
  Status s;
  for (auto cf : column_families) {
    s = ValidateOptions(db_opts, cf.options);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

// Validate self-consistency of DB options and its consistency with cf options
Status DBPlugin::ValidateOptionsForDB(
    DBPlugin::OpenMode open_mode, const std::string& db_name,
    const DBOptions& db_opts, const std::vector<ColumnFamilyDescriptor>& cfds) {
  Status s = ValidateOptionsByTable(db_opts, cfds);
  if (s.ok()) {
    for (const auto p : db_opts.plugins) {
      if (!p->SupportsOpenMode(open_mode)) {
        s = UnsupportedMode(open_mode, p->Name());
      } else {
        s = p->ValidateCB(open_mode, db_name, db_opts, cfds);
      }
      if (!s.ok()) {
        return s;
      }
    }
  }
  if (!s.ok()) {
    return s;
  }
  for (auto& cfd : cfds) {
    s = ColumnFamilyData::ValidateOptions(db_opts, cfd.options);
    if (!s.ok()) {
      return s;
    }
  }
  return DBImpl::ValidateOptions(db_opts);
}

Status DBPlugin::OpenCB(OpenMode mode, DB* db,
                        const std::vector<ColumnFamilyHandle*>& /*handles*/,
                        DB** wrapped) {
  if (SupportsOpenMode(mode)) {
    *wrapped = db;
    return Status::OK();
  } else {
    return UnsupportedMode(mode, Name());
  }
}

Status DBPlugin::Open(DBPlugin::OpenMode open_mode, DB* db,
                      const std::vector<ColumnFamilyHandle*>& handles,
                      DB** wrapped) {
  *wrapped = db;
  DBOptions db_opts = db->GetDBOptions();
  Status s = CheckOpenMode(open_mode, db_opts.plugins);
  if (s.ok()) {
    for (const auto p : db_opts.plugins) {
      s = p->OpenCB(open_mode, *wrapped, handles, wrapped);
      if (!s.ok()) {
        return s;
      }
    }
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
