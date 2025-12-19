//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/option_change_migration.h"

#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE {
namespace {
// Return a version of Options `opts` that allow us to open/write into a DB
// without triggering an automatic compaction or stalling. This is guaranteed
// by disabling automatic compactions and using huge values for stalling
// triggers.
Options GetNoCompactionOptions(const Options& opts) {
  Options ret_opts = opts;
  ret_opts.disable_auto_compactions = true;
  ret_opts.level0_slowdown_writes_trigger = 999999;
  ret_opts.level0_stop_writes_trigger = 999999;
  ret_opts.soft_pending_compaction_bytes_limit = 0;
  ret_opts.hard_pending_compaction_bytes_limit = 0;
  return ret_opts;
}

// Compact a specific CF to a specific level
//  cf_handle should not be null
Status CompactToLevel(DB* db, ColumnFamilyHandle* cf_handle, int dest_level) {
  assert(cf_handle != nullptr);

  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = dest_level;

  if (dest_level == 0) {
    // cannot use kForceOptimized because the compaction is expected to
    // generate one output file so to force the full compaction to skip trivial
    // move to L0
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  }

  return db->CompactRange(cro, cf_handle, nullptr, nullptr);
}

Status MigrateToUniversal(DB* db, ColumnFamilyHandle* cf_handle,
                          int old_num_levels, int new_num_levels) {
  assert(cf_handle != nullptr);

  if (old_num_levels <= new_num_levels) {
    return Status::OK();
  }

  // Check if compaction is needed
  ColumnFamilyMetaData metadata;
  db->GetColumnFamilyMetaData(cf_handle, &metadata);

  if (!metadata.levels.empty() &&
      metadata.levels.back().level >= new_num_levels) {
    // Need to compact to fit new num_levels
    return CompactToLevel(db, cf_handle, new_num_levels - 1);
  }

  return Status::OK();
}

Status MigrateToLevelBase(DB* db, ColumnFamilyHandle* cf_handle,
                          int old_num_levels, int new_num_levels,
                          bool dynamic_level_bytes) {
  assert(cf_handle != nullptr);

  if (!dynamic_level_bytes) {
    // Non-dynamic level mode
    if (old_num_levels == 1) {
      return Status::OK();
    }
    // Compact to L1
    return CompactToLevel(db, cf_handle, 1);

  } else {
    // Dynamic level mode
    if (old_num_levels == 1) {
      return Status::OK();
    }
    // Compact to last level
    return CompactToLevel(db, cf_handle, new_num_levels - 1);
  }
}

Status MigrateToFIFO(DB* db, ColumnFamilyHandle* cf_handle) {
  assert(cf_handle != nullptr);
  return CompactToLevel(db, cf_handle, 0);
}

Status MigrateSingleColumnFamily(DB* db, ColumnFamilyHandle* cf_handle,
                                 const Options& old_opts,
                                 const Options& new_opts) {
  assert(cf_handle != nullptr);

  if (old_opts.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    return Status::OK();
  }

  if (new_opts.compaction_style == CompactionStyle::kCompactionStyleUniversal) {
    return MigrateToUniversal(db, cf_handle, old_opts.num_levels,
                              new_opts.num_levels);
  } else if (new_opts.compaction_style ==
             CompactionStyle::kCompactionStyleLevel) {
    return MigrateToLevelBase(db, cf_handle, old_opts.num_levels,
                              new_opts.num_levels,
                              new_opts.level_compaction_dynamic_level_bytes);
  } else if (new_opts.compaction_style ==
             CompactionStyle::kCompactionStyleFIFO) {
    return MigrateToFIFO(db, cf_handle);
  }

  return Status::NotSupported(
      "Do not know how to migrate to this compaction style");
}

Status ValidateCFDescriptors(
    const std::vector<ColumnFamilyDescriptor>& old_cf_descs,
    const std::vector<ColumnFamilyDescriptor>& new_cf_descs) {
  if (old_cf_descs.size() != new_cf_descs.size()) {
    return Status::InvalidArgument(
        "old_cf_descs and new_cf_descs must have the same number of column "
        "families. Got " +
        std::to_string(old_cf_descs.size()) + " old CFs and " +
        std::to_string(new_cf_descs.size()) +
        " new CFs. Adding or dropping CFs is not supported.");
  }

  for (size_t i = 0; i < old_cf_descs.size(); ++i) {
    if (old_cf_descs[i].name != new_cf_descs[i].name) {
      return Status::InvalidArgument(
          "Column family mismatch at index " + std::to_string(i) + ": " +
          "old has '" + old_cf_descs[i].name + "', " + "new has '" +
          new_cf_descs[i].name + "'. CF names and order must match exactly.");
    }
  }

  return Status::OK();
}

struct BaseOptionsResult {
  ColumnFamilyOptions base_opts;
  bool need_reopen = true;
};

BaseOptionsResult DetermineBaseOptions(const ColumnFamilyOptions& old_opts,
                                       const ColumnFamilyOptions& new_opts) {
  BaseOptionsResult result;

  if (new_opts.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    if (!new_opts.level_compaction_dynamic_level_bytes) {
      result.base_opts = old_opts;
      result.base_opts.target_file_size_base = new_opts.target_file_size_base;
    } else {
      if (new_opts.num_levels > old_opts.num_levels) {
        result.base_opts = new_opts;
        result.need_reopen = false;
      } else {
        result.base_opts = old_opts;
        result.base_opts.target_file_size_base = new_opts.target_file_size_base;
      }
    }
  } else {
    result.base_opts = old_opts;
  }

  return result;
}

void ApplySpecialSingleLevelSettings(const ColumnFamilyOptions& new_opts,
                                     ColumnFamilyOptions* base_opts) {
  if (((new_opts.compaction_style ==
            CompactionStyle::kCompactionStyleUniversal ||
        new_opts.compaction_style == CompactionStyle::kCompactionStyleLevel) &&
       new_opts.num_levels == 1) ||
      new_opts.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    base_opts->target_file_size_base = 999999999999999;
    base_opts->max_compaction_bytes = 999999999999999;
  }
}

std::vector<ColumnFamilyDescriptor> PrepareNoCompactionCFDescriptors(
    const DBOptions& old_db_opts,
    const std::vector<ColumnFamilyDescriptor>& old_cf_descs,
    const std::vector<ColumnFamilyDescriptor>& new_cf_descs,
    bool* any_need_reopen) {
  assert(old_cf_descs.size() == new_cf_descs.size());

  std::vector<ColumnFamilyDescriptor> no_compact_cf_descs;
  *any_need_reopen = false;

  for (size_t i = 0; i < old_cf_descs.size(); ++i) {
    const std::string& cf_name = old_cf_descs[i].name;
    const ColumnFamilyOptions& old_opts = old_cf_descs[i].options;
    const ColumnFamilyOptions& new_opts = new_cf_descs[i].options;

    BaseOptionsResult result = DetermineBaseOptions(old_opts, new_opts);
    ColumnFamilyOptions base_opts = result.base_opts;

    if (result.need_reopen) {
      *any_need_reopen = true;
    }

    ApplySpecialSingleLevelSettings(new_opts, &base_opts);

    Options tmp_opts(old_db_opts, base_opts);
    Options no_compact_opts = GetNoCompactionOptions(tmp_opts);

    no_compact_cf_descs.emplace_back(cf_name,
                                     ColumnFamilyOptions(no_compact_opts));
  }

  return no_compact_cf_descs;
}

Status OpenDBWithCFs(const DBOptions& db_opts, const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& cf_descs,
                     std::unique_ptr<DB>* db,
                     std::vector<ColumnFamilyHandle*>* handles) {
  handles->clear();
  DB* tmpdb;
  Status s = DB::Open(db_opts, dbname, cf_descs, handles, &tmpdb);

  if (s.ok()) {
    db->reset(tmpdb);
  } else {
    for (auto* handle : *handles) {
      delete handle;
    }
    handles->clear();
  }

  return s;
}

Status CleanupCFHandles(DB* db, std::vector<ColumnFamilyHandle*>* handles) {
  Status s;
  for (auto* handle : *handles) {
    if (handle != db->DefaultColumnFamily()) {
      Status destroy_status = db->DestroyColumnFamilyHandle(handle);
      if (!destroy_status.ok() && s.ok()) {
        s = destroy_status;
      }
    }
  }
  handles->clear();
  return s;
}

Status MigrateAllCFs(DB* db, const std::vector<ColumnFamilyHandle*>& handles,
                     const DBOptions& old_db_opts, const DBOptions& new_db_opts,
                     const std::vector<ColumnFamilyDescriptor>& old_cf_descs,
                     const std::vector<ColumnFamilyDescriptor>& new_cf_descs) {
  assert(handles.size() == old_cf_descs.size());
  assert(old_cf_descs.size() == new_cf_descs.size());

  for (size_t i = 0; i < handles.size(); ++i) {
    const ColumnFamilyOptions& old_cf_opts = old_cf_descs[i].options;
    const ColumnFamilyOptions& new_cf_opts = new_cf_descs[i].options;

    Options old_opts(old_db_opts, old_cf_opts);
    Options new_opts(new_db_opts, new_cf_opts);

    Status s = MigrateSingleColumnFamily(db, handles[i], old_opts, new_opts);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}
}  // namespace

Status OptionChangeMigration(
    const std::string& dbname, const DBOptions& old_db_opts,
    const std::vector<ColumnFamilyDescriptor>& old_cf_descs,
    const DBOptions& new_db_opts,
    const std::vector<ColumnFamilyDescriptor>& new_cf_descs) {
  // Step 1: Validate that old and new have same CFs in same order
  Status s = ValidateCFDescriptors(old_cf_descs, new_cf_descs);
  if (!s.ok()) {
    return s;
  }

  // Step 2: Prepare no-compaction CF descriptors
  bool any_need_reopen = false;
  std::vector<ColumnFamilyDescriptor> no_compact_cf_descs =
      PrepareNoCompactionCFDescriptors(old_db_opts, old_cf_descs, new_cf_descs,
                                       &any_need_reopen);

  // Step 3: Open DB with all CFs
  std::unique_ptr<DB> db;
  std::vector<ColumnFamilyHandle*> handles;
  s = OpenDBWithCFs(old_db_opts, dbname, no_compact_cf_descs, &db, &handles);
  if (!s.ok()) {
    return s;
  }
  assert(db != nullptr);

  // Step 4: Migrate all CFs
  s = MigrateAllCFs(db.get(), handles, old_db_opts, new_db_opts, old_cf_descs,
                    new_cf_descs);

  // Step 5: Cleanup CF handles
  Status cleanup_status = CleanupCFHandles(db.get(), &handles);
  if (s.ok() && !cleanup_status.ok()) {
    s = cleanup_status;
  }

  // Step 6: Close and reopen DB if needed to rewrite manifest
  if (s.ok() && any_need_reopen) {
    Status close_status = db->Close();
    if (!close_status.ok()) {
      return close_status;
    }
    db.reset();

    s = OpenDBWithCFs(old_db_opts, dbname, no_compact_cf_descs, &db, &handles);
    if (!s.ok()) {
      return s;
    }

    // Cleanup CF handles before final close
    cleanup_status = CleanupCFHandles(db.get(), &handles);
    if (!cleanup_status.ok() && s.ok()) {
      s = cleanup_status;
    }
  }

  // Final step: Close DB (either after reopening or without reopening)
  Status close_status = db->Close();
  if (!close_status.ok() && s.ok()) {
    s = close_status;
  }

  db.reset();

  return s;
}

Status OptionChangeMigration(const std::string& dbname, const Options& old_opts,
                             const Options& new_opts) {
  DBOptions old_db_opts(old_opts);
  DBOptions new_db_opts(new_opts);

  ColumnFamilyOptions old_cf_opts(old_opts);
  ColumnFamilyOptions new_cf_opts(new_opts);

  std::vector<ColumnFamilyDescriptor> old_cf_descs = {
      {kDefaultColumnFamilyName, old_cf_opts}};

  std::vector<ColumnFamilyDescriptor> new_cf_descs = {
      {kDefaultColumnFamilyName, new_cf_opts}};

  return OptionChangeMigration(dbname, old_db_opts, old_cf_descs, new_db_opts,
                               new_cf_descs);
}
}  // namespace ROCKSDB_NAMESPACE
