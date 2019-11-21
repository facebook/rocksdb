// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/column_family.h"
#include "rocksdb/status.h"

namespace rocksdb {

// An object that allows for dynamical db path selection for
// functions that need to decide which db_path to flush an sst file to.
class DbPathSupplier {
 public:
  explicit DbPathSupplier(const ImmutableCFOptions& ioptions):
    env(ioptions.env), cf_paths_(ioptions.cf_paths) {}

  virtual ~DbPathSupplier() = default;

  Status FsyncDbPath(uint32_t path_id) const;

  virtual uint32_t GetPathId(int level) const = 0;

  // Is the given path_id an acceptable path_id
  // for this supplier?
  //
  // This method is used in compaction to decide
  // if it is feasible to only change the level
  // of an sst file without actually moving the
  // data (if it's a trivial move).
  //
  // For a random path supplier, for example,
  // it doesn't matter which path_id is given
  // because path_ids are chosen randomly anyway.
  // For a fix path supplier, however, the given
  // path_id really needs to match the fixed
  // path_id in order for us to say it's trivial.
  virtual bool AcceptPathId(
      uint32_t path_id, int output_level) const = 0;

 protected:
  Env* env;
  std::vector<DbPath> cf_paths_;
};

class FixedDbPathSupplier: public DbPathSupplier {
 public:
  FixedDbPathSupplier(const ImmutableCFOptions& ioptions, uint32_t path_id)
    : DbPathSupplier(ioptions), path_id_(path_id) {}

  uint32_t GetPathId(int /* level */) const override {
    return path_id_;
  }

  bool AcceptPathId(
      uint32_t path_id, int /* output_level */) const override {
    return path_id == path_id_;
  }

 private:
  uint32_t path_id_;
};

class RandomDbPathSupplier: public DbPathSupplier {
 public:
  RandomDbPathSupplier(const ImmutableCFOptions& ioptions)
    : DbPathSupplier(ioptions) {}

  uint32_t GetPathId(int level) const override;

  bool AcceptPathId(
      uint32_t path_id, int output_level) const override;
};

class LeveledTargetSizeDbPathSupplier: public DbPathSupplier {
 public:
  LeveledTargetSizeDbPathSupplier(
      const ImmutableCFOptions& ioptions,
      const MutableCFOptions& moptions);

  uint32_t GetPathId(int level) const override;

  bool AcceptPathId(
      uint32_t path_id, int output_level) const override;

 private:
  const bool level_compaction_dynamic_level_bytes;
  const double max_bytes_for_level_multiplier;
  const uint64_t max_bytes_for_level_base;
  const std::vector<int> max_bytes_for_level_multiplier_additional;
};

class UniversalTargetSizeDbPathSupplier: public DbPathSupplier {
 public:
  UniversalTargetSizeDbPathSupplier(
      const ImmutableCFOptions& ioptions,
      const MutableCFOptions& moptions,
      uint64_t file_size);

  uint32_t GetPathId(int level) const override;

  bool AcceptPathId(
      uint32_t path_id, int output_level) const override;

 private:
  const uint64_t file_size_;
  const unsigned int size_ratio;
};

enum DbPathSupplierCallSite {
  kDbPathSupplierFactoryCallSiteFromFlush,
  kDbPathSupplierFactoryCallSiteFromAutoCompaction,
  kDbPathSupplierFactoryCallSiteFromManualCompaction,
};

struct DbPathSupplierContext {
  DbPathSupplierCallSite call_site;
  const ImmutableCFOptions& ioptions;
  const MutableCFOptions& moptions;
  uint64_t estimated_file_size;
  uint32_t manual_compaction_specified_path_id;
};

class DbPathSupplierFactory {
 public:
  DbPathSupplierFactory() = default;
  virtual ~DbPathSupplierFactory() = default;

  virtual std::unique_ptr<DbPathSupplier> CreateDbPathSupplier(
      const DbPathSupplierContext& ctx);

  // Sanity check for cf_paths and db_paths
  // in options passed by the user. Different
  // factories may have different requirements
  // for the paths.
  virtual Status CfPathsSanityCheck(
      const ColumnFamilyOptions& cf_options,
      const DBOptions& db_options);

  virtual const char* Name() = 0;
};

// Fill DbPaths according to the target size set with the db path.
//
// Newer data is placed into paths specified earlier in the vector while
// older data gradually moves to paths specified later in the vector.
//
// For example, you have a flash device with 10GB allocated for the DB,
// as well as a hard drive of 2TB, you should config it to be:
//   [{"/flash_path", 10GB}, {"/hard_drive", 2TB}]
//
// The system will try to guarantee data under each path is close to but
// not larger than the target size. But current and future file sizes used
// by determining where to place a file are based on best-effort estimation,
// which means there is a chance that the actual size under the directory
// is slightly more than target size under some workloads. User should give
// some buffer room for those cases.
//
// If none of the paths has sufficient room to place a file, the file will
// be placed to the last path anyway, despite to the target size.
//
// Placing newer data to earlier paths is also best-efforts. User should
// expect user files to be placed in higher levels in some extreme cases.
class GradualMoveOldDataDbPathSupplierFactory: public DbPathSupplierFactory {
 public:
  GradualMoveOldDataDbPathSupplierFactory() = default;

  std::unique_ptr<DbPathSupplier> CreateDbPathSupplier(
      const DbPathSupplierContext& ctx) override;

  Status CfPathsSanityCheck(
      const ColumnFamilyOptions& cf_options,
      const DBOptions& db_options) override {
    return CfPathsSanityCheckStatic(cf_options, db_options);
  }

  // We need the sanity check function to be static here because
  // there's a place where the sanity check is called before a
  // factory instance is set in the options. Since this class
  // is the default factory, we'll fall back to this static check
  // when the factory is not set yet.
  static Status CfPathsSanityCheckStatic(
      const ColumnFamilyOptions& cf_options,
      const DBOptions& db_options);

  const char* Name() override {
    return "GradualMoveOldDataDbPathSupplierFactory";
  }
};

// Randomly distribute files into the list of db paths.
//
// For example, you have a few data drives on your host that are mounted
// as [/sdb1, /sdc1, /sdd1, /sde1]. Say that the database will create 6
// table files -- 0[0-5].sst, then they will end up on in these places:
//
// /sdb1/02.sst
// /sdb1/04.sst
// /sdc1/05.sst
// /sdc1/03.sst
// /sdd1/00.sst
// /sde1/01.sst
//
// This is useful if you want the database to evenly use a set of disks
// mounted on your host.
//
// Note that the target_size attr in DbPath will not be useful if this
// strategy is chosen.
class RandomDbPathSupplierFactory: public DbPathSupplierFactory {
 public:
  RandomDbPathSupplierFactory() = default;

  std::unique_ptr<DbPathSupplier> CreateDbPathSupplier(
      const DbPathSupplierContext& ctx) override;

  Status CfPathsSanityCheck(
      const ColumnFamilyOptions& cf_options,
      const DBOptions& db_options) override;

  const char* Name() override { return "RandomDbPathSupplierFactory"; }
};

// Static factory methods
extern DbPathSupplierFactory* NewGradualMoveOldDataDbPathSupplierFactory();
extern DbPathSupplierFactory* NewRandomDbPathSupplierFactory();

}
