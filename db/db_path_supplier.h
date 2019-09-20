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

// An object vendored by column family to dynamically supply db path to
// functions that need to decide which db_path to flush an sst file to. The
// supplier object is mutable (because you can add file size to it) and
// can update global file counters, so usage should be inside proper locking.
class DbPathSupplier {
 public:
  explicit DbPathSupplier(const ImmutableCFOptions& ioptions):
    cf_paths_(ioptions.cf_paths) {}

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

}
