
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
  DbPathSupplier(const ImmutableCFOptions& ioptions): ioptions_(ioptions) {}

  virtual ~DbPathSupplier() = default;

  virtual uint32_t GetPathId(int level) const {
    return 0;
  }

  virtual Status FsyncDbPath(uint32_t path_id) const;

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
      uint32_t path_id, int output_level) const {
    return false;
  }

 protected:
  const ImmutableCFOptions ioptions_;
};

class FixedDbPathSupplier: public DbPathSupplier {
 public:
  FixedDbPathSupplier(const ImmutableCFOptions& ioptions, uint32_t path_id)
    : DbPathSupplier(ioptions), path_id_(path_id) {}

  uint32_t GetPathId(int level) const override {
    return path_id_;
  }

  bool AcceptPathId(
      uint32_t path_id, int output_level) const override {
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
      const MutableCFOptions& moptions)
    : DbPathSupplier(ioptions), moptions_(moptions) {}

  uint32_t GetPathId(int level) const override;

  bool AcceptPathId(
      uint32_t path_id, int output_level) const override;

private:
  const MutableCFOptions moptions_;
};

class UniversalTargetSizeDbPathSupplier: public DbPathSupplier {
 public:
  UniversalTargetSizeDbPathSupplier(
      const ImmutableCFOptions& ioptions,
      const MutableCFOptions& moptions,
      uint64_t file_size)
    : DbPathSupplier(ioptions), file_size_(file_size),
      moptions_(moptions) {}

  uint32_t GetPathId(int level) const override;

  bool AcceptPathId(
      uint32_t path_id, int output_level) const override;

 private:
  uint64_t file_size_;
  const MutableCFOptions moptions_;
};

}
