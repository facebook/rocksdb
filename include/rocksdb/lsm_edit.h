//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "rocksdb/advanced_options.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;

// EXPERIMENTAL
//
// One external SST file to install, together with the level it will occupy.
// See DB::ApplyLsmEdit().
struct LsmEditFile {
  // Path to the external SST file. The file is linked, moved or copied into
  // the DB directory according to LsmEditOptions.
  std::string path;

  // The level this file will occupy after the edit. RocksDB does not search
  // for a level: the file is installed here or the edit fails. Files declared
  // at the same level > 0 must not overlap each other, and must not overlap
  // any surviving file already at that level. Files declared at level 0 may
  // overlap; among those, later entries in LsmEdit::add_files are treated as
  // newer, matching the order L0 files are consulted on a read. When files at
  // different levels overlap, every entry affecting the same key in the
  // higher level must have a greater sequence number. ApplyLsmEdit validates
  // this ordering, including range tombstones. Sequence number zero is valid,
  // but cannot establish ordering between different levels.
  int level = 0;

  // Whole-file checksum of `path` and the name of the function that produced
  // it. Both must be non-empty or both empty, and an edit must supply them for
  // either every file or no files. See LsmEditOptions::verify_file_checksum
  // for how they are used.
  std::string checksum;
  std::string checksum_func_name;
};

// EXPERIMENTAL
//
// Options for DB::ApplyLsmEdit(), applying to every LsmEdit in the call.
struct LsmEditOptions {
  // Hard-link the files into the DB instead of copying them, falling back to a
  // copy when the filesystem cannot link across the two locations. The source
  // paths are removed on success. Cannot be combined with `link_files`.
  bool move_files = false;

  // Hard-link the files into the DB instead of copying them, leaving the
  // source paths in place. Appropriate when the files belong to a live DB that
  // the caller will drop separately. Cannot be combined with `move_files`.
  bool link_files = false;

  // Recompute each file's whole-file checksum and compare it against the
  // LsmEditFile::checksum supplied by the caller. Only meaningful when the DB
  // has a file_checksum_gen_factory and the caller supplied checksums.
  bool verify_file_checksum = true;

  // Flush any memtable holding keys inside an edit's range, so the edit is not
  // shadowed by unflushed writes. When false, such an edit fails rather than
  // stalling writes for a flush.
  bool allow_blocking_flush = true;

  // Populate the block cache with blocks read while installing the files.
  // Grafted data is usually cold at install time, so the default avoids
  // evicting live working-set blocks.
  bool fill_cache = false;

  // How to treat an existing file whose key range crosses an edit's range
  // boundary while lying partly outside it ("straddling"). Such a file cannot
  // simply be dropped, because the part outside the range must survive.
  //
  // When true, RocksDB probes the straddling file for keys inside the range.
  // The file is retained untouched if it holds none, so an edit whose range is
  // logically empty succeeds even though the range is not aligned to file
  // boundaries. Each probe costs roughly one seek.
  //
  // When false, any straddling file fails the edit, and it is the caller's
  // responsibility to align the range to file boundaries first (typically by
  // compacting it).
  bool probe_straddling_files = true;

  // Temperature hint for reading the files being installed.
  Temperature file_temperature = Temperature::kUnknown;

  // Max threads used to open the installed files while committing.
  int file_opening_threads = 16;
};

// EXPERIMENTAL
//
// The edit to one column family: after it is applied, `range` contains exactly
// `add_files`, placed at the levels those files declare. See
// DB::ApplyLsmEdit().
struct LsmEdit {
  ColumnFamilyHandle* column_family = nullptr;

  // The key range this edit owns, half-open: `start` inclusive, `limit`
  // exclusive. Every file in `add_files` must lie inside it. Leaving both
  // endpoints unset means the whole column family; setting only one is not
  // supported.
  RangeOpt range;

  // The files that will occupy `range`. Must not be empty; an edit that only
  // removes data is not expressible yet.
  std::vector<LsmEditFile> add_files;
};

}  // namespace ROCKSDB_NAMESPACE
