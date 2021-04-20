//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_builder.h"

#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/blob/blob_file_meta.h"
#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "port/port.h"
#include "table/table_reader.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

bool NewestFirstBySeqNo(FileMetaData* a, FileMetaData* b) {
  if (a->fd.largest_seqno != b->fd.largest_seqno) {
    return a->fd.largest_seqno > b->fd.largest_seqno;
  }
  if (a->fd.smallest_seqno != b->fd.smallest_seqno) {
    return a->fd.smallest_seqno > b->fd.smallest_seqno;
  }
  // Break ties by file number
  return a->fd.GetNumber() > b->fd.GetNumber();
}

namespace {
bool BySmallestKey(FileMetaData* a, FileMetaData* b,
                   const InternalKeyComparator* cmp) {
  int r = cmp->Compare(a->smallest, b->smallest);
  if (r != 0) {
    return (r < 0);
  }
  // Break ties by file number
  return (a->fd.GetNumber() < b->fd.GetNumber());
}
}  // namespace

class VersionBuilder::Rep {
 private:
  // Helper to sort files_ in v
  // kLevel0 -- NewestFirstBySeqNo
  // kLevelNon0 -- BySmallestKey
  struct FileComparator {
    enum SortMethod { kLevel0 = 0, kLevelNon0 = 1, } sort_method;
    const InternalKeyComparator* internal_comparator;

    FileComparator() : internal_comparator(nullptr) {}

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      switch (sort_method) {
        case kLevel0:
          return NewestFirstBySeqNo(f1, f2);
        case kLevelNon0:
          return BySmallestKey(f1, f2, internal_comparator);
      }
      assert(false);
      return false;
    }
  };

  struct LevelState {
    std::unordered_set<uint64_t> deleted_files;
    // Map from file number to file meta data.
    std::unordered_map<uint64_t, FileMetaData*> added_files;
  };

  class BlobFileMetaDataDelta {
   public:
    bool IsEmpty() const {
      return !shared_meta_ && !additional_garbage_count_ &&
             !additional_garbage_bytes_ && newly_linked_ssts_.empty() &&
             newly_unlinked_ssts_.empty();
    }

    std::shared_ptr<SharedBlobFileMetaData> GetSharedMeta() const {
      return shared_meta_;
    }

    uint64_t GetAdditionalGarbageCount() const {
      return additional_garbage_count_;
    }

    uint64_t GetAdditionalGarbageBytes() const {
      return additional_garbage_bytes_;
    }

    const std::unordered_set<uint64_t>& GetNewlyLinkedSsts() const {
      return newly_linked_ssts_;
    }

    const std::unordered_set<uint64_t>& GetNewlyUnlinkedSsts() const {
      return newly_unlinked_ssts_;
    }

    void SetSharedMeta(std::shared_ptr<SharedBlobFileMetaData> shared_meta) {
      assert(!shared_meta_);
      assert(shared_meta);

      shared_meta_ = std::move(shared_meta);
    }

    void AddGarbage(uint64_t count, uint64_t bytes) {
      additional_garbage_count_ += count;
      additional_garbage_bytes_ += bytes;
    }

    void LinkSst(uint64_t sst_file_number) {
      assert(newly_linked_ssts_.find(sst_file_number) ==
             newly_linked_ssts_.end());

      // Reconcile with newly unlinked SSTs on the fly. (Note: an SST can be
      // linked to and unlinked from the same blob file in the case of a trivial
      // move.)
      auto it = newly_unlinked_ssts_.find(sst_file_number);

      if (it != newly_unlinked_ssts_.end()) {
        newly_unlinked_ssts_.erase(it);
      } else {
        newly_linked_ssts_.emplace(sst_file_number);
      }
    }

    void UnlinkSst(uint64_t sst_file_number) {
      assert(newly_unlinked_ssts_.find(sst_file_number) ==
             newly_unlinked_ssts_.end());

      // Reconcile with newly linked SSTs on the fly. (Note: an SST can be
      // linked to and unlinked from the same blob file in the case of a trivial
      // move.)
      auto it = newly_linked_ssts_.find(sst_file_number);

      if (it != newly_linked_ssts_.end()) {
        newly_linked_ssts_.erase(it);
      } else {
        newly_unlinked_ssts_.emplace(sst_file_number);
      }
    }

   private:
    std::shared_ptr<SharedBlobFileMetaData> shared_meta_;
    uint64_t additional_garbage_count_ = 0;
    uint64_t additional_garbage_bytes_ = 0;
    std::unordered_set<uint64_t> newly_linked_ssts_;
    std::unordered_set<uint64_t> newly_unlinked_ssts_;
  };

  const FileOptions& file_options_;
  const ImmutableCFOptions* const ioptions_;
  TableCache* table_cache_;
  VersionStorageInfo* base_vstorage_;
  VersionSet* version_set_;
  int num_levels_;
  LevelState* levels_;
  // Store sizes of levels larger than num_levels_. We do this instead of
  // storing them in levels_ to avoid regression in case there are no files
  // on invalid levels. The version is not consistent if in the end the files
  // on invalid levels don't cancel out.
  std::unordered_map<int, size_t> invalid_level_sizes_;
  // Whether there are invalid new files or invalid deletion on levels larger
  // than num_levels_.
  bool has_invalid_levels_;
  // Current levels of table files affected by additions/deletions.
  std::unordered_map<uint64_t, int> table_file_levels_;
  FileComparator level_zero_cmp_;
  FileComparator level_nonzero_cmp_;

  // Metadata delta for all blob files affected by the series of version edits.
  std::map<uint64_t, BlobFileMetaDataDelta> blob_file_meta_deltas_;

 public:
  Rep(const FileOptions& file_options, const ImmutableCFOptions* ioptions,
      TableCache* table_cache, VersionStorageInfo* base_vstorage,
      VersionSet* version_set)
      : file_options_(file_options),
        ioptions_(ioptions),
        table_cache_(table_cache),
        base_vstorage_(base_vstorage),
        version_set_(version_set),
        num_levels_(base_vstorage->num_levels()),
        has_invalid_levels_(false) {
    assert(ioptions_);

    levels_ = new LevelState[num_levels_];
    level_zero_cmp_.sort_method = FileComparator::kLevel0;
    level_nonzero_cmp_.sort_method = FileComparator::kLevelNon0;
    level_nonzero_cmp_.internal_comparator =
        base_vstorage_->InternalComparator();
  }

  ~Rep() {
    for (int level = 0; level < num_levels_; level++) {
      const auto& added = levels_[level].added_files;
      for (auto& pair : added) {
        UnrefFile(pair.second);
      }
    }

    delete[] levels_;
  }

  void UnrefFile(FileMetaData* f) {
    f->refs--;
    if (f->refs <= 0) {
      if (f->table_reader_handle) {
        assert(table_cache_ != nullptr);
        table_cache_->ReleaseHandle(f->table_reader_handle);
        f->table_reader_handle = nullptr;
      }
      delete f;
    }
  }

  bool IsBlobFileInVersion(uint64_t blob_file_number) const {
    auto delta_it = blob_file_meta_deltas_.find(blob_file_number);
    if (delta_it != blob_file_meta_deltas_.end()) {
      if (delta_it->second.GetSharedMeta()) {
        return true;
      }
    }

    assert(base_vstorage_);

    const auto& base_blob_files = base_vstorage_->GetBlobFiles();

    auto base_it = base_blob_files.find(blob_file_number);
    if (base_it != base_blob_files.end()) {
      assert(base_it->second);
      assert(base_it->second->GetSharedMeta());

      return true;
    }

    return false;
  }

  using ExpectedLinkedSsts =
      std::unordered_map<uint64_t, BlobFileMetaData::LinkedSsts>;

  static void UpdateExpectedLinkedSsts(
      uint64_t table_file_number, uint64_t blob_file_number,
      ExpectedLinkedSsts* expected_linked_ssts) {
    assert(expected_linked_ssts);

    if (blob_file_number == kInvalidBlobFileNumber) {
      return;
    }

    (*expected_linked_ssts)[blob_file_number].emplace(table_file_number);
  }

  Status CheckConsistencyDetails(VersionStorageInfo* vstorage) {
    // Make sure the files are sorted correctly and that the links between
    // table files and blob files are consistent. The latter is checked using
    // the following mapping, which is built using the forward links
    // (table file -> blob file), and is subsequently compared with the inverse
    // mapping stored in the BlobFileMetaData objects.
    ExpectedLinkedSsts expected_linked_ssts;

    for (int level = 0; level < num_levels_; level++) {
      auto& level_files = vstorage->LevelFiles(level);

      if (level_files.empty()) {
        continue;
      }

      assert(level_files[0]);
      UpdateExpectedLinkedSsts(level_files[0]->fd.GetNumber(),
                               level_files[0]->oldest_blob_file_number,
                               &expected_linked_ssts);
      for (size_t i = 1; i < level_files.size(); i++) {
        assert(level_files[i]);
        UpdateExpectedLinkedSsts(level_files[i]->fd.GetNumber(),
                                 level_files[i]->oldest_blob_file_number,
                                 &expected_linked_ssts);

        auto f1 = level_files[i - 1];
        auto f2 = level_files[i];
        if (level == 0) {
#ifndef NDEBUG
          auto pair = std::make_pair(&f1, &f2);
          TEST_SYNC_POINT_CALLBACK("VersionBuilder::CheckConsistency0", &pair);
#endif
          if (!level_zero_cmp_(f1, f2)) {
            return Status::Corruption("L0 files are not sorted properly");
          }

          if (f2->fd.smallest_seqno == f2->fd.largest_seqno) {
            // This is an external file that we ingested
            SequenceNumber external_file_seqno = f2->fd.smallest_seqno;
            if (!(external_file_seqno < f1->fd.largest_seqno ||
                  external_file_seqno == 0)) {
              return Status::Corruption(
                  "L0 file with seqno " +
                  NumberToString(f1->fd.smallest_seqno) + " " +
                  NumberToString(f1->fd.largest_seqno) +
                  " vs. file with global_seqno" +
                  NumberToString(external_file_seqno) + " with fileNumber " +
                  NumberToString(f1->fd.GetNumber()));
            }
          } else if (f1->fd.smallest_seqno <= f2->fd.smallest_seqno) {
            return Status::Corruption(
                "L0 files seqno " + NumberToString(f1->fd.smallest_seqno) +
                " " + NumberToString(f1->fd.largest_seqno) + " " +
                NumberToString(f1->fd.GetNumber()) + " vs. " +
                NumberToString(f2->fd.smallest_seqno) + " " +
                NumberToString(f2->fd.largest_seqno) + " " +
                NumberToString(f2->fd.GetNumber()));
          }
        } else {
#ifndef NDEBUG
          auto pair = std::make_pair(&f1, &f2);
          TEST_SYNC_POINT_CALLBACK("VersionBuilder::CheckConsistency1", &pair);
#endif
          if (!level_nonzero_cmp_(f1, f2)) {
            return Status::Corruption(
                "L" + NumberToString(level) +
                " files are not sorted properly: files #" +
                NumberToString(f1->fd.GetNumber()) + ", #" +
                NumberToString(f2->fd.GetNumber()));
          }

          // Make sure there is no overlap in levels > 0
          if (vstorage->InternalComparator()->Compare(f1->largest,
                                                      f2->smallest) >= 0) {
            return Status::Corruption(
                "L" + NumberToString(level) +
                " have overlapping ranges: file #" +
                NumberToString(f1->fd.GetNumber()) +
                " largest key: " + (f1->largest).DebugString(true) +
                " vs. file #" + NumberToString(f2->fd.GetNumber()) +
                " smallest key: " + (f2->smallest).DebugString(true));
          }
        }
      }
    }

    // Make sure that all blob files in the version have non-garbage data.
    const auto& blob_files = vstorage->GetBlobFiles();
    for (const auto& pair : blob_files) {
      const uint64_t blob_file_number = pair.first;
      const auto& blob_file_meta = pair.second;
      assert(blob_file_meta);

      if (blob_file_meta->GetGarbageBlobCount() >=
          blob_file_meta->GetTotalBlobCount()) {
        std::ostringstream oss;
        oss << "Blob file #" << blob_file_number
            << " consists entirely of garbage";

        return Status::Corruption("VersionBuilder", oss.str());
      }

      if (blob_file_meta->GetLinkedSsts() !=
          expected_linked_ssts[blob_file_number]) {
        std::ostringstream oss;
        oss << "Links are inconsistent between table files and blob file #"
            << blob_file_number;

        return Status::Corruption("VersionBuilder", oss.str());
      }
    }

    Status ret_s;
    TEST_SYNC_POINT_CALLBACK("VersionBuilder::CheckConsistencyBeforeReturn",
                             &ret_s);
    return ret_s;
  }

  Status CheckConsistency(VersionStorageInfo* vstorage) {
    // Always run consistency checks in debug build
#ifdef NDEBUG
    if (!vstorage->force_consistency_checks()) {
      return Status::OK();
    }
#endif
    Status s = CheckConsistencyDetails(vstorage);
    if (s.IsCorruption() && s.getState()) {
      // Make it clear the error is due to force_consistency_checks = 1 or
      // debug build
#ifdef NDEBUG
      auto prefix = "force_consistency_checks";
#else
      auto prefix = "force_consistency_checks(DEBUG)";
#endif
      s = Status::Corruption(prefix, s.getState());
    } else {
      // was only expecting corruption with message, or OK
      assert(s.ok());
    }
    return s;
  }

  bool CheckConsistencyForNumLevels() const {
    // Make sure there are no files on or beyond num_levels().
    if (has_invalid_levels_) {
      return false;
    }

    for (const auto& pair : invalid_level_sizes_) {
      const size_t level_size = pair.second;
      if (level_size != 0) {
        return false;
      }
    }

    return true;
  }

  Status ApplyBlobFileAddition(const BlobFileAddition& blob_file_addition) {
    const uint64_t blob_file_number = blob_file_addition.GetBlobFileNumber();

    if (IsBlobFileInVersion(blob_file_number)) {
      std::ostringstream oss;
      oss << "Blob file #" << blob_file_number << " already added";

      return Status::Corruption("VersionBuilder", oss.str());
    }

    // Note: we use C++11 for now but in C++14, this could be done in a more
    // elegant way using generalized lambda capture.
    VersionSet* const vs = version_set_;
    const ImmutableCFOptions* const ioptions = ioptions_;

    auto deleter = [vs, ioptions](SharedBlobFileMetaData* shared_meta) {
      if (vs) {
        assert(ioptions);
        assert(!ioptions->cf_paths.empty());
        assert(shared_meta);

        vs->AddObsoleteBlobFile(shared_meta->GetBlobFileNumber(),
                                ioptions->cf_paths.front().path);
      }

      delete shared_meta;
    };

    auto shared_meta = SharedBlobFileMetaData::Create(
        blob_file_number, blob_file_addition.GetTotalBlobCount(),
        blob_file_addition.GetTotalBlobBytes(),
        blob_file_addition.GetChecksumMethod(),
        blob_file_addition.GetChecksumValue(), deleter);

    blob_file_meta_deltas_[blob_file_number].SetSharedMeta(
        std::move(shared_meta));

    return Status::OK();
  }

  Status ApplyBlobFileGarbage(const BlobFileGarbage& blob_file_garbage) {
    const uint64_t blob_file_number = blob_file_garbage.GetBlobFileNumber();

    if (!IsBlobFileInVersion(blob_file_number)) {
      std::ostringstream oss;
      oss << "Blob file #" << blob_file_number << " not found";

      return Status::Corruption("VersionBuilder", oss.str());
    }

    blob_file_meta_deltas_[blob_file_number].AddGarbage(
        blob_file_garbage.GetGarbageBlobCount(),
        blob_file_garbage.GetGarbageBlobBytes());

    return Status::OK();
  }

  int GetCurrentLevelForTableFile(uint64_t file_number) const {
    auto it = table_file_levels_.find(file_number);
    if (it != table_file_levels_.end()) {
      return it->second;
    }

    assert(base_vstorage_);
    return base_vstorage_->GetFileLocation(file_number).GetLevel();
  }

  uint64_t GetOldestBlobFileNumberForTableFile(int level,
                                               uint64_t file_number) const {
    assert(level < num_levels_);

    const auto& added_files = levels_[level].added_files;

    auto it = added_files.find(file_number);
    if (it != added_files.end()) {
      const FileMetaData* const meta = it->second;
      assert(meta);

      return meta->oldest_blob_file_number;
    }

    assert(base_vstorage_);
    const FileMetaData* const meta =
        base_vstorage_->GetFileMetaDataByNumber(file_number);
    assert(meta);

    return meta->oldest_blob_file_number;
  }

  uint64_t GetMinOldestBlobFileNumber() const {
    uint64_t min_oldest_blob_file_num = std::numeric_limits<uint64_t>::max();
    for (int level = 0; level < num_levels_; ++level) {
      const auto& base_files = base_vstorage_->LevelFiles(level);
      for (const auto* fmeta : base_files) {
        assert(fmeta);
        min_oldest_blob_file_num =
            std::min(min_oldest_blob_file_num, fmeta->oldest_blob_file_number);
      }
      const auto& added_files = levels_[level].added_files;
      for (const auto& elem : added_files) {
        assert(elem.second);
        min_oldest_blob_file_num = std::min(
            min_oldest_blob_file_num, elem.second->oldest_blob_file_number);
      }
    }
    if (min_oldest_blob_file_num == std::numeric_limits<uint64_t>::max()) {
      min_oldest_blob_file_num = kInvalidBlobFileNumber;
    }
    return min_oldest_blob_file_num;
  }

  Status ApplyFileDeletion(int level, uint64_t file_number) {
    assert(level != VersionStorageInfo::FileLocation::Invalid().GetLevel());

    const int current_level = GetCurrentLevelForTableFile(file_number);

    if (level != current_level) {
      if (level >= num_levels_) {
        has_invalid_levels_ = true;
      }

      std::ostringstream oss;
      oss << "Cannot delete table file #" << file_number << " from level "
          << level << " since it is ";
      if (current_level ==
          VersionStorageInfo::FileLocation::Invalid().GetLevel()) {
        oss << "not in the LSM tree";
      } else {
        oss << "on level " << current_level;
      }

      return Status::Corruption("VersionBuilder", oss.str());
    }

    if (level >= num_levels_) {
      assert(invalid_level_sizes_[level] > 0);
      --invalid_level_sizes_[level];

      table_file_levels_[file_number] =
          VersionStorageInfo::FileLocation::Invalid().GetLevel();

      return Status::OK();
    }

    const uint64_t blob_file_number =
        GetOldestBlobFileNumberForTableFile(level, file_number);

    if (blob_file_number != kInvalidBlobFileNumber &&
        IsBlobFileInVersion(blob_file_number)) {
      blob_file_meta_deltas_[blob_file_number].UnlinkSst(file_number);
    }

    auto& level_state = levels_[level];

    auto& add_files = level_state.added_files;
    auto add_it = add_files.find(file_number);
    if (add_it != add_files.end()) {
      UnrefFile(add_it->second);
      add_files.erase(add_it);
    }

    auto& del_files = level_state.deleted_files;
    assert(del_files.find(file_number) == del_files.end());
    del_files.emplace(file_number);

    table_file_levels_[file_number] =
        VersionStorageInfo::FileLocation::Invalid().GetLevel();

    return Status::OK();
  }

  Status ApplyFileAddition(int level, const FileMetaData& meta) {
    assert(level != VersionStorageInfo::FileLocation::Invalid().GetLevel());

    const uint64_t file_number = meta.fd.GetNumber();

    const int current_level = GetCurrentLevelForTableFile(file_number);

    if (current_level !=
        VersionStorageInfo::FileLocation::Invalid().GetLevel()) {
      if (level >= num_levels_) {
        has_invalid_levels_ = true;
      }

      std::ostringstream oss;
      oss << "Cannot add table file #" << file_number << " to level " << level
          << " since it is already in the LSM tree on level " << current_level;
      return Status::Corruption("VersionBuilder", oss.str());
    }

    if (level >= num_levels_) {
      ++invalid_level_sizes_[level];
      table_file_levels_[file_number] = level;

      return Status::OK();
    }

    auto& level_state = levels_[level];

    auto& del_files = level_state.deleted_files;
    auto del_it = del_files.find(file_number);
    if (del_it != del_files.end()) {
      del_files.erase(del_it);
    }

    FileMetaData* const f = new FileMetaData(meta);
    f->refs = 1;

    auto& add_files = level_state.added_files;
    assert(add_files.find(file_number) == add_files.end());
    add_files.emplace(file_number, f);

    const uint64_t blob_file_number = f->oldest_blob_file_number;

    if (blob_file_number != kInvalidBlobFileNumber &&
        IsBlobFileInVersion(blob_file_number)) {
      blob_file_meta_deltas_[blob_file_number].LinkSst(file_number);
    }

    table_file_levels_[file_number] = level;

    return Status::OK();
  }

  // Apply all of the edits in *edit to the current state.
  Status Apply(VersionEdit* edit) {
    {
      const Status s = CheckConsistency(base_vstorage_);
      if (!s.ok()) {
        return s;
      }
    }

    // Note: we process the blob file related changes first because the
    // table file addition/deletion logic depends on the blob files
    // already being there.

    // Add new blob files
    for (const auto& blob_file_addition : edit->GetBlobFileAdditions()) {
      const Status s = ApplyBlobFileAddition(blob_file_addition);
      if (!s.ok()) {
        return s;
      }
    }

    // Increase the amount of garbage for blob files affected by GC
    for (const auto& blob_file_garbage : edit->GetBlobFileGarbages()) {
      const Status s = ApplyBlobFileGarbage(blob_file_garbage);
      if (!s.ok()) {
        return s;
      }
    }

    // Delete table files
    for (const auto& deleted_file : edit->GetDeletedFiles()) {
      const int level = deleted_file.first;
      const uint64_t file_number = deleted_file.second;

      const Status s = ApplyFileDeletion(level, file_number);
      if (!s.ok()) {
        return s;
      }
    }

    // Add new table files
    for (const auto& new_file : edit->GetNewFiles()) {
      const int level = new_file.first;
      const FileMetaData& meta = new_file.second;

      const Status s = ApplyFileAddition(level, meta);
      if (!s.ok()) {
        return s;
      }
    }

    return Status::OK();
  }

  static BlobFileMetaData::LinkedSsts ApplyLinkedSstChanges(
      const BlobFileMetaData::LinkedSsts& base,
      const std::unordered_set<uint64_t>& newly_linked,
      const std::unordered_set<uint64_t>& newly_unlinked) {
    BlobFileMetaData::LinkedSsts result(base);

    for (uint64_t sst_file_number : newly_unlinked) {
      assert(result.find(sst_file_number) != result.end());

      result.erase(sst_file_number);
    }

    for (uint64_t sst_file_number : newly_linked) {
      assert(result.find(sst_file_number) == result.end());

      result.emplace(sst_file_number);
    }

    return result;
  }

  static std::shared_ptr<BlobFileMetaData> CreateMetaDataForNewBlobFile(
      const BlobFileMetaDataDelta& delta) {
    auto shared_meta = delta.GetSharedMeta();
    assert(shared_meta);

    assert(delta.GetNewlyUnlinkedSsts().empty());

    auto meta = BlobFileMetaData::Create(
        std::move(shared_meta), delta.GetNewlyLinkedSsts(),
        delta.GetAdditionalGarbageCount(), delta.GetAdditionalGarbageBytes());

    return meta;
  }

  static std::shared_ptr<BlobFileMetaData>
  GetOrCreateMetaDataForExistingBlobFile(
      const std::shared_ptr<BlobFileMetaData>& base_meta,
      const BlobFileMetaDataDelta& delta) {
    assert(base_meta);
    assert(!delta.GetSharedMeta());

    if (delta.IsEmpty()) {
      return base_meta;
    }

    auto shared_meta = base_meta->GetSharedMeta();
    assert(shared_meta);

    auto linked_ssts = ApplyLinkedSstChanges(base_meta->GetLinkedSsts(),
                                             delta.GetNewlyLinkedSsts(),
                                             delta.GetNewlyUnlinkedSsts());

    auto meta = BlobFileMetaData::Create(
        std::move(shared_meta), std::move(linked_ssts),
        base_meta->GetGarbageBlobCount() + delta.GetAdditionalGarbageCount(),
        base_meta->GetGarbageBlobBytes() + delta.GetAdditionalGarbageBytes());

    return meta;
  }

  // Add the blob file specified by meta to *vstorage if it is determined to
  // contain valid data (blobs). We make this decision based on the amount
  // of garbage in the file, and whether the file or any lower-numbered blob
  // files have any linked SSTs. The latter condition is tracked using the
  // flag *found_first_non_empty.
  void AddBlobFileIfNeeded(VersionStorageInfo* vstorage,
                           const std::shared_ptr<BlobFileMetaData>& meta,
                           bool* found_first_non_empty) const {
    assert(vstorage);
    assert(meta);
    assert(found_first_non_empty);

    if (!meta->GetLinkedSsts().empty()) {
      (*found_first_non_empty) = true;
    } else if (!(*found_first_non_empty) ||
               meta->GetGarbageBlobCount() >= meta->GetTotalBlobCount()) {
      return;
    }

    vstorage->AddBlobFile(meta);
  }

  // Merge the blob file metadata from the base version with the changes (edits)
  // applied, and save the result into *vstorage.
  void SaveBlobFilesTo(VersionStorageInfo* vstorage) const {
    assert(base_vstorage_);
    assert(vstorage);

    bool found_first_non_empty = false;

    const auto& base_blob_files = base_vstorage_->GetBlobFiles();
    auto base_it = base_blob_files.begin();
    const auto base_it_end = base_blob_files.end();

    auto delta_it = blob_file_meta_deltas_.begin();
    const auto delta_it_end = blob_file_meta_deltas_.end();

    while (base_it != base_it_end && delta_it != delta_it_end) {
      const uint64_t base_blob_file_number = base_it->first;
      const uint64_t delta_blob_file_number = delta_it->first;

      if (base_blob_file_number < delta_blob_file_number) {
        const auto& base_meta = base_it->second;

        AddBlobFileIfNeeded(vstorage, base_meta, &found_first_non_empty);

        ++base_it;
      } else if (delta_blob_file_number < base_blob_file_number) {
        const auto& delta = delta_it->second;

        auto meta = CreateMetaDataForNewBlobFile(delta);

        AddBlobFileIfNeeded(vstorage, meta, &found_first_non_empty);

        ++delta_it;
      } else {
        assert(base_blob_file_number == delta_blob_file_number);

        const auto& base_meta = base_it->second;
        const auto& delta = delta_it->second;

        auto meta = GetOrCreateMetaDataForExistingBlobFile(base_meta, delta);

        AddBlobFileIfNeeded(vstorage, meta, &found_first_non_empty);

        ++base_it;
        ++delta_it;
      }
    }

    while (base_it != base_it_end) {
      const auto& base_meta = base_it->second;

      AddBlobFileIfNeeded(vstorage, base_meta, &found_first_non_empty);

      ++base_it;
    }

    while (delta_it != delta_it_end) {
      const auto& delta = delta_it->second;

      auto meta = CreateMetaDataForNewBlobFile(delta);

      AddBlobFileIfNeeded(vstorage, meta, &found_first_non_empty);

      ++delta_it;
    }
  }

  // Save the current state in *vstorage.
  Status SaveTo(VersionStorageInfo* vstorage) {
    Status s = CheckConsistency(base_vstorage_);
    if (!s.ok()) {
      return s;
    }

    s = CheckConsistency(vstorage);
    if (!s.ok()) {
      return s;
    }

    for (int level = 0; level < num_levels_; level++) {
      const auto& cmp = (level == 0) ? level_zero_cmp_ : level_nonzero_cmp_;
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const auto& base_files = base_vstorage_->LevelFiles(level);
      const auto& unordered_added_files = levels_[level].added_files;
      vstorage->Reserve(level,
                        base_files.size() + unordered_added_files.size());

      // Sort added files for the level.
      std::vector<FileMetaData*> added_files;
      added_files.reserve(unordered_added_files.size());
      for (const auto& pair : unordered_added_files) {
        added_files.push_back(pair.second);
      }
      std::sort(added_files.begin(), added_files.end(), cmp);

#ifndef NDEBUG
      FileMetaData* prev_added_file = nullptr;
      for (const auto& added : added_files) {
        if (level > 0 && prev_added_file != nullptr) {
          assert(base_vstorage_->InternalComparator()->Compare(
                     prev_added_file->smallest, added->smallest) <= 0);
        }
        prev_added_file = added;
      }
#endif

      auto base_iter = base_files.begin();
      auto base_end = base_files.end();
      auto added_iter = added_files.begin();
      auto added_end = added_files.end();
      while (added_iter != added_end || base_iter != base_end) {
        if (base_iter == base_end ||
                (added_iter != added_end && cmp(*added_iter, *base_iter))) {
          MaybeAddFile(vstorage, level, *added_iter++);
        } else {
          MaybeAddFile(vstorage, level, *base_iter++);
        }
      }
    }

    SaveBlobFilesTo(vstorage);

    s = CheckConsistency(vstorage);
    return s;
  }

  Status LoadTableHandlers(InternalStats* internal_stats, int max_threads,
                           bool prefetch_index_and_filter_in_cache,
                           bool is_initial_load,
                           const SliceTransform* prefix_extractor,
                           size_t max_file_size_for_l0_meta_pin) {
    assert(table_cache_ != nullptr);

    size_t table_cache_capacity = table_cache_->get_cache()->GetCapacity();
    bool always_load = (table_cache_capacity == TableCache::kInfiniteCapacity);
    size_t max_load = port::kMaxSizet;

    if (!always_load) {
      // If it is initial loading and not set to always loading all the
      // files, we only load up to kInitialLoadLimit files, to limit the
      // time reopening the DB.
      const size_t kInitialLoadLimit = 16;
      size_t load_limit;
      // If the table cache is not 1/4 full, we pin the table handle to
      // file metadata to avoid the cache read costs when reading the file.
      // The downside of pinning those files is that LRU won't be followed
      // for those files. This doesn't matter much because if number of files
      // of the DB excceeds table cache capacity, eventually no table reader
      // will be pinned and LRU will be followed.
      if (is_initial_load) {
        load_limit = std::min(kInitialLoadLimit, table_cache_capacity / 4);
      } else {
        load_limit = table_cache_capacity / 4;
      }

      size_t table_cache_usage = table_cache_->get_cache()->GetUsage();
      if (table_cache_usage >= load_limit) {
        // TODO (yanqin) find a suitable status code.
        return Status::OK();
      } else {
        max_load = load_limit - table_cache_usage;
      }
    }

    // <file metadata, level>
    std::vector<std::pair<FileMetaData*, int>> files_meta;
    std::vector<Status> statuses;
    for (int level = 0; level < num_levels_; level++) {
      for (auto& file_meta_pair : levels_[level].added_files) {
        auto* file_meta = file_meta_pair.second;
        // If the file has been opened before, just skip it.
        if (!file_meta->table_reader_handle) {
          files_meta.emplace_back(file_meta, level);
          statuses.emplace_back(Status::OK());
        }
        if (files_meta.size() >= max_load) {
          break;
        }
      }
      if (files_meta.size() >= max_load) {
        break;
      }
    }

    std::atomic<size_t> next_file_meta_idx(0);
    std::function<void()> load_handlers_func([&]() {
      while (true) {
        size_t file_idx = next_file_meta_idx.fetch_add(1);
        if (file_idx >= files_meta.size()) {
          break;
        }

        auto* file_meta = files_meta[file_idx].first;
        int level = files_meta[file_idx].second;
        statuses[file_idx] = table_cache_->FindTable(
            ReadOptions(), file_options_,
            *(base_vstorage_->InternalComparator()), file_meta->fd,
            &file_meta->table_reader_handle, prefix_extractor, false /*no_io */,
            true /* record_read_stats */,
            internal_stats->GetFileReadHist(level), false, level,
            prefetch_index_and_filter_in_cache, max_file_size_for_l0_meta_pin);
        if (file_meta->table_reader_handle != nullptr) {
          // Load table_reader
          file_meta->fd.table_reader = table_cache_->GetTableReaderFromHandle(
              file_meta->table_reader_handle);
        }
      }
    });

    std::vector<port::Thread> threads;
    for (int i = 1; i < max_threads; i++) {
      threads.emplace_back(load_handlers_func);
    }
    load_handlers_func();
    for (auto& t : threads) {
      t.join();
    }
    Status ret;
    for (const auto& s : statuses) {
      if (!s.ok()) {
        if (ret.ok()) {
          ret = s;
        }
      }
    }
    return ret;
  }

  void MaybeAddFile(VersionStorageInfo* vstorage, int level, FileMetaData* f) {
    const uint64_t file_number = f->fd.GetNumber();

    const auto& level_state = levels_[level];

    const auto& del_files = level_state.deleted_files;
    const auto del_it = del_files.find(file_number);

    if (del_it != del_files.end()) {
      // f is to-be-deleted table file
      vstorage->RemoveCurrentStats(f);
    } else {
      const auto& add_files = level_state.added_files;
      const auto add_it = add_files.find(file_number);

      // Note: if the file appears both in the base version and in the added
      // list, the added FileMetaData supersedes the one in the base version.
      if (add_it != add_files.end() && add_it->second != f) {
        vstorage->RemoveCurrentStats(f);
      } else {
        vstorage->AddFile(level, f);
      }
    }
  }
};

VersionBuilder::VersionBuilder(const FileOptions& file_options,
                               const ImmutableCFOptions* ioptions,
                               TableCache* table_cache,
                               VersionStorageInfo* base_vstorage,
                               VersionSet* version_set)
    : rep_(new Rep(file_options, ioptions, table_cache, base_vstorage,
                   version_set)) {}

VersionBuilder::~VersionBuilder() = default;

bool VersionBuilder::CheckConsistencyForNumLevels() {
  return rep_->CheckConsistencyForNumLevels();
}

Status VersionBuilder::Apply(VersionEdit* edit) { return rep_->Apply(edit); }

Status VersionBuilder::SaveTo(VersionStorageInfo* vstorage) {
  return rep_->SaveTo(vstorage);
}

Status VersionBuilder::LoadTableHandlers(
    InternalStats* internal_stats, int max_threads,
    bool prefetch_index_and_filter_in_cache, bool is_initial_load,
    const SliceTransform* prefix_extractor,
    size_t max_file_size_for_l0_meta_pin) {
  return rep_->LoadTableHandlers(
      internal_stats, max_threads, prefetch_index_and_filter_in_cache,
      is_initial_load, prefix_extractor, max_file_size_for_l0_meta_pin);
}

uint64_t VersionBuilder::GetMinOldestBlobFileNumber() const {
  return rep_->GetMinOldestBlobFileNumber();
}

BaseReferencedVersionBuilder::BaseReferencedVersionBuilder(
    ColumnFamilyData* cfd)
    : version_builder_(new VersionBuilder(
          cfd->current()->version_set()->file_options(), cfd->ioptions(),
          cfd->table_cache(), cfd->current()->storage_info(),
          cfd->current()->version_set())),
      version_(cfd->current()) {
  version_->Ref();
}

BaseReferencedVersionBuilder::BaseReferencedVersionBuilder(
    ColumnFamilyData* cfd, Version* v)
    : version_builder_(new VersionBuilder(
          cfd->current()->version_set()->file_options(), cfd->ioptions(),
          cfd->table_cache(), v->storage_info(), v->version_set())),
      version_(v) {
  assert(version_ != cfd->current());
}

BaseReferencedVersionBuilder::~BaseReferencedVersionBuilder() {
  version_->Unref();
}

}  // namespace ROCKSDB_NAMESPACE
