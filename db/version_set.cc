//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#define __STDC_FORMAT_MACROS
#include "db/version_set.h"

#include <inttypes.h>
#include <algorithm>
#include <map>
#include <set>
#include <climits>
#include <unordered_map>
#include <stdio.h>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/table_cache.h"
#include "db/compaction.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "table/table_reader.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/stop_watch.h"

namespace rocksdb {

static uint64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  uint64_t sum = 0;
  for (size_t i = 0; i < files.size() && files[i]; i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < num_levels_; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        if (f->table_reader_handle) {
          cfd_->table_cache()->ReleaseHandle(f->table_reader_handle);
          f->table_reader_handle = nullptr;
        }
        vset_->obsolete_files_.push_back(f);
      }
    }
  }
  delete[] files_;
}

int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files,
             const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

static bool AfterFile(const Comparator* ucmp,
                      const Slice* user_key, const FileMetaData* f) {
  // nullptr user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp,
                       const Slice* user_key, const FileMetaData* f) {
  // nullptr user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small(*smallest_user_key, kMaxSequenceNumber,kValueTypeForSeek);
    index = FindFile(icmp, files, small.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp),
        flist_(flist),
        index_(flist->size()) {        // Marks as invalid
  }
  virtual bool Valid() const {
    return index_ < flist_->size();
  }
  virtual void Seek(const Slice& target) {
    index_ = FindFile(icmp_, *flist_, target);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  virtual void Next() {
    assert(Valid());
    index_++;
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_+8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }
 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const EnvOptions& soptions,
                                 const InternalKeyComparator& icomparator,
                                 const Slice& file_value, bool for_compaction) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    ReadOptions options_copy;
    if (options.prefix) {
      // suppress prefix filtering since we have already checked the
      // filters once at this point
      options_copy = options;
      options_copy.prefix = nullptr;
    }
    FileMetaData meta(DecodeFixed64(file_value.data()),
                      DecodeFixed64(file_value.data() + 8));
    return cache->NewIterator(
        options.prefix ? options_copy : options, soptions, icomparator, meta,
        nullptr /* don't need reference to table*/, for_compaction);
  }
}

bool Version::PrefixMayMatch(const ReadOptions& options,
                             const EnvOptions& soptions,
                             const Slice& internal_prefix,
                             Iterator* level_iter) const {
  bool may_match = true;
  level_iter->Seek(internal_prefix);
  if (!level_iter->Valid()) {
    // we're past end of level
    may_match = false;
  } else if (ExtractUserKey(level_iter->key()).starts_with(
                                             ExtractUserKey(internal_prefix))) {
    // TODO(tylerharter): do we need this case?  Or are we guaranteed
    // key() will always be the biggest value for this SST?
    may_match = true;
  } else {
    may_match = cfd_->table_cache()->PrefixMayMatch(
        options, cfd_->internal_comparator(),
        DecodeFixed64(level_iter->value().data()),
        DecodeFixed64(level_iter->value().data() + 8), internal_prefix,
        nullptr);
  }
  return may_match;
}

Status Version::GetPropertiesOfAllTables(TablePropertiesCollection* props) {
  auto table_cache = cfd_->table_cache();
  auto options = cfd_->options();
  for (int level = 0; level < num_levels_; level++) {
    for (const auto& file_meta : files_[level]) {
      auto fname = TableFileName(vset_->dbname_, file_meta->number);
      // 1. If the table is already present in table cache, load table
      // properties from there.
      std::shared_ptr<const TableProperties> table_properties;
      Status s = table_cache->GetTableProperties(
          vset_->storage_options_, cfd_->internal_comparator(), *file_meta,
          &table_properties, true /* no io */);
      if (s.ok()) {
        props->insert({fname, table_properties});
        continue;
      }

      // We only ignore error type `Incomplete` since it's by design that we
      // disallow table when it's not in table cache.
      if (!s.IsIncomplete()) {
        return s;
      }

      // 2. Table is not present in table cache, we'll read the table properties
      // directly from the properties block in the file.
      std::unique_ptr<RandomAccessFile> file;
      s = options->env->NewRandomAccessFile(fname, &file,
                                            vset_->storage_options_);
      if (!s.ok()) {
        return s;
      }

      TableProperties* raw_table_properties;
      // By setting the magic number to kInvalidTableMagicNumber, we can by
      // pass the magic number check in the footer.
      s = ReadTableProperties(
          file.get(), file_meta->file_size,
          Footer::kInvalidTableMagicNumber /* table's magic number */,
          vset_->env_, options->info_log.get(), &raw_table_properties);
      if (!s.ok()) {
        return s;
      }
      RecordTick(options->statistics.get(),
                 NUMBER_DIRECT_LOAD_TABLE_PROPERTIES);

      props->insert({fname, std::shared_ptr<const TableProperties>(
                                raw_table_properties)});
    }
  }

  return Status::OK();
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            const EnvOptions& soptions,
                                            int level) const {
  Iterator* level_iter =
      new LevelFileNumIterator(cfd_->internal_comparator(), &files_[level]);
  if (options.prefix) {
    InternalKey internal_prefix(*options.prefix, 0, kTypeValue);
    if (!PrefixMayMatch(options, soptions,
                        internal_prefix.Encode(), level_iter)) {
      delete level_iter;
      // nothing in this level can match the prefix
      return NewEmptyIterator();
    }
  }
  return NewTwoLevelIterator(level_iter, &GetFileIterator, cfd_->table_cache(),
                             options, soptions, cfd_->internal_comparator());
}

void Version::AddIterators(const ReadOptions& options,
                           const EnvOptions& soptions,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (const FileMetaData* file : files_[0]) {
    iters->push_back(cfd_->table_cache()->NewIterator(
        options, soptions, cfd_->internal_comparator(), *file));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < num_levels_; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, soptions, level));
    }
  }
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
  kMerge // saver contains the current merge result (the operands)
};
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  bool* value_found; // Is value set correctly? Used by KeyMayExist
  std::string* value;
  const MergeOperator* merge_operator;
  // the merge operations encountered;
  MergeContext* merge_context;
  Logger* logger;
  bool didIO;    // did we do any disk io?
  Statistics* statistics;
};
}

// Called from TableCache::Get and Table::Get when file/block in which
// key may  exist are not there in TableCache/BlockCache respectively. In this
// case we  can't guarantee that key does not exist and are not permitted to do
// IO to be  certain.Set the status=kFound and value_found=false to let the
// caller know that key may exist but is not there in memory
static void MarkKeyMayExist(void* arg) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  s->state = kFound;
  if (s->value_found != nullptr) {
    *(s->value_found) = false;
  }
}

static bool SaveValue(void* arg, const ParsedInternalKey& parsed_key,
                      const Slice& v, bool didIO) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  MergeContext* merge_contex = s->merge_context;
  std::string merge_result;  // temporary area for merge results later

  assert(s != nullptr && merge_contex != nullptr);

  // TODO: didIO and Merge?
  s->didIO = didIO;
  if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
    // Key matches. Process it
    switch (parsed_key.type) {
      case kTypeValue:
        if (kNotFound == s->state) {
          s->state = kFound;
          s->value->assign(v.data(), v.size());
        } else if (kMerge == s->state) {
          assert(s->merge_operator != nullptr);
          s->state = kFound;
          if (!s->merge_operator->FullMerge(s->user_key, &v,
                                            merge_contex->GetOperands(),
                                            s->value, s->logger)) {
            RecordTick(s->statistics, NUMBER_MERGE_FAILURES);
            s->state = kCorrupt;
          }
        } else {
          assert(false);
        }
        return false;

      case kTypeDeletion:
        if (kNotFound == s->state) {
          s->state = kDeleted;
        } else if (kMerge == s->state) {
          s->state = kFound;
          if (!s->merge_operator->FullMerge(s->user_key, nullptr,
                                            merge_contex->GetOperands(),
                                            s->value, s->logger)) {
            RecordTick(s->statistics, NUMBER_MERGE_FAILURES);
            s->state = kCorrupt;
          }
        } else {
          assert(false);
        }
        return false;

      case kTypeMerge:
        assert(s->state == kNotFound || s->state == kMerge);
        s->state = kMerge;
        merge_contex->PushOperand(v);
        return true;

      default:
        assert(false);
        break;
    }
  }

  // s->state could be Corrupt, merge or notfound

  return false;
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}
static bool NewestFirstBySeqNo(FileMetaData* a, FileMetaData* b) {
  if (a->smallest_seqno > b->smallest_seqno) {
    assert(a->largest_seqno > b->largest_seqno);
    return true;
  }
  assert(a->largest_seqno <= b->largest_seqno);
  return false;
}

Version::Version(ColumnFamilyData* cfd, VersionSet* vset,
                 uint64_t version_number)
    : cfd_(cfd),
      vset_(vset),
      next_(this),
      prev_(this),
      refs_(0),
      // cfd is nullptr if Version is dummy
      num_levels_(cfd == nullptr ? 0 : cfd->NumberLevels()),
      files_(new std::vector<FileMetaData*>[num_levels_]),
      files_by_size_(num_levels_),
      next_file_to_compact_by_size_(num_levels_),
      file_to_compact_(nullptr),
      file_to_compact_level_(-1),
      compaction_score_(num_levels_),
      compaction_level_(num_levels_),
      version_number_(version_number) {}

void Version::Get(const ReadOptions& options,
                  const LookupKey& k,
                  std::string* value,
                  Status* status,
                  MergeContext* merge_context,
                  GetStats* stats,
                  const Options& db_options,
                  bool* value_found) {
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const Comparator* ucmp = cfd_->internal_comparator().user_comparator();

  auto merge_operator = db_options.merge_operator.get();
  auto logger = db_options.info_log.get();

  assert(status->ok() || status->IsMergeInProgress());
  Saver saver;
  saver.state = status->ok()? kNotFound : kMerge;
  saver.ucmp = ucmp;
  saver.user_key = user_key;
  saver.value_found = value_found;
  saver.value = value;
  saver.merge_operator = merge_operator;
  saver.merge_context = merge_context;
  saver.logger = logger;
  saver.didIO = false;
  saver.statistics = db_options.statistics.get();

  stats->seek_file = nullptr;
  stats->seek_file_level = -1;
  FileMetaData* last_file_read = nullptr;
  int last_file_read_level = -1;

  // We can search level-by-level since entries never hop across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant (unless we
  // are MergeInProgress).
  for (int level = 0; level < num_levels_; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Get the list of files to search in this level
    FileMetaData* const* files = &files_[level][0];

    // Some files may overlap each other. We find
    // all files that overlap user_key and process them in order from
    // newest to oldest. In the context of merge-operator,
    // this can occur at any level. Otherwise, it only occurs
    // at Level-0 (since Put/Deletes are always compacted into a single entry).
    uint32_t start_index;
    if (level == 0) {
      // On Level-0, we read through all files to check for overlap.
      start_index = 0;
    } else {
      // On Level-n (n>=1), files are sorted.
      // Binary search to find earliest index whose largest key >= ikey.
      // We will also stop when the file no longer overlaps ikey
      start_index = FindFile(cfd_->internal_comparator(), files_[level], ikey);
    }

    // Traverse each relevant file to find the desired key
#ifndef NDEBUG
    FileMetaData* prev_file = nullptr;
#endif
    for (uint32_t i = start_index; i < num_files; ++i) {
      FileMetaData* f = files[i];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0 ||
          ucmp->Compare(user_key, f->largest.user_key()) > 0) {
        // Only process overlapping files.
        if (level > 0) {
          // If on Level-n (n>=1) then the files are sorted.
          // So we can stop looking when we are past the ikey.
          break;
        }
        // TODO: do we want to check file ranges for level0 files at all?
        // For new SST format where Get() is fast, we might want to consider
        // to avoid those two comparisons, if it can filter out too few files.
        continue;
      }
#ifndef NDEBUG
      // Sanity check to make sure that the files are correctly sorted
      if (prev_file) {
        if (level != 0) {
          int comp_sign = cfd_->internal_comparator().Compare(
              prev_file->largest, f->smallest);
          assert(comp_sign < 0);
        } else {
          // level == 0, the current file cannot be newer than the previous one.
          if (cfd_->options()->compaction_style == kCompactionStyleUniversal) {
            assert(!NewestFirstBySeqNo(f, prev_file));
          } else {
            assert(!NewestFirst(f, prev_file));
          }
        }
      }
      prev_file = f;
#endif
      bool tableIO = false;
      *status = cfd_->table_cache()->Get(options, cfd_->internal_comparator(),
                                         *f, ikey, &saver, SaveValue, &tableIO,
                                         MarkKeyMayExist);
      // TODO: examine the behavior for corrupted key
      if (!status->ok()) {
        return;
      }

      if (last_file_read != nullptr && stats->seek_file == nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        stats->seek_file = last_file_read;
        stats->seek_file_level = last_file_read_level;
      }

      // If we did any IO as part of the read, then we remember it because
      // it is a possible candidate for seek-based compaction. saver.didIO
      // is true if the block had to be read in from storage and was not
      // pre-exisiting in the block cache. Also, if this file was not pre-
      // existing in the table cache and had to be freshly opened that needed
      // the index blocks to be read-in, then tableIO is true. One thing
      // to note is that the index blocks are not part of the block cache.
      if (saver.didIO || tableIO) {
        last_file_read = f;
        last_file_read_level = level;
      }

      switch (saver.state) {
        case kNotFound:
          break;      // Keep searching in other files
        case kFound:
          return;
        case kDeleted:
          *status = Status::NotFound();  // Use empty error message for speed
          return;
        case kCorrupt:
          *status = Status::Corruption("corrupted key for ", user_key);
          return;
        case kMerge:
          break;
      }
    }
  }


  if (kMerge == saver.state) {
    // merge_operands are in saver and we hit the beginning of the key history
    // do a final merge of nullptr and operands;
    if (merge_operator->FullMerge(user_key, nullptr,
                                  saver.merge_context->GetOperands(),
                                  value, logger)) {
      *status = Status::OK();
    } else {
      RecordTick(db_options.statistics.get(), NUMBER_MERGE_FAILURES);
      *status = Status::Corruption("could not perform end-of-key merge for ",
                                   user_key);
    }
  } else {
    *status = Status::NotFound(); // Use an empty error message for speed
  }
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

void Version::ComputeCompactionScore(
    std::vector<uint64_t>& size_being_compacted) {
  double max_score = 0;
  int max_score_level = 0;

  int num_levels_to_check =
      (cfd_->options()->compaction_style != kCompactionStyleUniversal)
          ? NumberLevels() - 1
          : 1;

  for (int level = 0; level < num_levels_to_check; level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      int numfiles = 0;
      for (unsigned int i = 0; i < files_[level].size(); i++) {
        if (!files_[level][i]->being_compacted) {
          numfiles++;
        }
      }

      // If we are slowing down writes, then we better compact that first
      if (numfiles >= cfd_->options()->level0_stop_writes_trigger) {
        score = 1000000;
        // Log(options_->info_log, "XXX score l0 = 1000000000 max");
      } else if (numfiles >= cfd_->options()->level0_slowdown_writes_trigger) {
        score = 10000;
        // Log(options_->info_log, "XXX score l0 = 1000000 medium");
      } else {
        score = static_cast<double>(numfiles) /
                cfd_->options()->level0_file_num_compaction_trigger;
        if (score >= 1) {
          // Log(options_->info_log, "XXX score l0 = %d least", (int)score);
        }
      }
    } else {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes =
          TotalFileSize(files_[level]) - size_being_compacted[level];
      score = static_cast<double>(level_bytes) /
              cfd_->compaction_picker()->MaxBytesForLevel(level);
      if (score > 1) {
        // Log(options_->info_log, "XXX score l%d = %d ", level, (int)score);
      }
      if (max_score < score) {
        max_score = score;
        max_score_level = level;
      }
    }
    compaction_level_[level] = level;
    compaction_score_[level] = score;
  }

  // update the max compaction score in levels 1 to n-1
  max_compaction_score_ = max_score;
  max_compaction_score_level_ = max_score_level;

  // sort all the levels based on their score. Higher scores get listed
  // first. Use bubble sort because the number of entries are small.
  for (int i = 0; i < NumberLevels() - 2; i++) {
    for (int j = i + 1; j < NumberLevels() - 1; j++) {
      if (compaction_score_[i] < compaction_score_[j]) {
        double score = compaction_score_[i];
        int level = compaction_level_[i];
        compaction_score_[i] = compaction_score_[j];
        compaction_level_[i] = compaction_level_[j];
        compaction_score_[j] = score;
        compaction_level_[j] = level;
      }
    }
  }
}

namespace {

// Compator that is used to sort files based on their size
// In normal mode: descending size
bool CompareSizeDescending(const Version::Fsize& first,
                           const Version::Fsize& second) {
  return (first.file->file_size > second.file->file_size);
}
// A static compator used to sort files based on their seqno
// In universal style : descending seqno
bool CompareSeqnoDescending(const Version::Fsize& first,
                            const Version::Fsize& second) {
  if (first.file->smallest_seqno > second.file->smallest_seqno) {
    assert(first.file->largest_seqno > second.file->largest_seqno);
    return true;
  }
  assert(first.file->largest_seqno <= second.file->largest_seqno);
  return false;
}

} // anonymous namespace

void Version::UpdateFilesBySize() {
  // No need to sort the highest level because it is never compacted.
  int max_level =
      (cfd_->options()->compaction_style == kCompactionStyleUniversal)
          ? NumberLevels()
          : NumberLevels() - 1;

  for (int level = 0; level < max_level; level++) {
    const std::vector<FileMetaData*>& files = files_[level];
    std::vector<int>& files_by_size = files_by_size_[level];
    assert(files_by_size.size() == 0);

    // populate a temp vector for sorting based on size
    std::vector<Fsize> temp(files.size());
    for (unsigned int i = 0; i < files.size(); i++) {
      temp[i].index = i;
      temp[i].file = files[i];
    }

    // sort the top number_of_files_to_sort_ based on file size
    if (cfd_->options()->compaction_style == kCompactionStyleUniversal) {
      int num = temp.size();
      std::partial_sort(temp.begin(), temp.begin() + num, temp.end(),
                        CompareSeqnoDescending);
    } else {
      int num = Version::number_of_files_to_sort_;
      if (num > (int)temp.size()) {
        num = temp.size();
      }
      std::partial_sort(temp.begin(), temp.begin() + num, temp.end(),
                        CompareSizeDescending);
    }
    assert(temp.size() == files.size());

    // initialize files_by_size_
    for (unsigned int i = 0; i < temp.size(); i++) {
      files_by_size.push_back(temp[i].index);
    }
    next_file_to_compact_by_size_[level] = 0;
    assert(files_[level].size() == files_by_size_[level].size());
  }
}

void Version::Ref() {
  ++refs_;
}

bool Version::Unref() {
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
    return true;
  }
  return false;
}

bool Version::NeedsCompaction() const {
  if (file_to_compact_ != nullptr) {
    return true;
  }
  // In universal compaction case, this check doesn't really
  // check the compaction condition, but checks num of files threshold
  // only. We are not going to miss any compaction opportunity
  // but it's likely that more compactions are scheduled but
  // ending up with nothing to do. We can improve it later.
  // TODO(sdong): improve this function to be accurate for universal
  //              compactions.
  int num_levels_to_check =
      (cfd_->options()->compaction_style != kCompactionStyleUniversal)
          ? NumberLevels() - 1
          : 1;
  for (int i = 0; i < num_levels_to_check; i++) {
    if (compaction_score_[i] >= 1) {
      return true;
    }
  }
  return false;
}

bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(cfd_->internal_comparator(), (level > 0),
                               files_[level], smallest_user_key,
                               largest_user_key);
}

int Version::PickLevelForMemTableOutput(
    const Slice& smallest_user_key,
    const Slice& largest_user_key) {
  int level = 0;
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    int max_mem_compact_level = cfd_->options()->max_mem_compaction_level;
    while (max_mem_compact_level > 0 && level < max_mem_compact_level) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      if (level + 2 >= num_levels_) {
        level++;
        break;
      }
      GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
      const uint64_t sum = TotalFileSize(overlaps);
      if (sum > cfd_->compaction_picker()->MaxGrandParentOverlapBytes(level)) {
        break;
      }
      level++;
    }
  }

  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
// If hint_index is specified, then it points to a file in the
// overlapping range.
// The file_index returns a pointer to any file in an overlapping range.
void Version::GetOverlappingInputs(int level,
                                   const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs,
                                   int hint_index,
                                   int* file_index) {
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  if (file_index) {
    *file_index = -1;
  }
  const Comparator* user_cmp = cfd_->internal_comparator().user_comparator();
  if (begin != nullptr && end != nullptr && level > 0) {
    GetOverlappingInputsBinarySearch(level, user_begin, user_end, inputs,
      hint_index, file_index);
    return;
  }
  for (size_t i = 0; i < files_[level].size(); ) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr
            && user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      } else if (file_index) {
        *file_index = i-1;
      }
    }
  }
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
// Employ binary search to find at least one file that overlaps the
// specified range. From that file, iterate backwards and
// forwards to find all overlapping files.
void Version::GetOverlappingInputsBinarySearch(
    int level,
    const Slice& user_begin,
    const Slice& user_end,
    std::vector<FileMetaData*>* inputs,
    int hint_index,
    int* file_index) {
  assert(level > 0);
  int min = 0;
  int mid = 0;
  int max = files_[level].size() -1;
  bool foundOverlap = false;
  const Comparator* user_cmp = cfd_->internal_comparator().user_comparator();

  // if the caller already knows the index of a file that has overlap,
  // then we can skip the binary search.
  if (hint_index != -1) {
    mid = hint_index;
    foundOverlap = true;
  }

  while (!foundOverlap && min <= max) {
    mid = (min + max)/2;
    FileMetaData* f = files_[level][mid];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (user_cmp->Compare(file_limit, user_begin) < 0) {
      min = mid + 1;
    } else if (user_cmp->Compare(user_end, file_start) < 0) {
      max = mid - 1;
    } else {
      foundOverlap = true;
      break;
    }
  }

  // If there were no overlapping files, return immediately.
  if (!foundOverlap) {
    return;
  }
  // returns the index where an overlap is found
  if (file_index) {
    *file_index = mid;
  }
  ExtendOverlappingInputs(level, user_begin, user_end, inputs, mid);
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
// The midIndex specifies the index of at least one file that
// overlaps the specified range. From that file, iterate backward
// and forward to find all overlapping files.
void Version::ExtendOverlappingInputs(
    int level,
    const Slice& user_begin,
    const Slice& user_end,
    std::vector<FileMetaData*>* inputs,
    unsigned int midIndex) {

  const Comparator* user_cmp = cfd_->internal_comparator().user_comparator();
#ifndef NDEBUG
  {
    // assert that the file at midIndex overlaps with the range
    assert(midIndex < files_[level].size());
    FileMetaData* f = files_[level][midIndex];
    const Slice fstart = f->smallest.user_key();
    const Slice flimit = f->largest.user_key();
    if (user_cmp->Compare(fstart, user_begin) >= 0) {
      assert(user_cmp->Compare(fstart, user_end) <= 0);
    } else {
      assert(user_cmp->Compare(flimit, user_begin) >= 0);
    }
  }
#endif
  int startIndex = midIndex + 1;
  int endIndex = midIndex;
  int count __attribute__((unused)) = 0;

  // check backwards from 'mid' to lower indices
  for (int i = midIndex; i >= 0 ; i--) {
    FileMetaData* f = files_[level][i];
    const Slice file_limit = f->largest.user_key();
    if (user_cmp->Compare(file_limit, user_begin) >= 0) {
      startIndex = i;
      assert((count++, true));
    } else {
      break;
    }
  }
  // check forward from 'mid+1' to higher indices
  for (unsigned int i = midIndex+1; i < files_[level].size(); i++) {
    FileMetaData* f = files_[level][i];
    const Slice file_start = f->smallest.user_key();
    if (user_cmp->Compare(file_start, user_end) <= 0) {
      assert((count++, true));
      endIndex = i;
    } else {
      break;
    }
  }
  assert(count == endIndex - startIndex + 1);

  // insert overlapping files into vector
  for (int i = startIndex; i <= endIndex; i++) {
    FileMetaData* f = files_[level][i];
    inputs->push_back(f);
  }
}

// Returns true iff the first or last file in inputs contains
// an overlapping user key to the file "just outside" of it (i.e.
// just after the last file, or just before the first file)
// REQUIRES: "*inputs" is a sorted list of non-overlapping files
bool Version::HasOverlappingUserKey(
    const std::vector<FileMetaData*>* inputs,
    int level) {

  // If inputs empty, there is no overlap.
  // If level == 0, it is assumed that all needed files were already included.
  if (inputs->empty() || level == 0){
    return false;
  }

  const Comparator* user_cmp = cfd_->internal_comparator().user_comparator();
  const std::vector<FileMetaData*>& files = files_[level];
  const size_t kNumFiles = files.size();

  // Check the last file in inputs against the file after it
  size_t last_file = FindFile(cfd_->internal_comparator(), files,
                              inputs->back()->largest.Encode());
  assert(0 <= last_file && last_file < kNumFiles);  // File should exist!
  if (last_file < kNumFiles-1) {                    // If not the last file
    const Slice last_key_in_input = files[last_file]->largest.user_key();
    const Slice first_key_after = files[last_file+1]->smallest.user_key();
    if (user_cmp->Compare(last_key_in_input, first_key_after) == 0) {
      // The last user key in input overlaps with the next file's first key
      return true;
    }
  }

  // Check the first file in inputs against the file just before it
  size_t first_file = FindFile(cfd_->internal_comparator(), files,
                               inputs->front()->smallest.Encode());
  assert(0 <= first_file && first_file <= last_file);   // File should exist!
  if (first_file > 0) {                                 // If not first file
    const Slice& first_key_in_input = files[first_file]->smallest.user_key();
    const Slice& last_key_before = files[first_file-1]->largest.user_key();
    if (user_cmp->Compare(first_key_in_input, last_key_before) == 0) {
      // The first user key in input overlaps with the previous file's last key
      return true;
    }
  }

  return false;
}

int64_t Version::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < NumberLevels());
  return TotalFileSize(files_[level]);
}

const char* Version::LevelSummary(LevelSummaryStorage* scratch) const {
  int len = snprintf(scratch->buffer, sizeof(scratch->buffer), "files[");
  for (int i = 0; i < NumberLevels(); i++) {
    int sz = sizeof(scratch->buffer) - len;
    int ret = snprintf(scratch->buffer + len, sz, "%d ", int(files_[i].size()));
    if (ret < 0 || ret >= sz) break;
    len += ret;
  }
  snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, "]");
  return scratch->buffer;
}

const char* Version::LevelFileSummary(FileSummaryStorage* scratch,
                                      int level) const {
  int len = snprintf(scratch->buffer, sizeof(scratch->buffer), "files_size[");
  for (const auto& f : files_[level]) {
    int sz = sizeof(scratch->buffer) - len;
    int ret = snprintf(scratch->buffer + len, sz,
                       "#%lu(seq=%lu,sz=%lu,%lu) ",
                       (unsigned long)f->number,
                       (unsigned long)f->smallest_seqno,
                       (unsigned long)f->file_size,
                       (unsigned long)f->being_compacted);
    if (ret < 0 || ret >= sz)
      break;
    len += ret;
  }
  snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, "]");
  return scratch->buffer;
}

int64_t Version::MaxNextLevelOverlappingBytes() {
  uint64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < NumberLevels() - 1; level++) {
    for (const auto& f : files_[level]) {
      GetOverlappingInputs(level + 1, &f->smallest, &f->largest, &overlaps);
      const uint64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

void Version::AddLiveFiles(std::set<uint64_t>* live) {
  for (int level = 0; level < NumberLevels(); level++) {
    const std::vector<FileMetaData*>& files = files_[level];
    for (const auto& file : files) {
      live->insert(file->number);
    }
  }
}

std::string Version::DebugString(bool hex) const {
  std::string r;
  for (int level = 0; level < num_levels_; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" --- version# ");
    AppendNumberTo(&r, version_number_);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString(hex));
      r.append(" .. ");
      r.append(files[i]->largest.DebugString(hex));
      r.append("]\n");
    }
  }
  return r;
}

// this is used to batch writes to the manifest file
struct VersionSet::ManifestWriter {
  Status status;
  bool done;
  port::CondVar cv;
  ColumnFamilyData* cfd;
  VersionEdit* edit;

  explicit ManifestWriter(port::Mutex* mu, ColumnFamilyData* cfd,
                          VersionEdit* e)
      : done(false), cv(mu), cfd(cfd), edit(e) {}
};

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  ColumnFamilyData* cfd_;
  Version* base_;
  LevelState* levels_;

 public:
  Builder(ColumnFamilyData* cfd) : cfd_(cfd), base_(cfd->current()) {
    base_->Ref();
    levels_ = new LevelState[base_->NumberLevels()];
    BySmallestKey cmp;
    cmp.internal_comparator = &cfd_->internal_comparator();
    for (int level = 0; level < base_->NumberLevels(); level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < base_->NumberLevels(); level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin();
          it != added->end(); ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          if (f->table_reader_handle) {
            cfd_->table_cache()->ReleaseHandle(f->table_reader_handle);
            f->table_reader_handle = nullptr;
          }
          delete f;
        }
      }
    }

    delete[] levels_;
    base_->Unref();
  }

  void CheckConsistency(Version* v) {
#ifndef NDEBUG
    for (int level = 0; level < v->NumberLevels(); level++) {
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i-1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (cfd_->internal_comparator().Compare(prev_end, this_begin) >= 0) {
            fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                    prev_end.DebugString().c_str(),
                    this_begin.DebugString().c_str());
            abort();
          }
        }
      }
    }
#endif
  }

  void CheckConsistencyForDeletes(VersionEdit* edit, unsigned int number,
                                  int level) {
#ifndef NDEBUG
      // a file to be deleted better exist in the previous version
      bool found = false;
      for (int l = 0; !found && l < base_->NumberLevels(); l++) {
        const std::vector<FileMetaData*>& base_files = base_->files_[l];
        for (unsigned int i = 0; i < base_files.size(); i++) {
          FileMetaData* f = base_files[i];
          if (f->number == number) {
            found =  true;
            break;
          }
        }
      }
      // if the file did not exist in the previous version, then it
      // is possibly moved from lower level to higher level in current
      // version
      for (int l = level+1; !found && l < base_->NumberLevels(); l++) {
        const FileSet* added = levels_[l].added_files;
        for (FileSet::const_iterator added_iter = added->begin();
             added_iter != added->end(); ++added_iter) {
          FileMetaData* f = *added_iter;
          if (f->number == number) {
            found = true;
            break;
          }
        }
      }

      // maybe this file was added in a previous edit that was Applied
      if (!found) {
        const FileSet* added = levels_[level].added_files;
        for (FileSet::const_iterator added_iter = added->begin();
             added_iter != added->end(); ++added_iter) {
          FileMetaData* f = *added_iter;
          if (f->number == number) {
            found = true;
            break;
          }
        }
      }
      assert(found);
#endif
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    CheckConsistency(base_);

    // Delete files
    const VersionEdit::DeletedFileSet& del = edit->deleted_files_;
    for (const auto& del_file : del) {
      const auto level = del_file.first;
      const auto number = del_file.second;
      levels_[level].deleted_files.insert(number);
      CheckConsistencyForDeletes(edit, number, level);
    }

    // Add new files
    for (const auto& new_file : edit->new_files_) {
      const int level = new_file.first;
      FileMetaData* f = new FileMetaData(new_file.second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = (f->file_size / 16384);
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
    CheckConsistency(base_);
    CheckConsistency(v);
    BySmallestKey cmp;
    cmp.internal_comparator = &cfd_->internal_comparator();
    for (int level = 0; level < base_->NumberLevels(); level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const auto& base_files = base_->files_[level];
      auto base_iter = base_files.begin();
      auto base_end = base_files.end();
      const auto& added_files = *levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added_files.size());

      for (const auto& added : added_files) {
        // Add all smaller files listed in base_
        for (auto bpos = std::upper_bound(base_iter, base_end, added, cmp);
             base_iter != bpos;
             ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, added);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }
    }

    // TODO(icanadi) do it in the loop above, which already sorts the files
    // Pre-sort level0 for Get()
    if (cfd_->options()->compaction_style == kCompactionStyleUniversal) {
      std::sort(v->files_[0].begin(), v->files_[0].end(), NewestFirstBySeqNo);
    } else {
      std::sort(v->files_[0].begin(), v->files_[0].end(), NewestFirst);
    }
    CheckConsistency(v);
  }

  void LoadTableHandlers() {
    for (int level = 0; level < cfd_->NumberLevels(); level++) {
      for (auto& file_meta : *(levels_[level].added_files)) {
        assert (!file_meta->table_reader_handle);
        bool table_io;
        cfd_->table_cache()->FindTable(
            base_->vset_->storage_options_, cfd_->internal_comparator(),
            file_meta->number, file_meta->file_size,
            &file_meta->table_reader_handle, &table_io, false);
      }
    }
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      auto* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(cfd_->internal_comparator().Compare(
                   (*files)[files->size() - 1]->largest, f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname, const DBOptions* options,
                       const EnvOptions& storage_options, Cache* table_cache)
    : column_family_set_(new ColumnFamilySet(dbname, options, storage_options,
                                             table_cache)),
      env_(options->env),
      dbname_(dbname),
      options_(options),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      pending_manifest_file_number_(0),
      last_sequence_(0),
      prev_log_number_(0),
      current_version_number_(0),
      manifest_file_size_(0),
      storage_options_(storage_options),
      storage_options_compactions_(storage_options_) {}

VersionSet::~VersionSet() {
  // we need to delete column_family_set_ because its destructor depends on
  // VersionSet
  column_family_set_.reset();
  for (auto file : obsolete_files_) {
    delete file;
  }
  obsolete_files_.clear();
}

void VersionSet::AppendVersion(ColumnFamilyData* column_family_data,
                               Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  Version* current = column_family_data->current();
  assert(v != current);
  if (current != nullptr) {
    assert(current->refs_ > 0);
    current->Unref();
  }
  column_family_data->SetCurrent(v);
  v->Ref();

  // Append to linked list
  v->prev_ = column_family_data->dummy_versions()->prev_;
  v->next_ = column_family_data->dummy_versions();
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::LogAndApply(ColumnFamilyData* column_family_data,
                               VersionEdit* edit, port::Mutex* mu,
                               Directory* db_directory, bool new_descriptor_log,
                               const ColumnFamilyOptions* options) {
  mu->AssertHeld();

  // column_family_data can be nullptr only if this is column_family_add.
  // in that case, we also need to specify ColumnFamilyOptions
  if (column_family_data == nullptr) {
    assert(edit->is_column_family_add_);
    assert(options != nullptr);
  }

  // queue our request
  ManifestWriter w(mu, column_family_data, edit);
  manifest_writers_.push_back(&w);
  while (!w.done && &w != manifest_writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }
  if (column_family_data != nullptr && column_family_data->IsDropped()) {
    // if column family is dropped by the time we get here, no need to write
    // anything to the manifest
    manifest_writers_.pop_front();
    // Notify new head of write queue
    if (!manifest_writers_.empty()) {
      manifest_writers_.front()->cv.Signal();
    }
    return Status::OK();
  }

  std::vector<VersionEdit*> batch_edits;
  Version* v = nullptr;
  std::unique_ptr<Builder> builder(nullptr);

  // process all requests in the queue
  ManifestWriter* last_writer = &w;
  assert(!manifest_writers_.empty());
  assert(manifest_writers_.front() == &w);
  if (edit->IsColumnFamilyManipulation()) {
    // no group commits for column family add or drop
    LogAndApplyCFHelper(edit);
    batch_edits.push_back(edit);
  } else {
    v = new Version(column_family_data, this, current_version_number_++);
    builder.reset(new Builder(column_family_data));
    for (const auto& writer : manifest_writers_) {
      if (writer->edit->IsColumnFamilyManipulation() ||
          writer->cfd->GetID() != column_family_data->GetID()) {
        // no group commits for column family add or drop
        // also, group commits across column families are not supported
        break;
      }
      last_writer = writer;
      LogAndApplyHelper(column_family_data, builder.get(), v, last_writer->edit,
                        mu);
      batch_edits.push_back(last_writer->edit);
    }
    builder->SaveTo(v);
  }

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  uint64_t new_manifest_file_size = 0;
  Status s;

  assert(pending_manifest_file_number_ == 0);
  if (!descriptor_log_ ||
      manifest_file_size_ > options_->max_manifest_file_size) {
    pending_manifest_file_number_ = NewFileNumber();
    batch_edits.back()->SetNextFile(next_file_number_);
    new_descriptor_log = true;
  } else {
    pending_manifest_file_number_ = manifest_file_number_;
  }

  if (new_descriptor_log) {
    // if we're writing out new snapshot make sure to persist max column family
    if (column_family_set_->GetMaxColumnFamily() > 0) {
      edit->SetMaxColumnFamily(column_family_set_->GetMaxColumnFamily());
    }
  }

  // Unlock during expensive operations. New writes cannot get here
  // because &w is ensuring that all new writes get queued.
  {
    std::vector<uint64_t> size_being_compacted;
    if (!edit->IsColumnFamilyManipulation()) {
      size_being_compacted.resize(v->NumberLevels() - 1);
      // calculate the amount of data being compacted at every level
      column_family_data->compaction_picker()->SizeBeingCompacted(
          size_being_compacted);
    }

    mu->Unlock();

    if (!edit->IsColumnFamilyManipulation() && options_->max_open_files == -1) {
      // unlimited table cache. Pre-load table handle now.
      // Need to do it out of the mutex.
      builder->LoadTableHandlers();
    }

    // This is fine because everything inside of this block is serialized --
    // only one thread can be here at the same time
    if (new_descriptor_log) {
      unique_ptr<WritableFile> descriptor_file;
      s = env_->NewWritableFile(
          DescriptorFileName(dbname_, pending_manifest_file_number_),
          &descriptor_file, env_->OptimizeForManifestWrite(storage_options_));
      if (s.ok()) {
        descriptor_log_.reset(new log::Writer(std::move(descriptor_file)));
        s = WriteSnapshot(descriptor_log_.get());
      }
    }

    if (!edit->IsColumnFamilyManipulation()) {
      // The calls to ComputeCompactionScore and UpdateFilesBySize are cpu-heavy
      // and is best called outside the mutex.
      v->ComputeCompactionScore(size_being_compacted);
      v->UpdateFilesBySize();
    }

    // Write new record to MANIFEST log
    if (s.ok()) {
      for (auto& e : batch_edits) {
        std::string record;
        e->EncodeTo(&record);
        s = descriptor_log_->AddRecord(record);
        if (!s.ok()) {
          break;
        }
      }
      if (s.ok()) {
        if (options_->use_fsync) {
          StopWatch sw(env_, options_->statistics.get(),
                       MANIFEST_FILE_SYNC_MICROS);
          s = descriptor_log_->file()->Fsync();
        } else {
          StopWatch sw(env_, options_->statistics.get(),
                       MANIFEST_FILE_SYNC_MICROS);
          s = descriptor_log_->file()->Sync();
        }
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
        bool all_records_in = true;
        for (auto& e : batch_edits) {
          std::string record;
          e->EncodeTo(&record);
          if (!ManifestContains(pending_manifest_file_number_, record)) {
            all_records_in = false;
            break;
          }
        }
        if (all_records_in) {
          Log(options_->info_log,
              "MANIFEST contains log record despite error; advancing to new "
              "version to prevent mismatch between in-memory and logged state"
              " If paranoid is set, then the db is now in readonly mode.");
          s = Status::OK();
        }
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && new_descriptor_log) {
      s = SetCurrentFile(env_, dbname_, pending_manifest_file_number_);
      if (s.ok() && pending_manifest_file_number_ > manifest_file_number_) {
        // delete old manifest file
        Log(options_->info_log,
            "Deleting manifest %" PRIu64 " current manifest %" PRIu64 "\n",
            manifest_file_number_, pending_manifest_file_number_);
        // we don't care about an error here, PurgeObsoleteFiles will take care
        // of it later
        env_->DeleteFile(DescriptorFileName(dbname_, manifest_file_number_));
      }
      if (!options_->disableDataSync && db_directory != nullptr) {
        db_directory->Fsync();
      }
    }

    if (s.ok()) {
      // find offset in manifest file where this version is stored.
      new_manifest_file_size = descriptor_log_->file()->GetFileSize();
    }

    LogFlush(options_->info_log);
    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    if (edit->is_column_family_add_) {
      // no group commit on column family add
      assert(batch_edits.size() == 1);
      assert(options != nullptr);
      CreateColumnFamily(*options, edit);
    } else if (edit->is_column_family_drop_) {
      assert(batch_edits.size() == 1);
      column_family_data->SetDropped();
      if (column_family_data->Unref()) {
        delete column_family_data;
      }
    } else {
      uint64_t max_log_number_in_batch  = 0;
      for (auto& e : batch_edits) {
        if (e->has_log_number_) {
          max_log_number_in_batch =
              std::max(max_log_number_in_batch, e->log_number_);
        }
      }
      if (max_log_number_in_batch != 0) {
        assert(column_family_data->GetLogNumber() < max_log_number_in_batch);
        column_family_data->SetLogNumber(max_log_number_in_batch);
      }
      AppendVersion(column_family_data, v);
    }

    manifest_file_number_ = pending_manifest_file_number_;
    manifest_file_size_ = new_manifest_file_size;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    Log(options_->info_log, "Error in committing version %lu",
        (unsigned long)v->GetVersionNumber());
    delete v;
    if (new_descriptor_log) {
      descriptor_log_.reset();
      env_->DeleteFile(
          DescriptorFileName(dbname_, pending_manifest_file_number_));
    }
  }
  pending_manifest_file_number_ = 0;

  // wake up all the waiting writers
  while (true) {
    ManifestWriter* ready = manifest_writers_.front();
    manifest_writers_.pop_front();
    if (ready != &w) {
      ready->status = s;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }
  // Notify new head of write queue
  if (!manifest_writers_.empty()) {
    manifest_writers_.front()->cv.Signal();
  }
  return s;
}

void VersionSet::LogAndApplyCFHelper(VersionEdit* edit) {
  assert(edit->IsColumnFamilyManipulation());
  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);
  if (edit->is_column_family_drop_) {
    // if we drop column family, we have to make sure to save max column family,
    // so that we don't reuse existing ID
    edit->SetMaxColumnFamily(column_family_set_->GetMaxColumnFamily());
  }
}

void VersionSet::LogAndApplyHelper(ColumnFamilyData* cfd, Builder* builder,
                                   Version* v, VersionEdit* edit,
                                   port::Mutex* mu) {
  mu->AssertHeld();
  assert(!edit->IsColumnFamilyManipulation());

  if (edit->has_log_number_) {
    assert(edit->log_number_ >= cfd->GetLogNumber());
    assert(edit->log_number_ < next_file_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }
  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  builder->Apply(edit);
}

Status VersionSet::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families) {
  std::unordered_map<std::string, ColumnFamilyOptions> cf_name_to_options;
  for (auto cf : column_families) {
    cf_name_to_options.insert({cf.name, cf.options});
  }
  // keeps track of column families in manifest that were not found in
  // column families parameters. if those column families are not dropped
  // by subsequent manifest records, Recover() will return failure status
  std::set<int> column_families_not_found;

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string manifest_filename;
  Status s = ReadFileToString(
      env_, CurrentFileName(dbname_), &manifest_filename
  );
  if (!s.ok()) {
    return s;
  }
  if (manifest_filename.empty() ||
      manifest_filename.back() != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  // remove the trailing '\n'
  manifest_filename.resize(manifest_filename.size() - 1);
  FileType type;
  bool parse_ok =
      ParseFileName(manifest_filename, &manifest_file_number_, &type);
  if (!parse_ok || type != kDescriptorFile) {
    return Status::Corruption("CURRENT file corrupted");
  }

  Log(options_->info_log, "Recovering from manifest file:%s\n",
      manifest_filename.c_str());

  manifest_filename = dbname_ + "/" + manifest_filename;
  unique_ptr<SequentialFile> manifest_file;
  s = env_->NewSequentialFile(manifest_filename, &manifest_file,
                              storage_options_);
  if (!s.ok()) {
    return s;
  }
  uint64_t manifest_file_size;
  s = env_->GetFileSize(manifest_filename, &manifest_file_size);
  if (!s.ok()) {
    return s;
  }

  bool have_version_number = false;
  bool log_number_decrease = false;
  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  uint32_t max_column_family = 0;
  std::unordered_map<uint32_t, Builder*> builders;

  // add default column family
  auto default_cf_iter = cf_name_to_options.find(default_column_family_name);
  if (default_cf_iter == cf_name_to_options.end()) {
    return Status::InvalidArgument("Default column family not specified");
  }
  VersionEdit default_cf_edit;
  default_cf_edit.AddColumnFamily(default_column_family_name);
  default_cf_edit.SetColumnFamily(0);
  ColumnFamilyData* default_cfd =
      CreateColumnFamily(default_cf_iter->second, &default_cf_edit);
  builders.insert({0, new Builder(default_cfd)});

  {
    VersionSet::LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(std::move(manifest_file), &reporter, true /*checksum*/,
                       0 /*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (!s.ok()) {
        break;
      }

      // Not found means that user didn't supply that column
      // family option AND we encountered column family add
      // record. Once we encounter column family drop record,
      // we will delete the column family from
      // column_families_not_found.
      bool cf_in_not_found =
          column_families_not_found.find(edit.column_family_) !=
          column_families_not_found.end();
      // in builders means that user supplied that column family
      // option AND that we encountered column family add record
      bool cf_in_builders =
          builders.find(edit.column_family_) != builders.end();

      // they can't both be true
      assert(!(cf_in_not_found && cf_in_builders));

      ColumnFamilyData* cfd = nullptr;

      if (edit.is_column_family_add_) {
        if (cf_in_builders || cf_in_not_found) {
          s = Status::Corruption(
              "Manifest adding the same column family twice");
          break;
        }
        auto cf_options = cf_name_to_options.find(edit.column_family_name_);
        if (cf_options == cf_name_to_options.end()) {
          column_families_not_found.insert(edit.column_family_);
        } else {
          cfd = CreateColumnFamily(cf_options->second, &edit);
          builders.insert({edit.column_family_, new Builder(cfd)});
        }
      } else if (edit.is_column_family_drop_) {
        if (cf_in_builders) {
          auto builder = builders.find(edit.column_family_);
          assert(builder != builders.end());
          delete builder->second;
          builders.erase(builder);
          cfd = column_family_set_->GetColumnFamily(edit.column_family_);
          if (cfd->Unref()) {
            delete cfd;
            cfd = nullptr;
          } else {
            // who else can have reference to cfd!?
            assert(false);
          }
        } else if (cf_in_not_found) {
          column_families_not_found.erase(edit.column_family_);
        } else {
          s = Status::Corruption(
              "Manifest - dropping non-existing column family");
          break;
        }
      } else if (!cf_in_not_found) {
        if (!cf_in_builders) {
          s = Status::Corruption(
              "Manifest record referencing unknown column family");
          break;
        }

        cfd = column_family_set_->GetColumnFamily(edit.column_family_);
        // this should never happen since cf_in_builders is true
        assert(cfd != nullptr);
        if (edit.max_level_ >= cfd->current()->NumberLevels()) {
          s = Status::InvalidArgument(
              "db has more levels than options.num_levels");
          break;
        }

        // if it is not column family add or column family drop,
        // then it's a file add/delete, which should be forwarded
        // to builder
        auto builder = builders.find(edit.column_family_);
        assert(builder != builders.end());
        builder->second->Apply(&edit);
      }

      if (cfd != nullptr) {
        if (edit.has_log_number_) {
          if (cfd->GetLogNumber() > edit.log_number_) {
            log_number_decrease = true;
          } else {
            cfd->SetLogNumber(edit.log_number_);
            have_log_number = true;
          }
        }
        if (edit.has_comparator_ &&
            edit.comparator_ != cfd->user_comparator()->Name()) {
          s = Status::InvalidArgument(
              cfd->user_comparator()->Name(),
              "does not match existing comparator " + edit.comparator_);
          break;
        }
      }

      if (edit.has_version_number_) {
        have_version_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_max_column_family_) {
        max_column_family = edit.max_column_family_;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }

    if (s.ok() && log_number_decrease) {
      // Since release 2.8, version number is added into MANIFEST file.
      // Prior release 2.8, a bug in LogAndApply() can cause log_number
      // to be smaller than the one from previous edit. To ensure backward
      // compatibility, only fail for MANIFEST genearated by release 2.8
      // and after.
      if (have_version_number) {
        s = Status::Corruption(
            "MANIFEST corruption - Log numbers in records NOT "
            "monotonically increasing");
      } else {
        Log(options_->info_log,
            "MANIFEST corruption detected, but ignored - Log numbers in "
            "records NOT monotonically increasing");
      }
    }
  }

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    column_family_set_->UpdateMaxColumnFamily(max_column_family);

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  // there were some column families in the MANIFEST that weren't specified
  // in the argument
  if (column_families_not_found.size() > 0) {
    s = Status::InvalidArgument(
        "Found unexpected column families. You have to specify all column "
        "families when opening the DB");
  }

  if (s.ok()) {
    for (auto cfd : *column_family_set_) {
      auto builders_iter = builders.find(cfd->GetID());
      assert(builders_iter != builders.end());
      auto builder = builders_iter->second;

      if (options_->max_open_files == -1) {
      // unlimited table cache. Pre-load table handle now.
      // Need to do it out of the mutex.
        builder->LoadTableHandlers();
      }

      Version* v = new Version(cfd, this, current_version_number_++);
      builder->SaveTo(v);

      // Install recovered version
      std::vector<uint64_t> size_being_compacted(v->NumberLevels() - 1);
      cfd->compaction_picker()->SizeBeingCompacted(size_being_compacted);
      v->ComputeCompactionScore(size_being_compacted);
      v->UpdateFilesBySize();
      AppendVersion(cfd, v);
    }

    manifest_file_size_ = manifest_file_size;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    prev_log_number_ = prev_log_number;

    Log(options_->info_log, "Recovered from manifest file:%s succeeded,"
        "manifest_file_number is %lu, next_file_number is %lu, "
        "last_sequence is %lu, log_number is %lu,"
        "prev_log_number is %lu,"
        "max_column_family is %u\n",
        manifest_filename.c_str(),
        (unsigned long)manifest_file_number_,
        (unsigned long)next_file_number_,
        (unsigned long)last_sequence_,
        (unsigned long)log_number,
        (unsigned long)prev_log_number_,
        column_family_set_->GetMaxColumnFamily());

    for (auto cfd : *column_family_set_) {
      Log(options_->info_log,
          "Column family \"%s\", log number is %" PRIu64 "\n",
          cfd->GetName().c_str(), cfd->GetLogNumber());
    }
  }

  for (auto builder : builders) {
    delete builder.second;
  }

  return s;
}

Status VersionSet::ListColumnFamilies(std::vector<std::string>* column_families,
                                      const std::string& dbname, Env* env) {
  // these are just for performance reasons, not correcntes,
  // so we're fine using the defaults
  EnvOptions soptions;
  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env, CurrentFileName(dbname), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname + "/" + current;
  unique_ptr<SequentialFile> file;
  s = env->NewSequentialFile(dscname, &file, soptions);
  if (!s.ok()) {
    return s;
  }

  std::map<uint32_t, std::string> column_family_names;
  // default column family is always implicitly there
  column_family_names.insert({0, default_column_family_name});
  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(std::move(file), &reporter, true /*checksum*/,
                     0 /*initial_offset*/);
  Slice record;
  std::string scratch;
  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      break;
    }
    if (edit.is_column_family_add_) {
      if (column_family_names.find(edit.column_family_) !=
          column_family_names.end()) {
        s = Status::Corruption("Manifest adding the same column family twice");
        break;
      }
      column_family_names.insert(
          {edit.column_family_, edit.column_family_name_});
    } else if (edit.is_column_family_drop_) {
      if (column_family_names.find(edit.column_family_) ==
          column_family_names.end()) {
        s = Status::Corruption(
            "Manifest - dropping non-existing column family");
        break;
      }
      column_family_names.erase(edit.column_family_);
    }
  }

  column_families->clear();
  if (s.ok()) {
    for (const auto& iter : column_family_names) {
      column_families->push_back(iter.second);
    }
  }

  return s;
}

Status VersionSet::ReduceNumberOfLevels(const std::string& dbname,
                                        const Options* options,
                                        const EnvOptions& storage_options,
                                        int new_levels) {
  if (new_levels <= 1) {
    return Status::InvalidArgument(
        "Number of levels needs to be bigger than 1");
  }

  ColumnFamilyOptions cf_options(*options);
  std::shared_ptr<Cache> tc(NewLRUCache(
      options->max_open_files - 10, options->table_cache_numshardbits,
      options->table_cache_remove_scan_count_limit));
  VersionSet versions(dbname, options, storage_options, tc.get());
  Status status;

  std::vector<ColumnFamilyDescriptor> dummy;
  ColumnFamilyDescriptor dummy_descriptor(default_column_family_name,
                                          ColumnFamilyOptions(*options));
  dummy.push_back(dummy_descriptor);
  status = versions.Recover(dummy);
  if (!status.ok()) {
    return status;
  }

  Version* current_version =
      versions.GetColumnFamilySet()->GetDefault()->current();
  int current_levels = current_version->NumberLevels();

  if (current_levels <= new_levels) {
    return Status::OK();
  }

  // Make sure there are file only on one level from
  // (new_levels-1) to (current_levels-1)
  int first_nonempty_level = -1;
  int first_nonempty_level_filenum = 0;
  for (int i = new_levels - 1; i < current_levels; i++) {
    int file_num = current_version->NumLevelFiles(i);
    if (file_num != 0) {
      if (first_nonempty_level < 0) {
        first_nonempty_level = i;
        first_nonempty_level_filenum = file_num;
      } else {
        char msg[255];
        snprintf(msg, sizeof(msg),
                 "Found at least two levels containing files: "
                 "[%d:%d],[%d:%d].\n",
                 first_nonempty_level, first_nonempty_level_filenum, i,
                 file_num);
        return Status::InvalidArgument(msg);
      }
    }
  }

  std::vector<FileMetaData*>* old_files_list = current_version->files_;
  // we need to allocate an array with the old number of levels size to
  // avoid SIGSEGV in WriteSnapshot()
  // however, all levels bigger or equal to new_levels will be empty
  std::vector<FileMetaData*>* new_files_list =
      new std::vector<FileMetaData*>[current_levels];
  for (int i = 0; i < new_levels - 1; i++) {
    new_files_list[i] = old_files_list[i];
  }

  if (first_nonempty_level > 0) {
    new_files_list[new_levels - 1] = old_files_list[first_nonempty_level];
  }

  delete[] current_version->files_;
  current_version->files_ = new_files_list;
  current_version->num_levels_ = new_levels;

  VersionEdit ve;
  port::Mutex dummy_mutex;
  MutexLock l(&dummy_mutex);
  return versions.LogAndApply(versions.GetColumnFamilySet()->GetDefault(), &ve,
                              &dummy_mutex, nullptr, true);
}

Status VersionSet::DumpManifest(Options& options, std::string& dscname,
                                bool verbose, bool hex) {
  // Open the specified manifest file.
  unique_ptr<SequentialFile> file;
  Status s = options.env->NewSequentialFile(dscname, &file, storage_options_);
  if (!s.ok()) {
    return s;
  }

  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t prev_log_number = 0;
  int count = 0;
  std::unordered_map<uint32_t, std::string> comparators;
  std::unordered_map<uint32_t, Builder*> builders;

  // add default column family
  VersionEdit default_cf_edit;
  default_cf_edit.AddColumnFamily(default_column_family_name);
  default_cf_edit.SetColumnFamily(0);
  ColumnFamilyData* default_cfd =
      CreateColumnFamily(ColumnFamilyOptions(options), &default_cf_edit);
  builders.insert({0, new Builder(default_cfd)});

  {
    VersionSet::LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(std::move(file), &reporter, true/*checksum*/,
                       0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (!s.ok()) {
        break;
      }

      // Write out each individual edit
      if (verbose) {
        printf("*************************Edit[%d] = %s\n",
                count, edit.DebugString(hex).c_str());
      }
      count++;

      bool cf_in_builders =
          builders.find(edit.column_family_) != builders.end();

      if (edit.has_comparator_) {
        comparators.insert({edit.column_family_, edit.comparator_});
      }

      ColumnFamilyData* cfd = nullptr;

      if (edit.is_column_family_add_) {
        if (cf_in_builders) {
          s = Status::Corruption(
              "Manifest adding the same column family twice");
          break;
        }
        cfd = CreateColumnFamily(ColumnFamilyOptions(options), &edit);
        builders.insert({edit.column_family_, new Builder(cfd)});
      } else if (edit.is_column_family_drop_) {
        if (!cf_in_builders) {
          s = Status::Corruption(
              "Manifest - dropping non-existing column family");
          break;
        }
        auto builder_iter = builders.find(edit.column_family_);
        delete builder_iter->second;
        builders.erase(builder_iter);
        comparators.erase(edit.column_family_);
        cfd = column_family_set_->GetColumnFamily(edit.column_family_);
        assert(cfd != nullptr);
        cfd->Unref();
        delete cfd;
        cfd = nullptr;
      } else {
        if (!cf_in_builders) {
          s = Status::Corruption(
              "Manifest record referencing unknown column family");
          break;
        }

        cfd = column_family_set_->GetColumnFamily(edit.column_family_);
        // this should never happen since cf_in_builders is true
        assert(cfd != nullptr);

        // if it is not column family add or column family drop,
        // then it's a file add/delete, which should be forwarded
        // to builder
        auto builder = builders.find(edit.column_family_);
        assert(builder != builders.end());
        builder->second->Apply(&edit);
      }

      if (cfd != nullptr && edit.has_log_number_) {
        cfd->SetLogNumber(edit.log_number_);
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }

      if (edit.has_max_column_family_) {
        column_family_set_->UpdateMaxColumnFamily(edit.max_column_family_);
      }
    }
  }
  file.reset();

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
      printf("no meta-nextfile entry in descriptor");
    } else if (!have_last_sequence) {
      printf("no last-sequence-number entry in descriptor");
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }
  }

  if (s.ok()) {
    for (auto cfd : *column_family_set_) {
      auto builders_iter = builders.find(cfd->GetID());
      assert(builders_iter != builders.end());
      auto builder = builders_iter->second;

      Version* v = new Version(cfd, this, current_version_number_++);
      builder->SaveTo(v);
      std::vector<uint64_t> size_being_compacted(v->NumberLevels() - 1);
      cfd->compaction_picker()->SizeBeingCompacted(size_being_compacted);
      v->ComputeCompactionScore(size_being_compacted);
      v->UpdateFilesBySize();
      delete builder;

      printf("--------------- Column family \"%s\"  (ID %u) --------------\n",
             cfd->GetName().c_str(), (unsigned int)cfd->GetID());
      printf("log number: %lu\n", (unsigned long)cfd->GetLogNumber());
      auto comparator = comparators.find(cfd->GetID());
      if (comparator != comparators.end()) {
        printf("comparator: %s\n", comparator->second.c_str());
      } else {
        printf("comparator: <NO COMPARATOR>\n");
      }
      printf("%s \n", v->DebugString(hex).c_str());
      delete v;
    }

    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    prev_log_number_ = prev_log_number;

    printf(
        "next_file_number %lu last_sequence "
        "%lu  prev_log_number %lu max_column_family %u\n",
        (unsigned long)next_file_number_, (unsigned long)last_sequence,
        (unsigned long)prev_log_number,
        column_family_set_->GetMaxColumnFamily());
  }

  return s;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // WARNING: This method doesn't hold a mutex!!

  bool first_record = false;

  // This is done without DB mutex lock held, but only within single-threaded
  // LogAndApply. Column family manipulations can only happen within LogAndApply
  // (the same single thread), so we're safe to iterate.
  for (auto cfd : *column_family_set_) {
    {
      // Store column family info
      VersionEdit edit;
      if (first_record) {
        edit.SetVersionNumber();
        first_record = false;
      }
      if (cfd->GetID() != 0) {
        // default column family is always there,
        // no need to explicitly write it
        edit.AddColumnFamily(cfd->GetName());
        edit.SetColumnFamily(cfd->GetID());
      }
      edit.SetComparatorName(
          cfd->internal_comparator().user_comparator()->Name());
      std::string record;
      edit.EncodeTo(&record);
      Status s = log->AddRecord(record);
      if (!s.ok()) {
        return s;
      }
    }

    {
      // Save files
      VersionEdit edit;
      edit.SetColumnFamily(cfd->GetID());

      for (int level = 0; level < cfd->NumberLevels(); level++) {
        for (const auto& f : cfd->current()->files_[level]) {
          edit.AddFile(level,
                       f->number,
                       f->file_size,
                       f->smallest,
                       f->largest,
                       f->smallest_seqno,
                       f->largest_seqno);
        }
      }
      edit.SetLogNumber(cfd->GetLogNumber());
      std::string record;
      edit.EncodeTo(&record);
      Status s = log->AddRecord(record);
      if (!s.ok()) {
        return s;
      }
    }
  }

  return Status::OK();
}

// Opens the mainfest file and reads all records
// till it finds the record we are looking for.
bool VersionSet::ManifestContains(uint64_t manifest_file_number,
                                  const std::string& record) const {
  std::string fname =
      DescriptorFileName(dbname_, manifest_file_number);
  Log(options_->info_log, "ManifestContains: checking %s\n", fname.c_str());
  unique_ptr<SequentialFile> file;
  Status s = env_->NewSequentialFile(fname, &file, storage_options_);
  if (!s.ok()) {
    Log(options_->info_log, "ManifestContains: %s\n", s.ToString().c_str());
    Log(options_->info_log,
        "ManifestContains: is unable to reopen the manifest file  %s",
        fname.c_str());
    return false;
  }
  log::Reader reader(std::move(file), nullptr, true/*checksum*/, 0);
  Slice r;
  std::string scratch;
  bool result = false;
  while (reader.ReadRecord(&r, &scratch)) {
    if (r == Slice(record)) {
      result = true;
      break;
    }
  }
  Log(options_->info_log, "ManifestContains: result = %d\n", result ? 1 : 0);
  return result;
}


uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < v->NumberLevels(); level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (v->cfd_->internal_comparator().Compare(files[i]->largest, ikey) <=
          0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (v->cfd_->internal_comparator().Compare(files[i]->smallest,
                                                        ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        TableReader* table_reader_ptr;
        Iterator* iter = v->cfd_->table_cache()->NewIterator(
            ReadOptions(), storage_options_, v->cfd_->internal_comparator(),
            *(files[i]), &table_reader_ptr);
        if (table_reader_ptr != nullptr) {
          result += table_reader_ptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::vector<uint64_t>* live_list) {
  // pre-calculate space requirement
  int64_t total_files = 0;
  for (auto cfd : *column_family_set_) {
    Version* dummy_versions = cfd->dummy_versions();
    for (Version* v = dummy_versions->next_; v != dummy_versions;
         v = v->next_) {
      for (int level = 0; level < v->NumberLevels(); level++) {
        total_files += v->files_[level].size();
      }
    }
  }

  // just one time extension to the right size
  live_list->reserve(live_list->size() + total_files);

  for (auto cfd : *column_family_set_) {
    Version* dummy_versions = cfd->dummy_versions();
    for (Version* v = dummy_versions->next_; v != dummy_versions;
         v = v->next_) {
      for (int level = 0; level < v->NumberLevels(); level++) {
        for (const auto& f : v->files_[level]) {
          live_list->push_back(f->number);
        }
      }
    }
  }
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums =
      c->column_family_data()->options()->verify_checksums_in_compaction;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs(0)->size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs(which)->empty()) {
      if (c->level() + which == 0) {
        for (const auto& file : *c->inputs(which)) {
          list[num++] = c->column_family_data()->table_cache()->NewIterator(
              options, storage_options_compactions_,
              c->column_family_data()->internal_comparator(), *file, nullptr,
              true /* for compaction */);
        }
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(
                c->column_family_data()->internal_comparator(),
                c->inputs(which)),
            &GetFileIterator, c->column_family_data()->table_cache(), options,
            storage_options_, c->column_family_data()->internal_comparator(),
            true /* for compaction */);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(
      env_, &c->column_family_data()->internal_comparator(), list, num);
  delete[] list;
  return result;
}

// verify that the files listed in this compaction are present
// in the current version
bool VersionSet::VerifyCompactionFileConsistency(Compaction* c) {
#ifndef NDEBUG
  Version* version = c->column_family_data()->current();
  if (c->input_version() != version) {
    Log(options_->info_log, "VerifyCompactionFileConsistency version mismatch");
  }

  // verify files in level
  int level = c->level();
  for (int i = 0; i < c->num_input_files(0); i++) {
    uint64_t number = c->input(0,i)->number;

    // look for this file in the current version
    bool found = false;
    for (unsigned int j = 0; j < version->files_[level].size(); j++) {
      FileMetaData* f = version->files_[level][j];
      if (f->number == number) {
        found = true;
        break;
      }
    }
    if (!found) {
      return false; // input files non existant in current version
    }
  }
  // verify level+1 files
  level++;
  for (int i = 0; i < c->num_input_files(1); i++) {
    uint64_t number = c->input(1,i)->number;

    // look for this file in the current version
    bool found = false;
    for (unsigned int j = 0; j < version->files_[level].size(); j++) {
      FileMetaData* f = version->files_[level][j];
      if (f->number == number) {
        found = true;
        break;
      }
    }
    if (!found) {
      return false; // input files non existant in current version
    }
  }
#endif
  return true;     // everything good
}

Status VersionSet::GetMetadataForFile(uint64_t number, int* filelevel,
                                      FileMetaData** meta,
                                      ColumnFamilyData** cfd) {
  for (auto cfd_iter : *column_family_set_) {
    Version* version = cfd_iter->current();
    for (int level = 0; level < version->NumberLevels(); level++) {
      for (const auto& file : version->files_[level]) {
        if (file->number == number) {
          *meta = file;
          *filelevel = level;
          *cfd = cfd_iter;
          return Status::OK();
        }
      }
    }
  }
  return Status::NotFound("File not present in any level");
}

void VersionSet::GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) {
  for (auto cfd : *column_family_set_) {
    for (int level = 0; level < cfd->NumberLevels(); level++) {
      for (const auto& file : cfd->current()->files_[level]) {
        LiveFileMetaData filemetadata;
        filemetadata.name = TableFileName("", file->number);
        filemetadata.level = level;
        filemetadata.size = file->file_size;
        filemetadata.smallestkey = file->smallest.user_key().ToString();
        filemetadata.largestkey = file->largest.user_key().ToString();
        filemetadata.smallest_seqno = file->smallest_seqno;
        filemetadata.largest_seqno = file->largest_seqno;
        metadata->push_back(filemetadata);
      }
    }
  }
}

void VersionSet::GetObsoleteFiles(std::vector<FileMetaData*>* files) {
  files->insert(files->end(), obsolete_files_.begin(), obsolete_files_.end());
  obsolete_files_.clear();
}

ColumnFamilyData* VersionSet::CreateColumnFamily(
    const ColumnFamilyOptions& options, VersionEdit* edit) {
  assert(edit->is_column_family_add_);

  Version* dummy_versions = new Version(nullptr, this);
  auto new_cfd = column_family_set_->CreateColumnFamily(
      edit->column_family_name_, edit->column_family_, dummy_versions, options);

  Version* v = new Version(new_cfd, this, current_version_number_++);

  AppendVersion(new_cfd, v);
  new_cfd->CreateNewMemtable();
  new_cfd->SetLogNumber(edit->log_number_);
  return new_cfd;
}

}  // namespace rocksdb
