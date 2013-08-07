// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <climits>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/merge_operator.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/stop_watch.h"

namespace leveldb {

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
  for (int level = 0; level < vset_->NumberLevels(); level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
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

static Iterator* GetFileIterator(void* arg,
                                 const ReadOptions& options,
                                 const EnvOptions& soptions,
                                 const Slice& file_value,
                                 bool for_compaction) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options,
                              soptions,
                              DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8),
                              nullptr /* don't need reference to table*/,
                              for_compaction);
  }
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            const EnvOptions& soptions,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]),
      &GetFileIterator, vset_->table_cache_, options, soptions);
}

void Version::AddIterators(const ReadOptions& options,
                           const EnvOptions& soptions,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(
        vset_->table_cache_->NewIterator(
            options, soptions, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < vset_->NumberLevels(); level++) {
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
  std::deque<std::string>* merge_operands;  // the merge operations encountered
  Logger* logger;
  bool didIO;    // did we do any disk io?
};
}

// Called from TableCache::Get and InternalGet when file/block in which key may
// exist are not there in TableCache/BlockCache respectively. In this case we
// can't guarantee that key does not exist and are not permitted to do IO to be
// certain.Set the status=kFound and value_found=false to let the caller know
// that key may exist but is not there in memory
static void MarkKeyMayExist(void* arg) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  s->state = kFound;
  if (s->value_found != nullptr) {
    *(s->value_found) = false;
  }
}

static bool SaveValue(void* arg, const Slice& ikey, const Slice& v, bool didIO){
  Saver* s = reinterpret_cast<Saver*>(arg);
  std::deque<std::string>* const ops = s->merge_operands; // shorter alias
  std::string merge_result;  // temporary area for merge results later

  assert(s != nullptr && ops != nullptr);

  ParsedInternalKey parsed_key;
  // TODO: didIO and Merge?
  s->didIO = didIO;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    // TODO: what about corrupt during Merge?
    s->state = kCorrupt;
  } else {
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
            if (!s->merge_operator->Merge(s->user_key, &v, *ops,
                                          s->value, s->logger)) {
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
            if (!s->merge_operator->Merge(s->user_key, nullptr, *ops,
                                          s->value, s->logger)) {
              s->state = kCorrupt;
            }
          } else {
            assert(false);
          }
          return false;

        case kTypeMerge:
          assert(s->state == kNotFound || s->state == kMerge);
          s->state = kMerge;
          ops->push_front(v.ToString());
          while (ops->size() >= 2) {
            // Attempt to merge operands together via user associateive merge
            if (s->merge_operator->PartialMerge(s->user_key,
                                                Slice((*ops)[0]),
                                                Slice((*ops)[1]),
                                                &merge_result,
                                                s->logger)) {
              ops->pop_front();
              swap(ops->front(), merge_result);
            } else {
              // Associative merge returns false ==> stack the operands
              break;
            }
          }
          return true;
      }
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

Version::Version(VersionSet* vset, uint64_t version_number)
    : vset_(vset), next_(this), prev_(this), refs_(0),
      files_by_size_(vset->NumberLevels()),
      next_file_to_compact_by_size_(vset->NumberLevels()),
      file_to_compact_(nullptr),
      file_to_compact_level_(-1),
      compaction_score_(vset->NumberLevels()),
      compaction_level_(vset->NumberLevels()),
      offset_manifest_file_(0),
      version_number_(version_number) {
  files_ = new std::vector<FileMetaData*>[vset->NumberLevels()];
}

void Version::Get(const ReadOptions& options,
                  const LookupKey& k,
                  std::string* value,
                  Status* status,
                  std::deque<std::string>* operands,
                  GetStats* stats,
                  const Options& db_options,
                  const bool no_io,
                  bool* value_found) {
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  auto merge_operator = db_options.merge_operator;
  auto logger = db_options.info_log;

  assert(status->ok() || status->IsMergeInProgress());
  if (no_io) {
    assert(status->ok());
  }
  Saver saver;
  saver.state = status->ok()? kNotFound : kMerge;
  saver.ucmp = ucmp;
  saver.user_key = user_key;
  saver.value_found = value_found;
  saver.value = value;
  saver.merge_operator = merge_operator;
  saver.merge_operands = operands;
  saver.logger = logger.get();
  saver.didIO = false;

  stats->seek_file = nullptr;
  stats->seek_file_level = -1;
  FileMetaData* last_file_read = nullptr;
  int last_file_read_level = -1;

  // We can search level-by-level since entries never hop across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant (unless we
  // are MergeInProgress).
  std::vector<FileMetaData*> important_files;
  for (int level = 0; level < vset_->NumberLevels(); level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Get the list of files to search in this level
    FileMetaData* const* files = &files_[level][0];
    important_files.clear();
    important_files.reserve(num_files);

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
      start_index = FindFile(vset_->icmp_, files_[level], ikey);
    }

    // Traverse the list, finding all overlapping files.
    for (uint32_t i = start_index; i < num_files; i++) {
      FileMetaData* f = files[i];
      if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
          ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
        important_files.push_back(f);
      } else if (level > 0) {
        // If on Level-n (n>=1) then the files are sorted.
        // So we can stop looking when we are past the ikey.
        break;
      }
    }

    if (important_files.empty()) continue;

    if (level == 0) {
      if (vset_->options_->compaction_style == kCompactionStyleUniversal) {
        std::sort(important_files.begin(), important_files.end(), NewestFirstBySeqNo);
      } else {
        std::sort(important_files.begin(), important_files.end(), NewestFirst);
      }
    } else {
      // Sanity check to make sure that the files are correctly sorted
#ifndef NDEBUG
      num_files = important_files.size();
      for (uint32_t i = 1; i < num_files; ++i) {
        FileMetaData* a = important_files[i-1];
        FileMetaData* b = important_files[i];
        int comp_sign = vset_->icmp_.Compare(a->largest, b->smallest);
        assert(comp_sign < 0);
      }
#endif
    }

    // Traverse each relevant file to find the desired key
    num_files = important_files.size();
    for (uint32_t i = 0; i < num_files; ++i) {
      FileMetaData* f = important_files[i];
      bool tableIO = false;
      *status = vset_->table_cache_->Get(options, f->number, f->file_size,
                                         ikey, &saver, SaveValue, &tableIO,
                                         MarkKeyMayExist, no_io);
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
          *status = Status::NotFound(Slice());  // Use empty error message for speed
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
    if (merge_operator->Merge(user_key, nullptr, *saver.merge_operands,
                              value, logger.get())) {
      *status = Status::OK();
    } else {
      *status = Status::Corruption("could not perform end-of-key merge for ",
                                   user_key);
    }
  } else {
    *status = Status::NotFound(Slice()); // Use an empty error message for speed
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

void Version::Ref() {
  ++refs_;
}

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
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
    int max_mem_compact_level = vset_->options_->max_mem_compaction_level;
    while (max_mem_compact_level > 0 && level < max_mem_compact_level) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      if (level + 2 >= vset_->NumberLevels()) {
        level++;
        break;
      }
      GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
      const uint64_t sum = TotalFileSize(overlaps);
      if (sum > vset_->MaxGrandParentOverlapBytes(level)) {
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
void Version::GetOverlappingInputs(
    int level,
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
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
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
  const Comparator* user_cmp = vset_->icmp_.user_comparator();

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

  const Comparator* user_cmp = vset_->icmp_.user_comparator();
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

  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  const std::vector<FileMetaData*>& files = files_[level];
  const size_t kNumFiles = files.size();

  // Check the last file in inputs against the file after it
  size_t last_file = FindFile(vset_->icmp_, files,
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
  size_t first_file = FindFile(vset_->icmp_, files,
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

std::string Version::DebugString(bool hex) const {
  std::string r;
  for (int level = 0; level < vset_->NumberLevels(); level++) {
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
  VersionEdit* edit;

  explicit ManifestWriter(port::Mutex* mu, VersionEdit* e) :
             done(false), cv(mu), edit(e) {}
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

  VersionSet* vset_;
  Version* base_;
  LevelState* levels_;

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base)
      : vset_(vset),
        base_(base) {
    base_->Ref();
    levels_ = new LevelState[vset_->NumberLevels()];
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < vset_->NumberLevels(); level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < vset_->NumberLevels(); level++) {
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
          delete f;
        }
      }
    }
    delete[] levels_;
    base_->Unref();
  }

  void CheckConsistency(Version* v) {
#ifndef NDEBUG
    for (int level = 0; level < vset_->NumberLevels(); level++) {
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i-1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
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

  void CheckConsistencyForDeletes(
    VersionEdit* edit,
    unsigned int number,
    int level) {
#ifndef NDEBUG
      // a file to be deleted better exist in the previous version
      bool found = false;
      for (int l = 0; !found && l < edit->number_levels_; l++) {
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
      for (int l = level+1; !found && l < edit->number_levels_; l++) {
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

    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    const VersionEdit::DeletedFileSet& del = edit->deleted_files_;
    for (VersionEdit::DeletedFileSet::const_iterator iter = del.begin();
         iter != del.end();
         ++iter) {
      const int level = iter->first;
      const uint64_t number = iter->second;
      levels_[level].deleted_files.insert(number);
      CheckConsistencyForDeletes(edit, number, level);
    }

    // Add new files
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
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
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < vset_->NumberLevels(); level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added->size());
      for (FileSet::const_iterator added_iter = added->begin();
           added_iter != added->end();
           ++added_iter) {
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos
                 = std::upper_bound(base_iter, base_end, *added_iter, cmp);
             base_iter != bpos;
             ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, *added_iter);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }
    }
    CheckConsistency(v);
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       const EnvOptions& storage_options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      num_levels_(options_->num_levels),
      dummy_versions_(this),
      current_(nullptr),
      compactions_in_progress_(options_->num_levels),
      current_version_number_(0),
      last_observed_manifest_size_(0),
      storage_options_(storage_options),
      storage_options_compactions_(storage_options_)  {
  compact_pointer_ = new std::string[options_->num_levels];
  Init(options_->num_levels);
  AppendVersion(new Version(this, current_version_number_++));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete[] compact_pointer_;
  delete[] max_file_size_;
  delete[] level_max_bytes_;
}

void VersionSet::Init(int num_levels) {
  max_file_size_ = new uint64_t[num_levels];
  level_max_bytes_ = new uint64_t[num_levels];
  int target_file_size_multiplier = options_->target_file_size_multiplier;
  int max_bytes_multiplier = options_->max_bytes_for_level_multiplier;
  for (int i = 0; i < num_levels; i++) {
    if (i == 0 && options_->compaction_style == kCompactionStyleUniversal) {
      max_file_size_[i] = ULLONG_MAX;
      level_max_bytes_[i] = options_->max_bytes_for_level_base;
    } else if (i > 1) {
      max_file_size_[i] = max_file_size_[i-1] * target_file_size_multiplier;
      level_max_bytes_[i] = level_max_bytes_[i-1] * max_bytes_multiplier *
        options_->max_bytes_for_level_multiplier_additional[i-1];
    } else {
      max_file_size_[i] = options_->target_file_size_base;
      level_max_bytes_[i] = options_->max_bytes_for_level_base;
    }
  }
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    assert(current_->refs_ > 0);
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu,
    bool new_descriptor_log) {
  mu->AssertHeld();

  // queue our request
  ManifestWriter w(mu, edit);
  manifest_writers_.push_back(&w);
  while (!w.done && &w != manifest_writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  std::vector<VersionEdit*> batch_edits;
  Version* v = new Version(this, current_version_number_++);
  Builder builder(this, current_);

  // process all requests in the queue
  ManifestWriter* last_writer = &w;
  assert(!manifest_writers_.empty());
  assert(manifest_writers_.front() == &w);
  std::deque<ManifestWriter*>::iterator iter = manifest_writers_.begin();
  for (; iter != manifest_writers_.end(); ++iter) {
    last_writer = *iter;
    LogAndApplyHelper(&builder, v, last_writer->edit, mu);
    batch_edits.push_back(last_writer->edit);
  }
  builder.SaveTo(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  uint64_t new_manifest_file_size = 0;
  Status s;

  //  No need to perform this check if a new Manifest is being created anyways.
  if (!descriptor_log_ ||
      last_observed_manifest_size_ > options_->max_manifest_file_size) {
    new_descriptor_log = true;
    manifest_file_number_ = NewFileNumber(); // Change manifest file no.
  }

  if (!descriptor_log_ || new_descriptor_log) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(!descriptor_log_ || new_descriptor_log);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    unique_ptr<WritableFile> descriptor_file;
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file,
                              storage_options_);
    if (s.ok()) {
      descriptor_log_.reset(new log::Writer(std::move(descriptor_file)));
      s = WriteSnapshot(descriptor_log_.get());
    }
  }

  // Unlock during expensive MANIFEST log write. New writes cannot get here
  // because &w is ensuring that all new writes get queued.
  {
    // calculate the amount of data being compacted at every level
    std::vector<uint64_t> size_being_compacted(NumberLevels()-1);
    SizeBeingCompacted(size_being_compacted);

    mu->Unlock();

    // The calls to Finalize and UpdateFilesBySize are cpu-heavy
    // and is best called outside the mutex.
    Finalize(v, size_being_compacted);
    UpdateFilesBySize(v);

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      for (unsigned int i = 0; i < batch_edits.size(); i++) {
        batch_edits[i]->EncodeTo(&record);
        s = descriptor_log_->AddRecord(record);
        if (!s.ok()) {
          break;
        }
      }
      if (s.ok()) {
        if (options_->use_fsync) {
          StopWatch sw(env_, options_->statistics, MANIFEST_FILE_SYNC_MICROS);
          s = descriptor_log_->file()->Fsync();
        } else {
          StopWatch sw(env_, options_->statistics, MANIFEST_FILE_SYNC_MICROS);
          s = descriptor_log_->file()->Sync();
        }
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
        if (ManifestContains(record)) {
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
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    // find offset in manifest file where this version is stored.
    new_manifest_file_size = descriptor_log_->file()->GetFileSize();

    mu->Lock();
    // cache the manifest_file_size so that it can be used to rollover in the
    // next call to LogAndApply
    last_observed_manifest_size_ = new_manifest_file_size;
  }

  // Install the new version
  if (s.ok()) {
    v->offset_manifest_file_ = new_manifest_file_size;
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;

  } else {
    Log(options_->info_log, "Error in committing version %ld",
        v->GetVersionNumber());
    delete v;
    if (!new_manifest_file.empty()) {
      descriptor_log_.reset();
      env_->DeleteFile(new_manifest_file);
    }
  }

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

void VersionSet::LogAndApplyHelper(Builder* builder, Version* v,
  VersionEdit* edit, port::Mutex* mu) {
  mu->AssertHeld();

  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  builder->Apply(edit);
}

Status VersionSet::Recover() {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  Log(options_->info_log, "Recovering from manifest file:%s\n",
      current.c_str());

  std::string dscname = dbname_ + "/" + current;
  unique_ptr<SequentialFile> file;
  s = env_->NewSequentialFile(dscname, &file, storage_options_);
  if (!s.ok()) {
    return s;
  }
  uint64_t manifest_file_size;
  s = env_->GetFileSize(dscname, &manifest_file_size);
  if (!s.ok()) {
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(std::move(file), &reporter, true/*checksum*/,
                       0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit(NumberLevels());
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + "does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
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
    }
  }
  file.reset();

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

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this, current_version_number_++);
    builder.SaveTo(v);

    // Install recovered version
    std::vector<uint64_t> size_being_compacted(NumberLevels()-1);
    SizeBeingCompacted(size_being_compacted);
    Finalize(v, size_being_compacted);

    v->offset_manifest_file_ = manifest_file_size;
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    Log(options_->info_log, "Recovered from manifest file:%s succeeded,"
        "manifest_file_number is %ld, next_file_number is %ld, "
        "last_sequence is %ld, log_number is %ld,"
        "prev_log_number is %ld\n",
        current.c_str(), manifest_file_number_, next_file_number_,
        last_sequence_, log_number_, prev_log_number_);
  }

  return s;
}

Status VersionSet::DumpManifest(Options& options, std::string& dscname,
    bool verbose, bool hex) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Open the specified manifest file.
  unique_ptr<SequentialFile> file;
  Status s = options.env->NewSequentialFile(dscname, &file, storage_options_);
  if (!s.ok()) {
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  int count = 0;
  VersionSet::Builder builder(this, current_);

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(std::move(file), &reporter, true/*checksum*/,
                       0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit(NumberLevels());
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + "does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      // Write out each individual edit
      if (verbose) {
        printf("*************************Edit[%d] = %s\n",
                count, edit.DebugString().c_str());
      }
      count++;

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
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
    }
  }
  file.reset();

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
      printf("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
      printf("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      printf("no last-sequence-number entry in descriptor");
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this, 0);
    builder.SaveTo(v);

    // Install recovered version
    std::vector<uint64_t> size_being_compacted(NumberLevels()-1);
    SizeBeingCompacted(size_being_compacted);
    Finalize(v, size_being_compacted);

    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    printf("manifest_file_number %ld next_file_number %ld last_sequence %ld log_number %ld  prev_log_number %ld\n",
           manifest_file_number_, next_file_number_,
           last_sequence, log_number, prev_log_number);
    printf("%s \n", v->DebugString(hex).c_str());
  }

  return s;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v,
  std::vector<uint64_t>& size_being_compacted) {

  double max_score = 0;
  int max_score_level = 0;

  for (int level = 0; level < NumberLevels()-1; level++) {
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
      for (unsigned int i = 0; i < v->files_[level].size(); i++) {
        if (!v->files_[level][i]->being_compacted) {
          numfiles++;
        }
      }

      // If we are slowing down writes, then we better compact that first
      if (numfiles >= options_->level0_stop_writes_trigger) {
        score = 1000000;
        // Log(options_->info_log, "XXX score l0 = 1000000000 max");
      } else if (numfiles >= options_->level0_slowdown_writes_trigger) {
        score = 10000;
        // Log(options_->info_log, "XXX score l0 = 1000000 medium");
      } else {
        score = numfiles /
          static_cast<double>(options_->level0_file_num_compaction_trigger);
        if (score >= 1) {
          // Log(options_->info_log, "XXX score l0 = %d least", (int)score);
        }
      }
    } else {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]) -
                                   size_being_compacted[level];
      score = static_cast<double>(level_bytes) / MaxBytesForLevel(level);
      if (score > 1) {
        // Log(options_->info_log, "XXX score l%d = %d ", level, (int)score);
      }
      if (max_score < score) {
        max_score = score;
        max_score_level = level;
      }
    }
    v->compaction_level_[level] = level;
    v->compaction_score_[level] = score;
  }

  // update the max compaction score in levels 1 to n-1
  v->max_compaction_score_ = max_score;
  v->max_compaction_score_level_ = max_score_level;

  // sort all the levels based on their score. Higher scores get listed
  // first. Use bubble sort because the number of entries are small.
  for (int i = 0; i <  NumberLevels()-2; i++) {
    for (int j = i+1; j < NumberLevels()-1; j++) {
      if (v->compaction_score_[i] < v->compaction_score_[j]) {
        double score = v->compaction_score_[i];
        int level = v->compaction_level_[i];
        v->compaction_score_[i] = v->compaction_score_[j];
        v->compaction_level_[i] = v->compaction_level_[j];
        v->compaction_score_[j] = score;
        v->compaction_level_[j] = level;
      }
    }
  }
}

// A static compator used to sort files based on their size
// In normal mode: descending size
static bool compareSizeDescending(const VersionSet::Fsize& first,
  const VersionSet::Fsize& second) {
  return (first.file->file_size > second.file->file_size);
}
// A static compator used to sort files based on their seqno
// In universal style : descending seqno
static bool compareSeqnoDescending(const VersionSet::Fsize& first,
  const VersionSet::Fsize& second) {
  if (first.file->smallest_seqno > second.file->smallest_seqno) {
    assert(first.file->largest_seqno > second.file->largest_seqno);
    return true;
  }
  assert(first.file->largest_seqno <= second.file->largest_seqno);
  return false;
}

// sort all files in level1 to level(n-1) based on file size
void VersionSet::UpdateFilesBySize(Version* v) {

  // No need to sort the highest level because it is never compacted.
  int max_level = (options_->compaction_style == kCompactionStyleUniversal) ?
                  NumberLevels() : NumberLevels() - 1;

  for (int level = 0; level < max_level; level++) {

    const std::vector<FileMetaData*>& files = v->files_[level];
    std::vector<int>& files_by_size = v->files_by_size_[level];
    assert(files_by_size.size() == 0);

    // populate a temp vector for sorting based on size
    std::vector<Fsize> temp(files.size());
    for (unsigned int i = 0; i < files.size(); i++) {
      temp[i].index = i;
      temp[i].file = files[i];
    }

    // sort the top number_of_files_to_sort_ based on file size
    if (options_->compaction_style == kCompactionStyleUniversal) {
      int num = temp.size();
      std::partial_sort(temp.begin(),  temp.begin() + num,
                        temp.end(), compareSeqnoDescending);
    } else {
      int num = Version::number_of_files_to_sort_;
      if (num > (int)temp.size()) {
        num = temp.size();
      }
      std::partial_sort(temp.begin(),  temp.begin() + num,
                        temp.end(), compareSizeDescending);
    }
    assert(temp.size() == files.size());

    // initialize files_by_size_
    for (unsigned int i = 0; i < temp.size(); i++) {
      files_by_size.push_back(temp[i].index);
    }
    v->next_file_to_compact_by_size_[level] = 0;
    assert(v->files_[level].size() == v->files_by_size_[level].size());
  }
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit(NumberLevels());
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < NumberLevels(); level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < NumberLevels(); level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest,
                   f->smallest_seqno, f->largest_seqno);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < NumberLevels());
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  int len = snprintf(scratch->buffer, sizeof(scratch->buffer), "files[");
  for (int i = 0; i < NumberLevels(); i++) {
    int sz = sizeof(scratch->buffer) - len;
    int ret = snprintf(scratch->buffer + len, sz, "%d ",
        int(current_->files_[i].size()));
    if (ret < 0 || ret >= sz)
      break;
    len += ret;
  }
  snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, "]");
  return scratch->buffer;
}

const char* VersionSet::LevelDataSizeSummary(
    LevelSummaryStorage* scratch) const {
  int len = snprintf(scratch->buffer, sizeof(scratch->buffer), "files_size[");
  for (int i = 0; i < NumberLevels(); i++) {
    int sz = sizeof(scratch->buffer) - len;
    int ret = snprintf(scratch->buffer + len, sz, "%ld ",
        NumLevelBytes(i));
    if (ret < 0 || ret >= sz)
      break;
    len += ret;
  }
  snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, "]");
  return scratch->buffer;
}

const char* VersionSet::LevelFileSummary(
    FileSummaryStorage* scratch, int level) const {
  int len = snprintf(scratch->buffer, sizeof(scratch->buffer), "files_size[");
  for (unsigned int i = 0; i < current_->files_[level].size(); i++) {
    FileMetaData* f = current_->files_[level][i];
    int sz = sizeof(scratch->buffer) - len;
    int ret = snprintf(scratch->buffer + len, sz, "#%ld(seq=%ld,sz=%ld,%d) ",
                       f->number, f->smallest_seqno,
                       f->file_size, f->being_compacted);
    if (ret < 0 || ret >= sz)
      break;
    len += ret;
  }
  snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, "]");
  return scratch->buffer;
}

// Opens the mainfest file and reads all records
// till it finds the record we are looking for.
bool VersionSet::ManifestContains(const std::string& record) const {
  std::string fname = DescriptorFileName(dbname_, manifest_file_number_);
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
  for (int level = 0; level < NumberLevels(); level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
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
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), storage_options_, files[i]->number,
            files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
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
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < NumberLevels(); level++) {
      total_files += v->files_[level].size();
    }
  }

  // just one time extension to the right size
  live_list->reserve(live_list->size() + total_files);

  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < NumberLevels(); level++) {
      for (const auto& f : v->files_[level]) {
        live_list->push_back(f->number);
      }
    }
  }
}

void VersionSet::AddLiveFilesCurrentVersion(std::set<uint64_t>* live) {
  Version* v = current_;
  for (int level = 0; level < NumberLevels(); level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      live->insert(files[i]->number);
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < NumberLevels());
  assert(current_);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  uint64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < NumberLevels() - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level+1, &f->smallest, &f->largest,
                                     &overlaps);
      const uint64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest,
                          InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest,
                           InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(
              options, storage_options_compactions_,
              files[i]->number, files[i]->file_size, nullptr,
              true /* for compaction */);
        }
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options, storage_options_,
            true /* for compaction */);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

double VersionSet::MaxBytesForLevel(int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  assert(level >= 0);
  assert(level < NumberLevels());
  return level_max_bytes_[level];
}

uint64_t VersionSet::MaxFileSizeForLevel(int level) {
  assert(level >= 0);
  assert(level < NumberLevels());
  return max_file_size_[level];
}

uint64_t VersionSet::ExpandedCompactionByteSizeLimit(int level) {
  uint64_t result = MaxFileSizeForLevel(level);
  result *= options_->expanded_compaction_factor;
  return result;
}

uint64_t VersionSet::MaxGrandParentOverlapBytes(int level) {
  uint64_t result = MaxFileSizeForLevel(level);
  result *= options_->max_grandparent_overlap_factor;
  return result;
}

// verify that the files listed in this compaction are present
// in the current version
bool VersionSet::VerifyCompactionFileConsistency(Compaction* c) {
#ifndef NDEBUG
  if (c->input_version_ != current_) {
    Log(options_->info_log, "VerifyCompactionFileConsistency version mismatch");
  }

  // verify files in level
  int level = c->level();
  for (int i = 0; i < c->num_input_files(0); i++) {
    uint64_t number = c->input(0,i)->number;

    // look for this file in the current version
    bool found = false;
    for (unsigned int j = 0; j < current_->files_[level].size(); j++) {
      FileMetaData* f = current_->files_[level][j];
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
    for (unsigned int j = 0; j < current_->files_[level].size(); j++) {
      FileMetaData* f = current_->files_[level][j];
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

// Clear all files to indicate that they are not being compacted
// Delete this compaction from the list of running compactions.
void VersionSet::ReleaseCompactionFiles(Compaction* c, Status status) {
  c->MarkFilesBeingCompacted(false);
  compactions_in_progress_[c->level()].erase(c);
  if (!status.ok()) {
    c->ResetNextCompactionIndex();
  }
}

// The total size of files that are currently being compacted
// at at every level upto the penultimate level.
void VersionSet::SizeBeingCompacted(std::vector<uint64_t>& sizes) {
  for (int level = 0; level < NumberLevels()-1; level++) {
    uint64_t total = 0;
    for (std::set<Compaction*>::iterator it =
         compactions_in_progress_[level].begin();
         it != compactions_in_progress_[level].end();
         ++it) {
      Compaction* c = (*it);
      assert(c->level() == level);
      for (int i = 0; i < c->num_input_files(0); i++) {
        total += c->input(0,i)->file_size;
      }
    }
    sizes[level] = total;
  }
}

Compaction* VersionSet::PickCompactionUniversal(int level, double score) {
  assert (level == 0);

  // percentage flexibilty while comparing file sizes
  uint64_t ratio = options_->compaction_options_universal.size_ratio;
  unsigned int min_merge_width =
    options_->compaction_options_universal.min_merge_width;
  unsigned int max_merge_width =
    options_->compaction_options_universal.max_merge_width;

  if ((current_->files_[level].size() <=
      (unsigned int)options_->level0_file_num_compaction_trigger)) {
    Log(options_->info_log, "Universal: nothing to do\n");
    return nullptr;
  }
  VersionSet::FileSummaryStorage tmp;
  Log(options_->info_log, "Universal: candidate files(%lu): %s\n",
      current_->files_[level].size(),
      LevelFileSummary(&tmp, 0));

  Compaction* c = nullptr;
  c = new Compaction(level, level, MaxFileSizeForLevel(level),
                     LLONG_MAX, NumberLevels());
  c->score_ = score;

  // The files are sorted from newest first to oldest last.
  std::vector<int>& file_by_time = current_->files_by_size_[level];
  FileMetaData* f = nullptr;
  bool done = false;
  assert(file_by_time.size() == current_->files_[level].size());

  unsigned int max_files_to_compact = std::min(max_merge_width, UINT_MAX);

  // Make two pass. The first pass considers a candidate file
  // only if it is smaller than the total size accumulated so far.
  // The second pass does not look at the slope of the
  // file-size  curve to decide what to pick for compaction.
  for (int iter = 0; !done && iter < 2; iter++) {

    for (unsigned int loop = 0; loop < file_by_time.size(); ) {

      // Skip files that are already being compacted
      for (f = nullptr; loop < file_by_time.size(); loop++) {
        int index = file_by_time[loop];
        f = current_->files_[level][index];

        if (!f->being_compacted) {
          break;
        }
        Log(options_->info_log, "Universal: file %ld[%d] being compacted, skipping",
            f->number, loop);
        f = nullptr;
      }

      // This file is not being compacted. Consider it as the
      // first candidate to be compacted.
      unsigned int candidate_count = 1;
      uint64_t candidate_size =  f != nullptr? f->file_size : 0;
      if (f != nullptr) {
        Log(options_->info_log, "Universal: Possible candidate file %ld[%d] %s.",
            f->number, loop, iter == 0? "" : "forced ");
      }

      // Check if the suceeding files need compaction.
      for (unsigned int i = loop+1;
           candidate_count < max_files_to_compact && i < file_by_time.size();
           i++) {
        int index = file_by_time[i];
        FileMetaData* f = current_->files_[level][index];
        if (f->being_compacted) {
          break;
        }
        // If this is the first iteration, then we pick files if the
        // total candidate file size (increased by the specified ratio)
        // is still larger than the next candidate file.
        if (iter == 0) {
          uint64_t sz = (candidate_size * (100 + ratio)) /100;
          if (sz < f->file_size) {
            break;
          }
        }
        candidate_count++;
        candidate_size += f->file_size;
      }

      // Found a series of consecutive files that need compaction.
      if (candidate_count >= (unsigned int)min_merge_width) {
        for (unsigned int i = loop; i < loop + candidate_count; i++) {
          int index = file_by_time[i];
          FileMetaData* f = current_->files_[level][index];
          c->inputs_[0].push_back(f);
          Log(options_->info_log, "Universal: Picking file %ld[%d] with size %ld %s",
              f->number, i, f->file_size,
              (iter == 0 ? "" : "forced"));
        }
        done = true;
        break;
      } else {
        for (unsigned int i = loop;
             i < loop + candidate_count && i < file_by_time.size(); i++) {
         int index = file_by_time[i];
         FileMetaData* f = current_->files_[level][index];
         Log(options_->info_log, "Universal: Skipping file %ld[%d] with size %ld %d %s",
             f->number, i, f->file_size, f->being_compacted,
              (iter == 0 ? "" : "forced"));
        }
      }
      loop += candidate_count;
    }
    assert(done || c->inputs_[0].size() == 0);

    // If we are unable to find a normal compaction run and we are still
    // above the compaction threshold, iterate again to pick compaction
    // candidates, this time without considering their size differences.
    if (!done) {
      int files_not_in_compaction = 0;
      for (unsigned int i = 0; i < current_->files_[level].size(); i++) {
        f = current_->files_[level][i];
        if (!f->being_compacted) {
          files_not_in_compaction++;
        }
      }
      int expected_num_files = files_not_in_compaction +
                compactions_in_progress_[level].size();
      if (expected_num_files <=
          options_->level0_file_num_compaction_trigger + 1) {
        done = true;     // nothing more to do
      } else {
        max_files_to_compact = std::min((int)max_merge_width,
          expected_num_files - options_->level0_file_num_compaction_trigger);
        Log(options_->info_log, "Universal: second loop with maxfiles %d",
            max_files_to_compact);
      }
    }
  }
  if (c->inputs_[0].size() <= 1) {
    Log(options_->info_log, "Universal: only %ld files, nothing to do.\n",
        c->inputs_[0].size());
    delete c;
    return nullptr;
  }

  // validate that all the chosen files are non overlapping in time
  FileMetaData* newerfile __attribute__((unused)) = nullptr;
  for (unsigned int i = 0; i < c->inputs_[0].size(); i++) {
    FileMetaData* f = c->inputs_[0][i];
    assert (f->smallest_seqno <= f->largest_seqno);
    assert(newerfile == nullptr ||
           newerfile->smallest_seqno > f->largest_seqno);
    newerfile = f;
  }

  // update statistics
  if (options_->statistics != nullptr) {
    options_->statistics->measureTime(NUM_FILES_IN_SINGLE_COMPACTION,
                                      c->inputs_[0].size());
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // mark all the files that are being compacted
  c->MarkFilesBeingCompacted(true);

  // remember this currently undergoing compaction
  compactions_in_progress_[level].insert(c);

  return c;
}

Compaction* VersionSet::PickCompactionBySize(int level, double score) {
  Compaction* c = nullptr;

  // level 0 files are overlapping. So we cannot pick more
  // than one concurrent compactions at this level. This
  // could be made better by looking at key-ranges that are
  // being compacted at level 0.
  if (level == 0 && compactions_in_progress_[level].size() == 1) {
    return nullptr;
  }

  assert(level >= 0);
  assert(level+1 < NumberLevels());
  c = new Compaction(level, level+1, MaxFileSizeForLevel(level+1),
      MaxGrandParentOverlapBytes(level), NumberLevels());
  c->score_ = score;

  // Pick the largest file in this level that is not already
  // being compacted
  std::vector<int>& file_size = current_->files_by_size_[level];

  // record the first file that is not yet compacted
  int nextIndex = -1;

  for (unsigned int i = current_->next_file_to_compact_by_size_[level];
       i < file_size.size(); i++) {
    int index = file_size[i];
    FileMetaData* f = current_->files_[level][index];

    // check to verify files are arranged in descending size
    assert((i == file_size.size() - 1) ||
           (i >= Version::number_of_files_to_sort_-1) ||
          (f->file_size >= current_->files_[level][file_size[i+1]]->file_size));

    // do not pick a file to compact if it is being compacted
    // from n-1 level.
    if (f->being_compacted) {
      continue;
    }

    // remember the startIndex for the next call to PickCompaction
    if (nextIndex == -1) {
      nextIndex = i;
    }

    //if (i > Version::number_of_files_to_sort_) {
    //  Log(options_->info_log, "XXX Looking at index %d", i);
    //}

    // Do not pick this file if its parents at level+1 are being compacted.
    // Maybe we can avoid redoing this work in SetupOtherInputs
    int parent_index = -1;
    if (ParentRangeInCompaction(&f->smallest, &f->largest, level,
                                &parent_index)) {
      continue;
    }
    c->inputs_[0].push_back(f);
    c->base_index_ = index;
    c->parent_index_ = parent_index;
    break;
  }

  if (c->inputs_[0].empty()) {
    delete c;
    c = nullptr;
  }

  // store where to start the iteration in the next call to PickCompaction
  current_->next_file_to_compact_by_size_[level] = nextIndex;

  return c;
}

Compaction* VersionSet::PickCompaction() {
  Compaction* c = nullptr;
  int level = -1;

  // Compute the compactions needed. It is better to do it here
  // and also in LogAndApply(), otherwise the values could be stale.
  std::vector<uint64_t> size_being_compacted(NumberLevels()-1);
  current_->vset_->SizeBeingCompacted(size_being_compacted);
  Finalize(current_, size_being_compacted);

  // In universal style of compaction, compact L0 files back into L0.
  if (options_->compaction_style ==  kCompactionStyleUniversal) {
    int level = 0;
    c = PickCompactionUniversal(level, current_->compaction_score_[level]);
    return c;
  }

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  //
  // Find the compactions by size on all levels.
  for (int i = 0; i < NumberLevels()-1; i++) {
    assert(i == 0 || current_->compaction_score_[i] <=
                     current_->compaction_score_[i-1]);
    level = current_->compaction_level_[i];
    if ((current_->compaction_score_[i] >= 1)) {
      c = PickCompactionBySize(level, current_->compaction_score_[i]);
      ExpandWhileOverlapping(c);
      if (c != nullptr) {
        break;
      }
    }
  }

  // Find compactions needed by seeks
  FileMetaData* f = current_->file_to_compact_;
  if (c == nullptr && f != nullptr && !f->being_compacted) {

    level = current_->file_to_compact_level_;
    int parent_index = -1;

    // Only allow one level 0 compaction at a time.
    // Do not pick this file if its parents at level+1 are being compacted.
    if (level != 0 || compactions_in_progress_[0].empty()) {
      if(!ParentRangeInCompaction(&f->smallest, &f->largest, level,
                                  &parent_index)) {
        c = new Compaction(level, level, MaxFileSizeForLevel(level+1),
                MaxGrandParentOverlapBytes(level), NumberLevels(), true);
        c->inputs_[0].push_back(f);
        c->parent_index_ = parent_index;
        current_->file_to_compact_ = nullptr;
        ExpandWhileOverlapping(c);
      }
    }
  }

  if (c == nullptr) {
    return nullptr;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Two level 0 compaction won't run at the same time, so don't need to worry
  // about files on level 0 being compacted.
  if (level == 0) {
    assert(compactions_in_progress_[0].empty());
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    c->inputs_[0].clear();
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);

    // If we include more L0 files in the same compaction run it can
    // cause the 'smallest' and 'largest' key to get extended to a
    // larger range. So, re-invoke GetRange to get the new key range
    GetRange(c->inputs_[0], &smallest, &largest);
    if (ParentRangeInCompaction(&smallest, &largest,
                                level, &c->parent_index_)) {
      delete c;
      return nullptr;
    }
    assert(!c->inputs_[0].empty());
  }


  // Setup "level+1" files (inputs_[1])
  SetupOtherInputs(c);

  // mark all the files that are being compacted
  c->MarkFilesBeingCompacted(true);

  // remember this currently undergoing compaction
  compactions_in_progress_[level].insert(c);

  return c;
}

// Returns true if any one of the parent files are being compacted
bool VersionSet::ParentRangeInCompaction(const InternalKey* smallest,
  const InternalKey* largest, int level, int* parent_index) {
  std::vector<FileMetaData*> inputs;

  current_->GetOverlappingInputs(level+1, smallest, largest,
                                 &inputs, *parent_index, parent_index);
  return FilesInCompaction(inputs);
}

// Returns true if any one of specified files are being compacted
bool VersionSet::FilesInCompaction(std::vector<FileMetaData*>& files) {
  for (unsigned int i = 0; i < files.size(); i++) {
    if (files[i]->being_compacted) {
      return true;
    }
  }
  return false;
}

// Add more files to the inputs on "level" to make sure that
// no newer version of a key is compacted to "level+1" while leaving an older
// version in a "level". Otherwise, any Get() will search "level" first,
// and will likely return an old/stale value for the key, since it always
// searches in increasing order of level to find the value. This could
// also scramble the order of merge operands. This function should be
// called any time a new Compaction is created, and its inputs_[0] are
// populated.
//
// Will set c to nullptr if it is impossible to apply this compaction.
void VersionSet::ExpandWhileOverlapping(Compaction* c) {
  // If inputs are empty then there is nothing to expand.
  if (!c || c->inputs_[0].empty()) {
    return;
  }

  // GetOverlappingInputs will always do the right thing for level-0.
  // So we don't need to do any expansion if level == 0.
  if (c->level() == 0) {
    return;
  }

  const int level = c->level();
  InternalKey smallest, largest;

  // Keep expanding c->inputs_[0] until we are sure that there is a
  // "clean cut" boundary between the files in input and the surrounding files.
  // This will ensure that no parts of a key are lost during compaction.
  int hint_index = -1;
  size_t old_size;
  do {
    old_size = c->inputs_[0].size();
    GetRange(c->inputs_[0], &smallest, &largest);
    c->inputs_[0].clear();
    current_->GetOverlappingInputs(level, &smallest, &largest, &c->inputs_[0],
                                   hint_index, &hint_index);
  } while(c->inputs_[0].size() > old_size);

  // Get the new range
  GetRange(c->inputs_[0], &smallest, &largest);

  // If, after the expansion, there are files that are already under
  // compaction, then we must drop/cancel this compaction.
  int parent_index = -1;
  if (FilesInCompaction(c->inputs_[0]) ||
      ParentRangeInCompaction(&smallest, &largest, level, &parent_index)) {
    c->inputs_[0].clear();
    c->inputs_[1].clear();
    delete c;
    c = nullptr;
  }
}

// Populates the set of inputs from "level+1" that overlap with "level".
// Will also attempt to expand "level" if that doesn't expand "level+1"
// or cause "level" to include a file for compaction that has an overlapping
// user-key with another file.
void VersionSet::SetupOtherInputs(Compaction* c) {
  // If inputs are empty, then there is nothing to expand.
  if (c->inputs_[0].empty()) {
    return;
  }

  const int level = c->level();
  InternalKey smallest, largest;

  // Get the range one last time.
  GetRange(c->inputs_[0], &smallest, &largest);

  // Populate the set of next-level files (inputs_[1]) to include in compaction
  current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1],
                                 c->parent_index_, &c->parent_index_);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can further grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up. We also choose NOT
  // to expand if this would cause "level" to include some entries for some
  // user key, while excluding other entries for the same user key. This
  // can happen when one user key spans multiple files.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0,
                                   c->base_index_, nullptr);
    const uint64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const uint64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const uint64_t expanded0_size = TotalFileSize(expanded0);
    uint64_t limit = ExpandedCompactionByteSizeLimit(level);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size < limit &&
        !FilesInCompaction(expanded0) &&
        !current_->HasOverlappingUserKey(&expanded0, level)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level+1, &new_start, &new_limit,
                                     &expanded1, c->parent_index_,
                                     &c->parent_index_);
      if (expanded1.size() == c->inputs_[1].size() &&
          !FilesInCompaction(expanded1)) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level,
            int(c->inputs_[0].size()),
            int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size),
            int(expanded0.size()),
            int(expanded1.size()),
            long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < NumberLevels()) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  if (false) {
    Log(options_->info_log, "Compacting %d '%s' .. '%s'",
        level,
        smallest.DebugString().c_str(),
        largest.DebugString().c_str());
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_->SetCompactPointer(level, largest);
}

Compaction* VersionSet::CompactRange(
    int level,
    const InternalKey* begin,
    const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(level) *
                         options_->source_compaction_factor;
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); ++i) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }
  int out_level = (options_->compaction_style == kCompactionStyleUniversal) ?
                  level : level+1;

  Compaction* c = new Compaction(level, out_level, MaxFileSizeForLevel(out_level),
    MaxGrandParentOverlapBytes(level), NumberLevels());

  c->inputs_[0] = inputs;
  ExpandWhileOverlapping(c);
  if (c == nullptr) {
    Log(options_->info_log, "Could not compact due to expansion failure.\n");
    return nullptr;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();
  SetupOtherInputs(c);

  // These files that are to be manaully compacted do not trample
  // upon other files because manual compactions are processed when
  // the system has a max of 1 background compaction thread.
  c->MarkFilesBeingCompacted(true);
  return c;
}

Compaction::Compaction(int level, int out_level, uint64_t target_file_size,
  uint64_t max_grandparent_overlap_bytes, int number_levels,
  bool seek_compaction)
    : level_(level),
      out_level_(out_level),
      max_output_file_size_(target_file_size),
      maxGrandParentOverlapBytes_(max_grandparent_overlap_bytes),
      input_version_(nullptr),
      number_levels_(number_levels),
      seek_compaction_(seek_compaction),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0),
      base_index_(-1),
      parent_index_(-1),
      score_(0),
      level_ptrs_(std::vector<size_t>(number_levels)) {
  edit_ = new VersionEdit(number_levels_);
  for (int i = 0; i < number_levels_; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  delete edit_;
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

bool Compaction::IsTrivialMove() const {
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 &&
          num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <= maxGrandParentOverlapBytes_);
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->DeleteFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < number_levels_; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    for (; level_ptrs_[lvl] < files.size(); ) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &input_version_->vset_->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
      icmp->Compare(internal_key,
                    grandparents_[grandparent_index_]->largest.Encode()) > 0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    assert(grandparent_index_ + 1 >= grandparents_.size() ||
           icmp->Compare(grandparents_[grandparent_index_]->largest.Encode(),
                         grandparents_[grandparent_index_+1]->smallest.Encode())
                         < 0);
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > maxGrandParentOverlapBytes_) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

// Mark (or clear) each file that is being compacted
void Compaction::MarkFilesBeingCompacted(bool value) {
  for (int i = 0; i < 2; i++) {
    std::vector<FileMetaData*> v = inputs_[i];
    for (unsigned int j = 0; j < inputs_[i].size(); j++) {
      assert(value ? !inputs_[i][j]->being_compacted :
                      inputs_[i][j]->being_compacted);
      inputs_[i][j]->being_compacted = value;
    }
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

void Compaction::ResetNextCompactionIndex() {
  input_version_->ResetNextCompactionIndex(level_);
}

static void InputSummary(std::vector<FileMetaData*>& files,
    char* output,
    int len) {
  int write = 0;
  for (unsigned int i = 0; i < files.size(); i++) {
    int sz = len - write;
    int ret = snprintf(output + write, sz, "%lu(%lu) ",
        files.at(i)->number,
        files.at(i)->file_size);
    if (ret < 0 || ret >= sz)
      break;
    write += ret;
  }
}

void Compaction::Summary(char* output, int len) {
  int write = snprintf(output, len,
      "Base version %ld Base level %d, seek compaction:%d, inputs:",
      input_version_->GetVersionNumber(), level_, seek_compaction_);
  if (write < 0 || write > len) {
    return;
  }

  char level_low_summary[100];
  InputSummary(inputs_[0], level_low_summary, sizeof(level_low_summary));
  char level_up_summary[100];
  if (inputs_[1].size()) {
    InputSummary(inputs_[1], level_up_summary, sizeof(level_up_summary));
  } else {
    level_up_summary[0] = '\0';
  }

  snprintf(output + write, len - write, "[%s],[%s]",
      level_low_summary, level_up_summary);
}

}  // namespace leveldb
