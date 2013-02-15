// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size() && files[i]; i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

static int64_t TotalFileSize(const std::vector<FileGroup*>& file_groups) {
  int64_t sum = 0;
  for (size_t i = 0; i < file_groups.size() && file_groups[i]; i++) {
    sum += file_groups[i]->total_file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Free FileGroups
  for (int level = 0; level < vset_->NumberLevels(); level++) {
    for (size_t i = 0; i < file_groups_[level].size(); i++) {
      delete file_groups_[level][i];
    }
  }
  delete[] file_groups_;

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
  // NULL user_key occurs before all keys and is therefore never after *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp,
                       const Slice* user_key, const FileMetaData* f) {
  // NULL user_key occurs after all keys and is therefore never before *f
  return (user_key != NULL &&
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
  if (smallest_user_key != NULL) {
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

Iterator* FileGroup::NewIterator(TableCache* cache,
                                 const InternalKeyComparator* icmp,
                                 const ReadOptions& options) {
  if (files.size() == 1) {
    Iterator* iter = cache->NewIterator(options, files[0]->number,
                                        files[0]->file_size);
    return iter;
  }

  Iterator** iters = new Iterator*[files.size()];
  for (size_t i = 0; i < files.size(); ++i) {
    iters[i] = cache->NewIterator(options, files[i]->number,
                                  files[i]->file_size);
  }
  Iterator* iter = NewMergingIterator(icmp, iters, files.size());
  delete[] iters;
  return iter;
}

namespace {
// Represents either the start or end of a file. "start" indicates which key we
// use of "file".
struct EndPoint {
  FileMetaData* file;
  bool start; // If true we use the first key; otherwise, the last.

  EndPoint(FileMetaData* file, bool start)
    : file(file), start(start) {
  }
};
// Comparator to sort end points. Sort by the key of two end points. If a two
// endpoints have the same key and one is a start and the other an end end
// point, then the end end point is ordered before the start end point.
struct BySmallestEndPoint {
  const InternalKeyComparator* internal_comparator;

  BySmallestEndPoint(const InternalKeyComparator* internal_comparator)
    : internal_comparator(internal_comparator) {
  }

  bool operator()(const EndPoint& a, const EndPoint& b) {
    const InternalKey& a_key = a.start?a.file->smallest:a.file->largest;
    const InternalKey& b_key = b.start?b.file->smallest:b.file->largest;

    int r = internal_comparator->Compare(a_key, b_key);

    // if a_key < b_key or else if a_key == b_key and "a" is an start point and
    // "b" is a end point then "a" comes before "b".
    return (r < 0) || (r == 0 && a.start && !b.start);
  }
};
}  // namespace
void GroupFiles(
    const InternalKeyComparator* internal_comparator,
    const std::vector<FileMetaData*>& files,
    std::vector<FileGroup*>& file_groups) {
  file_groups.clear();

  // Store and sort the end points of the files.
  std::vector<EndPoint> end_points;
  end_points.reserve(files.size() * 2);
  for (size_t i = 0; i < files.size(); ++i) {
    end_points.push_back(EndPoint(files[i], false));
    end_points.push_back(EndPoint(files[i], true));
  }
  std::sort(end_points.begin(), end_points.end(),
            BySmallestEndPoint(internal_comparator));

  // Loop through end points finding groups of overlapping files.
  size_t cur_unended_files = 0; // Number of files of which we've seen the
                                // start point, but not the end point.
  for (size_t i = 0; i < end_points.size(); ++i) {
    FileGroup* cur_group;
    if (end_points[i].start) {
      if (cur_unended_files == 0) {
        // Create a new group.
        cur_group = new FileGroup();
        file_groups.push_back(cur_group);

        // First file in this group, so update the smallest key.
        cur_group->smallest = end_points[i].file->smallest;
      } else {
        cur_group = file_groups[file_groups.size()-1];
      }

      // Add files to group when we first encounter them.
      cur_group->files.push_back(end_points[i].file);
      cur_group->total_file_size += end_points[i].file->file_size;

      ++cur_unended_files;
    } else {
      --cur_unended_files;
      assert(cur_unended_files >= 0);
      if (cur_unended_files == 0) {
        // No more files are going to overlap with current group so set the
        // largest key in the group, cause we are either done or going to start
        // a new group the next iteration.

        cur_group = file_groups[file_groups.size()-1];
        cur_group->largest = end_points[i].file->largest;
      }
    }
  }
}

size_t FindGroup(const InternalKeyComparator& icmp,
                 const std::vector<FileGroup*>& groups,
                 const Slice& key) {
  size_t left = 0;
  size_t right = groups.size();
  while (left < right) {
    size_t mid = (left + right) / 2;
    const FileGroup* g = groups[mid];
    if (icmp.InternalKeyComparator::Compare(g->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // groups at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all groups
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

bool SomeGroupOverlapsRange(
    const InternalKeyComparator& icmp,
    const std::vector<FileGroup*>& groups,
    const Slice* smallest_user_key,
    const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();

  // Binary search over group list
  uint32_t index = 0;
  if (smallest_user_key != NULL) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small(*smallest_user_key, kMaxSequenceNumber,
                      kValueTypeForSeek);
    index = FindGroup(icmp, groups, small.Encode());
  }

  if (index >= groups.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return (largest_user_key == NULL ||
          ucmp->Compare(*largest_user_key,
                        groups[index]->smallest.user_key()) >= 0);
}

namespace {
// An internal iterator. For a given list of groups, yields information about
// those groups. For a given entry, key() is the largest key that occurs in the
// group, and value() is an 8-byte value containing the index of the group in
// the list, encoded using EncodeFixed64.
class LevelGroupIdxIterator : public Iterator {
 public:
  LevelGroupIdxIterator(const InternalKeyComparator& icmp,
                        const std::vector<FileGroup*>& groups)
      : icmp_(icmp),
        groups_(groups),
        index_(groups_.size()) {        // Marks as invalid
    assert(!groups.empty());
  }
  virtual bool Valid() const {
    return index_ < groups_.size();
  }
  virtual void Seek(const Slice& target) {
    index_ = FindGroup(icmp_, groups_, target);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = groups_.empty() ? 0 : groups_.size() - 1;
  }
  virtual void Next() {
    assert(Valid());
    index_++;
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = groups_.size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const {
    assert(Valid());
    return groups_[index_]->largest.Encode();
  }
  Slice value() const {
    assert(Valid());
    EncodeFixed64(value_buf_, index_);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }
 private:
  const InternalKeyComparator icmp_;
  uint64_t level_;
  const std::vector<FileGroup*>& groups_;
  size_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[8];
};

// Struct that contains all the information that GetGroupIterator() needs to
// function.
struct GetGroupIteratorArg {
  const InternalKeyComparator* icmp;
  TableCache* cache;
  const std::vector<FileGroup*>& groups;

  GetGroupIteratorArg(const InternalKeyComparator* icmp,
                      TableCache* cache,
                      const std::vector<FileGroup*>& groups)
    : icmp(icmp), cache(cache), groups(groups) {
  }

  // Function that gets passed to an Iterator in order to free an instance of
  // this class.
  static void IterDelete(void* arg1, void*) {
    GetGroupIteratorArg* ggia = reinterpret_cast<GetGroupIteratorArg*>(arg1);
    delete ggia;
  }
};
// Returns an iterator over the specifided group.
static Iterator* GetGroupIterator(void* arg,
                                  const ReadOptions& options,
                                  const Slice& index) {
  GetGroupIteratorArg* ggia = reinterpret_cast<GetGroupIteratorArg*>(arg);
  Iterator* iter;
  if (index.size() != 8) {
    iter = NewErrorIterator(
        Status::Corruption("GetGroupIterator() invoked with unexpected index"));
  } else {
    uint64_t iindex = DecodeFixed64(index.data());
    assert(iindex < ggia->groups.size());
    iter = ggia->groups[iindex]->NewIterator(ggia->cache, ggia->icmp, options);
  }
  return iter;
}

// Returns an iterator that concatenates together the given groups.
// REQUIRES: The groups are sorted in order and are non-overlapping
Iterator* NewGroupsConcatenatingIterator(
        const ReadOptions& options,
        TableCache* cache,
        const InternalKeyComparator* icmp,
        const std::vector<FileGroup*>& groups) {
  assert(!groups.empty());

  GetGroupIteratorArg* ggia = new GetGroupIteratorArg(icmp, cache, groups);
  Iterator* iter = NewTwoLevelIterator(
      new LevelGroupIdxIterator(*icmp, groups),
      &GetGroupIterator, ggia, options);
  iter->RegisterCleanup(&GetGroupIteratorArg::IterDelete, ggia, NULL);

  return iter;
}
}  // namespace

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(
        vset_->table_cache_->NewIterator(
            options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping groups in the level, opening them
  // lazily.
  for (int level = 1; level < vset_->NumberLevels(); level++) {
    if (!file_groups_[level].empty()) {
      iters->push_back(NewGroupsConcatenatingIterator(options,
                                                      vset_->table_cache_,
                                                      &vset_->icmp_,
                                                      file_groups_[level]));
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
};
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
  SequenceNumber seq;
  bool didIO;    // did we do any disk io?
};
}
static void SaveValue(void* arg, const Slice& ikey, const Slice& v, bool didIO){
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  s->didIO = didIO;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
        s->seq = parsed_key.sequence;
      }
    }
  }
}


static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

Version::Version(VersionSet* vset, uint64_t version_number)
    : vset_(vset), next_(this), prev_(this), refs_(0),
      files_by_size_(vset->NumberLevels()),
      next_file_to_compact_by_size_(vset->NumberLevels()),
      file_to_compact_(NULL),
      file_to_compact_level_(-1),
      compaction_score_(vset->NumberLevels()),
      compaction_level_(vset->NumberLevels()),
      offset_manifest_file_(0),
      version_number_(version_number) {
  files_ = new std::vector<FileMetaData*>[vset->NumberLevels()];
  file_groups_ = new std::vector<FileGroup*>[vset->NumberLevels()]();
}

Status Version::Get(const ReadOptions& options,
                    const LookupKey& k,
                    std::string* value,
                    GetStats* stats,
                    bool short_circuit) {
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  Status s;

  stats->seek_file = NULL;
  stats->seek_file_level = -1;
  FileMetaData* last_file_read = NULL;
  int last_file_read_level = -1;

  // We can search level-by-level since entries never hop across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant.
  std::vector<FileMetaData*> files;
  for (int level = 0; level < vset_->NumberLevels(); level++) {
    size_t num_groups = file_groups_[level].size();
    if (files_[level].size() == 0) continue;
    assert(num_groups > 0);

    const std::vector<FileGroup*>& groups = file_groups_[level];
    size_t index = FindGroup(vset_->icmp_, groups, ikey);
    if (index >= num_groups) {
      continue;
    } else if (ucmp->Compare(user_key,
                             groups[index]->smallest.user_key()) >= 0) {
      // Although user keys can span multiple groups we don't have to worry
      // about checking the next group as we are only interested in the user
      // key with the greatest sequence number and as internal keys with the same
      // user key are sorted in order of decreasing sequence number, this group
      // is guaranteed to contain the user key with the largest sequence
      // number.

      for (size_t i = 0; i < groups[index]->files.size(); i++) {
        FileMetaData* f = file_groups_[level][index]->files[i];

        if (i != 0 && ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
          // We know the user key is greater than the smallest key of the first
          // file (which is also the smallest key of the group) so we avoid
          // calling the comparator.
          // If the smallest key in the current file is greater than the user
          // key, then no other file will overlap with the key so we can stop.
          break;
        }

        if (ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
          files.push_back(f);
        }
      }
    }

    if (files.empty()) continue;

    // Sort files since in level 0 we can simply look starting from the newest
    // file.
    if (level == 0 || short_circuit) {
      std::sort(files.begin(), files.end(), NewestFirst);
    }

    Saver saver;
    saver.ucmp = ucmp;
    std::string saved_value;
    SequenceNumber saved_seq = 0;
    SaverState saved_state = kNotFound;

    for (uint32_t i = 0; i < files.size(); ++i) {

      FileMetaData* f = files[i];
      saver.state = kNotFound;
      saver.ucmp = ucmp;
      saver.user_key = user_key;
      saver.value = value;
      saver.didIO = false;
      bool tableIO = false;
      s = vset_->table_cache_->Get(options, f->number, f->file_size,
                                   ikey, &saver, SaveValue, &tableIO);
      if (!s.ok()) {
        return s;
      }

      if (last_file_read != NULL && stats->seek_file == NULL) {
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
          if (level == 0 || short_circuit || files.size() == 1) {
            return s; // no need to look at other files
          }

          if (saver.seq >= saved_seq) { // save most recent
            saved_value = *saver.value;
            saved_seq = saver.seq;
            saved_state = kFound;
          }
          break; // keep searching in other files
        case kDeleted:
          if (level == 0 || short_circuit || files.size() == 1) {
            s = Status::NotFound(Slice());  // Use empty error message for speed
            return s;
          }

          if (saver.seq >= saved_seq) { // save most recent
            saved_seq = saver.seq;
            saved_state = kNotFound;
          }
          break;
        case kCorrupt:
          s = Status::Corruption("corrupted key for ", user_key);
          return s;
      }
    }

    assert(saved_state == kNotFound || files.size() > 1);
    assert(s.ok());
    switch (saved_state) {
      case kFound:
        *value = saved_value;
        return s;
      case kDeleted:
        s = Status::NotFound(Slice());// Use empty error message for speed
        return s;
      default:
        assert(saved_state == kNotFound);
    }
    files.clear();
  }

  return Status::NotFound(Slice());  // Use an empty error message for speed
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != NULL) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == NULL &&
        stats.seek_file_level < vset_->NumberLevels()-1) {
      // We don't allow seek compactions to happen on the level with the
      // greatest number.
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
  return SomeGroupOverlapsRange(vset_->icmp_, file_groups_[level],
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
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > vset_->MaxGrandParentOverlapBytes(level)) {
        break;
      }
      level++;
    }
  }

  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
// Including all files that overlap files in those range, files that overlap
// those files in turn and so on.
void Version::GetOverlappingInputs(
    int level,
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < vset_->NumberLevels());
  inputs->clear();

  const std::vector<FileGroup*>& groups = file_groups_[level];

  Slice user_begin, user_end;
  size_t start_index = 0;
  if (begin != NULL) {
    user_begin = begin->user_key();
    start_index = FindGroup(vset_->icmp_, groups, begin->Encode());
  }
  if (end != NULL) {
    user_end = end->user_key();
  }

  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  while (start_index < groups.size()) {
    const FileGroup* group = groups[start_index];

    // If the current group is larger than the end range, stop searching.
    if (end != NULL &&
        user_cmp->Compare(group->smallest.user_key(), user_end) > 0) {
      break;
    }

    inputs->insert(inputs->end(), group->files.begin(), group->files.end());

    ++start_index;
  }
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

void Version::RebuildGroups(int level) {
  for (size_t i = 0; i < file_groups_[level].size(); ++i) {
    delete file_groups_[level][i];
  }
  GroupFiles(&vset_->icmp_, files_[level], file_groups_[level]);
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
#endif
  }

  void CheckConsistencyForDeletes(VersionEdit* edit, int number, int level) {
#ifndef NDEBUG
      // a file to be deleted better exist in the previous version
      bool found = false;
      for (int l = 0; !found && l < edit->number_levels_; l++) {
        const std::vector<FileMetaData*>& base_files = base_->files_[l];
        for (int i = 0; i < base_files.size(); i++) {
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

      v->RebuildGroups(level);
    }
    CheckConsistency(v);
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
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
      current_(NULL),
      compactions_in_progress_(options_->num_levels),
      current_version_number_(0),
      last_observed_manifest_size_(0) {
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
    if (i > 1) {
      max_file_size_[i] = max_file_size_[i-1] * target_file_size_multiplier;
      level_max_bytes_[i] = level_max_bytes_[i-1] * max_bytes_multiplier;
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
  if (current_ != NULL) {
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
  if (last_observed_manifest_size_ > options_->max_manifest_file_size) {
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
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file);
    if (s.ok()) {
      descriptor_log_.reset(new log::Writer(std::move(descriptor_file)));
      s = WriteSnapshot(descriptor_log_.get());
    }
  }

  // Unlock during expensive MANIFEST log write. New writes cannot get here
  // because &w is ensuring that all new writes get queued.
  {
    mu->Unlock();

    // The calles to Finalize and UpdateFilesBySize are cpu-heavy
    // and is best called outside the mutex.
    Finalize(v);
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
          s = descriptor_log_->file()->Fsync();
        } else {
          s = descriptor_log_->file()->Sync();
        }
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
        if (ManifestContains(record)) {
          Log(options_->info_log,
              "MANIFEST contains log record despite error; advancing to new "
              "version to prevent mismatch between in-memory and logged state");
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
  s = env_->NewSequentialFile(dscname, &file);
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
    Finalize(v);
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
  Status s = options.env->NewSequentialFile(dscname, &file);
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
    Finalize(v);
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

void VersionSet::Finalize(Version* v) {

  double max_score = 0;
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
                                   SizeBeingCompacted(level);
      score = static_cast<double>(level_bytes) / MaxBytesForLevel(level);
      if (score > 1) {
        // Log(options_->info_log, "XXX score l%d = %d ", level, (int)score);
      }
      if (max_score < score) {
        max_score = score;
      }
    }
    v->compaction_level_[level] = level;
    v->compaction_score_[level] = score;
  }

  // update the max compaction score in levels 1 to n-1
  v->max_compaction_score_ = max_score;

  // sort all the levels based on their score. Higher scores get listed
  // first. Use bubble sort because the number of entries are small.
  for(int i = 0; i <  NumberLevels()-2; i++) {
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

// a static compator used to sort files based on their size
static bool compareSize(const VersionSet::Fsize& first,
  const VersionSet::Fsize& second) {
  return (first.file->file_size > second.file->file_size);
}

// sort all files in level1 to level(n-1) based on file size
void VersionSet::UpdateFilesBySize(Version* v) {

  // No need to sort the highest level because it is never compacted.
  for (int level = 0; level < NumberLevels()-1; level++) {

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
    int num = Version::number_of_files_to_sort_;
    if (num > (int)temp.size()) {
      num = temp.size();
    }
    std::partial_sort(temp.begin(),  temp.begin() + num,
                      temp.end(), compareSize);
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
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
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

// Opens the mainfest file and reads all records
// till it finds the record we are looking for.
bool VersionSet::ManifestContains(const std::string& record) const {
  std::string fname = DescriptorFileName(dbname_, manifest_file_number_);
  Log(options_->info_log, "ManifestContains: checking %s\n", fname.c_str());
  unique_ptr<SequentialFile> file;
  Status s = env_->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    Log(options_->info_log, "ManifestContains: %s\n", s.ToString().c_str());
    return false;
  }
  log::Reader reader(std::move(file), NULL, true/*checksum*/, 0);
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

uint64_t FileGroup::ApproximateOffsetOf(const VersionSet* vset,
                                        const InternalKey& ikey) {
  // ikey is before FileGroup so no overlap occurs.
  if (vset->icmp_.Compare(ikey, smallest) < 0) return 0;
  // Entire FileGroup is after ikey so just use total size.
  if (vset->icmp_.Compare(largest, ikey) <= 0) return total_file_size;

  uint64_t result = 0;
  for (size_t i = 0; i < files.size(); i++) {
    if (vset->icmp_.Compare(files[i]->largest, ikey) <= 0) {
      // Entire file is before "ikey", so just add the file size
      result += files[i]->file_size;
    } else if (vset->icmp_.Compare(files[i]->smallest, ikey) > 0) {
      // Entire file is after "ikey", so ignore

      // files are sorted by start point so no more overlaps will occur.
      break;
    } else {
      // "ikey" falls in the range for this table.  Add the
      // approximate offset of "ikey" within the table.
      Table* tableptr;
      Iterator* iter = vset->table_cache_->NewIterator(
          ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
      if (tableptr != NULL) {
        result += tableptr->ApproximateOffsetOf(ikey.Encode());
      }
      delete iter;
    }
  }
  return result;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < NumberLevels(); level++) {
    const std::vector<FileGroup*>& groups = v->file_groups_[level];
    for (size_t i = 0; i < groups.size(); i++) {
      // We can use FileGroup's ApproximateOffsetOf as it is efficient.
      uint64_t group_offset = groups[i]->ApproximateOffsetOf(this, ikey);
      result += group_offset;

      if (group_offset == 0) {
        // Since groups are non-empty we are guaranteed other groups after this
        // one won't contain the key, as either the key falls before the
        // current group or it is the first key in the group.
        //
        // So we can stop looping.
        break;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < NumberLevels(); level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
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
  if(current_ && level < (int)current_->files_->size())
    return TotalFileSize(current_->files_[level]);
  else
    return 0;
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < NumberLevels() - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level+1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
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

namespace {
void DeleteFileGroups(void* arg1, void* arg2) {
  std::vector<FileGroup*>* groups =
    reinterpret_cast<std::vector<FileGroup*>*>(arg1);

  for (size_t i = 0; i < groups->size(); ++i) {
    delete (*groups)[i];
  }
  delete groups;
}
}
Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // We construct groups of the input files, one for each level, then we merge
  // these iterators together.
  const int space = 2;
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      std::vector<FileGroup*>* groups = new std::vector<FileGroup*>();
      GroupFiles(&icmp_, c->inputs_[which], *groups);
      list[num] = NewGroupsConcatenatingIterator(options, table_cache_,
                                                 &icmp_, *groups);
      list[num]->RegisterCleanup(&DeleteFileGroups, groups, NULL);
      ++num;
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

int64_t VersionSet::ExpandedCompactionByteSizeLimit(int level) {
  uint64_t result = MaxFileSizeForLevel(level);
  result *= options_->expanded_compaction_factor;
  return result;
}

int64_t VersionSet::MaxGrandParentOverlapBytes(int level) {
  uint64_t result = MaxFileSizeForLevel(level);
  result *= options_->max_grandparent_overlap_factor;
  return result;
}

// verify that the files listed in this compaction are present
// in the current version
bool VersionSet::VerifyCompactionFileConsistency(Compaction* c) {
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
uint64_t VersionSet::SizeBeingCompacted(int level) {
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
  return total;
}

Compaction* VersionSet::PickCompactionBySize(int level, double score) {
  Compaction* c = NULL;

  // level 0 files are overlapping. So we cannot pick more
  // than one concurrent compactions at this level. This
  // could be made better by looking at key-ranges that are
  // being compacted at level 0.
  if (level == 0 && compactions_in_progress_[level].size() == 1) {
    return NULL;
  }

  assert(level >= 0);
  assert(level+1 < NumberLevels());
  c = new Compaction(level, MaxFileSizeForLevel(level),
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
    if (ParentRangeInCompaction(&f->smallest, &f->largest, level)) {
      continue;
    }
    c->inputs_[0].push_back(f);
    break;
  }

  if (c->inputs_[0].empty()) {
    delete c;
    c = NULL;
  }

  // store where to start the iteration in the next call to PickCompaction
  current_->next_file_to_compact_by_size_[level] = nextIndex;

  return c;
}

Compaction* VersionSet::PickCompaction() {
  Compaction* c = NULL;
  int level = -1;

  // compute the compactions needed. It is better to do it here
  // and also in LogAndApply(), otherwise the values could be stale.
  Finalize(current_);

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
      if (c != NULL) {
        break;
      }
    }
  }

  // Find compactions needed by seeks
  if (c == NULL && (current_->file_to_compact_ != NULL)) {
    level = current_->file_to_compact_level_;

    // Only allow one level 0 compaction at a time.
    if (level != 0 || compactions_in_progress_[0].empty()) {
      c = new Compaction(level, MaxFileSizeForLevel(level),
      MaxGrandParentOverlapBytes(level), NumberLevels(), true);
      c->inputs_[0].push_back(current_->file_to_compact_);
    }
  }

  if (c == NULL) {
    return NULL;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files may overlap each other, so pick up all overlapping ones
  {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    c->inputs_[0].clear();
    current_->GetOverlappingInputs(level, &smallest, &largest, &c->inputs_[0]);
    if (ParentRangeInCompaction(&smallest, &largest, level)) {
      delete c;
      return NULL;
    }
    for (unsigned int i = 0; i < c->inputs_[0].size(); i++) {
      FileMetaData* f = c->inputs_[0][i];
      if (f->being_compacted) {
        delete c;
        return NULL;
      }
    }
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);

  // mark all the files that are being compacted
  c->MarkFilesBeingCompacted(true);

  // remember this currently undergoing compaction
  compactions_in_progress_[level].insert(c);

  return c;
}

// Returns true if any one of the parent files are being compacted
bool VersionSet::ParentRangeInCompaction(const InternalKey* smallest,
  const InternalKey* largest, int level) {
  assert(level >= 0);
  assert(level < NumberLevels()-1);
  std::vector<FileMetaData*> inputs;

  current_->GetOverlappingInputs(level+1, smallest, largest,
                                 &inputs);
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

void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  assert(level >= 0);
  assert(level < NumberLevels()-1);

  InternalKey smallest, largest;
  GetRange(c->inputs_[0], &smallest, &largest);

  current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    int64_t limit = ExpandedCompactionByteSizeLimit(level);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size < limit &&
        !FilesInCompaction(expanded0)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level+1, &new_start, &new_limit,
                                     &expanded1);
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
    std::vector<FileMetaData*> grandparents;
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &grandparents);
    GroupFiles(&icmp_, grandparents, c->grandparents_);
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
    return NULL;
  }

  // Avoid compacting too much in one shot in case the range is large.
  const uint64_t limit = MaxFileSizeForLevel(level) *
                         options_->source_compaction_factor;
  uint64_t total = 0;
  for (size_t i = 0; i < inputs.size(); i++) {
    uint64_t s = inputs[i]->file_size;
    total += s;
    if (total >= limit) {
      inputs.resize(i + 1);
      break;
    }
  }

  Compaction* c = new Compaction(level, MaxFileSizeForLevel(level),
    MaxGrandParentOverlapBytes(level), NumberLevels());
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);

  // These files that are to be manaully compacted do not trample
  // upon other files because manual compactions are processed when
  // the system has a max of 1 background compaction thread.
  c->MarkFilesBeingCompacted(true);
  return c;
}

Compaction::Compaction(int level, uint64_t target_file_size,
  uint64_t max_grandparent_overlap_bytes, int number_levels,
  bool seek_compaction)
    : level_(level),
      max_output_file_size_(target_file_size),
      maxGrandParentOverlapBytes_(max_grandparent_overlap_bytes),
      input_version_(NULL),
      number_levels_(number_levels),
      seek_compaction_(seek_compaction),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0),
      score_(0) {
  edit_ = new VersionEdit(number_levels_);
  level_ptrs_ = new size_t[number_levels_];
  for (int i = 0; i < number_levels_; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  delete[] level_ptrs_;
  delete edit_;
  if (input_version_ != NULL) {
    input_version_->Unref();
  }
  for (FileGroup* g: grandparents_) {
    delete g;
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
      overlapped_bytes_ += grandparents_[grandparent_index_]->total_file_size;
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
  if (input_version_ != NULL) {
    input_version_->Unref();
    input_version_ = NULL;
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
  if(write < 0 || write > len)
    return;

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
