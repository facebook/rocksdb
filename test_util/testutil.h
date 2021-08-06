//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <algorithm>
#include <deque>
#include <string>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "file/writable_file_writer.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "table/internal_iterator.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
class FileSystem;
class Random;
class SequentialFile;
class SequentialFileReader;

namespace test {

extern const uint32_t kDefaultFormatVersion;
extern const uint32_t kLatestFormatVersion;

// Return a random key with the specified length that may contain interesting
// characters (e.g. \x00, \xff, etc.).
enum RandomKeyType : char { RANDOM, LARGEST, SMALLEST, MIDDLE };
extern std::string RandomKey(Random* rnd, int len,
                             RandomKeyType type = RandomKeyType::RANDOM);

// Store in *dst a string of length "len" that will compress to
// "N*compressed_fraction" bytes and return a Slice that references
// the generated data.
extern Slice CompressibleString(Random* rnd, double compressed_fraction,
                                int len, std::string* dst);

// A wrapper that allows injection of errors.
class ErrorEnv : public EnvWrapper {
 public:
  bool writable_file_error_;
  int num_writable_file_errors_;

  ErrorEnv(Env* _target)
      : EnvWrapper(_target),
        writable_file_error_(false),
        num_writable_file_errors_(0) {}

  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& soptions) override {
    result->reset();
    if (writable_file_error_) {
      ++num_writable_file_errors_;
      return Status::IOError(fname, "fake error");
    }
    return target()->NewWritableFile(fname, result, soptions);
  }
};

#ifndef NDEBUG
// An internal comparator that just forward comparing results from the
// user comparator in it. Can be used to test entities that have no dependency
// on internal key structure but consumes InternalKeyComparator, like
// BlockBasedTable.
class PlainInternalKeyComparator : public InternalKeyComparator {
 public:
  explicit PlainInternalKeyComparator(const Comparator* c)
      : InternalKeyComparator(c) {}

  virtual ~PlainInternalKeyComparator() {}

  virtual int Compare(const Slice& a, const Slice& b) const override {
    return user_comparator()->Compare(a, b);
  }
};
#endif

// A test comparator which compare two strings in this way:
// (1) first compare prefix of 8 bytes in alphabet order,
// (2) if two strings share the same prefix, sort the other part of the string
//     in the reverse alphabet order.
// This helps simulate the case of compounded key of [entity][timestamp] and
// latest timestamp first.
class SimpleSuffixReverseComparator : public Comparator {
 public:
  SimpleSuffixReverseComparator() {}
  static const char* kClassName() { return "SimpleSuffixReverseComparator"; }
  virtual const char* Name() const override { return kClassName(); }

  virtual int Compare(const Slice& a, const Slice& b) const override {
    Slice prefix_a = Slice(a.data(), 8);
    Slice prefix_b = Slice(b.data(), 8);
    int prefix_comp = prefix_a.compare(prefix_b);
    if (prefix_comp != 0) {
      return prefix_comp;
    } else {
      Slice suffix_a = Slice(a.data() + 8, a.size() - 8);
      Slice suffix_b = Slice(b.data() + 8, b.size() - 8);
      return -(suffix_a.compare(suffix_b));
    }
  }
  virtual void FindShortestSeparator(std::string* /*start*/,
                                     const Slice& /*limit*/) const override {}

  virtual void FindShortSuccessor(std::string* /*key*/) const override {}
};

// Returns a user key comparator that can be used for comparing two uint64_t
// slices. Instead of comparing slices byte-wise, it compares all the 8 bytes
// at once. Assumes same endian-ness is used though the database's lifetime.
// Symantics of comparison would differ from Bytewise comparator in little
// endian machines.
extern const Comparator* Uint64Comparator();

// Iterator over a vector of keys/values
class VectorIterator : public InternalIterator {
 public:
  explicit VectorIterator(const std::vector<std::string>& keys)
      : keys_(keys), current_(keys.size()) {
    std::sort(keys_.begin(), keys_.end());
    values_.resize(keys.size());
  }

  VectorIterator(const std::vector<std::string>& keys,
      const std::vector<std::string>& values)
    : keys_(keys), values_(values), current_(keys.size()) {
    assert(keys_.size() == values_.size());
  }

  virtual bool Valid() const override { return current_ < keys_.size(); }

  virtual void SeekToFirst() override { current_ = 0; }
  virtual void SeekToLast() override { current_ = keys_.size() - 1; }

  virtual void Seek(const Slice& target) override {
    current_ = std::lower_bound(keys_.begin(), keys_.end(), target.ToString()) -
               keys_.begin();
  }

  virtual void SeekForPrev(const Slice& target) override {
    current_ = std::upper_bound(keys_.begin(), keys_.end(), target.ToString()) -
               keys_.begin();
    if (!Valid()) {
      SeekToLast();
    } else {
      Prev();
    }
  }

  virtual void Next() override { current_++; }
  virtual void Prev() override { current_--; }

  virtual Slice key() const override { return Slice(keys_[current_]); }
  virtual Slice value() const override { return Slice(values_[current_]); }

  virtual Status status() const override { return Status::OK(); }

  virtual bool IsKeyPinned() const override { return true; }
  virtual bool IsValuePinned() const override { return true; }

 private:
  std::vector<std::string> keys_;
  std::vector<std::string> values_;
  size_t current_;
};

class StringSink : public FSWritableFile {
 public:
  std::string contents_;

  explicit StringSink(Slice* reader_contents = nullptr)
      : FSWritableFile(),
        contents_(""),
        reader_contents_(reader_contents),
        last_flush_(0) {
    if (reader_contents_ != nullptr) {
      *reader_contents_ = Slice(contents_.data(), 0);
    }
  }

  const std::string& contents() const { return contents_; }

  IOStatus Truncate(uint64_t size, const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
    contents_.resize(static_cast<size_t>(size));
    return IOStatus::OK();
  }
  IOStatus Close(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }
  IOStatus Flush(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    if (reader_contents_ != nullptr) {
      assert(reader_contents_->size() <= last_flush_);
      size_t offset = last_flush_ - reader_contents_->size();
      *reader_contents_ = Slice(
          contents_.data() + offset,
          contents_.size() - offset);
      last_flush_ = contents_.size();
    }

    return IOStatus::OK();
  }
  IOStatus Sync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  using FSWritableFile::Append;
  IOStatus Append(const Slice& slice, const IOOptions& /*opts*/,
                  IODebugContext* /*dbg*/) override {
    contents_.append(slice.data(), slice.size());
    return IOStatus::OK();
  }
  void Drop(size_t bytes) {
    if (reader_contents_ != nullptr) {
      contents_.resize(contents_.size() - bytes);
      *reader_contents_ = Slice(
          reader_contents_->data(), reader_contents_->size() - bytes);
      last_flush_ = contents_.size();
    }
  }

 private:
  Slice* reader_contents_;
  size_t last_flush_;
};

// A wrapper around a StringSink to give it a RandomRWFile interface
class RandomRWStringSink : public FSRandomRWFile {
 public:
  explicit RandomRWStringSink(StringSink* ss) : ss_(ss) {}

  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& /*opts*/,
                 IODebugContext* /*dbg*/) override {
    if (offset + data.size() > ss_->contents_.size()) {
      ss_->contents_.resize(static_cast<size_t>(offset) + data.size(), '\0');
    }

    char* pos = const_cast<char*>(ss_->contents_.data() + offset);
    memcpy(pos, data.data(), data.size());
    return IOStatus::OK();
  }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*opts*/,
                Slice* result, char* /*scratch*/,
                IODebugContext* /*dbg*/) const override {
    *result = Slice(nullptr, 0);
    if (offset < ss_->contents_.size()) {
      size_t str_res_sz =
          std::min(static_cast<size_t>(ss_->contents_.size() - offset), n);
      *result = Slice(ss_->contents_.data() + offset, str_res_sz);
    }
    return IOStatus::OK();
  }

  IOStatus Flush(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  IOStatus Sync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  IOStatus Close(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  const std::string& contents() const { return ss_->contents(); }

 private:
  StringSink* ss_;
};

// Like StringSink, this writes into a string.  Unlink StringSink, it
// has some initial content and overwrites it, just like a recycled
// log file.
class OverwritingStringSink : public FSWritableFile {
 public:
  explicit OverwritingStringSink(Slice* reader_contents)
      : FSWritableFile(),
        contents_(""),
        reader_contents_(reader_contents),
        last_flush_(0) {}

  const std::string& contents() const { return contents_; }

  IOStatus Truncate(uint64_t size, const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
    contents_.resize(static_cast<size_t>(size));
    return IOStatus::OK();
  }
  IOStatus Close(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }
  IOStatus Flush(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    if (last_flush_ < contents_.size()) {
      assert(reader_contents_->size() >= contents_.size());
      memcpy((char*)reader_contents_->data() + last_flush_,
             contents_.data() + last_flush_, contents_.size() - last_flush_);
      last_flush_ = contents_.size();
    }
    return IOStatus::OK();
  }
  IOStatus Sync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  using FSWritableFile::Append;
  IOStatus Append(const Slice& slice, const IOOptions& /*opts*/,
                  IODebugContext* /*dbg*/) override {
    contents_.append(slice.data(), slice.size());
    return IOStatus::OK();
  }
  void Drop(size_t bytes) {
    contents_.resize(contents_.size() - bytes);
    if (last_flush_ > contents_.size()) last_flush_ = contents_.size();
  }

 private:
  std::string contents_;
  Slice* reader_contents_;
  size_t last_flush_;
};

class StringSource : public FSRandomAccessFile {
 public:
  explicit StringSource(const Slice& contents, uint64_t uniq_id = 0,
                        bool mmap = false)
      : contents_(contents.data(), contents.size()),
        uniq_id_(uniq_id),
        mmap_(mmap),
        total_reads_(0) {}

  virtual ~StringSource() { }

  uint64_t Size() const { return contents_.size(); }

  IOStatus Prefetch(uint64_t /*offset*/, size_t /*n*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    // If we are using mmap_, it is equivalent to performing a prefetch
    if (mmap_) {
      return IOStatus::OK();
    } else {
      return IOStatus::NotSupported("Prefetch not supported");
    }
  }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*opts*/,
                Slice* result, char* scratch,
                IODebugContext* /*dbg*/) const override {
    total_reads_++;
    if (offset > contents_.size()) {
      return IOStatus::InvalidArgument("invalid Read offset");
    }
    if (offset + n > contents_.size()) {
      n = contents_.size() - static_cast<size_t>(offset);
    }
    if (!mmap_) {
      memcpy(scratch, &contents_[static_cast<size_t>(offset)], n);
      *result = Slice(scratch, n);
    } else {
      *result = Slice(&contents_[static_cast<size_t>(offset)], n);
    }
    return IOStatus::OK();
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    if (max_size < 20) {
      return 0;
    }

    char* rid = id;
    rid = EncodeVarint64(rid, uniq_id_);
    rid = EncodeVarint64(rid, 0);
    return static_cast<size_t>(rid-id);
  }

  int total_reads() const { return total_reads_; }

  void set_total_reads(int tr) { total_reads_ = tr; }

 private:
  std::string contents_;
  uint64_t uniq_id_;
  bool mmap_;
  mutable int total_reads_;
};

class NullLogger : public Logger {
 public:
  using Logger::Logv;
  virtual void Logv(const char* /*format*/, va_list /*ap*/) override {}
  virtual size_t GetLogFileSize() const override { return 0; }
};

// Corrupts key by changing the type
extern void CorruptKeyType(InternalKey* ikey);

extern std::string KeyStr(const std::string& user_key,
                          const SequenceNumber& seq, const ValueType& t,
                          bool corrupt = false);

extern std::string KeyStr(uint64_t ts, const std::string& user_key,
                          const SequenceNumber& seq, const ValueType& t,
                          bool corrupt = false);

class SleepingBackgroundTask {
 public:
  SleepingBackgroundTask()
      : bg_cv_(&mutex_),
        should_sleep_(true),
        done_with_sleep_(false),
        sleeping_(false) {}

  bool IsSleeping() {
    MutexLock l(&mutex_);
    return sleeping_;
  }
  void DoSleep() {
    MutexLock l(&mutex_);
    sleeping_ = true;
    bg_cv_.SignalAll();
    while (should_sleep_) {
      bg_cv_.Wait();
    }
    sleeping_ = false;
    done_with_sleep_ = true;
    bg_cv_.SignalAll();
  }
  void WaitUntilSleeping() {
    MutexLock l(&mutex_);
    while (!sleeping_ || !should_sleep_) {
      bg_cv_.Wait();
    }
  }
  // Waits for the status to change to sleeping,
  // otherwise times out.
  // wait_time is in microseconds.
  // Returns true when times out, false otherwise.
  bool TimedWaitUntilSleeping(uint64_t wait_time);

  void WakeUp() {
    MutexLock l(&mutex_);
    should_sleep_ = false;
    bg_cv_.SignalAll();
  }
  void WaitUntilDone() {
    MutexLock l(&mutex_);
    while (!done_with_sleep_) {
      bg_cv_.Wait();
    }
  }
  // Similar to TimedWaitUntilSleeping.
  // Waits until the task is done.
  bool TimedWaitUntilDone(uint64_t wait_time);

  bool WokenUp() {
    MutexLock l(&mutex_);
    return should_sleep_ == false;
  }

  void Reset() {
    MutexLock l(&mutex_);
    should_sleep_ = true;
    done_with_sleep_ = false;
  }

  static void DoSleepTask(void* arg) {
    reinterpret_cast<SleepingBackgroundTask*>(arg)->DoSleep();
  }

 private:
  port::Mutex mutex_;
  port::CondVar bg_cv_;  // Signalled when background work finishes
  bool should_sleep_;
  bool done_with_sleep_;
  bool sleeping_;
};

// Filters merge operands and values that are equal to `num`.
class FilterNumber : public CompactionFilter {
 public:
  explicit FilterNumber(uint64_t num) : num_(num) {}

  std::string last_merge_operand_key() { return last_merge_operand_key_; }

  bool Filter(int /*level*/, const ROCKSDB_NAMESPACE::Slice& /*key*/,
              const ROCKSDB_NAMESPACE::Slice& value, std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    if (value.size() == sizeof(uint64_t)) {
      return num_ == DecodeFixed64(value.data());
    }
    return true;
  }

  bool FilterMergeOperand(
      int /*level*/, const ROCKSDB_NAMESPACE::Slice& key,
      const ROCKSDB_NAMESPACE::Slice& value) const override {
    last_merge_operand_key_ = key.ToString();
    if (value.size() == sizeof(uint64_t)) {
      return num_ == DecodeFixed64(value.data());
    }
    return true;
  }

  const char* Name() const override { return "FilterBadMergeOperand"; }

 private:
  mutable std::string last_merge_operand_key_;
  uint64_t num_;
};

inline std::string EncodeInt(uint64_t x) {
  std::string result;
  PutFixed64(&result, x);
  return result;
}

class SeqStringSource : public FSSequentialFile {
 public:
  SeqStringSource(const std::string& data, std::atomic<int>* read_count)
      : data_(data), offset_(0), read_count_(read_count) {}
  ~SeqStringSource() override {}
  IOStatus Read(size_t n, const IOOptions& /*opts*/, Slice* result,
                char* scratch, IODebugContext* /*dbg*/) override {
    std::string output;
    if (offset_ < data_.size()) {
      n = std::min(data_.size() - offset_, n);
      memcpy(scratch, data_.data() + offset_, n);
      offset_ += n;
      *result = Slice(scratch, n);
    } else {
      return IOStatus::InvalidArgument(
          "Attempt to read when it already reached eof.");
    }
    (*read_count_)++;
    return IOStatus::OK();
  }

  IOStatus Skip(uint64_t n) override {
    if (offset_ >= data_.size()) {
      return IOStatus::InvalidArgument(
          "Attempt to read when it already reached eof.");
    }
    // TODO(yhchiang): Currently doesn't handle the overflow case.
    offset_ += static_cast<size_t>(n);
    return IOStatus::OK();
  }

 private:
  std::string data_;
  size_t offset_;
  std::atomic<int>* read_count_;
};

class StringFS : public FileSystemWrapper {
 public:
  class StringSink : public FSWritableFile {
   public:
    explicit StringSink(std::string* contents)
        : FSWritableFile(), contents_(contents) {}
    IOStatus Truncate(uint64_t size, const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
      contents_->resize(static_cast<size_t>(size));
      return IOStatus::OK();
    }
    IOStatus Close(const IOOptions& /*opts*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions& /*opts*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }

    using FSWritableFile::Append;
    IOStatus Append(const Slice& slice, const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
      contents_->append(slice.data(), slice.size());
      return IOStatus::OK();
    }

   private:
    std::string* contents_;
  };

  explicit StringFS(const std::shared_ptr<FileSystem>& t)
      : FileSystemWrapper(t) {}
  ~StringFS() override {}

  const std::string& GetContent(const std::string& f) { return files_[f]; }

  const IOStatus WriteToNewFile(const std::string& file_name,
                                const std::string& content) {
    std::unique_ptr<FSWritableFile> r;
    FileOptions file_opts;
    IOOptions io_opts;

    auto s = NewWritableFile(file_name, file_opts, &r, nullptr);
    if (s.ok()) {
      s = r->Append(content, io_opts, nullptr);
    }
    if (s.ok()) {
      s = r->Flush(io_opts, nullptr);
    }
    if (s.ok()) {
      s = r->Close(io_opts, nullptr);
    }
    assert(!s.ok() || files_[file_name] == content);
    return s;
  }

  // The following text is boilerplate that forwards all methods to target()
  IOStatus NewSequentialFile(const std::string& f,
                             const FileOptions& /*options*/,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* /*dbg*/) override {
    auto iter = files_.find(f);
    if (iter == files_.end()) {
      return IOStatus::NotFound("The specified file does not exist", f);
    }
    r->reset(new SeqStringSource(iter->second, &num_seq_file_read_));
    return IOStatus::OK();
  }

  IOStatus NewRandomAccessFile(const std::string& /*f*/,
                               const FileOptions& /*options*/,
                               std::unique_ptr<FSRandomAccessFile>* /*r*/,
                               IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus NewWritableFile(const std::string& f, const FileOptions& /*options*/,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* /*dbg*/) override {
    auto iter = files_.find(f);
    if (iter != files_.end()) {
      return IOStatus::IOError("The specified file already exists", f);
    }
    r->reset(new StringSink(&files_[f]));
    return IOStatus::OK();
  }
  IOStatus NewDirectory(const std::string& /*name*/,
                        const IOOptions& /*options*/,
                        std::unique_ptr<FSDirectory>* /*result*/,
                        IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus FileExists(const std::string& f, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    if (files_.find(f) == files_.end()) {
      return IOStatus::NotFound();
    }
    return IOStatus::OK();
  }

  IOStatus GetChildren(const std::string& /*dir*/, const IOOptions& /*options*/,
                       std::vector<std::string>* /*r*/,
                       IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus DeleteFile(const std::string& f, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    files_.erase(f);
    return IOStatus::OK();
  }

  IOStatus CreateDir(const std::string& /*d*/, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus CreateDirIfMissing(const std::string& /*d*/,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus DeleteDir(const std::string& /*d*/, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus GetFileSize(const std::string& f, const IOOptions& /*options*/,
                       uint64_t* s, IODebugContext* /*dbg*/) override {
    auto iter = files_.find(f);
    if (iter == files_.end()) {
      return IOStatus::NotFound("The specified file does not exist:", f);
    }
    *s = iter->second.size();
    return IOStatus::OK();
  }

  IOStatus GetFileModificationTime(const std::string& /*fname*/,
                                   const IOOptions& /*options*/,
                                   uint64_t* /*file_mtime*/,
                                   IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus RenameFile(const std::string& /*s*/, const std::string& /*t*/,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus LinkFile(const std::string& /*s*/, const std::string& /*t*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus LockFile(const std::string& /*f*/, const IOOptions& /*options*/,
                    FileLock** /*l*/, IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus UnlockFile(FileLock* /*l*/, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  std::atomic<int> num_seq_file_read_;

 protected:
  std::unordered_map<std::string, std::string> files_;
};

// Randomly initialize the given DBOptions
void RandomInitDBOptions(DBOptions* db_opt, Random* rnd);

// Randomly initialize the given ColumnFamilyOptions
// Note that the caller is responsible for releasing non-null
// cf_opt->compaction_filter.
void RandomInitCFOptions(ColumnFamilyOptions* cf_opt, DBOptions&, Random* rnd);

// A dummy merge operator which can change its name
class ChanglingMergeOperator : public MergeOperator {
 public:
  explicit ChanglingMergeOperator(const std::string& name)
      : name_(name + "MergeOperator") {}
  ~ChanglingMergeOperator() {}

  void SetName(const std::string& name) { name_ = name; }

  virtual bool FullMergeV2(const MergeOperationInput& /*merge_in*/,
                           MergeOperationOutput* /*merge_out*/) const override {
    return false;
  }
  virtual bool PartialMergeMulti(const Slice& /*key*/,
                                 const std::deque<Slice>& /*operand_list*/,
                                 std::string* /*new_value*/,
                                 Logger* /*logger*/) const override {
    return false;
  }
  virtual const char* Name() const override { return name_.c_str(); }

 protected:
  std::string name_;
};

// Returns a dummy merge operator with random name.
MergeOperator* RandomMergeOperator(Random* rnd);

// A dummy compaction filter which can change its name
class ChanglingCompactionFilter : public CompactionFilter {
 public:
  explicit ChanglingCompactionFilter(const std::string& name)
      : name_(name + "CompactionFilter") {}
  ~ChanglingCompactionFilter() {}

  void SetName(const std::string& name) { name_ = name; }

  bool Filter(int /*level*/, const Slice& /*key*/,
              const Slice& /*existing_value*/, std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    return false;
  }

  const char* Name() const override { return name_.c_str(); }

 private:
  std::string name_;
};

// Returns a dummy compaction filter with a random name.
CompactionFilter* RandomCompactionFilter(Random* rnd);

// A dummy compaction filter factory which can change its name
class ChanglingCompactionFilterFactory : public CompactionFilterFactory {
 public:
  explicit ChanglingCompactionFilterFactory(const std::string& name)
      : name_(name + "CompactionFilterFactory") {}
  ~ChanglingCompactionFilterFactory() {}

  void SetName(const std::string& name) { name_ = name; }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::unique_ptr<CompactionFilter>();
  }

  // Returns a name that identifies this compaction filter factory.
  const char* Name() const override { return name_.c_str(); }

 protected:
  std::string name_;
};

extern const Comparator* ComparatorWithU64Ts();

CompressionType RandomCompressionType(Random* rnd);

void RandomCompressionTypeVector(const size_t count,
                                 std::vector<CompressionType>* types,
                                 Random* rnd);

CompactionFilterFactory* RandomCompactionFilterFactory(Random* rnd);

const SliceTransform* RandomSliceTransform(Random* rnd, int pre_defined = -1);

TableFactory* RandomTableFactory(Random* rnd, int pre_defined = -1);

std::string RandomName(Random* rnd, const size_t len);

bool IsDirectIOSupported(Env* env, const std::string& dir);

bool IsPrefetchSupported(const std::shared_ptr<FileSystem>& fs,
                         const std::string& dir);

// Return the number of lines where a given pattern was found in a file.
size_t GetLinesCount(const std::string& fname, const std::string& pattern);

// TEST_TMPDIR may be set to /dev/shm in Makefile,
// but /dev/shm does not support direct IO.
// Tries to set TEST_TMPDIR to a directory supporting direct IO.
void ResetTmpDirForDirectIO();

Status CorruptFile(Env* env, const std::string& fname, int offset,
                   int bytes_to_corrupt, bool verify_checksum = true);
Status TruncateFile(Env* env, const std::string& fname, uint64_t length);

// Try and delete a directory if it exists
Status TryDeleteDir(Env* env, const std::string& dirname);

// Delete a directory if it exists
void DeleteDir(Env* env, const std::string& dirname);

// Creates an Env from the system environment by looking at the system
// environment variables.
Status CreateEnvFromSystem(const ConfigOptions& options, Env** result,
                           std::shared_ptr<Env>* guard);
}  // namespace test
}  // namespace ROCKSDB_NAMESPACE
