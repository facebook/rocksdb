// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include <map>
#include <string.h>
#include <string>
#include <vector>

namespace rocksdb {

namespace {

class MemFile {
 public:
  enum Mode {
    READ = 0,
    WRITE = 1,
  };

  MemFile(Mode mode) : mode_(mode), refs_(0) {}

  void Ref() {
    MutexLock lock(&mutex_);
    ++refs_;
  }

  void Unref() {
    bool do_delete = false;
    {
      MutexLock lock(&mutex_);
      --refs_;
      assert(refs_ >= 0);
      if (refs_ <= 0) {
        do_delete = true;
      }
    }

    if (do_delete) {
      delete this;
    }
  }

  void SetMode(Mode mode) {
    mode_ = mode;
  }

  uint64_t Size() const { return data_.size(); }

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
    assert(mode_ == READ);
    if (offset > Size()) {
      return Status::IOError("Offset greater than file size.");
    }
    const uint64_t available = Size() - offset;
    if (n > available) {
      n = available;
    }
    if (n == 0) {
      *result = Slice();
      return Status::OK();
    }
    if (scratch) {
      memcpy(scratch, &(data_[offset]), n);
      *result = Slice(scratch, n);
    } else {
      *result = Slice(&(data_[offset]), n);
    }
    return Status::OK();
  }

  Status Append(const Slice& data) {
    assert(mode_ == WRITE);
    data_.append(data.data(), data.size());
    return Status::OK();
  }

  Status Fsync() {
    return Status::OK();
  }

 private:
  // Private since only Unref() should be used to delete it.
  ~MemFile() {
    assert(refs_ == 0);
  }

  // No copying allowed.
  MemFile(const MemFile&);
  void operator=(const MemFile&);

  Mode mode_;
  port::Mutex mutex_;
  int refs_;  // Protected by mutex_;

  std::string data_;
};

class SequentialFileImpl : public SequentialFile {
 public:
  explicit SequentialFileImpl(MemFile* file) : file_(file), pos_(0) {
    file_->Ref();
  }

  ~SequentialFileImpl() {
    file_->Unref();
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s = file_->Read(pos_, n, result, scratch);
    if (s.ok()) {
      pos_ += result->size();
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    if (pos_ > file_->Size()) {
      return Status::IOError("pos_ > file_->Size()");
    }
    const size_t available = file_->Size() - pos_;
    if (n > available) {
      n = available;
    }
    pos_ += n;
    return Status::OK();
  }

 private:
  MemFile* file_;
  size_t pos_;
};

class RandomAccessFileImpl : public RandomAccessFile {
 public:
  explicit RandomAccessFileImpl(MemFile* file) : file_(file) {
    file_->Ref();
  }

  ~RandomAccessFileImpl() {
    file_->Unref();
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    return file_->Read(offset, n, result, scratch);
  }

 private:
  MemFile* file_;
};

class WritableFileImpl : public WritableFile {
 public:
  WritableFileImpl(MemFile* file) : file_(file) {
    file_->Ref();
  }

  ~WritableFileImpl() {
    file_->Unref();
  }

  virtual Status Append(const Slice& data) {
    return file_->Append(data);
  }

  virtual Status Close() {
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    return file_->Fsync();
  }

 private:
  MemFile* file_;
};

class TestMemDirectory : public Directory {
 public:
  virtual Status Fsync() { return Status::OK(); }
};

class TestMemEnv : public EnvWrapper {
 public:
  explicit TestMemEnv(Env* base_env) : EnvWrapper(base_env) { }

  virtual ~TestMemEnv() {
    for (FileSystem::iterator i = file_map_.begin(); i != file_map_.end(); ++i){
      i->second->Unref();
    }
  }

  // Partial implementation of the Env interface.
  virtual Status NewSequentialFile(const std::string& fname,
                                   unique_ptr<SequentialFile>* result,
                                   const EnvOptions& soptions) {
    MutexLock lock(&mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      *result = NULL;
      return Status::IOError(fname, "File not found");
    }
    auto* f = file_map_[fname];
    f->SetMode(MemFile::READ);
    result->reset(new SequentialFileImpl(f));
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& soptions) {
    MutexLock lock(&mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      *result = NULL;
      return Status::IOError(fname, "File not found");
    }
    auto* f = file_map_[fname];
    f->SetMode(MemFile::READ);
    result->reset(new RandomAccessFileImpl(f));
    return Status::OK();
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 unique_ptr<WritableFile>* result,
                                 const EnvOptions& soptions) {
    MutexLock lock(&mutex_);
    if (file_map_.find(fname) != file_map_.end()) {
      DeleteFileInternal(fname);
    }
    MemFile* file = new MemFile(MemFile::WRITE);
    file->Ref();
    file_map_[fname] = file;

    result->reset(new WritableFileImpl(file));
    return Status::OK();
  }

  virtual Status NewRandomRWFile(const std::string& fname,
                                 unique_ptr<RandomRWFile>* result,
                                 const EnvOptions& options) {
    return Status::OK();
  }

  virtual Status NewDirectory(const std::string& name,
                              unique_ptr<Directory>* result) {
    result->reset(new TestMemDirectory());
    return Status::OK();
  }

  virtual bool FileExists(const std::string& fname) {
    MutexLock lock(&mutex_);
    return file_map_.find(fname) != file_map_.end();
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    MutexLock lock(&mutex_);
    result->clear();

    for (FileSystem::iterator i = file_map_.begin(); i != file_map_.end(); ++i){
      const std::string& filename = i->first;

      if (filename.size() >= dir.size() + 1 && filename[dir.size()] == '/' &&
          Slice(filename).starts_with(Slice(dir))) {
        result->push_back(filename.substr(dir.size() + 1));
      }
    }

    return Status::OK();
  }

  void DeleteFileInternal(const std::string& fname) {
    if (file_map_.find(fname) == file_map_.end()) {
      return;
    }

    file_map_[fname]->Unref();
    file_map_.erase(fname);
  }

  virtual Status DeleteFile(const std::string& fname) {
    MutexLock lock(&mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      return Status::IOError(fname, "File not found");
    }

    DeleteFileInternal(fname);
    return Status::OK();
  }

  virtual Status CreateDir(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status CreateDirIfMissing(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status DeleteDir(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) {
    MutexLock lock(&mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      return Status::IOError(fname, "File not found");
    }

    *file_size = file_map_[fname]->Size();
    return Status::OK();
  }

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* time) {
    return Status::NotSupported("getFileMTime", "Not supported in MemEnv");
  }

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) {
    MutexLock lock(&mutex_);
    if (file_map_.find(src) == file_map_.end()) {
      return Status::IOError(src, "File not found");
    }

    DeleteFileInternal(target);
    file_map_[target] = file_map_[src];
    file_map_.erase(src);
    return Status::OK();
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    *lock = new FileLock;
    return Status::OK();
  }

  virtual Status UnlockFile(FileLock* lock) {
    delete lock;
    return Status::OK();
  }

  virtual Status GetTestDirectory(std::string* path) {
    *path = "/test";
    return Status::OK();
  }

 private:
  // Map from filenames to MemFile objects, representing a simple file system.
  typedef std::map<std::string, MemFile*> FileSystem;
  port::Mutex mutex_;
  FileSystem file_map_;  // Protected by mutex_.
};

}  // namespace

Env* NewTestMemEnv(Env* base_env) {
  return new TestMemEnv(base_env);
}

}  // namespace rocksdb
