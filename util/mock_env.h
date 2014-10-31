//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <map>
#include <string>
#include <vector>
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "port/port.h"
#include "util/mutexlock.h"

namespace rocksdb {

class MemFile;
class MockEnv : public EnvWrapper {
 public:
  explicit MockEnv(Env* base_env);

  virtual ~MockEnv();

  // Partial implementation of the Env interface.
  virtual Status NewSequentialFile(const std::string& fname,
                                   unique_ptr<SequentialFile>* result,
                                   const EnvOptions& soptions);

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& soptions);

  virtual Status NewWritableFile(const std::string& fname,
                                 unique_ptr<WritableFile>* result,
                                 const EnvOptions& env_options);

  virtual Status NewRandomRWFile(const std::string& fname,
                                 unique_ptr<RandomRWFile>* result,
                                 const EnvOptions& options);

  virtual Status NewDirectory(const std::string& name,
                              unique_ptr<Directory>* result);

  virtual bool FileExists(const std::string& fname);

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result);

  void DeleteFileInternal(const std::string& fname);

  virtual Status DeleteFile(const std::string& fname);

  virtual Status CreateDir(const std::string& dirname);

  virtual Status CreateDirIfMissing(const std::string& dirname);

  virtual Status DeleteDir(const std::string& dirname);

  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size);

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* time);

  virtual Status RenameFile(const std::string& src,
                            const std::string& target);

  virtual Status NewLogger(const std::string& fname,
                           shared_ptr<Logger>* result);

  virtual Status LockFile(const std::string& fname, FileLock** flock);

  virtual Status UnlockFile(FileLock* flock);

  virtual Status GetTestDirectory(std::string* path);

  // Non-virtual functions, specific to MockEnv
  Status Truncate(const std::string& fname, size_t size);

  Status CorruptBuffer(const std::string& fname);

 private:
  std::string NormalizePath(const std::string path);

  // Map from filenames to MemFile objects, representing a simple file system.
  typedef std::map<std::string, MemFile*> FileSystem;
  port::Mutex mutex_;
  FileSystem file_map_;  // Protected by mutex_.
};

}  // namespace rocksdb
