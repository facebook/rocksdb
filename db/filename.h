//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// File names used by DB code

#pragma once
#include <stdint.h>
#include <string>
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/transaction_log.h"
#include "port/port.h"

namespace rocksdb {

class Env;

enum FileType {
  kLogFile,
  kDBLockFile,
  kTableFile,
  kDescriptorFile,
  kCurrentFile,
  kTempFile,
  kInfoLogFile,  // Either the current one, or an old one
  kMetaDatabase,
  kIdentityFile
};

// Return the name of the log file with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
extern std::string LogFileName(const std::string& dbname, uint64_t number);

static const std::string ARCHIVAL_DIR = "archive";

extern std::string ArchivalDirectory(const std::string& dbname);

//  Return the name of the archived log file with the specified number
//  in the db named by "dbname". The result will be prefixed with "dbname".
extern std::string ArchivedLogFileName(const std::string& dbname,
                                       uint64_t num);

// Return the name of the sstable with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
extern std::string TableFileName(const std::string& dbname, uint64_t number);

// Return the name of the descriptor file for the db named by
// "dbname" and the specified incarnation number.  The result will be
// prefixed with "dbname".
extern std::string DescriptorFileName(const std::string& dbname,
                                      uint64_t number);

// Return the name of the current file.  This file contains the name
// of the current manifest file.  The result will be prefixed with
// "dbname".
extern std::string CurrentFileName(const std::string& dbname);

// Return the name of the lock file for the db named by
// "dbname".  The result will be prefixed with "dbname".
extern std::string LockFileName(const std::string& dbname);

// Return the name of a temporary file owned by the db named "dbname".
// The result will be prefixed with "dbname".
extern std::string TempFileName(const std::string& dbname, uint64_t number);

// Return the name of the info log file for "dbname".
extern std::string InfoLogFileName(const std::string& dbname,
    const std::string& db_path="", const std::string& log_dir="");

// Return the name of the old info log file for "dbname".
extern std::string OldInfoLogFileName(const std::string& dbname, uint64_t ts,
    const std::string& db_path="", const std::string& log_dir="");

// Return the name to use for a metadatabase. The result will be prefixed with
// "dbname".
extern std::string MetaDatabaseName(const std::string& dbname,
                                    uint64_t number);

// Return the name of the Identity file which stores a unique number for the db
// that will get regenerated if the db loses all its data and is recreated fresh
// either from a backup-image or empty
extern std::string IdentityFileName(const std::string& dbname);

// If filename is a rocksdb file, store the type of the file in *type.
// The number encoded in the filename is stored in *number.  If the
// filename was successfully parsed, returns true.  Else return false.
extern bool ParseFileName(const std::string& filename,
                          uint64_t* number,
                          FileType* type,
                          WalFileType* log_type = nullptr);

// Make the CURRENT file point to the descriptor file with the
// specified number.
extern Status SetCurrentFile(Env* env, const std::string& dbname,
                             uint64_t descriptor_number);

// Make the IDENTITY file for the db
extern Status SetIdentityFile(Env* env, const std::string& dbname);

}  // namespace rocksdb
