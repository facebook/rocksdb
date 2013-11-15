//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/filename.h"

#include <ctype.h>
#include <stdio.h>
#include "db/dbformat.h"
#include "rocksdb/env.h"
#include "util/logging.h"

namespace rocksdb {

// Given a path, flatten the path name by replacing all chars not in
// {[0-9,a-z,A-Z,-,_,.]} with _. And append '\0' at the end.
// Return the number of chars stored in dest not including the trailing '\0'.
static int FlattenPath(const std::string& path, char* dest, int len) {
  int write_idx = 0;
  int i = 0;
  int src_len = path.size();

  while (i < src_len && write_idx < len - 1) {
    if ((path[i] >= 'a' && path[i] <= 'z') ||
        (path[i] >= '0' && path[i] <= '9') ||
        (path[i] >= 'A' && path[i] <= 'Z') ||
        path[i] == '-' ||
        path[i] == '.' ||
        path[i] == '_'){
      dest[write_idx++] = path[i];
    } else {
      if (i > 0)
        dest[write_idx++] = '_';
    }
    i++;
  }

  dest[write_idx] = '\0';
  return write_idx;
}

// A utility routine: write "data" to the named file and Sync() it.
extern Status WriteStringToFileSync(Env* env, const Slice& data,
                                    const std::string& fname);

static std::string MakeFileName(const std::string& name, uint64_t number,
                                const char* suffix) {
  char buf[100];
  snprintf(buf, sizeof(buf), "/%06llu.%s",
           static_cast<unsigned long long>(number),
           suffix);
  return name + buf;
}

std::string LogFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  return MakeFileName(name, number, "log");
}

std::string ArchivalDirectory(const std::string& dir) {
  return dir + "/" + ARCHIVAL_DIR;
}
std::string ArchivedLogFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  return MakeFileName(name + "/" + ARCHIVAL_DIR, number, "log");
}

std::string TableFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  return MakeFileName(name, number, "sst");
}

std::string DescriptorFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  char buf[100];
  snprintf(buf, sizeof(buf), "/MANIFEST-%06llu",
           static_cast<unsigned long long>(number));
  return dbname + buf;
}

std::string CurrentFileName(const std::string& dbname) {
  return dbname + "/CURRENT";
}

std::string LockFileName(const std::string& dbname) {
  return dbname + "/LOCK";
}

std::string TempFileName(const std::string& dbname, uint64_t number) {
  assert(number >= 0);
  return MakeFileName(dbname, number, "dbtmp");
}

std::string InfoLogFileName(const std::string& dbname,
    const std::string& db_path, const std::string& log_dir) {
  if (log_dir.empty())
    return dbname + "/LOG";

  char flatten_db_path[256];
  FlattenPath(db_path, flatten_db_path, 256);
  return log_dir + "/" + flatten_db_path + "_LOG";
}

// Return the name of the old info log file for "dbname".
std::string OldInfoLogFileName(const std::string& dbname, uint64_t ts,
    const std::string& db_path, const std::string& log_dir) {
  char buf[50];
  snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(ts));

  if (log_dir.empty())
    return dbname + "/LOG.old." + buf;

  char flatten_db_path[256];
  FlattenPath(db_path, flatten_db_path, 256);
  return log_dir + "/" + flatten_db_path + "_LOG.old." + buf;
}

std::string MetaDatabaseName(const std::string& dbname, uint64_t number) {
  char buf[100];
  snprintf(buf, sizeof(buf), "/METADB-%llu",
           static_cast<unsigned long long>(number));
  return dbname + buf;
}

std::string IdentityFileName(const std::string& dbname) {
  return dbname + "/IDENTITY";
}

// Owned filenames have the form:
//    dbname/IDENTITY
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old.[0-9]+
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst)
//    dbname/METADB-[0-9]+
//    Disregards / at the beginning
bool ParseFileName(const std::string& fname,
                   uint64_t* number,
                   FileType* type,
                   WalFileType* log_type) {
  Slice rest(fname);
  if (fname.length() > 1 && fname[0] == '/') {
    rest.remove_prefix(1);
  }
  if (rest == "IDENTITY") {
    *number = 0;
    *type = kIdentityFile;
  } else if (rest == "CURRENT") {
    *number = 0;
    *type = kCurrentFile;
  } else if (rest == "LOCK") {
    *number = 0;
    *type = kDBLockFile;
  } else if (rest == "LOG" || rest == "LOG.old") {
    *number = 0;
    *type = kInfoLogFile;
  } else if (rest.starts_with("LOG.old.")) {
    uint64_t ts_suffix;
    // sizeof also counts the trailing '\0'.
    rest.remove_prefix(sizeof("LOG.old.") - 1);
    if (!ConsumeDecimalNumber(&rest, &ts_suffix)) {
      return false;
    }
    *number = ts_suffix;
    *type = kInfoLogFile;
  } else if (rest.starts_with("MANIFEST-")) {
    rest.remove_prefix(strlen("MANIFEST-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kDescriptorFile;
    *number = num;
  } else if (rest.starts_with("METADB-")) {
    rest.remove_prefix(strlen("METADB-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kMetaDatabase;
    *number = num;
  } else {
    // Avoid strtoull() to keep filename format independent of the
    // current locale
    bool archive_dir_found = false;
    if (rest.starts_with(ARCHIVAL_DIR)) {
      if (rest.size() <= ARCHIVAL_DIR.size()) {
        return false;
      }
      rest.remove_prefix(ARCHIVAL_DIR.size() + 1); // Add 1 to remove / also
      if (log_type) {
        *log_type = kArchivedLogFile;
      }
      archive_dir_found = true;
    }
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    Slice suffix = rest;
    if (suffix == Slice(".log")) {
      *type = kLogFile;
      if (log_type && !archive_dir_found) {
        *log_type = kAliveLogFile;
      }
    } else if (archive_dir_found) {
      return false; // Archive dir can contain only log files
    } else if (suffix == Slice(".sst")) {
      *type = kTableFile;
    } else if (suffix == Slice(".dbtmp")) {
      *type = kTempFile;
    } else {
      return false;
    }
    *number = num;
  }
  return true;
}

Status SetCurrentFile(Env* env, const std::string& dbname,
                      uint64_t descriptor_number) {
  // Remove leading "dbname/" and add newline to manifest file name
  std::string manifest = DescriptorFileName(dbname, descriptor_number);
  Slice contents = manifest;
  assert(contents.starts_with(dbname + "/"));
  contents.remove_prefix(dbname.size() + 1);
  std::string tmp = TempFileName(dbname, descriptor_number);
  Status s = WriteStringToFileSync(env, contents.ToString() + "\n", tmp);
  if (s.ok()) {
    s = env->RenameFile(tmp, CurrentFileName(dbname));
  }
  if (!s.ok()) {
    env->DeleteFile(tmp);
  }
  return s;
}

Status SetIdentityFile(Env* env, const std::string& dbname) {
  std::string id = env->GenerateUniqueId();
  assert(!id.empty());
  // Reserve the filename dbname/000000.dbtmp for the temporary identity file
  std::string tmp = TempFileName(dbname, 0);
  Status s = WriteStringToFileSync(env, id, tmp);
  if (s.ok()) {
    s = env->RenameFile(tmp, IdentityFileName(dbname));
  }
  if (!s.ok()) {
    env->DeleteFile(tmp);
  }
  return s;
}

}  // namespace rocksdb
