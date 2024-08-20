// Copyright (c) 2017 Rockset

#pragma once

#include <rocksdb/slice.h>

#include <algorithm>
#include <functional>
#include <string>

//
// These are inlined methods to deal with pathnames and filenames.

namespace {

// trim from start
static std::string& ltrim(std::string& s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(),
                                  [](int ch) { return !std::isspace(ch); }));
  return s;
}

// trim from end
static std::string& rtrim(std::string& s) {
  s.erase(std::find_if(s.rbegin(), s.rend(),
                       [](int ch) { return !std::isspace(ch); })
              .base(),
          s.end());
  return s;
}

// trim from both ends
inline std::string& trim(std::string& s) { return ltrim(rtrim(s)); }

// Extract basename from a full pathname
inline std::string basename(std::string const& pathname) {
  auto pos = pathname.rfind('/');
  if (pos == std::string::npos) {
    return pathname;
  }
  return pathname.substr(pos + 1);
}
inline std::string dirname(std::string const& pathname) {
  auto pos = pathname.rfind('/');
  if (pos == std::string::npos) {
    return "";
  }
  return pathname.substr(0, pos);
}

// If s doesn't end with '/', it appends it.
// Special case: if s is empty, we don't append '/'
inline std::string ensure_ends_with_pathsep(std::string s) {
  if (!s.empty() && s.back() != '/') {
    s += '/';
  }
  return s;
}

// If the last char of the string is the specified character, then return a
// string that has the last character removed.
inline std::string rtrim_if(std::string s, char c) {
  if (s.length() > 0 && s[s.length() - 1] == c) {
    s.erase(s.begin() + s.size() - 1);
  }
  return s;
}
// If the first char of the string is the specified character, then return a
// string that has the first character removed.
inline std::string ltrim_if(std::string s, char c) {
  if (s.length() > 0 && s[0] == c) {
    s.erase(0, 1);
  }
  return s;
}

// Returns true if 'value' has a suffix of 'ending'
inline bool ends_with(std::string const& value, std::string const& ending) {
  if (ending.size() > value.size()) return false;
  return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}

inline std::string MakeCloudManifestFile(const std::string& cookie) {
  return cookie.empty() ? "CLOUDMANIFEST" : "CLOUDMANIFEST-" + cookie;
}
inline std::string MakeCloudManifestFile(const std::string& dbname,
                                         const std::string& cookie) {
  assert(!dbname.empty());
  return dbname + "/" + MakeCloudManifestFile(cookie);
}

inline std::string ManifestFileWithEpoch(const std::string& epoch) {
  return epoch.empty() ? "MANIFEST" : "MANIFEST-" + epoch;
}
inline std::string ManifestFileWithEpoch(const std::string& dbname,
                                         const std::string& epoch) {
  assert(!dbname.empty());
  return dbname + "/" + ManifestFileWithEpoch(epoch);
}

inline std::string RemoveEpoch(const std::string& path) {
  auto lastDash = path.rfind('-');
  if (lastDash == std::string::npos) {
    return path;
  }
  auto lastPathPos = path.rfind('/');
  if (lastPathPos == std::string::npos || lastDash > lastPathPos) {
    return path.substr(0, lastDash);
  }
  return path;
}

// Get the cookie suffix from cloud manifest file path
inline std::string GetCookie(const std::string& cloud_manifest_file_path) {
  auto cloud_manifest_fname = basename(cloud_manifest_file_path);
  auto firstDash = cloud_manifest_fname.find('-');
  if (firstDash == std::string::npos) {
    // no cookie suffix in cloud manifest file path
    return "";
  }

  return cloud_manifest_fname.substr(firstDash + 1);
}

// pathaname seperator
const std::string pathsep = "/";

// types of rocksdb files
const std::string sst = ".sst";
const std::string ldb = ".ldb";
const std::string log = ".log";

// Is this a sst file, i.e. ends in ".sst" or ".ldb"
inline bool IsSstFile(const std::string& pathname) {
  if (pathname.size() < sst.size()) {
    return false;
  }
  const char* ptr = pathname.c_str() + pathname.size() - sst.size();
  if ((memcmp(ptr, sst.c_str(), sst.size()) == 0) ||
      (memcmp(ptr, ldb.c_str(), ldb.size()) == 0)) {
    return true;
  }
  return false;
}

// A log file has ".log" suffix
inline bool IsWalFile(const std::string& pathname) {
  if (pathname.size() < log.size()) {
    return false;
  }
  const char* ptr = pathname.c_str() + pathname.size() - log.size();
  if (memcmp(ptr, log.c_str(), log.size()) == 0) {
    return true;
  }
  return false;
}

inline bool IsManifestFile(const std::string& pathname) {
  // extract last component of the path
  rocksdb::Slice fname;
  size_t offset = pathname.find_last_of(pathsep);
  if (offset != std::string::npos) {
    fname = rocksdb::Slice(&pathname[offset + 1], pathname.size() - offset - 1);
  } else {
    fname = pathname;
  }
  if (fname.starts_with("MANIFEST")) {
    return true;
  }
  return false;
}

inline bool IsIdentityFile(const std::string& pathname) {
  // extract last component of the path
  std::string fname;
  size_t offset = pathname.find_last_of(pathsep);
  if (offset != std::string::npos) {
    fname = pathname.substr(offset + 1, pathname.size());
  } else {
    fname = pathname;
  }
  if (fname.find("IDENTITY") == 0) {
    return true;
  }
  return false;
}

// A log file has ".log" suffix
inline bool IsLogFile(const std::string& pathname) {
  return IsWalFile(pathname);
}

inline bool IsCloudManifestFile(const std::string& pathname) {
  std::string fname = basename(pathname);
  if (fname.find("CLOUDMANIFEST") == 0) {
    return true;
  }
  return false;
}

enum class RocksDBFileType {
  kSstFile,
  kLogFile,
  kManifestFile,
  kIdentityFile,
  kUnknown
};

// Determine type of a file based on filename. Rules:
// 1. filename ends with a .sst is a sst file.
// 2. filename ends with .log or starts with MANIFEST is a logfile
// 3. filename starts with MANIFEST is a manifest file
// 3. filename starts with IDENTITY is a ID file
inline RocksDBFileType GetFileType(const std::string& fname_with_epoch) {
  auto fname = RemoveEpoch(fname_with_epoch);
  if (IsSstFile(fname)) {
    return RocksDBFileType::kSstFile;
  }
  if (IsLogFile(fname)) {
    return RocksDBFileType::kLogFile;
  }
  if (IsManifestFile(fname)) {
    return RocksDBFileType::kManifestFile;
  }
  if (IsIdentityFile(fname)) {
    return RocksDBFileType::kIdentityFile;
  }
  return RocksDBFileType::kUnknown;
}

}  // namespace
