// Copyright (c) 2013 Facebook.

#ifndef STORAGE_LEVELDB_UTIL_STRINGUTIL_H_
#define STORAGE_LEVELDB_UTIL_STRINGUTIL_H_

namespace rocksdb {

extern std::vector<std::string> stringSplit(std::string arg, char delim);

}

#endif  // STORAGE_LEVELDB_UTIL_STRINGUTIL_H_
