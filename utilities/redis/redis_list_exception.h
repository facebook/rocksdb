/**
 * A simple structure for exceptions in RedisLists.
 *
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#ifndef LEVELDB_REDIS_LIST_EXCEPTION_H
#define LEVELDB_REDIS_LIST_EXCEPTION_H

#include <exception>

namespace rocksdb {

class RedisListException: public std::exception {
 public:
  const char* what() const throw() {
    return "Invalid operation or corrupt data in Redis List.";
  }
};

} // namespace rocksdb

#endif // LEVELDB_REDIS_LIST_EXCEPTION_H
