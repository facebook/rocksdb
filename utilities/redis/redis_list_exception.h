/**
 * A simple structure for exceptions in RedisLists.
 *
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#pragma once
#include <exception>

namespace rocksdb {

class RedisListException: public std::exception {
 public:
  const char* what() const throw() {
    return "Invalid operation or corrupt data in Redis List.";
  }
};

} // namespace rocksdb
