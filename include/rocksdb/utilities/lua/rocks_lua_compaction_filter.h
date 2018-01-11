//  Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if defined(LUA) && !defined(ROCKSDB_LITE)
// lua headers
extern "C" {
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>
}

#include <mutex>
#include <string>
#include <vector>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/lua/rocks_lua_custom_library.h"
#include "rocksdb/utilities/lua/rocks_lua_util.h"

namespace rocksdb {
namespace lua {

struct RocksLuaCompactionFilterOptions {
  // The lua script in string that implements all necessary CompactionFilter
  // virtual functions.  The specified lua_script must implement the following
  // functions, which are Name and Filter, as described below.
  //
  // 0. The Name function simply returns a string representing the name of
  //    the lua script.  If there's any erorr in the Name function, an
  //    empty string will be used.
  //    --- Example
  //      function Name()
  //        return "DefaultLuaCompactionFilter"
  //      end
  //
  //
  // 1. The script must contains a function called Filter, which implements
  //    CompactionFilter::Filter() , takes three input arguments, and returns
  //    three values as the following API:
  //
  //   function Filter(level, key, existing_value)
  //     ...
  //     return is_filtered, is_changed, new_value
  //   end
  //
  //   Note that if ignore_value is set to true, then Filter should implement
  //   the following API:
  //
  //   function Filter(level, key)
  //     ...
  //     return is_filtered
  //   end
  //
  //   If there're any error in the Filter() function, then it will keep
  //   the input key / value pair.
  //
  //   -- Input
  //   The function must take three arguments (integer, string, string),
  //   which map to "level", "key", and "existing_value" passed from
  //   RocksDB.
  //
  //   -- Output
  //   The function must return three values (boolean, boolean, string).
  //     - is_filtered: if the first return value is true, then it indicates
  //       the input key / value pair should be filtered.
  //     - is_changed: if the second return value is true, then it indicates
  //       the existing_value needs to be changed, and the resulting value
  //       is stored in the third return value.
  //     - new_value: if the second return value is true, then this third
  //       return value stores the new value of the input key / value pair.
  //
  //   -- Examples
  //     -- a filter that keeps all key-value pairs
  //     function Filter(level, key, existing_value)
  //       return false, false, ""
  //     end
  //
  //     -- a filter that keeps all keys and change their values to "Rocks"
  //     function Filter(level, key, existing_value)
  //       return false, true, "Rocks"
  //     end

  std::string lua_script;

  // If set to true, then existing_value will not be passed to the Filter
  // function, and the Filter function only needs to return a single boolean
  // flag indicating whether to filter out this key or not.
  //
  //   function Filter(level, key)
  //     ...
  //     return is_filtered
  //   end
  bool ignore_value = false;

  // A boolean flag to determine whether to ignore snapshots.
  bool ignore_snapshots = false;

  // When specified a non-null pointer, the first "error_limit_per_filter"
  // errors of each CompactionFilter that is lua related will be included
  // in this log.
  std::shared_ptr<Logger> error_log;

  // The number of errors per CompactionFilter will be printed
  // to error_log.
  int error_limit_per_filter = 1;

  // A string to luaL_reg array map that allows the Lua CompactionFilter
  // to use custom C library.  The string will be used as the library
  // name in Lua.
  std::vector<std::shared_ptr<RocksLuaCustomLibrary>> libraries;

  ///////////////////////////////////////////////////////////////////////////
  //  NOT YET SUPPORTED
  // The name of the Lua function in "lua_script" that implements
  // CompactionFilter::FilterMergeOperand().  The function must take
  // three input arguments (integer, string, string), which map to "level",
  // "key", and "operand" passed from the RocksDB.  In addition, the
  // function must return a single boolean value, indicating whether
  // to filter the input key / operand.
  //
  // DEFAULT:  the default implementation always returns false.
  // @see CompactionFilter::FilterMergeOperand
};

class RocksLuaCompactionFilterFactory : public CompactionFilterFactory {
 public:
  explicit RocksLuaCompactionFilterFactory(
      const RocksLuaCompactionFilterOptions opt);

  virtual ~RocksLuaCompactionFilterFactory() {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override;

  // Change the Lua script so that the next compaction after this
  // function call will use the new Lua script.
  void SetScript(const std::string& new_script);

  // Obtain the current Lua script
  std::string GetScript();

  const char* Name() const override;

 private:
  RocksLuaCompactionFilterOptions opt_;
  std::string name_;
  // A lock to protect "opt_" to make it dynamically changeable.
  std::mutex opt_mutex_;
};

// A wrapper class that invokes Lua script to perform CompactionFilter
// functions.
class RocksLuaCompactionFilter : public rocksdb::CompactionFilter {
 public:
  explicit RocksLuaCompactionFilter(const RocksLuaCompactionFilterOptions& opt)
      : options_(opt),
        lua_state_wrapper_(opt.lua_script, opt.libraries),
        error_count_(0),
        name_("") {}

  virtual bool Filter(int level, const Slice& key, const Slice& existing_value,
                      std::string* new_value,
                      bool* value_changed) const override;
  // Not yet supported
  virtual bool FilterMergeOperand(int level, const Slice& key,
                                  const Slice& operand) const override {
    return false;
  }
  virtual bool IgnoreSnapshots() const override;
  virtual const char* Name() const override;

 protected:
  void LogLuaError(const char* format, ...) const;

  RocksLuaCompactionFilterOptions options_;
  LuaStateWrapper lua_state_wrapper_;
  mutable int error_count_;
  mutable std::string name_;
};

}  // namespace lua
}  // namespace rocksdb
#endif  // defined(LUA) && !defined(ROCKSDB_LITE)
