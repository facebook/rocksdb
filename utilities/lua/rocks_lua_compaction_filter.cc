//  Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if defined(LUA) && !defined(ROCKSDB_LITE)
#include "rocksdb/utilities/lua/rocks_lua_compaction_filter.h"

extern "C" {
#include <luaconf.h>
}

#include "rocksdb/compaction_filter.h"

namespace rocksdb {
namespace lua {

const std::string kFilterFunctionName = "Filter";
const std::string kNameFunctionName = "Name";

void RocksLuaCompactionFilter::LogLuaError(const char* format, ...) const {
  if (options_.error_log.get() != nullptr &&
      error_count_ < options_.error_limit_per_filter) {
    error_count_++;

    va_list ap;
    va_start(ap, format);
    options_.error_log->Logv(InfoLogLevel::ERROR_LEVEL, format, ap);
    va_end(ap);
  }
}

bool RocksLuaCompactionFilter::Filter(int level, const Slice& key,
                                      const Slice& existing_value,
                                      std::string* new_value,
                                      bool* value_changed) const {
  auto* lua_state = lua_state_wrapper_.GetLuaState();
  // push the right function into the lua stack
  lua_getglobal(lua_state, kFilterFunctionName.c_str());

  int error_no = 0;
  int num_input_values;
  int num_return_values;
  if (options_.ignore_value == false) {
    // push input arguments into the lua stack
    lua_pushnumber(lua_state, level);
    lua_pushlstring(lua_state, key.data(), key.size());
    lua_pushlstring(lua_state, existing_value.data(), existing_value.size());
    num_input_values = 3;
    num_return_values = 3;
  } else {
    // If ignore_value is set to true, then we only put two arguments
    // and expect one return value
    lua_pushnumber(lua_state, level);
    lua_pushlstring(lua_state, key.data(), key.size());
    num_input_values = 2;
    num_return_values = 1;
  }

  // perform the lua call
  if ((error_no =
           lua_pcall(lua_state, num_input_values, num_return_values, 0)) != 0) {
    LogLuaError("[Lua] Error(%d) in Filter function --- %s", error_no,
                lua_tostring(lua_state, -1));
    // pops out the lua error from stack
    lua_pop(lua_state, 1);
    return false;
  }

  // As lua_pcall went successfully, it can be guaranteed that the top
  // three elements in the Lua stack are the three returned values.

  bool has_error = false;
  const int kIndexIsFiltered = -num_return_values;
  const int kIndexValueChanged = -num_return_values + 1;
  const int kIndexNewValue = -num_return_values + 2;

  // check the types of three return values
  // is_filtered
  if (!lua_isboolean(lua_state, kIndexIsFiltered)) {
    LogLuaError(
        "[Lua] Error in Filter function -- "
        "1st return value (is_filtered) is not a boolean "
        "while a boolean is expected.");
    has_error = true;
  }

  if (options_.ignore_value == false) {
    // value_changed
    if (!lua_isboolean(lua_state, kIndexValueChanged)) {
      LogLuaError(
          "[Lua] Error in Filter function -- "
          "2nd return value (value_changed) is not a boolean "
          "while a boolean is expected.");
      has_error = true;
    }
    // new_value
    if (!lua_isstring(lua_state, kIndexNewValue)) {
      LogLuaError(
          "[Lua] Error in Filter function -- "
          "3rd return value (new_value) is not a string "
          "while a string is expected.");
      has_error = true;
    }
  }

  if (has_error) {
    lua_pop(lua_state, num_return_values);
    return false;
  }

  // Fetch the return values
  bool is_filtered = false;
  if (!has_error) {
    is_filtered = lua_toboolean(lua_state, kIndexIsFiltered);
    if (options_.ignore_value == false) {
      *value_changed = lua_toboolean(lua_state, kIndexValueChanged);
      if (*value_changed) {
        const char* new_value_buf = lua_tostring(lua_state, kIndexNewValue);
        const size_t new_value_size = lua_strlen(lua_state, kIndexNewValue);
        // Note that any string that lua_tostring returns always has a zero at
        // its end, bu/t it can have other zeros inside it
        assert(new_value_buf[new_value_size] == '\0');
        assert(strlen(new_value_buf) <= new_value_size);
        new_value->assign(new_value_buf, new_value_size);
      }
    } else {
      *value_changed = false;
    }
  }
  // pops the three return values.
  lua_pop(lua_state, num_return_values);
  return is_filtered;
}

const char* RocksLuaCompactionFilter::Name() const {
  if (name_ != "") {
    return name_.c_str();
  }
  auto* lua_state = lua_state_wrapper_.GetLuaState();
  // push the right function into the lua stack
  lua_getglobal(lua_state, kNameFunctionName.c_str());

  // perform the call (0 arguments, 1 result)
  int error_no;
  if ((error_no = lua_pcall(lua_state, 0, 1, 0)) != 0) {
    LogLuaError("[Lua] Error(%d) in Name function --- %s", error_no,
                lua_tostring(lua_state, -1));
    // pops out the lua error from stack
    lua_pop(lua_state, 1);
    return name_.c_str();
  }

  // check the return value
  if (!lua_isstring(lua_state, -1)) {
    LogLuaError(
        "[Lua] Error in Name function -- "
        "return value is not a string while string is expected");
  } else {
    const char* name_buf = lua_tostring(lua_state, -1);
    const size_t name_size __attribute__((__unused__)) = lua_strlen(lua_state, -1);
    assert(name_buf[name_size] == '\0');
    assert(strlen(name_buf) <= name_size);
    name_ = name_buf;
  }
  lua_pop(lua_state, 1);
  return name_.c_str();
}

/* Not yet supported
bool RocksLuaCompactionFilter::FilterMergeOperand(
    int level, const Slice& key, const Slice& operand) const {
  auto* lua_state = lua_state_wrapper_.GetLuaState();
  // push the right function into the lua stack
  lua_getglobal(lua_state, "FilterMergeOperand");

  // push input arguments into the lua stack
  lua_pushnumber(lua_state, level);
  lua_pushlstring(lua_state, key.data(), key.size());
  lua_pushlstring(lua_state, operand.data(), operand.size());

  // perform the call (3 arguments, 1 result)
  int error_no;
  if ((error_no = lua_pcall(lua_state, 3, 1, 0)) != 0) {
    LogLuaError("[Lua] Error(%d) in FilterMergeOperand function --- %s",
        error_no, lua_tostring(lua_state, -1));
    // pops out the lua error from stack
    lua_pop(lua_state, 1);
    return false;
  }

  bool is_filtered = false;
  // check the return value
  if (!lua_isboolean(lua_state, -1)) {
    LogLuaError("[Lua] Error in FilterMergeOperand function -- "
                "return value is not a boolean while boolean is expected");
  } else {
    is_filtered = lua_toboolean(lua_state, -1);
  }

  lua_pop(lua_state, 1);

  return is_filtered;
}
*/

bool RocksLuaCompactionFilter::IgnoreSnapshots() const {
  return options_.ignore_snapshots;
}

RocksLuaCompactionFilterFactory::RocksLuaCompactionFilterFactory(
    const RocksLuaCompactionFilterOptions opt)
    : opt_(opt) {
  auto filter = CreateCompactionFilter(CompactionFilter::Context());
  name_ = std::string("RocksLuaCompactionFilterFactory::") +
          std::string(filter->Name());
}

std::unique_ptr<CompactionFilter>
RocksLuaCompactionFilterFactory::CreateCompactionFilter(
    const CompactionFilter::Context& context) {
  std::lock_guard<std::mutex> lock(opt_mutex_);
  return std::unique_ptr<CompactionFilter>(new RocksLuaCompactionFilter(opt_));
}

std::string RocksLuaCompactionFilterFactory::GetScript() {
  std::lock_guard<std::mutex> lock(opt_mutex_);
  return opt_.lua_script;
}

void RocksLuaCompactionFilterFactory::SetScript(const std::string& new_script) {
  std::lock_guard<std::mutex> lock(opt_mutex_);
  opt_.lua_script = new_script;
}

const char* RocksLuaCompactionFilterFactory::Name() const {
  return name_.c_str();
}

}  // namespace lua
}  // namespace rocksdb
#endif  // defined(LUA) && !defined(ROCKSDB_LITE)
