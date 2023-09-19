//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/expected_state_dump_tool.h"

#include <cinttypes>
#include <cstring>
#include <sstream>
#include <string>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// arg_name would include all prefix, e.g. "--my_arg="
// arg_val is the parses value.
// True if there is a match. False otherwise.
// Woud exit after printing errmsg if cannot be parsed.
bool ParseIntArg(const char* arg, const std::string arg_name,
                 const std::string err_msg, int64_t* arg_val) {
  if (strncmp(arg, arg_name.c_str(), arg_name.size()) == 0) {
    std::string input_str = arg + arg_name.size();
    std::istringstream iss(input_str);
    iss >> *arg_val;
    if (iss.fail()) {
      fprintf(stderr, "%s\n", err_msg.c_str());
      exit(1);
    }
    return true;
  }
  return false;
}
}  // namespace

int ExpectedStateDumpTool::Run(int argc, char const* const* argv) {
  const char* file_path = nullptr;
  size_t max_key = 0;
  size_t num_column_families = 0;
  int64_t tmp_val;
  int64_t key = 0;
  int cf = 0;
  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--file=", 7) == 0) {
      file_path = argv[i] + 7;
    } else if (ParseIntArg(argv[i], "--max_key=", "max_key must be numeric",
                           &tmp_val)) {
      max_key = static_cast<size_t>(tmp_val);
    } else if (ParseIntArg(argv[i], "--num_column_families=",
                           "num_column_families must be numeric", &tmp_val)) {
      num_column_families = static_cast<size_t>(tmp_val);
    } else if (ParseIntArg(argv[i], "--key=", "key must be numeric",
                           &tmp_val)) {
      key = static_cast<int64_t>(tmp_val);
    } else if (ParseIntArg(argv[i], "--cf=", "cf must be numeric", &tmp_val)) {
      cf = static_cast<int>(tmp_val);
    }
  }
  if (file_path) {
    fprintf(stdout, "Opening %s...\n", file_path);
    std::string path(file_path);
    FileExpectedState file_expected_state(path, max_key, num_column_families);
    Status s = file_expected_state.Open(false /* create */);
    fprintf(stdout, "Status - %s\n", s.ToString().c_str());

    auto value = file_expected_state.Get(cf, key);
    fprintf(stdout, "column family %u key %" PRId64, cf, key);
    if (value.Exists()) {
      fprintf(stdout, "Exists - Value: %d ", value.Read());
    }
    if (value.PendingDelete()) {
      fprintf(stdout, " PendingDelete");
    }
    if (value.PendingWrite()) {
      fprintf(stdout, " PendingWrite");
    }
    if (value.IsDeleted()) {
      fprintf(stdout, " IsDeleted");
    }
    fprintf(stdout, "\n");

    return 0;
  }
  return 1;
}

}  // namespace ROCKSDB_NAMESPACE
