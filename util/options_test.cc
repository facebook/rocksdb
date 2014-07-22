//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <gflags/gflags.h>

#include "rocksdb/options.h"
#include "util/testharness.h"

using GFLAGS::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");

namespace rocksdb {

class OptionsTest {};

class StderrLogger : public Logger {
 public:
  virtual void Logv(const char* format, va_list ap) override {
    vprintf(format, ap);
    printf("\n");
  }
};

Options PrintAndGetOptions(size_t total_write_buffer_limit,
                           int read_amplification_threshold,
                           int write_amplification_threshold,
                           uint64_t target_db_size = 68719476736) {
  StderrLogger logger;

  if (FLAGS_enable_print) {
    printf(
        "---- total_write_buffer_limit: %zu "
        "read_amplification_threshold: %d write_amplification_threshold: %d "
        "target_db_size %" PRIu64 " ----\n",
        total_write_buffer_limit, read_amplification_threshold,
        write_amplification_threshold, target_db_size);
  }

  Options options =
      GetOptions(total_write_buffer_limit, read_amplification_threshold,
                 write_amplification_threshold, target_db_size);
  if (FLAGS_enable_print) {
    options.Dump(&logger);
    printf("-------------------------------------\n\n\n");
  }
  return options;
}

TEST(OptionsTest, LooseCondition) {
  Options options;
  PrintAndGetOptions(static_cast<size_t>(10) * 1024 * 1024 * 1024, 100, 100);

  // Less mem table memory budget
  PrintAndGetOptions(32 * 1024 * 1024, 100, 100);

  // Tight read amplification
  options = PrintAndGetOptions(128 * 1024 * 1024, 8, 100);
  ASSERT_EQ(options.compaction_style, kCompactionStyleLevel);

  // Tight write amplification
  options = PrintAndGetOptions(128 * 1024 * 1024, 64, 10);
  ASSERT_EQ(options.compaction_style, kCompactionStyleUniversal);

  // Both tight amplifications
  PrintAndGetOptions(128 * 1024 * 1024, 4, 8);
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);
  return rocksdb::test::RunAllTests();
}
