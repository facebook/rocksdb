//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef ROCKSDB_LITE

#include "tools/ldb_cmd.h"
#include "util/testharness.h"

class LdbCmdTest : public testing::Test {};

TEST_F(LdbCmdTest, HexToString) {
  // map input to expected outputs.
  map<string, vector<int>> inputMap = {
      {"0x7", {7}},         {"0x5050", {80, 80}}, {"0xFF", {-1}},
      {"0x1234", {18, 52}}, {"0xaa", {-86}}, {"0x123", {18, 3}},
  };

  for (const auto& inPair : inputMap) {
    auto actual = rocksdb::LDBCommand::HexToString(inPair.first);
    auto expected = inPair.second;
    for (unsigned int i = 0; i < actual.length(); i++) {
      ASSERT_EQ(expected[i], static_cast<int>(actual[i]));
    }
  }
}

TEST_F(LdbCmdTest, HexToStringBadInputs) {
  const vector<string> badInputs = {
      "0xZZ", "123", "0xx5", "0x11G", "Ox12", "0xT", "0x1Q1",
  };
  for (const auto badInput : badInputs) {
    try {
      rocksdb::LDBCommand::HexToString(badInput);
      std::cerr << "Should fail on bad hex value: " << badInput << "\n";
      FAIL();
    } catch (...) {
    }
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#else
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr, "SKIPPED as LDBCommand is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE
