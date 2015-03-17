//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "db/write_controller.h"

#include "util/testharness.h"

namespace rocksdb {

class WriteControllerTest : public testing::Test {};

TEST_F(WriteControllerTest, SanityTest) {
  WriteController controller;
  auto stop_token_1 = controller.GetStopToken();
  auto stop_token_2 = controller.GetStopToken();

  ASSERT_EQ(true, controller.IsStopped());
  stop_token_1.reset();
  ASSERT_EQ(true, controller.IsStopped());
  stop_token_2.reset();
  ASSERT_EQ(false, controller.IsStopped());

  auto delay_token_1 = controller.GetDelayToken(5);
  ASSERT_EQ(static_cast<uint64_t>(5), controller.GetDelay());
  auto delay_token_2 = controller.GetDelayToken(8);
  ASSERT_EQ(static_cast<uint64_t>(13), controller.GetDelay());

  delay_token_2.reset();
  ASSERT_EQ(static_cast<uint64_t>(5), controller.GetDelay());
  delay_token_1.reset();
  ASSERT_EQ(static_cast<uint64_t>(0), controller.GetDelay());
  delay_token_1.reset();
  ASSERT_EQ(false, controller.IsStopped());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
