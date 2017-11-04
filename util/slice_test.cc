//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "port/stack_trace.h"
#include "rocksdb/slice.h"
#include "util/testharness.h"

namespace rocksdb {

class SliceTest : public testing::Test {};

namespace {
void BumpCounter(void* arg1, void* arg2) {
  (*reinterpret_cast<int*>(arg1))++;
}
}  // anonymous namespace

TEST_F(SliceTest, PinnableSliceMoveConstruct) {
  for (int i = 0; i < 3; i++) {
    int orig_cleanup = 0;
    int moved_cleanup = 0;
    PinnableSlice* s1 = nullptr;
    std::string external_storage;
    switch (i) {
      case 0:
        s1 = new PinnableSlice();
        *(s1->GetSelf()) = "foo";
        s1->PinSelf();
        s1->RegisterCleanup(BumpCounter, &moved_cleanup, nullptr);
        break;
      case 1:
        s1 = new PinnableSlice(&external_storage);
        *(s1->GetSelf()) = "foo";
        s1->PinSelf();
        s1->RegisterCleanup(BumpCounter, &moved_cleanup, nullptr);
        break;
      case 2:
        s1 = new PinnableSlice();
        s1->PinSlice("foo", BumpCounter, &moved_cleanup, nullptr);
        break;
    }
    ASSERT_EQ("foo", s1->ToString());
    PinnableSlice* s2 = new PinnableSlice();
    s2->PinSelf("bar");
    ASSERT_EQ("bar", s2->ToString());
    s2->RegisterCleanup(BumpCounter, &orig_cleanup, nullptr);
    *s2 = std::move(*s1);
    ASSERT_FALSE(s1->IsPinned());
    ASSERT_EQ("foo", s2->ToString());
    ASSERT_EQ(1, orig_cleanup);
    ASSERT_EQ(0, moved_cleanup);
    delete s1;
    // ASAN will check if it will access storage of s1, which is deleted.
    ASSERT_EQ("foo", s2->ToString());
    ASSERT_EQ(1, orig_cleanup);
    ASSERT_EQ(0, moved_cleanup);
    delete s2;
    ASSERT_EQ(1, orig_cleanup);
    ASSERT_EQ(1, moved_cleanup);
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
