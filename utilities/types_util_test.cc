//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/types_util.h"

#include "db/dbformat.h"
#include "port/stack_trace.h"
#include "rocksdb/types.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {
namespace {
std::string IKey(const std::string& user_key, uint64_t seq, ValueType vt) {
  std::string encoded;
  AppendInternalKey(&encoded, ParsedInternalKey(user_key, seq, vt));
  return encoded;
}
}  // namespace

TEST(ParseEntryTest, InvalidInternalKey) {
  std::string invalid_ikey = "foo";
  Slice ikey_slice = invalid_ikey;
  ParsedEntryInfo parsed_entry;
  ASSERT_TRUE(ParseEntry(ikey_slice, &parsed_entry).IsInvalidArgument());
}

TEST(ParseEntryTest, Basic) {
  std::string ikey = IKey("foo", 3, ValueType::kTypeValue);
  Slice ikey_slice = ikey;

  ParsedEntryInfo parsed_entry;
  ASSERT_OK(ParseEntry(ikey_slice, &parsed_entry));
  ASSERT_EQ(parsed_entry.user_key, "foo");
  ASSERT_EQ(parsed_entry.sequence, 3);
  ASSERT_EQ(parsed_entry.type, EntryType::kEntryPut);

  ikey = IKey("bar", 5, ValueType::kTypeDeletion);
  ikey_slice = ikey;

  ASSERT_OK(ParseEntry(ikey_slice, &parsed_entry));
  ASSERT_EQ(parsed_entry.user_key, "bar");
  ASSERT_EQ(parsed_entry.sequence, 5);
  ASSERT_EQ(parsed_entry.type, EntryType::kEntryDelete);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
