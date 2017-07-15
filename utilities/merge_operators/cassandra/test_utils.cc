// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// This source code is also licensed under the GPLv2 license found in the
// COPYING file in the root directory of this source tree.

#include "test_utils.h"

namespace rocksdb {
namespace cassandra {
const char kData[] = {'d', 'a', 't', 'a'};
const char kExpiringData[] = {'e', 'd', 'a', 't', 'a'};
const int32_t kLocalDeletionTime = 1;
const int32_t kTtl = 100;
const int8_t kColumn = 0;
const int8_t kTombstone = 1;
const int8_t kExpiringColumn = 2;

std::unique_ptr<ColumnBase> CreateTestColumn(int8_t mask,
                                             int8_t index,
                                             int64_t timestamp) {
  if ((mask & ColumnTypeMask::DELETION_MASK) != 0) {
    return std::unique_ptr<Tombstone>(new Tombstone(
      mask, index, kLocalDeletionTime, timestamp));
  } else if ((mask & ColumnTypeMask::EXPIRATION_MASK) != 0) {
    return std::unique_ptr<ExpiringColumn>(new ExpiringColumn(
      mask, index, timestamp, sizeof(kExpiringData), kExpiringData, kTtl));
  } else {
    return std::unique_ptr<Column>(
      new Column(mask, index, timestamp, sizeof(kData), kData));
  }
}

RowValue CreateTestRowValue(
    std::vector<std::tuple<int8_t, int8_t, int64_t>> column_specs) {
  std::vector<std::unique_ptr<ColumnBase>> columns;
  int64_t last_modified_time = 0;
  for (auto spec: column_specs) {
    auto c = CreateTestColumn(std::get<0>(spec), std::get<1>(spec),
                              std::get<2>(spec));
    last_modified_time = std::max(last_modified_time, c -> Timestamp());
    columns.push_back(std::move(c));
  }
  return RowValue(std::move(columns), last_modified_time);
}

RowValue CreateRowTombstone(int64_t timestamp) {
  return RowValue(kLocalDeletionTime, timestamp);
}

void VerifyRowValueColumns(
  std::vector<std::unique_ptr<ColumnBase>> &columns,
  std::size_t index_of_vector,
  int8_t expected_mask,
  int8_t expected_index,
  int64_t expected_timestamp
) {
  EXPECT_EQ(expected_timestamp, columns[index_of_vector]->Timestamp());
  EXPECT_EQ(expected_mask, columns[index_of_vector]->Mask());
  EXPECT_EQ(expected_index, columns[index_of_vector]->Index());
}

}
}
