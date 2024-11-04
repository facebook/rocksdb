//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "db_stress_tool/expected_state.h"

namespace ROCKSDB_NAMESPACE {

class ExpectedStateDumpTool {
 public:
  int Run(int argc, char const* const* argv);
};

}  // namespace ROCKSDB_NAMESPACE
