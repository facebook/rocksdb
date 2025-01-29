// Copyright (c) 2013-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/tool_hooks.h"

namespace ROCKSDB_NAMESPACE {
int db_bench_tool(int argc, char** argv, ToolHooks& hooks = defaultHooks);
}  // namespace ROCKSDB_NAMESPACE
