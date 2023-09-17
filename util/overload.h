//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// A helper template that can combine multiple functors into a single one to be
// used with std::visit for example. It also works with lambdas, since it
// comes with an explicit deduction guide.
template <typename... Ts>
struct overload : Ts... {
  using Ts::operator()...;
};

template <typename... Ts>
overload(Ts...) -> overload<Ts...>;

}  // namespace ROCKSDB_NAMESPACE
