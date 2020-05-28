// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <functional>

namespace ROCKSDB_NAMESPACE {

// Defers the execution of the provided function until the Defer
// object goes out of scope.
//
// Usage example:
//
// Status DeferTest() {
//   Status s;
//   Defer defer([&s]() {
//     if (!s.ok()) {
//       // do cleanups ...
//     }
//   });
//   // do something ...
//   if (!s.ok()) return;
//   // do some other things ...
//   return s;
// }
//
// The above code ensures that cleanups will always happen on returning.
//
// Without the help of Defer, you can
// 1. every time when !s.ok(), do the cleanup;
// 2. instead of returning when !s.ok(), continue the work only when s.ok(),
//    but sometimes, this might lead to nested blocks of "if (s.ok()) {...}".
//
// With the help of Defer, you can centralize the cleanup logic inside the
// lambda passed to Defer, and you can return immediately on failure when necessary.
class Defer final {
 public:
  Defer(std::function<void()>&& fn) : fn_(std::move(fn)) {}
  ~Defer() { fn_(); }

  // Disallow copy.
  Defer(const Defer&) = delete;
  Defer& operator=(const Defer&) = delete;

 private:
  std::function<void()> fn_;
};

}  // namespace ROCKSDB_NAMESPACE
