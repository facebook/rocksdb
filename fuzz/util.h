//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#define CHECK_OK(expression)                       \
  do {                                             \
    auto status = (expression);                    \
    if (!status.ok()) {                            \
      std::cerr << status.ToString() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0)

#define CHECK_EQ(a, b)                                                      \
  if (a != b) {                                                             \
    std::cerr << "(" << #a << "=" << a << ") != (" << #b << "=" << b << ")" \
              << std::endl;                                                 \
    abort();                                                                \
  }

#define CHECK_TRUE(cond)                                      \
  if (!(cond)) {                                              \
    std::cerr << "\"" << #cond << "\" is false" << std::endl; \
    abort();                                                  \
  }
