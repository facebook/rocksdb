//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <gflags/gflags.h>

#include <functional>

#ifndef GFLAGS_NAMESPACE
// in case it's not defined in old versions, that's probably because it was
// still google by default.
#define GFLAGS_NAMESPACE google
#endif

#ifndef DEFINE_uint32
// DEFINE_uint32 does not appear in older versions of gflags. This should be
// a sane definition for those versions.
#include <cstdint>
#define DEFINE_uint32(name, val, txt)             \
  namespace gflags_compat {                       \
  DEFINE_int32(name, val, txt);                   \
  }                                               \
  std::reference_wrapper<uint32_t> FLAGS_##name = \
      std::ref(*reinterpret_cast<uint32_t *>(&gflags_compat::FLAGS_##name));

#define DECLARE_uint32(name) \
  extern std::reference_wrapper<uint32_t> FLAGS_##name;
#endif  // !DEFINE_uint32
