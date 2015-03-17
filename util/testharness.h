//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <gtest/gtest.h>

#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include <string>
#include "port/stack_trace.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "util/random.h"
#include "util/string_util.h"

namespace rocksdb {
namespace test {

// Return the directory to use for temporary storage.
extern std::string TmpDir(Env* env = Env::Default());

// Return a randomization seed for this run.  Typically returns the
// same number on repeated invocations of this binary, but automated
// runs may be able to vary the seed.
extern int RandomSeed();

#define ASSERT_OK(s) ASSERT_TRUE(((s).ok()))
#define ASSERT_NOK(s) ASSERT_FALSE(((s).ok()))
#define EXPECT_OK(s) EXPECT_TRUE(((s).ok()))
#define EXPECT_NOK(s) EXPECT_FALSE(((s).ok()))

}  // namespace test
}  // namespace rocksdb
