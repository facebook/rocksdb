// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#pragma once

#include <iostream>

#include "rocksjni/cplusplus_to_java_convert.h"

// Remove macro on windows
#ifdef DELETE
#undef DELETE
#endif

#include "rocksjni/portal/java_class.h"
#include "rocksjni/portal/java_exception.h"
#include "rocksjni/portal/rocks_d_b_native_class.h"
#include "rocksjni/portal/sub_code_jni.h"
#include "rocksjni/portal/code_jni.h"
#include "rocksjni/portal/status_jni.h"
#include "rocksjni/portal/rocks_d_b_exception_jni.h"

#include "rocksjni/portal/byte_jni.h"
#include "rocksjni/portal/integer_jni.h"
#include "rocksjni/portal/long_jni.h"
#include "rocksjni/portal/jni_util.h"
#include "rocksjni/portal/map_jni.h"
#include "rocksjni/portal/hash_map_jni.h"

