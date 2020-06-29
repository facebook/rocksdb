//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#ifdef USE_AWS

#include <aws/core/Aws.h>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
inline Aws::String ToAwsString(const std::string& s) {
  return Aws::String(s.data(), s.size());
}

}  // namespace ROCKSDB_NAMESPACE

#endif /* USE_AWS */
