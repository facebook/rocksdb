//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#ifdef USE_AWS

#include <aws/core/Aws.h>

namespace rocksdb {
inline Aws::String ToAwsString(const std::string& s) {
  return Aws::String(s.data(), s.size());
}

}  // namepace rocksdb

#endif /* USE_AWS */
