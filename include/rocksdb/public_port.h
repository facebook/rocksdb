// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

// Best used after all the headers in case windows #defines give you trouble
// Minimum subset

#ifndef ROCKSDB_PUBLIC_PORT_H__
#define ROCKSDB_PUBLIC_PORT_H__

#ifdef OS_WIN

#include <stdint.h>

#undef min
#undef max
#undef DeleteFile
#undef GetCurrentTime

#ifndef strcasecmp
#define strcasecmp _stricmp
#endif

#ifndef snprintf
#define snprintf _snprintf
#endif

#ifndef ROCKSDB_PRIszt
#define ROCKSDB_PRIszt "Iu"
#endif

#ifndef __thread
#define __thread __declspec(thread)
#endif

#define __attribute__(A)

#endif


#endif // ROCKSDB_PUBLIC_PORT_H__
