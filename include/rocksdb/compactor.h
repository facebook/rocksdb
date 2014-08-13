// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "rocksdb/listener.h"

namespace rocksdb {

class Compactor : public EventListener {
};

}  // namespace rocksdb
