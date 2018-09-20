//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

namespace rocksdb {

template<class Lambda>
struct c_style_callback_fetcher {
  template<class R, class... Args>
  static R invoke(void* vlamb, Args... args) {
    return (*(Lambda*)vlamb)(std::forward<Args>(args)...);
  }

  template<class R, class... Args>
  using target_callback = R (*)(void*, Args...);

  template<class R, class... Args>
  operator target_callback<R, Args...>() const {
    return &c_style_callback_fetcher::invoke<R, Args...>;
  }
};

template<class Lambda>
c_style_callback_fetcher<Lambda> c_style_callback(Lambda&) {
  return c_style_callback_fetcher<Lambda>();
}

}  // namespace rocksdb
