//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "port/stack_trace.h"
#include <assert.h>

namespace {
void f0() {
  char *p = nullptr;
  *p = 10;  /* SIGSEGV here!! */
}

void f1() {
  f0();
}

void f2() {
  f1();
}

void f3() {
  f2();
}
}  // namespace

int main() {
  rocksdb::port::InstallStackTraceHandler();

  f3();

  return 0;
}
