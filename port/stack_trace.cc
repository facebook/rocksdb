//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "util/stack_trace.h"

#ifdef OS_LINUX

#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

namespace rocksdb {

static const char* GetExecutableName()
{
  static char name[1024];

  char link[1024];
  snprintf(link, sizeof(link), "/proc/%d/exe", getpid());
  auto read = readlink(link, name, sizeof(name));
  if (-1 == read) {
    return nullptr;
  } else {
    name[read] = 0;
    return name;
  }
}

static void StackTraceHandler(int sig) {
  // reset to default handler
  signal(sig, SIG_DFL);

  fprintf(stderr, "Received signal %d (%s)\n", sig, strsignal(sig));

  const int kMaxFrames = 100;
  void *frames[kMaxFrames];

  auto num_frames = backtrace(frames, kMaxFrames);
  auto symbols = backtrace_symbols(frames, num_frames);

  auto executable = GetExecutableName();

  const int kSkip = 2; // skip the top two signal handler related frames

  for (int i = kSkip; i < num_frames; ++i)
  {
    fprintf(stderr, "#%-2d %p ", i - kSkip, frames[i]);
    if (symbols) {
      fprintf(stderr, "%s ", symbols[i]);
    }
    if (executable) {
      // out source to addr2line, for the address translation
      const int kLineMax = 256;
      char cmd[kLineMax];
      sprintf(cmd,"addr2line %p -e %s 2>&1", frames[i] , executable);
      auto f = popen(cmd, "r");
      if (f) {
        char line[kLineMax];
        while (fgets(line, sizeof(line), f)) {
          fprintf(stderr, "%s", line);
        }
        pclose(f);
      } else {
        fprintf(stderr, "\n");
      }
    } else {
      fprintf(stderr, "\n");
    }
  }

  // re-signal to default handler (so we still get core dump if needed...)
  raise(sig);
}

void InstallStackTraceHandler() {
  // just use the plain old signal as it's simple and sufficient
  // for this use case
  signal(SIGILL, StackTraceHandler);
  signal(SIGSEGV, StackTraceHandler);
  signal(SIGBUS, StackTraceHandler);
  signal(SIGABRT, StackTraceHandler);

  printf("Installed stack trace handler for SIGILL SIGSEGV SIGBUS SIGABRT\n");

}

}   // namespace rocksdb

#else // no-op for non-linux system for now

namespace rocksdb {

void InstallStackTraceHandler() {}

}

#endif // OS_LINUX
