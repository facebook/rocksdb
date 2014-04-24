//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "port/stack_trace.h"

namespace rocksdb {
namespace port {

#if defined(ROCKSDB_LITE) || !(defined(OS_LINUX) || defined(OS_MACOSX))

// noop

void InstallStackTraceHandler() {}
void PrintStack(int first_frames_to_skip) {}

#else

#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <cxxabi.h>

namespace {

#ifdef OS_LINUX
const char* GetExecutableName() {
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

void PrintStackTraceLine(const char* symbol, void* frame) {
  static const char* executable = GetExecutableName();
  if (symbol) {
    fprintf(stderr, "%s ", symbol);
  }
  if (executable) {
    // out source to addr2line, for the address translation
    const int kLineMax = 256;
    char cmd[kLineMax];
    snprintf(cmd, kLineMax, "addr2line %p -e %s -f -C 2>&1", frame, executable);
    auto f = popen(cmd, "r");
    if (f) {
      char line[kLineMax];
      while (fgets(line, sizeof(line), f)) {
        line[strlen(line) - 1] = 0;  // remove newline
        fprintf(stderr, "%s\t", line);
      }
      pclose(f);
    }
  } else {
    fprintf(stderr, " %p", frame);
  }

  fprintf(stderr, "\n");
}
#elif OS_MACOSX

void PrintStackTraceLine(const char* symbol, void* frame) {
  if (symbol) {
    char filename[64], function[512], plus[2], line[10];
    sscanf(symbol, "%*s %64s %*s %512s %2s %10s", filename, function, plus,
           line);
    int status;
    char* demangled = abi::__cxa_demangle(function, 0, 0, &status);
    fprintf(stderr, "%s   %s %s %s", filename,
            (status == 0) ? demangled : function, plus, line);
    if (demangled) {
      free(demangled);
    }
  }
  fprintf(stderr, " %p", frame);
  fprintf(stderr, "\n");
}

#endif

}  // namespace

void PrintStack(int first_frames_to_skip) {
  const int kMaxFrames = 100;
  void* frames[kMaxFrames];

  auto num_frames = backtrace(frames, kMaxFrames);
  auto symbols = backtrace_symbols(frames, num_frames);

  for (int i = first_frames_to_skip; i < num_frames; ++i) {
    fprintf(stderr, "#%-2d  ", i - first_frames_to_skip);
    PrintStackTraceLine((symbols != nullptr) ? symbols[i] : nullptr, frames[i]);
  }
}

static void StackTraceHandler(int sig) {
  // reset to default handler
  signal(sig, SIG_DFL);
  fprintf(stderr, "Received signal %d (%s)\n", sig, strsignal(sig));
  // skip the top three signal handler related frames
  PrintStack(3);
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
}

#endif

}  // namespace port
}  // namespace rocksdb
