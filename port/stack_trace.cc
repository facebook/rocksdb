//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "port/stack_trace.h"

#if defined(ROCKSDB_LITE) ||                                                  \
    !(defined(ROCKSDB_BACKTRACE) || defined(OS_MACOSX)) || defined(CYGWIN) || \
    defined(OS_SOLARIS) || defined(OS_WIN)

// noop

namespace ROCKSDB_NAMESPACE {
namespace port {
void InstallStackTraceHandler() {}
void PrintStack(int /*first_frames_to_skip*/) {}
void PrintAndFreeStack(void* /*callstack*/, int /*num_frames*/) {}
void* SaveStack(int* /*num_frames*/, int /*first_frames_to_skip*/) {
  return nullptr;
}
}  // namespace port
}  // namespace ROCKSDB_NAMESPACE

#else

#include <cxxabi.h>
#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#if defined(OS_FREEBSD)
#include <sys/sysctl.h>
#endif
#ifdef OS_LINUX
#include <sys/prctl.h>
#endif

#include "port/lang.h"

namespace ROCKSDB_NAMESPACE {
namespace port {

namespace {

#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_GNU_KFREEBSD)
const char* GetExecutableName() {
  static char name[1024];

#if !defined(OS_FREEBSD)
  char link[1024];
  snprintf(link, sizeof(link), "/proc/%d/exe", getpid());
  auto read = readlink(link, name, sizeof(name) - 1);
  if (-1 == read) {
    return nullptr;
  } else {
    name[read] = 0;
    return name;
  }
#else
  int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PATHNAME, -1};
  size_t namesz = sizeof(name);

  auto ret = sysctl(mib, 4, name, &namesz, nullptr, 0);
  if (-1 == ret) {
    return nullptr;
  } else {
    return name;
  }
#endif
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
#elif defined(OS_MACOSX)

void PrintStackTraceLine(const char* symbol, void* frame) {
  static int pid = getpid();
  // out source to atos, for the address translation
  const int kLineMax = 256;
  char cmd[kLineMax];
  snprintf(cmd, kLineMax, "xcrun atos %p -p %d  2>&1", frame, pid);
  auto f = popen(cmd, "r");
  if (f) {
    char line[kLineMax];
    while (fgets(line, sizeof(line), f)) {
      line[strlen(line) - 1] = 0;  // remove newline
      fprintf(stderr, "%s\t", line);
    }
    pclose(f);
  } else if (symbol) {
    fprintf(stderr, "%s ", symbol);
  }

  fprintf(stderr, "\n");
}

#endif

}  // namespace

void PrintStack(void* frames[], int num_frames) {
  auto symbols = backtrace_symbols(frames, num_frames);

  for (int i = 0; i < num_frames; ++i) {
    fprintf(stderr, "#%-2d  ", i);
    PrintStackTraceLine((symbols != nullptr) ? symbols[i] : nullptr, frames[i]);
  }
  free(symbols);
}

void PrintStack(int first_frames_to_skip) {
  const int kMaxFrames = 100;
  void* frames[kMaxFrames];

  auto num_frames = backtrace(frames, kMaxFrames);
  PrintStack(&frames[first_frames_to_skip], num_frames - first_frames_to_skip);
}

void PrintAndFreeStack(void* callstack, int num_frames) {
  PrintStack(static_cast<void**>(callstack), num_frames);
  free(callstack);
}

void* SaveStack(int* num_frames, int first_frames_to_skip) {
  const int kMaxFrames = 100;
  void* frames[kMaxFrames];

  auto count = backtrace(frames, kMaxFrames);
  *num_frames = count - first_frames_to_skip;
  void* callstack = malloc(sizeof(void*) * *num_frames);
  memcpy(callstack, &frames[first_frames_to_skip], sizeof(void*) * *num_frames);
  return callstack;
}

static void StackTraceHandler(int sig) {
  // reset to default handler
  signal(sig, SIG_DFL);
  fprintf(stderr, "Received signal %d (%s)\n", sig, strsignal(sig));
  // skip the top three signal handler related frames
  PrintStack(3);

  // Efforts to fix or suppress TSAN warnings "signal-unsafe call inside of
  // a signal" have failed, so just warn the user about them.
#ifdef __SANITIZE_THREAD__
  fprintf(stderr,
          "==> NOTE: any above warnings about \"signal-unsafe call\" are\n"
          "==> ignorable, as they are expected when generating a stack\n"
          "==> trace because of a signal under TSAN. Consider why the\n"
          "==> signal was generated to begin with, and the stack trace\n"
          "==> in the TSAN warning can be useful for that. (The stack\n"
          "==> trace printed by the signal handler is likely obscured\n"
          "==> by TSAN output.)\n");
#endif

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
  // Allow ouside debugger to attach, even with Yama security restrictions
#ifdef PR_SET_PTRACER_ANY
  (void)prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
#endif
}

}  // namespace port
}  // namespace ROCKSDB_NAMESPACE

#endif
