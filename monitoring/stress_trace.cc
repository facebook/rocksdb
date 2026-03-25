//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is governed by the Apache 2.0 and GPLv2 licenses found in
//  the COPYING and LICENSE.Apache files in the root directory.

// stress_trace.cc — implementation of the stress test execution tracer.
//
// This file is always compiled into db_stress. When ROCKSDB_STRESS_TRACE is
// defined (via STRESS_TRACE=1 make flag), it provides the full tracing
// implementation: __cyg_profile_func_enter hook, per-thread ring buffers,
// and crash-time dump logic. When not defined, it provides empty stubs so
// that db_stress_tool.cc can unconditionally call Install() and DumpAll().
//
// All tracing functions are marked ROCKSDB_NO_INSTRUMENT to prevent the
// compiler from inserting recursive instrumentation hooks.

#include "monitoring/stress_trace.h"

#ifdef ROCKSDB_STRESS_TRACE


#include <atomic>
#include <cstdio>
#include <cstring>
#include <string>

#ifndef OS_WIN
#include <dlfcn.h>
#include <fcntl.h>
#include <unistd.h>
#endif

#include "port/stack_trace.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
namespace stress_trace {

// ============================================================
// Global state
// ============================================================

// Linked list of per-thread state structures.
static std::atomic<ThreadState*> g_thread_list{nullptr};

ROCKSDB_NO_INSTRUMENT std::atomic<ThreadState*>& GlobalThreadList() {
  return g_thread_list;
}

// Path prefix for dump files. Set by Install().
static char g_dump_prefix[PATH_MAX];
static std::atomic<bool> g_installed{false};

// ============================================================
// Thread-local buffers
// ============================================================
// Each thread lazily allocates its FuncTraceBuffer and EventTraceBuffer on
// first use and registers them in the global list.

ROCKSDB_NO_INSTRUMENT static void RegisterThread(FuncTraceBuffer* fb,
                                                  EventTraceBuffer* eb,
                                                  BinaryTraceBuffer* bb) {
  // Allocate a ThreadState. We intentionally leak this object -- it must
  // outlive the thread for the crash handler to read it. The leak is bounded:
  // one small struct per thread (< 100 bytes).
  ThreadState* ts = new ThreadState();
  ts->func_buf = fb;
  ts->event_buf = eb;
  ts->binary_buf = bb;
  // Use a stable hash of thread_id for display (same technique as
  // InjectedErrorLog).
  ts->thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());

  // Prepend to the global list (lock-free).
  ThreadState* old_head = g_thread_list.load(std::memory_order_relaxed);
  do {
    ts->next = old_head;
  } while (!g_thread_list.compare_exchange_weak(old_head, ts,
                                                std::memory_order_release,
                                                std::memory_order_relaxed));
}

// Thread-local buffers and initialization state.
// We use heap-allocated buffers via malloc (not new) to avoid constructor
// calls that -finstrument-functions would instrument, causing infinite
// recursion: __cyg_profile_func_enter -> GetFuncBuffer ->
// new FuncTraceBuffer -> FuncTraceBuffer() [instrumented] ->
// __cyg_profile_func_enter -> GetFuncBuffer -> stack overflow.
//
// The buffers are intentionally leaked (never freed) so they survive thread
// exit. This is critical: the crash handler iterates all registered threads
// and reads their buffers. If buffers were thread-local stack storage, they
// would be reclaimed on thread exit, leaving dangling pointers that cause
// segfaults in the crash handler. The leak is bounded: ~165 KB per thread
// (128 KB func buffer + 36 KB event buffer).
//
// The recursion guard (tl_initializing) breaks any remaining cycle:
// during initialization, instrumented calls get nullptr and silently
// skip tracing. Once init completes, all subsequent calls hit the fast path.

// File-scope thread-local state shared by GetFuncBuffer/GetEventBuffer.
static thread_local bool tl_initializing = false;
static thread_local bool tl_registered = false;
static thread_local FuncTraceBuffer* tl_func_buf = nullptr;
static thread_local EventTraceBuffer* tl_evt_buf = nullptr;
static thread_local BinaryTraceBuffer* tl_bin_buf = nullptr;

ROCKSDB_NO_INSTRUMENT static void EnsureInitialized() {
  if (__builtin_expect(tl_func_buf != nullptr, 1)) {
    return;
  }
  // Recursion guard.
  if (tl_initializing) {
    return;
  }
  tl_initializing = true;

  // Heap-allocate via malloc + memset (no constructors, no instrumentation).
  // Intentionally leaked so crash handler can read after thread exit.
  tl_func_buf =
      static_cast<FuncTraceBuffer*>(malloc(sizeof(FuncTraceBuffer)));
  tl_evt_buf =
      static_cast<EventTraceBuffer*>(malloc(sizeof(EventTraceBuffer)));
  tl_bin_buf =
      static_cast<BinaryTraceBuffer*>(malloc(sizeof(BinaryTraceBuffer)));
  memset(static_cast<void*>(tl_func_buf), 0, sizeof(FuncTraceBuffer));
  memset(static_cast<void*>(tl_evt_buf), 0, sizeof(EventTraceBuffer));
  memset(static_cast<void*>(tl_bin_buf), 0, sizeof(BinaryTraceBuffer));

  if (!tl_registered) {
    tl_registered = true;
    RegisterThread(tl_func_buf, tl_evt_buf, tl_bin_buf);
  }

  tl_initializing = false;
}

ROCKSDB_NO_INSTRUMENT FuncTraceBuffer* GetFuncBuffer() {
  EnsureInitialized();
  return tl_func_buf;  // nullptr during recursive init — caller handles it
}

ROCKSDB_NO_INSTRUMENT EventTraceBuffer* GetEventBuffer() {
  EnsureInitialized();
  return tl_evt_buf;
}

ROCKSDB_NO_INSTRUMENT BinaryTraceBuffer* GetBinaryBuffer() {
  EnsureInitialized();
  return tl_bin_buf;
}

// ============================================================
// Public API
// ============================================================

ROCKSDB_NO_INSTRUMENT void RecordEvent(const char* fmt, ...) {
  EventTraceBuffer* eb = GetEventBuffer();
  va_list args;
  va_start(args, fmt);
  eb->Record(fmt, args);
  va_end(args);
}

ROCKSDB_NO_INSTRUMENT void Install(const std::string& dump_path_prefix) {
  size_t len = std::min(dump_path_prefix.size(), sizeof(g_dump_prefix) - 1);
  memcpy(g_dump_prefix, dump_path_prefix.data(), len);
  g_dump_prefix[len] = '\0';
  g_installed.store(true, std::memory_order_release);

  // Register our dump function as the crash callback so that on any signal
  // (SIGABRT, SIGSEGV, SIGQUIT, etc.) we dump the trace before the stack
  // trace is printed.
  //
  // NOTE: This will REPLACE any previously registered crash callback
  // (including the fault injection error log). In db_stress_tool.cc,
  // Install() should be called AFTER the fault injection callback is set up,
  // and the Install() callback should chain to the fault injection dump.
  // Alternatively, both can be unified into a single crash callback.
  //
  // For simplicity, we call DumpAll() here and let db_stress_tool.cc set up
  // a unified callback that calls both DumpAll() and
  // PrintRecentInjectedErrors().
}

// ============================================================
// Crash-time dump -- async-signal-safe
// ============================================================
// Rules for signal-safe code:
//   OK: open, write, close, snprintf, arithmetic
//   NOT OK: printf, fprintf, malloc, free, new, delete, STL containers,
//           dladdr (technically not async-signal-safe but widely used),
//           mutex operations

// Resolve a function address to a symbol name. Writes into buf (max buflen).
// Uses dladdr -- not strictly async-signal-safe but works in practice.
// Falls back to hex address if dladdr fails.
ROCKSDB_NO_INSTRUMENT static void ResolveSymbol(void* addr, char* buf,
                                                 size_t buflen) {
#ifndef OS_WIN
  Dl_info info;
  if (dladdr(addr, &info) && info.dli_sname) {
    // Found a symbol. Truncate to buflen-1.
    size_t n = 0;
    while (n < buflen - 1 && info.dli_sname[n]) {
      buf[n] = info.dli_sname[n];
      ++n;
    }
    buf[n] = '\0';
  } else {
    snprintf(buf, buflen, "%p", addr);
  }
#else
  snprintf(buf, buflen, "%p", addr);
#endif
}

// Write a fixed-size chunk to fd. Ignores partial writes (signal-handler
// context: nothing useful we can do).
ROCKSDB_NO_INSTRUMENT static void WriteAll(int fd, const char* buf, int len) {
  if (len <= 0) return;
  auto unused __attribute__((unused)) = write(fd, buf, (size_t)len);
}

// Format a TraceEventType to a short string.
ROCKSDB_NO_INSTRUMENT static const char* EventTypeName(TraceEventType t) {
  switch (t) {
    case TraceEventType::kMutexLock:        return "MUTEX_LOCK";
    case TraceEventType::kMutexUnlock:      return "MUTEX_UNLOCK";
    case TraceEventType::kCvWaitBegin:      return "CV_WAIT_BEGIN";
    case TraceEventType::kCvWaitEnd:        return "CV_WAIT_END";
    case TraceEventType::kCvTimedWaitBegin: return "CV_TIMED_WAIT_BEGIN";
    case TraceEventType::kCvTimedWaitEnd:   return "CV_TIMED_WAIT_END";
    case TraceEventType::kCvSignal:         return "CV_SIGNAL";
    case TraceEventType::kCvSignalAll:      return "CV_SIGNAL_ALL";
    case TraceEventType::kSyncPoint:        return "SYNCPOINT";
    case TraceEventType::kAtomicStore:      return "ATOMIC_STORE";
    case TraceEventType::kAtomicLoad:       return "ATOMIC_LOAD";
    case TraceEventType::kAtomicCas:        return "ATOMIC_CAS";
    case TraceEventType::kAtomicXchg:       return "ATOMIC_XCHG";
    case TraceEventType::kAtomicAdd:        return "ATOMIC_ADD";
    case TraceEventType::kAtomicSub:        return "ATOMIC_SUB";
    default:                                return "UNKNOWN";
  }
}

// Format a binary event into a text line. Returns length written.
ROCKSDB_NO_INSTRUMENT static int FormatBinaryEvent(char* buf, size_t buflen,
                                                    const BinaryEvent* ev) {
  TraceEventType type = ev->Type();
  uint64_t obj = ev->ObjPtr();
  const char* name = EventTypeName(type);

  switch (type) {
    case TraceEventType::kMutexLock:
    case TraceEventType::kMutexUnlock:
    case TraceEventType::kCvWaitBegin:
    case TraceEventType::kCvWaitEnd:
    case TraceEventType::kCvSignal:
    case TraceEventType::kCvSignalAll:
      return snprintf(buf, buflen, "  [%llu] EVENT %s obj=0x%llx\n",
                      (unsigned long long)ev->tsc, name,
                      (unsigned long long)obj);

    case TraceEventType::kCvTimedWaitBegin:
      return snprintf(buf, buflen,
                      "  [%llu] EVENT %s obj=0x%llx timeout=%llu\n",
                      (unsigned long long)ev->tsc, name,
                      (unsigned long long)obj,
                      (unsigned long long)ev->val1);

    case TraceEventType::kCvTimedWaitEnd:
      return snprintf(buf, buflen,
                      "  [%llu] EVENT %s obj=0x%llx timed_out=%d\n",
                      (unsigned long long)ev->tsc, name,
                      (unsigned long long)obj, (int)ev->val1);

    case TraceEventType::kSyncPoint:
      // val1 is a pointer to the name string (may be invalid after crash
      // if it was on a destroyed thread's stack, but usually it's a
      // string literal which lives in .rodata).
      return snprintf(buf, buflen, "  [%llu] EVENT %s %s\n",
                      (unsigned long long)ev->tsc, name,
                      ev->val1 ? (const char*)ev->val1 : "(null)");

    case TraceEventType::kAtomicStore:
    case TraceEventType::kAtomicLoad:
      return snprintf(buf, buflen,
                      "  [%llu] EVENT %s obj=0x%llx val=%llu\n",
                      (unsigned long long)ev->tsc, name,
                      (unsigned long long)obj,
                      (unsigned long long)ev->val1);

    case TraceEventType::kAtomicCas:
      return snprintf(buf, buflen,
                      "  [%llu] EVENT %s obj=0x%llx ok=%d val=%llu\n",
                      (unsigned long long)ev->tsc, name,
                      (unsigned long long)obj, (int)ev->val2,
                      (unsigned long long)ev->val1);

    case TraceEventType::kAtomicXchg:
      return snprintf(buf, buflen,
                      "  [%llu] EVENT %s obj=0x%llx old=%llu new=%llu\n",
                      (unsigned long long)ev->tsc, name,
                      (unsigned long long)obj,
                      (unsigned long long)ev->val1,
                      (unsigned long long)ev->val2);

    case TraceEventType::kAtomicAdd:
    case TraceEventType::kAtomicSub:
      return snprintf(buf, buflen,
                      "  [%llu] EVENT %s obj=0x%llx old=%llu delta=%lld\n",
                      (unsigned long long)ev->tsc, name,
                      (unsigned long long)obj,
                      (unsigned long long)ev->val1,
                      (long long)ev->val2);

    default:
      return snprintf(buf, buflen,
                      "  [%llu] EVENT type=%d obj=0x%llx v1=%llu v2=%llu\n",
                      (unsigned long long)ev->tsc, (int)type,
                      (unsigned long long)obj,
                      (unsigned long long)ev->val1,
                      (unsigned long long)ev->val2);
  }
}

// Dump one thread's combined (func + event) trace to fd, sorted by TSC.
// We interleave the two sorted sequences using a simple merge (no heap).
ROCKSDB_NO_INSTRUMENT static void DumpThread(int fd, const ThreadState* ts) {
  const FuncTraceBuffer* fb = ts->func_buf;
  const EventTraceBuffer* eb = ts->event_buf;
  const BinaryTraceBuffer* bb = ts->binary_buf;

  char hdr[256];
  int hlen = snprintf(hdr, sizeof(hdr),
                      "\n=== Stress Trace: thread %llu ===\n"
                      "    func_entries=%zu  event_entries=%zu"
                      "  binary_entries=%zu\n",
                      (unsigned long long)ts->thread_id,
                      std::min(fb->head, kMaxFuncEntries),
                      std::min(eb->head, kMaxEventEntries),
                      bb ? std::min(bb->head, kMaxBinaryEntries) : (size_t)0);
  WriteAll(fd, hdr, hlen);

  // Determine valid range in each ring buffer.
  size_t f_total = fb->head;
  size_t f_count = (f_total < kMaxFuncEntries) ? f_total : kMaxFuncEntries;
  size_t f_start =
      (f_total >= kMaxFuncEntries) ? (f_total % kMaxFuncEntries) : 0;

  size_t e_total = eb->head;
  size_t e_count = (e_total < kMaxEventEntries) ? e_total : kMaxEventEntries;
  size_t e_start =
      (e_total >= kMaxEventEntries) ? (e_total % kMaxEventEntries) : 0;

  size_t b_total = bb ? bb->head : 0;
  size_t b_count =
      (b_total < kMaxBinaryEntries) ? b_total : kMaxBinaryEntries;
  size_t b_start =
      (b_total >= kMaxBinaryEntries) ? (b_total % kMaxBinaryEntries) : 0;

  if (f_count == 0 && e_count == 0 && b_count == 0) {
    const char* empty = "  (empty)\n";
    WriteAll(fd, empty, 10);
    return;
  }

  // Merge-print: walk all three buffers in TSC order (oldest first).
  size_t fi = 0;  // index into func ring (logical)
  size_t ei = 0;  // index into event ring (logical)
  size_t bi = 0;  // index into binary ring (logical)

  char line[512];
  char sym[256];

  while (fi < f_count || ei < e_count || bi < b_count) {
    // Peek at next TSC from each buffer.
    uint64_t f_tsc = UINT64_MAX;
    uint64_t e_tsc = UINT64_MAX;
    uint64_t b_tsc = UINT64_MAX;

    if (fi < f_count) {
      size_t ridx = (f_start + fi) % kMaxFuncEntries;
      f_tsc = fb->entries[ridx].tsc;
    }
    if (ei < e_count) {
      size_t ridx = (e_start + ei) % kMaxEventEntries;
      e_tsc = eb->entries[ridx].tsc;
    }
    if (bi < b_count) {
      size_t ridx = (b_start + bi) % kMaxBinaryEntries;
      b_tsc = bb->entries[ridx].tsc;
    }

    if (f_tsc <= e_tsc && f_tsc <= b_tsc) {
      // Emit func entry.
      size_t ridx = (f_start + fi) % kMaxFuncEntries;
      void* addr = fb->entries[ridx].func_addr;
      ResolveSymbol(addr, sym, sizeof(sym));
      int n = snprintf(line, sizeof(line), "  [%llu] ENTER %s (%p)\n",
                       (unsigned long long)f_tsc, sym, addr);
      WriteAll(fd, line, n);
      ++fi;
    } else if (e_tsc <= b_tsc) {
      // Emit string event entry.
      size_t ridx = (e_start + ei) % kMaxEventEntries;
      char local_msg[kMaxEventMsgLen];
      const volatile char* src = eb->entries[ridx].msg;
      for (size_t i = 0; i < kMaxEventMsgLen; i++) local_msg[i] = src[i];
      int n = snprintf(line, sizeof(line), "  [%llu] EVENT %s\n",
                       (unsigned long long)e_tsc, local_msg);
      WriteAll(fd, line, n);
      ++ei;
    } else {
      // Emit binary event entry.
      size_t ridx = (b_start + bi) % kMaxBinaryEntries;
      int n = FormatBinaryEvent(line, sizeof(line), &bb->entries[ridx]);
      WriteAll(fd, line, n);
      ++bi;
    }
  }

  const char* end_marker = "=== End of thread trace ===\n";
  WriteAll(fd, end_marker, (int)strlen(end_marker));
}

ROCKSDB_NO_INSTRUMENT void DumpAll() {
  // Walk the global thread list and dump each thread's trace.
  // This is called from the crash callback (signal handler context).
  // The linked list was populated by RegisterThread() -- no locks needed.
  ThreadState* ts = g_thread_list.load(std::memory_order_acquire);

  if (!ts) {
    // No threads registered -- tracing was never triggered.
    const char* msg =
        "\n[stress_trace] No trace data (no threads registered)\n";
    auto unused __attribute__((unused)) =
        write(STDOUT_FILENO, msg, strlen(msg));
    return;
  }

  // Count threads for the summary header.
  int n_threads = 0;
  for (ThreadState* cur = ts; cur; cur = cur->next) ++n_threads;

  char hdr[256];
  int hlen =
      snprintf(hdr, sizeof(hdr),
               "\n========================================\n"
               " ROCKSDB STRESS TRACE DUMP (%d threads)\n"
               " Last %zu function entries + %zu events per thread\n"
               "========================================\n",
               n_threads, kMaxFuncEntries, kMaxEventEntries);
  auto unused __attribute__((unused)) = write(STDOUT_FILENO, hdr, hlen);

  while (ts) {
    int fd = STDOUT_FILENO;
    bool close_fd = false;

    // If a dump prefix was set, write to a per-thread file.
    if (g_dump_prefix[0] != '\0') {
      char path[PATH_MAX + 64];
      snprintf(path, sizeof(path), "%s-thread-%llu.txt", g_dump_prefix,
               (unsigned long long)ts->thread_id);
      int opened = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (opened >= 0) {
        fd = opened;
        close_fd = true;
      }
      // Also announce the file path to stdout.
      char announce[PATH_MAX + 128];
      int alen = snprintf(announce, sizeof(announce),
                          "[stress_trace] Thread %llu -> %s\n",
                          (unsigned long long)ts->thread_id, path);
      auto unused2 __attribute__((unused)) =
          write(STDOUT_FILENO, announce, alen);
    }

    DumpThread(fd, ts);

    if (close_fd) close(fd);
    ts = ts->next;
  }
}

}  // namespace stress_trace
}  // namespace ROCKSDB_NAMESPACE

// ============================================================
// Compiler instrumentation hooks
// ============================================================
// __cyg_profile_func_enter is called by the compiler at every function
// entry when -finstrument-functions is active. It must be fast and
// non-recursive (ROCKSDB_NO_INSTRUMENT prevents self-instrumentation).
//
// We ONLY record the func pointer (not caller) to save half the memory and
// keep the hot path as short as possible.

extern "C" {

ROCKSDB_NO_INSTRUMENT void __cyg_profile_func_enter(void* func,
                                                    void* /*caller*/) {
  ROCKSDB_NAMESPACE::stress_trace::FuncTraceBuffer* buf =
      ROCKSDB_NAMESPACE::stress_trace::GetFuncBuffer();
  // nullptr during thread-local initialization (recursion guard active).
  if (__builtin_expect(buf != nullptr, 1)) {
    buf->Record(func);
  }
}

// We don't record function exits -- it doubles the trace volume without
// adding much debugging value for our use case. If you want call durations,
// record both enter and exit and match by depth.
ROCKSDB_NO_INSTRUMENT void __cyg_profile_func_exit(void* /*func*/,
                                                    void* /*caller*/) {}

}  // extern "C"

#else  // !ROCKSDB_STRESS_TRACE

// When tracing is disabled, provide empty stubs so that any code that
// #includes this header and calls RecordEvent() still compiles.

namespace ROCKSDB_NAMESPACE {
namespace stress_trace {

ROCKSDB_NO_INSTRUMENT void Install(const std::string& /*dump_path_prefix*/) {}
ROCKSDB_NO_INSTRUMENT void DumpAll() {}
ROCKSDB_NO_INSTRUMENT void RecordEvent(const char* /*fmt*/, ...) {}
ROCKSDB_NO_INSTRUMENT FuncTraceBuffer* GetFuncBuffer() { return nullptr; }
ROCKSDB_NO_INSTRUMENT EventTraceBuffer* GetEventBuffer() { return nullptr; }
ROCKSDB_NO_INSTRUMENT BinaryTraceBuffer* GetBinaryBuffer() { return nullptr; }
ROCKSDB_NO_INSTRUMENT std::atomic<ThreadState*>& GlobalThreadList() {
  static std::atomic<ThreadState*> empty{nullptr};
  return empty;
}

}  // namespace stress_trace
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_STRESS_TRACE
