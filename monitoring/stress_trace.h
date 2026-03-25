//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is governed by the Apache 2.0 and GPLv2 licenses found in
//  the COPYING and LICENSE.Apache files in the root directory.

#pragma once

// stress_trace.h — Lightweight execution tracing for stress test debugging.
//
// This file provides two complementary tracing mechanisms:
//
// 1. AUTOMATIC function-call tracing via -finstrument-functions
//    When db_stress is built with ROCKSDB_STRESS_TRACE=1, the compiler inserts
//    a call to __cyg_profile_func_enter() at every function entry. This
//    implementation records the function address + TSC timestamp into a
//    per-thread lock-free ring buffer (last kMaxFuncEntries entries per
//    thread). On crash, all thread buffers are dumped and function addresses
//    are resolved to symbols via dladdr() or /proc/self/exe + addr2line.
//
//    Overhead: ~0% for functions doing real work (>= 1us). Can be ~2x for
//    trivial leaf functions. Exclude hot leaves with:
//      __attribute__((no_instrument_function))
//    See ROCKSDB_NO_INSTRUMENT below.
//
// 2. MANUAL semantic event tracing via STRESS_TRACE_EVENT()
//    A printf-style macro that records human-readable messages into a
//    per-thread ring buffer (last kMaxEventEntries entries). Use at key
//    decision points to capture context: flags, state, outcomes. These are
//    interleaved with function traces by timestamp in the crash dump.
//
//    Both buffers are printed on crash via RegisterCrashCallback().
//
// Usage:
//   Build: add -DROCKSDB_STRESS_TRACE=1 -finstrument-functions to CXXFLAGS
//          for db_stress targets only (see Makefile STRESS_TRACE_FLAGS).
//
//   In db_stress_tool.cc, call:
//     stress_trace::Install(crash_dump_path_prefix);
//
//   At key decision points:
//     STRESS_TRACE_EVENT("flush_reason=%d imm_count=%zu", reason, imm_count);
//
// Signal safety: PrintAll() uses only open/write/close/snprintf — no malloc,
// no stdio. Safe to call from a signal handler (same guarantee as
// InjectedErrorLog::PrintAll).

#include <atomic>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>
#include <thread>

#ifndef OS_WIN
#include <dlfcn.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#endif

#include "port/lang.h"
#include "rocksdb/rocksdb_namespace.h"

// PATH_MAX fallback
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

// ROCKSDB_NO_INSTRUMENT — mark a function so the compiler does NOT insert
// entry/exit hooks. Use on hot leaf functions and on the trace machinery
// itself to avoid infinite recursion and unnecessary overhead.
#if defined(__GNUC__) || defined(__clang__)
#define ROCKSDB_NO_INSTRUMENT __attribute__((no_instrument_function))
#else
#define ROCKSDB_NO_INSTRUMENT
#endif

// STRESS_TRACE_EVENT(fmt, ...) — record a semantic event into the per-thread
// event ring buffer. No-op unless ROCKSDB_STRESS_TRACE is defined.
#ifdef ROCKSDB_STRESS_TRACE
#define STRESS_TRACE_EVENT(fmt, ...)                            \
  ::ROCKSDB_NAMESPACE::stress_trace::RecordEvent(fmt, ##__VA_ARGS__)
#define STRESS_TRACE_BINARY(type, obj, v1, v2)                  \
  do {                                                          \
    auto* _buf = ::ROCKSDB_NAMESPACE::stress_trace::GetBinaryBuffer(); \
    if (__builtin_expect(_buf != nullptr, 1)) {                 \
      _buf->Record(type, obj, v1, v2);                          \
    }                                                           \
  } while (0)
#else
#define STRESS_TRACE_EVENT(fmt, ...) \
  do {                               \
  } while (0)
#define STRESS_TRACE_BINARY(type, obj, v1, v2) \
  do {                                          \
  } while (0)
#endif

namespace ROCKSDB_NAMESPACE {
namespace stress_trace {

// ============================================================
// Ring-buffer sizes
// ============================================================
// Per-thread function trace: last 8192 function entries per thread.
// Each entry is 16 bytes (func ptr + TSC). Total: 8192 * 16 = 128 KB per
// thread. At 1M function calls/sec per thread, this covers the last ~8ms.
static constexpr size_t kMaxFuncEntries = 8192;

// Per-thread event log: last 256 semantic events per thread.
// Each entry is 8+8+128 = 144 bytes. Total: ~36 KB per thread.
static constexpr size_t kMaxEventEntries = 256;
static constexpr size_t kMaxEventMsgLen = 128;

// ============================================================
// TSC helpers
// ============================================================
// We use rdtsc for timestamps: ~1ns resolution, zero syscall overhead.
// rdtsc is not serializing (may reorder with adjacent instructions), but
// for a ring buffer this is fine — we only need approximate ordering.
#if (defined(__i386__) || defined(__x86_64__)) && \
    (defined(__GNUC__) || defined(__clang__))
ROCKSDB_NO_INSTRUMENT static inline uint64_t ReadTSC() {
  uint64_t lo, hi;
  __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi));
  return (hi << 32) | lo;
}
#else
ROCKSDB_NO_INSTRUMENT static inline uint64_t ReadTSC() {
  // Fallback: use a simple atomic counter as a monotonic sequence number.
  static std::atomic<uint64_t> seq{0};
  return seq.fetch_add(1, std::memory_order_relaxed);
}
#endif

// ============================================================
// Thread-local function trace ring buffer
// ============================================================
struct FuncEntry {
  void* func_addr;  // function entry point (NOT return address)
  uint64_t tsc;     // rdtsc at entry
};

// Thread-local storage for function trace.
// We use a raw array + index rather than std::deque to keep everything
// async-signal-safe (no heap allocation, no virtual dispatch).
struct FuncTraceBuffer {
  FuncEntry entries[kMaxFuncEntries];
  // head is the index of the next slot to write. The ring contains
  // entries[(head - count) % kMaxFuncEntries ... (head-1) % kMaxFuncEntries].
  size_t head = 0;

  ROCKSDB_NO_INSTRUMENT void Record(void* func) {
    size_t idx = head & (kMaxFuncEntries - 1);
    entries[idx].func_addr = func;
    entries[idx].tsc = ReadTSC();
    ++head;
  }
};

// ============================================================
// Thread-local semantic event ring buffer
// ============================================================
struct EventEntry {
  uint64_t tsc;
  char msg[kMaxEventMsgLen];
};

struct EventTraceBuffer {
  EventEntry entries[kMaxEventEntries];
  size_t head = 0;

  ROCKSDB_NO_INSTRUMENT TSAN_SUPPRESSION void Record(const char* fmt,
                                                     va_list args) {
    size_t idx = head & (kMaxEventEntries - 1);
    entries[idx].tsc = ReadTSC();
    char local_buf[kMaxEventMsgLen];
    vsnprintf(local_buf, kMaxEventMsgLen, fmt, args);
    // Byte-by-byte copy to avoid TSAN-intercepted memcpy on shared memory.
    // See InjectedErrorLog::Record() in fault_injection_fs.h for rationale.
    const volatile char* src = local_buf;
    for (size_t i = 0; i < kMaxEventMsgLen; i++) {
      entries[idx].msg[i] = src[i];
    }
    ++head;
  }
};


// ============================================================
// Binary event tracing (zero-format, ~5ns per event)
// ============================================================
// Instead of vsnprintf into a char buffer, binary events store typed
// integers directly. Formatting happens only at dump time (signal handler).
// This is ~10x faster than STRESS_TRACE_EVENT for structured events.

enum class TraceEventType : uint8_t {
  kNone = 0,
  kMutexLock = 1,
  kMutexUnlock = 2,
  kCvWaitBegin = 3,
  kCvWaitEnd = 4,
  kCvTimedWaitBegin = 5,
  kCvTimedWaitEnd = 6,
  kCvSignal = 7,
  kCvSignalAll = 8,
  kSyncPoint = 9,
  kAtomicStore = 10,
  kAtomicLoad = 11,
  kAtomicCas = 12,
  kAtomicXchg = 13,
  kAtomicAdd = 14,
  kAtomicSub = 15,
};

struct BinaryEvent {
  uint64_t tsc;       // rdtsc timestamp
  uint64_t type_obj;  // high 8 bits = TraceEventType, low 56 bits = obj ptr
  uint64_t val1;      // meaning depends on type
  uint64_t val2;      // meaning depends on type

  ROCKSDB_NO_INSTRUMENT TraceEventType Type() const {
    return static_cast<TraceEventType>(type_obj >> 56);
  }
  ROCKSDB_NO_INSTRUMENT uint64_t ObjPtr() const {
    return type_obj & 0x00FFFFFFFFFFFFFF;
  }
};

static constexpr size_t kMaxBinaryEntries = 8192;

struct BinaryTraceBuffer {
  BinaryEvent entries[kMaxBinaryEntries];
  size_t head = 0;

  ROCKSDB_NO_INSTRUMENT void Record(TraceEventType type, const void* obj,
                                     uint64_t v1, uint64_t v2) {
    size_t idx = head & (kMaxBinaryEntries - 1);
    entries[idx].tsc = ReadTSC();
    entries[idx].type_obj =
        (static_cast<uint64_t>(type) << 56) |
        (reinterpret_cast<uint64_t>(obj) & 0x00FFFFFFFFFFFFFF);
    entries[idx].val1 = v1;
    entries[idx].val2 = v2;
    ++head;
  }
};

// ============================================================
// Global registry of per-thread buffers
// ============================================================
// When a thread is created, it registers its buffers here so the crash
// handler can iterate all threads. Registration uses a simple lock-free
// linked list (prepend only, never removed).
struct ThreadState {
  FuncTraceBuffer* func_buf;
  EventTraceBuffer* event_buf;
  BinaryTraceBuffer* binary_buf;
  uint64_t thread_id;
  ThreadState* next;
};

// Global linked list head. Modified only by thread registration (prepend).
// Read by crash handler — a benign race (worst case: miss last thread).
ROCKSDB_NO_INSTRUMENT extern std::atomic<ThreadState*>& GlobalThreadList();

// ============================================================
// Public API
// ============================================================

// Install the trace subsystem. Call once at startup (from db_stress_tool.cc).
// dump_path_prefix: path prefix for output files, e.g. "/tmp/stress-trace-<pid>".
//   Output files: <prefix>-thread-<tid>.txt (one per thread, on crash).
//   Pass "" to write everything to stdout.
ROCKSDB_NO_INSTRUMENT void Install(const std::string& dump_path_prefix);

// Called by the crash handler (via RegisterCrashCallback) to dump all buffers.
// Async-signal-safe.
ROCKSDB_NO_INSTRUMENT void DumpAll();

// Record a semantic event into the current thread's event buffer.
// Called by STRESS_TRACE_EVENT macro.
ROCKSDB_NO_INSTRUMENT void RecordEvent(const char* fmt, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((format(printf, 1, 2)))
#endif
    ;

// ============================================================
// Thread-local buffer accessors (inline, for performance)
// ============================================================

// Returns the current thread's FuncTraceBuffer, creating it on first call.
// Called from __cyg_profile_func_enter on every function entry.
ROCKSDB_NO_INSTRUMENT FuncTraceBuffer* GetFuncBuffer();

// Returns the current thread's EventTraceBuffer, creating it on first call.
ROCKSDB_NO_INSTRUMENT EventTraceBuffer* GetEventBuffer();

// Returns the current thread's BinaryTraceBuffer, creating it on first call.
ROCKSDB_NO_INSTRUMENT BinaryTraceBuffer* GetBinaryBuffer();

}  // namespace stress_trace
}  // namespace ROCKSDB_NAMESPACE

// ============================================================
// Compiler instrumentation hooks
// ============================================================
// These are defined with C linkage in stress_trace.cc (only compiled when
// ROCKSDB_STRESS_TRACE is defined). The compiler inserts calls to
// __cyg_profile_func_enter at every function entry when
// -finstrument-functions is active.
//
// IMPORTANT: these functions MUST be marked ROCKSDB_NO_INSTRUMENT, otherwise
// the compiler inserts recursive calls into them.
#ifdef ROCKSDB_STRESS_TRACE
extern "C" {
ROCKSDB_NO_INSTRUMENT void __cyg_profile_func_enter(void* func, void* caller);
ROCKSDB_NO_INSTRUMENT void __cyg_profile_func_exit(void* func, void* caller);
}
#endif
