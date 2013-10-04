#ifndef STACK_TRACE_H
#define STACK_TRACE_H

namespace rocksdb {

// Install a signal handler to print callstack on the following signals:
// SIGILL SIGSEGV SIGBUS SIGABRT
// Currently supports linux only. No-op otherwise.
void InstallStackTraceHandler();

}   // namespace rocksdb

#endif
