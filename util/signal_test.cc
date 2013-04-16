#include "util/stack_trace.h"
#include <assert.h>

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

int main() {
  leveldb::InstallStackTraceHandler();

  f3();

  return 0;
}
