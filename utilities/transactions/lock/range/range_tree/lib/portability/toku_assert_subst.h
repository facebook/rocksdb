//
// A replacement for toku_assert.h
//
#pragma once

#include <assert.h>
#include <errno.h>

#ifdef NDEBUG

#define assert_zero(a) ((void)(a))
#define invariant(a) ((void)(a))
#define invariant_notnull(a) ((void)(a))
#define invariant_zero(a) ((void)(a))

#else

#define assert_zero(a) assert((a) == 0)
#define invariant(a) assert(a)
#define invariant_notnull(a) assert(a)
#define invariant_zero(a) assert_zero(a)

#endif

#define lazy_assert_zero(a) assert_zero(a)

#define paranoid_invariant_zero(a) assert_zero(a)
#define paranoid_invariant_notnull(a) assert(a)
#define paranoid_invariant(a) assert(a)

#define ENSURE_POD(type)                                                    \
  static_assert(                                                            \
      std::is_standard_layout<type>::value && std::is_trivial<type>::value, \
      #type "isn't POD")

inline int get_error_errno(void) {
  invariant(errno);
  return errno;
}
