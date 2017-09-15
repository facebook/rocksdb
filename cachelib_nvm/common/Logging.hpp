#pragma once

#include <cassert>
#include <cstdlib>

#include "Defines.hpp"

#if CACHELIB_LOGGING
#include <glog/logging.h>
#elif !defined(CHECK)

#define CHECK(condition)        \
    do {                        \
      if (!(condition)) {       \
        std::abort();           \
      }                         \
    } while (false)

#define CHECK_EQ(val1, val2) CHECK((val1) == (val2))
#define CHECK_NE(val1, val2) CHECK((val1) != (val2))
#define CHECK_LE(val1, val2) CHECK((val1) <= (val2))
#define CHECK_LT(val1, val2) CHECK((val1) < (val2))
#define CHECK_GE(val1, val2) CHECK((val1) >= (val2))
#define CHECK_GT(val1, val2) CHECK((val1) > (val2))

#ifdef NDEBUG
#define DCHECK(condition) do {} while(false)
#else
#define DCHECK(condition) CHECK(condition)
#endif

#define DCHECK_EQ(val1, val2) DCHECK((val1) == (val2))
#define DCHECK_NE(val1, val2) DCHECK((val1) != (val2))
#define DCHECK_LE(val1, val2) DCHECK((val1) <= (val2))
#define DCHECK_LT(val1, val2) DCHECK((val1) < (val2))
#define DCHECK_GE(val1, val2) DCHECK((val1) >= (val2))
#define DCHECK_GT(val1, val2) DCHECK((val1) > (val2))

#endif
