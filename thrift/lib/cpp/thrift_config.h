#ifndef THRIFT_THRIFT_CONFIG_H_
#define THRIFT_THRIFT_CONFIG_H_

#include <features.h>

/* Define to 1 if you have the `clock_gettime' function. */
#define THRIFT_HAVE_CLOCK_GETTIME 1

/* Define to 1 if you have the <endian.h> header file. */
#define THRIFT_HAVE_ENDIAN_H 1

/* Define to 1 if you have the <inttypes.h> header file. */
#define THRIFT_HAVE_INTTYPES_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define THRIFT_HAVE_STDINT_H 1

/* Possible value for SIGNED_RIGHT_SHIFT_IS */
#define ARITHMETIC_RIGHT_SHIFT 1

/* Possible value for SIGNED_RIGHT_SHIFT_IS */
#define LOGICAL_RIGHT_SHIFT 2

/* Possible value for SIGNED_RIGHT_SHIFT_IS */
#define UNKNOWN_RIGHT_SHIFT 3

/* Indicates the effect of the right shift operator on negative signed
   integers */
#define SIGNED_RIGHT_SHIFT_IS 1

/*
 * Define to noexcept if the compiler supports noexcept
 *
 * If the compiler does not support noexcept, we define to the empty string
 * in optimized builds.  In debug builds, we define to throw(), so that the
 * compiler will complain if a child class does not use THRIFT_NOEXCEPT when
 * overriding a virtual method originally declared with THRIFT_NOEXCEPT.  In
 * debug mode, the program will also immediately call unexpected() if a
 * THRIFT_NOEXCEPT function does throw an exception.
 */
#ifdef NDEBUG
#define THRIFT_NOEXCEPT
#else
#define THRIFT_NOEXCEPT throw()
#endif

/*
 * We have std::unique_ptr if we're compiling with gcc-4.4 or greater
 * and C++0x features are enabled.
 */
#ifdef __GNUC__
#if __GNUC_PREREQ(4, 4)
#define THRIFT_HAVE_UNIQUE_PTR __GXX_EXPERIMENTAL_CXX0X__
#else
#define THRIFT_HAVE_UNIQUE_PTR 0
#endif
#else /* ! __GNUC__ */
#define THRIFT_HAVE_UNIQUE_PTR 0
#endif /* __GNUC__ */

#endif /* THRIFT_THRIFT_CONFIG_H_ */
