#pragma once

// noinline
#ifdef _MSC_VER
# define CACHELIB_NOINLINE __declspec(noinline)
#elif defined(__clang__) || defined(__GNUC__)
# define CACHELIB_NOINLINE __attribute__((__noinline__))
#else
# define CACHELIB_NOINLINE
#endif

// always inline
#ifdef _MSC_VER
# define CACHELIB_INLINE __forceinline
#elif defined(__clang__) || defined(__GNUC__)
# define CACHELIB_INLINE inline __attribute__((__always_inline__))
#else
# define CACHELIB_INLINE inline
#endif

// packed
#if defined(__clang__) || defined(__GNUC__)
# define CACHELIB_PACKED_ATTR __attribute__((__packed__))
#else
# error GCC and Clang only supported
#endif

// likely/unlikely
#if defined(__clang__) || (defined(__GNUC__) && __GNUC__ >= 4)
# define CACHELIB_LIKELY(x)   (__builtin_expect((x), 1))
# define CACHELIB_UNLIKELY(x) (__builtin_expect((x), 0))
#else
# define CACHELIB_LIKELY(x)   (x)
# define CACHELIB_UNLIKELY(x) (x)
#endif
