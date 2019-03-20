/*
  C bindings for TitanDB.  May be useful as a stable ABI that can be
  used by programs that keep rocksdb in a shared library, or for
  a JNI api.

  Does not support:
  . getters for the option types
  . custom comparators that implement key shortening
  . capturing post-write-snapshot
  . custom iter, db, env, cache implementations using just the C bindings

  Some conventions:

  (1) We expose just opaque struct pointers and functions to clients.
  This allows us to change internal representations without having to
  recompile clients.

  (2) For simplicity, there is no equivalent to the Slice type.  Instead,
  the caller has to pass the pointer and length as separate
  arguments.

  (3) Errors are represented by a null-terminated c string.  NULL
  means no error.  All operations that can raise an error are passed
  a "char** errptr" as the last argument.  One of the following must
  be true on entry:
     *errptr == NULL
     *errptr points to a malloc()ed null-terminated error message
  On success, a leveldb routine leaves *errptr unchanged.
  On failure, leveldb frees the old value of *errptr and
  set *errptr to a malloc()ed error message.

  (4) Bools have the type unsigned char (0 == false; rest == true)

  (5) All of the pointer arguments must be non-NULL.
*/

#ifndef ROCKSDB_TITAN_C_H
#define ROCKSDB_TITAN_C_H

#pragma once

#ifdef _WIN32
#ifdef ROCKSDB_DLL
#ifdef ROCKSDB_LIBRARY_EXPORTS
#define ROCKSDB_LIBRARY_API __declspec(dllexport)
#else
#define ROCKSDB_LIBRARY_API __declspec(dllimport)
#endif
#else
#define ROCKSDB_LIBRARY_API
#endif
#else
#define ROCKSDB_LIBRARY_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include "rocksdb/c.h"

/* Exported types */

// TitanDB

typedef struct titandb_options_t titandb_options_t;

extern ROCKSDB_LIBRARY_API rocksdb_t* titandb_open(
    const titandb_options_t* options, const char* name, char** errptr);

extern ROCKSDB_LIBRARY_API titandb_options_t* titandb_options_create();

extern ROCKSDB_LIBRARY_API void titandb_options_destroy(titandb_options_t*);

extern ROCKSDB_LIBRARY_API void titandb_options_set_rocksdb(
    titandb_options_t* options, rocksdb_options_t* rocksdb);

extern ROCKSDB_LIBRARY_API void titandb_options_set_dirname(
    titandb_options_t* options, const char* name);

extern ROCKSDB_LIBRARY_API void titandb_options_set_min_blob_size(
    titandb_options_t* options, uint64_t size);

extern ROCKSDB_LIBRARY_API void titandb_options_set_blob_file_compression(
    titandb_options_t* options, int compression);

extern ROCKSDB_LIBRARY_API void titandb_options_set_blob_cache(
    titandb_options_t* options, rocksdb_cache_t* blob_cache);

extern ROCKSDB_LIBRARY_API void titandb_options_set_disable_background_gc(
    titandb_options_t* options, unsigned char disable);

extern ROCKSDB_LIBRARY_API void titandb_options_set_max_gc_batch_size(
    titandb_options_t* options, uint64_t size);

extern ROCKSDB_LIBRARY_API void titandb_options_set_min_gc_batch_size(
    titandb_options_t* options, uint64_t size);

extern ROCKSDB_LIBRARY_API void titandb_options_set_blob_file_discardable_ratio(
    titandb_options_t* options, float ratio);

extern ROCKSDB_LIBRARY_API void titandb_options_set_sample_file_size_ratio(
    titandb_options_t* options, float ratio);

extern ROCKSDB_LIBRARY_API void titandb_options_set_merge_small_file_threshold(
    titandb_options_t* options, uint64_t size);

#ifdef __cplusplus
} /* end extern "C" */
#endif

#endif  // ROCKSDB_TITAN_C_H
