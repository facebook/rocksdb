#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT([rocksdb], [4.8.0], [jeff@google.com])
AC_CONFIG_SRCDIR([tools/sst_dump_test.cc])
AC_CONFIG_HEADER([config.h])
AC_CONFIG_MACRO_DIR([m4])
AM_SILENT_RULES([yes])
AM_INIT_AUTOMAKE([1.9 tar-pax])

PACKAGE_RELEASE=1
PACKAGE_URL=https://github.com/facebook/rocksdb
BITS=`if [[ 64 -eq $(getconf LONG_BIT) ]]; then echo -n $(getconf LONG_BIT); fi`
ARCH=`getconf LONG_BIT`
GIT_REV=`git rev-parse HEAD`
RDB_DATE=`date +%Y/%m/%d`
RDB_TIME=`date +%H:%M:%S`
RDB_FULLDATE="${RDB_DATE} ${RDB_TIME}"
AC_SUBST(PACKAGE_RELEASE)
AC_SUBST(PACKAGE_URL)
AC_SUBST(BITS)
AC_SUBST(ARCH)
AC_SUBST(GIT_REV)
AC_SUBST(RDB_FULLDATE)

# Age information of package
# current
ROCKSDB_MAJOR=`grep "ROCKSDB_MAJOR.[[0-9]]" include/rocksdb/version.h | cut -d ' ' -f 3`
# revision
ROCKSDB_MINOR=`grep "ROCKSDB_MINOR.[[0-9]]" include/rocksdb/version.h | cut -d ' ' -f 3`
# age
ROCKSDB_PATCH=`grep "ROCKSDB_PATCH.[[0-9]]" include/rocksdb/version.h | cut -d ' ' -f 3`
AC_SUBST(ROCKSDB_MAJOR)
AC_SUBST(ROCKSDB_MINOR)
AC_SUBST(ROCKSDB_PATCH)

# Doc path setting
DOXYGEN_OUTPUT_DIR=doc/doxygen
AC_SUBST(DOXYGEN_OUTPUT_DIR)

# Check for doxygen
BB_ENABLE_DOXYGEN

# Check for pthread, fix by cnangel: ac_fn_c_try_link: command not found
BB_CHECK_PTHREADS

# Checks for programs.
AC_PROG_AWK
AC_PROG_CXX
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_LIBTOOL

# Checks for libraries.
# FIXME: Replace `main' with a function in `-ldl':
AC_CHECK_LIB([dl], [main])
# FIXME: Replace `main' with a function in `-lpthread':
AC_CHECK_LIB([pthread], [main])

# Checks for header files.
AC_FUNC_ALLOCA
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_HEADER_SYS_WAIT
# AC_CHECK_HEADERS([arpa/inet.h fcntl.h float.h inttypes.h limits.h mach/mach.h malloc.h netdb.h stddef.h stdint.h stdlib.h string.h strings.h sys/ioctl.h sys/socket.h sys/statfs.h sys/time.h sys/timeb.h unistd.h wchar.h wctype.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_C_INLINE
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_C_RESTRICT
AC_TYPE_SIZE_T
AC_STRUCT_ST_BLOCKS
AC_HEADER_TIME
AC_STRUCT_TM
AC_C_VOLATILE
AC_CHECK_TYPES([ptrdiff_t])

# Checks for library functions.
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_FORK
AC_PROG_GCC_TRADITIONAL
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_MMAP
AC_FUNC_REALLOC
AC_TYPE_SIGNAL
AC_FUNC_STAT
AC_FUNC_STRERROR_R
AC_FUNC_STRFTIME
AC_FUNC_STRTOD
AC_FUNC_VPRINTF
# AC_CHECK_FUNCS([atexit clock_gettime dup2 fchdir fdatasync floor ftruncate getcwd gethostname getpagesize gettimeofday localtime_r memmove memset mkdir munmap pow regcomp rmdir socket sqrt strcasecmp strchr strdup strerror strrchr strstr strtol strtoul strtoull])

# Checks for extra options
AC_CHECK_EXTRA_OPTIONS

# Define some macros for LIBCONFIG and PKGCONFIG
ROCKSDB_CFLAGS=""
ROCKSDB_LIBS="-l$PACKAGE"
ROCKSDB_LIBDEPS=""
ROCKSDB_VERSION="$VERSION"

# Create some useful files
AC_CONFIG_LIBCONFIG_IN([rocksdb], [scripts])
AC_CONFIG_PKGCONFIG_IN([rocksdb], [rocksdb for pkg-config], [pkgconfig])


AC_CONFIG_FILES([Makefile
                 db/Makefile			 
                 examples/Makefile
                 memtable/Makefile
                 java/Makefile
                 java/rocksjni/Makefile
                 pkgconfig/rocksdb.pc
                 port/Makefile
                 scripts/rocksdb-config
                 table/Makefile
                 test/Makefile
                 test/util/Makefile
                 tools/Makefile			  
                 util/build_version.cc
                 utilities/table_properties_collectors/Makefile
                 utilities/redis/Makefile
                 utilities/backupable/Makefile
                 utilities/ttl/Makefile
                 utilities/compaction_filters/Makefile
                 utilities/transactions/Makefile
                 utilities/document/Makefile
                 utilities/spatialdb/Makefile
                 utilities/write_batch_with_index/Makefile
                 utilities/flashcache/Makefile
                 utilities/memory/Makefile
                 utilities/convenience/Makefile
                 utilities/Makefile
                 utilities/leveldb_options/Makefile
                 utilities/merge_operators/string_append/Makefile
                 utilities/merge_operators/Makefile
                 utilities/geodb/Makefile
                 utilities/options/Makefile
                 utilities/checkpoint/Makefile
                 util/Makefile])
AC_OUTPUT
