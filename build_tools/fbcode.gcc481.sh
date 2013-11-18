#!/bin/sh
#
# Set environment variables so that we can compile rocksdb using
# fbcode settings.  It uses the latest g++ compiler and also
# uses jemalloc

TOOLCHAIN_REV=53dc1fe83f84e9145b9ffb81b81aa7f6a49c87cc
CENTOS_VERSION=`rpm -q --qf "%{VERSION}" $(rpm -q --whatprovides redhat-release)`
if [ "$CENTOS_VERSION" = "6" ]; then
  TOOLCHAIN_EXECUTABLES="/mnt/gvfs/third-party/$TOOLCHAIN_REV/centos6-native"
else
  TOOLCHAIN_EXECUTABLES="/mnt/gvfs/third-party/$TOOLCHAIN_REV/centos5.2-native"
fi
TOOLCHAIN_LIB_BASE="/mnt/gvfs/third-party/$TOOLCHAIN_REV/gcc-4.8.1-glibc-2.17"
TOOL_JEMALLOC=jemalloc-3.3.1/4d53c6f

# location of libhdfs libraries
if test "$USE_HDFS"; then
  JAVA_HOME="/usr/local/jdk-6u22-64"
  JINCLUDE="-I$JAVA_HOME/include -I$JAVA_HOME/include/linux"
  GLIBC_RUNTIME_PATH="/usr/local/fbcode/gcc-4.8.1-glibc-2.17"
  HDFSLIB=" -Wl,--no-whole-archive hdfs/libhdfs.a -L$JAVA_HOME/jre/lib/amd64 "
  HDFSLIB+=" -L$JAVA_HOME/jre/lib/amd64/server -L$GLIBC_RUNTIME_PATH/lib "
  HDFSLIB+=" -ldl -lverify -ljava -ljvm "
fi

# location of libgcc
LIBGCC_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/libgcc/libgcc-4.8.1/8aac7fc/include"
LIBGCC_LIBS=" -L $TOOLCHAIN_LIB_BASE/libgcc/libgcc-4.8.1/8aac7fc/libs"

# location of glibc
GLIBC_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/glibc/glibc-2.17/99df8fc/include"
GLIBC_LIBS=" -L $TOOLCHAIN_LIB_BASE/glibc/glibc-2.17/99df8fc/lib"

# location of snappy headers and libraries
SNAPPY_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/snappy/snappy-1.0.3/43d84e2/include"
SNAPPY_LIBS=" $TOOLCHAIN_LIB_BASE/snappy/snappy-1.0.3/43d84e2/lib/libsnappy.a"

# location of zlib headers and libraries
ZLIB_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/zlib/zlib-1.2.5/c3f970a/include"
ZLIB_LIBS=" $TOOLCHAIN_LIB_BASE/zlib/zlib-1.2.5/c3f970a/lib/libz.a"

# location of bzip headers and libraries
BZIP_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/bzip2/bzip2-1.0.6/c3f970a/include"
BZIP_LIBS=" $TOOLCHAIN_LIB_BASE/bzip2/bzip2-1.0.6/c3f970a/lib/libbz2.a"

# location of libevent
LIBEVENT_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/libevent/libevent-1.4.14b/c3f970a/include"
LIBEVENT_LIBS=" -L $TOOLCHAIN_LIB_BASE/libevent/libevent-1.4.14b/c3f970a/lib"

# location of gflags headers and libraries
GFLAGS_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/gflags/gflags-1.6/c3f970a/include"
GFLAGS_LIBS=" $TOOLCHAIN_LIB_BASE/gflags/gflags-1.6/c3f970a/lib/libgflags.a"

# use Intel SSE support for checksum calculations
export USE_SSE=" -msse -msse4.2 "

CC="$TOOLCHAIN_EXECUTABLES/gcc/gcc-4.8.1/cc6c9dc/bin/gcc"
CXX="$TOOLCHAIN_EXECUTABLES/gcc/gcc-4.8.1/cc6c9dc/bin/g++ $JINCLUDE $SNAPPY_INCLUDE $ZLIB_INCLUDE $BZIP_INCLUDE $LIBEVENT_INCLUDE $GFLAGS_INCLUDE"
AR=$TOOLCHAIN_EXECUTABLES/binutils/binutils-2.21.1/da39a3e/bin/ar
RANLIB=$TOOLCHAIN_EXECUTABLES/binutils/binutils-2.21.1/da39a3e/bin/ranlib

CFLAGS="-B$TOOLCHAIN_EXECUTABLES/binutils/binutils-2.21.1/da39a3e/bin/gold -m64 -mtune=generic -fPIC"
CFLAGS+=" -I $TOOLCHAIN_LIB_BASE/jemalloc/$TOOL_JEMALLOC/include -DHAVE_JEMALLOC -nostdlib"
CFLAGS+=" $LIBGCC_INCLUDE $GLIBC_INCLUDE"
CFLAGS+=" -DROCKSDB_PLATFORM_POSIX -DROCKSDB_ATOMIC_PRESENT"
CFLAGS+=" -DSNAPPY -DGFLAGS -DZLIB -DBZIP2"

EXEC_LDFLAGS="-Wl,--dynamic-linker,/usr/local/fbcode/gcc-4.8.1-glibc-2.17/lib/ld.so"
EXEC_LDFLAGS+=" -Wl,--whole-archive $TOOLCHAIN_LIB_BASE/jemalloc/$TOOL_JEMALLOC/lib/libjemalloc.a"
EXEC_LDFLAGS+=" -Wl,--no-whole-archive $TOOLCHAIN_LIB_BASE/libunwind/libunwind-1.0.1/675d945/lib/libunwind.a"
EXEC_LDFLAGS+=" $HDFSLIB $SNAPPY_LIBS $ZLIB_LIBS $BZIP_LIBS $LIBEVENT_LIBS $GFLAGS_LIBS"

PLATFORM_LDFLAGS="$LIBGCC_LIBS $GLIBC_LIBS "

EXEC_LDFLAGS_SHARED="$SNAPPY_LIBS $ZLIB_LIBS $BZIP_LIBS $GFLAGS_LIBS"

VALGRIND_VER="$TOOLCHAIN_LIB_BASE/valgrind/valgrind-3.8.1/c3f970a/bin/"

export CC CXX AR RANLIB CFLAGS EXEC_LDFLAGS EXEC_LDFLAGS_SHARED VALGRIND_VER
