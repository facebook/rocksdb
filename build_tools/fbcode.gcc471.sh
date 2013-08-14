#!/bin/sh
#
# Set environment variables so that we can compile leveldb using
# fbcode settings.  It uses the latest g++ compiler and also
# uses jemalloc

TOOLCHAIN_REV=83eb773e262fa705eaebbf3de40db71d53fabf2e
TOOLCHAIN_EXECUTABLES="/mnt/gvfs/third-party/$TOOLCHAIN_REV/centos5.2-native"
TOOLCHAIN_LIB_BASE="/mnt/gvfs/third-party/$TOOLCHAIN_REV/gcc-4.7.1-glibc-2.14.1"
TOOL_JEMALLOC=jemalloc-3.3.1/2f45f3a

# location of libhdfs libraries
if test "$USE_HDFS"; then
  JAVA_HOME="/usr/local/jdk-6u22-64"
  JINCLUDE="-I$JAVA_HOME/include -I$JAVA_HOME/include/linux"
  GLIBC_RUNTIME_PATH="/usr/local/fbcode/gcc-4.7.1-glibc-2.14.1"
  HDFSLIB=" -Wl,--no-whole-archive hdfs/libhdfs.a -L$JAVA_HOME/jre/lib/amd64 "
  HDFSLIB+=" -L$JAVA_HOME/jre/lib/amd64/server -L$GLIBC_RUNTIME_PATH/lib " 
  HDFSLIB+=" -ldl -lverify -ljava -ljvm "
fi

# location of snappy headers and libraries
SNAPPY_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/snappy/snappy-1.0.3/7518bbe/include"
SNAPPY_LIBS=" $TOOLCHAIN_LIB_BASE/snappy/snappy-1.0.3/7518bbe/lib/libsnappy.a"

# location of zlib headers and libraries
ZLIB_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/zlib/zlib-1.2.5/91ddd43/include"
ZLIB_LIBS=" $TOOLCHAIN_LIB_BASE/zlib/zlib-1.2.5/91ddd43/lib/libz.a"

# location of boost headers and libraries
THRIFT_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/boost/boost-1.48.0/2a0840d/include"
THRIFT_INCLUDE+=" -I./thrift -I./thrift/gen-cpp -I./thrift/lib/cpp"
THRIFT_LIBS=" -L $TOOLCHAIN_LIB_BASE/boost/boost-1.48.0/2a0840d/lib"

# location of libevent
LIBEVENT_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/libevent/libevent-1.4.14b/91ddd43/include"
LIBEVENT_LIBS=" -L $TOOLCHAIN_LIB_BASE/libevent/libevent-1.4.14b/91ddd43/lib"

# use Intel SSE support for checksum calculations
export USE_SSE=" -msse -msse4.2 "

CC="$TOOLCHAIN_EXECUTABLES/gcc/gcc-4.7.1-glibc-2.14.1/bin/gcc"
CXX="$TOOLCHAIN_EXECUTABLES/gcc/gcc-4.7.1-glibc-2.14.1/bin/g++ $JINCLUDE $SNAPPY_INCLUDE $ZLIB_INCLUDE $THRIFT_INCLUDE $LIBEVENT_INCLUDE"
AR=$TOOLCHAIN_EXECUTABLES/binutils/binutils-2.21.1/da39a3e/bin/ar
RANLIB=$TOOLCHAIN_EXECUTABLES/binutils/binutils-2.21.1/da39a3e/bin/ranlib

CFLAGS="-B$TOOLCHAIN_EXECUTABLES/binutils/binutils-2.21.1/bin/gold -m64 -mtune=generic -fPIC"
CFLAGS+=" -I $TOOLCHAIN_LIB_BASE/jemalloc/$TOOL_JEMALLOC/include -DHAVE_JEMALLOC"

EXEC_LDFLAGS=" -Wl,--whole-archive $TOOLCHAIN_LIB_BASE/jemalloc/$TOOL_JEMALLOC/lib/libjemalloc.a"
EXEC_LDFLAGS+=" -Wl,--no-whole-archive $TOOLCHAIN_LIB_BASE/libunwind/libunwind-1.0.1/91ddd43/lib/libunwind.a"
EXEC_LDFLAGS+=" $HDFSLIB $SNAPPY_LIBS $ZLIB_LIBS $THRIFT_LIBS $LIBEVENT_LIBS"

PLATFORM_LDFLAGS="-L$TOOLCHAIN_LIB_BASE/libgcc/libgcc-4.7.1/afc21dc/lib -L$TOOLCHAIN_LIB_BASE/glibc/glibc-2.14.1/99df8fc/lib"

EXEC_LDFLAGS_SHARED="$SNAPPY_LIBS $ZLIB_LIBS"
SNAPPY_LDFLAGS="$SNAPPY_LIBS $ZLIB_LIBS"

VALGRIND_VER="$TOOLCHAIN_LIB_BASE/valgrind/valgrind-3.8.1/91ddd43/bin/"

export CC CXX AR RANLIB CFLAGS EXEC_LDFLAGS EXEC_LDFLAGS_SHARED SNAPPY_LDFLAGS VALGRIND_VER
