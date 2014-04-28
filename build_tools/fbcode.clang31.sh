#!/bin/sh
#
# Set environment variables so that we can compile leveldb using
# fbcode settings.  It uses the latest g++ compiler and also
# uses jemalloc

TOOLCHAIN_REV=fbe3b095a4cc4a3713730050d182b7b4a80c342f
TOOLCHAIN_EXECUTABLES="/mnt/gvfs/third-party/$TOOLCHAIN_REV/centos5.2-native"
TOOLCHAIN_LIB_BASE="/mnt/gvfs/third-party/$TOOLCHAIN_REV/gcc-4.7.1-glibc-2.14.1"
TOOL_JEMALLOC=jemalloc-3.3.1/9202ce3
GLIBC_RUNTIME_PATH=/usr/local/fbcode/gcc-4.7.1-glibc-2.14.1

# location of libgcc
LIBGCC_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/libgcc/libgcc-4.7.1/afc21dc/include"
LIBGCC_LIBS=" -L $TOOLCHAIN_LIB_BASE/libgcc/libgcc-4.7.1/afc21dc/libs"

# location of glibc
GLIBC_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/glibc/glibc-2.14.1/99df8fc/include"
GLIBC_LIBS=" -L $TOOLCHAIN_LIB_BASE/glibc/glibc-2.14.1/99df8fc/lib"

# location of snappy headers and libraries
SNAPPY_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/snappy/snappy-1.0.3/7518bbe/include"
SNAPPY_LIBS=" $TOOLCHAIN_LIB_BASE/snappy/snappy-1.0.3/7518bbe/lib/libsnappy.a"

# location of zlib headers and libraries
ZLIB_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/zlib/zlib-1.2.5/91ddd43/include"
ZLIB_LIBS=" $TOOLCHAIN_LIB_BASE/zlib/zlib-1.2.5/91ddd43/lib/libz.a"

# location of gflags headers and libraries
GFLAGS_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/gflags/gflags-1.6/91ddd43/include"
GFLAGS_LIBS=" $TOOLCHAIN_LIB_BASE/gflags/gflags-1.6/91ddd43/lib/libgflags.a"

# location of bzip headers and libraries
BZIP_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/bzip2/bzip2-1.0.6/91ddd43/include"
BZIP_LIBS=" $TOOLCHAIN_LIB_BASE/bzip2/bzip2-1.0.6/91ddd43/lib/libbz2.a"

# location of gflags headers and libraries
GFLAGS_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/gflags/gflags-1.6/91ddd43/include"
GFLAGS_LIBS=" $TOOLCHAIN_LIB_BASE/gflags/gflags-1.6/91ddd43/lib/libgflags.a"

# use Intel SSE support for checksum calculations
export USE_SSE=" -msse -msse4.2 "

CC="$TOOLCHAIN_EXECUTABLES/clang/clang-3.2/0b7c69d/bin/clang $CLANG_INCLUDES"
CXX="$TOOLCHAIN_EXECUTABLES/clang/clang-3.2/0b7c69d/bin/clang++ $CLANG_INCLUDES $JINCLUDE $SNAPPY_INCLUDE $ZLIB_INCLUDE $BZIP_INCLUDE $GFLAGS_INCLUDE"
AR=$TOOLCHAIN_EXECUTABLES/binutils/binutils-2.21.1/da39a3e/bin/ar
RANLIB=$TOOLCHAIN_EXECUTABLES/binutils/binutils-2.21.1/da39a3e/bin/ranlib

CFLAGS="-B$TOOLCHAIN_EXECUTABLES/binutils/binutils-2.21.1/da39a3e/bin -nostdlib "
CFLAGS+=" -nostdinc -isystem $TOOLCHAIN_LIB_BASE/libgcc/libgcc-4.7.1/afc21dc/include/c++/4.7.1 "
CFLAGS+=" -isystem $TOOLCHAIN_LIB_BASE/libgcc/libgcc-4.7.1/afc21dc/include/c++/4.7.1/x86_64-facebook-linux "
CFLAGS+=" -isystem $TOOLCHAIN_LIB_BASE/libgcc/libgcc-4.7.1/afc21dc/include/c++/4.7.1/backward "
CFLAGS+=" -isystem $TOOLCHAIN_LIB_BASE/glibc/glibc-2.14.1/99df8fc/include "
CFLAGS+=" -isystem $TOOLCHAIN_LIB_BASE/libgcc/libgcc-4.7.1/afc21dc/include "
CFLAGS+=" -isystem $TOOLCHAIN_LIB_BASE/clang/clang-3.2/0b7c69d/lib/clang/3.2/include "
CFLAGS+=" -isystem $TOOLCHAIN_LIB_BASE/kernel-headers/kernel-headers-3.2.18_70_fbk11_00129_gc8882d0/da39a3e/include/linux "
CFLAGS+=" -isystem $TOOLCHAIN_LIB_BASE/kernel-headers/kernel-headers-3.2.18_70_fbk11_00129_gc8882d0/da39a3e/include "
CFLAGS+=" -Wall -Wno-sign-compare -Wno-unused-variable -Winvalid-pch -Wno-deprecated -Woverloaded-virtual"
CFLAGS+=" $LIBGCC_INCLUDE $GLIBC_INCLUDE"
CXXFLAGS="$CFLAGS -nostdinc++"

CFLAGS+=" -I $TOOLCHAIN_LIB_BASE/jemalloc/$TOOL_JEMALLOC/include -DHAVE_JEMALLOC"

EXEC_LDFLAGS=" -Wl,--whole-archive $TOOLCHAIN_LIB_BASE/jemalloc/$TOOL_JEMALLOC/lib/libjemalloc.a"
EXEC_LDFLAGS+=" -Wl,--no-whole-archive $TOOLCHAIN_LIB_BASE/libunwind/libunwind-1.0.1/350336c/lib/libunwind.a"
EXEC_LDFLAGS+=" $HDFSLIB $SNAPPY_LIBS $ZLIB_LIBS $BZIP_LIBS $GFLAGS_LIBS"
EXEC_LDFLAGS+=" -Wl,--dynamic-linker,$GLIBC_RUNTIME_PATH/lib/ld-linux-x86-64.so.2"
EXEC_LDFLAGS+=" -B$TOOLCHAIN_EXECUTABLES/binutils/binutils-2.21.1/da39a3e/bin"

PLATFORM_LDFLAGS="$LIBGCC_LIBS $GLIBC_LIBS "

EXEC_LDFLAGS_SHARED="$SNAPPY_LIBS $ZLIB_LIBS $GFLAGS_LIBS"

export CC CXX AR RANLIB CFLAGS EXEC_LDFLAGS EXEC_LDFLAGS_SHARED 
