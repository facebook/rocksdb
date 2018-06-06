#!/bin/sh
#
# Set environment variables so that we can compile rocksdb using
# fbcode settings.  It uses the latest g++ compiler and also
# uses jemalloc

GCC_BASE=/mnt/gvfs/third-party2/gcc/8219ec1bcedf8ad9da05e121e193364de2cc4f61/5.x/centos6-native/c447969
CLANG_BASE=/mnt/gvfs/third-party2/llvm-fb/64d8d58e3d84f8bde7a029763d4f5baf39d0d5b9/stable/centos6-native/6aaf4de
LIBGCC_BASE=/mnt/gvfs/third-party2/libgcc/ba9be983c81de7299b59fe71950c664a84dcb5f8/5.x/gcc-5-glibc-2.23/339d858
GLIBC_BASE=/mnt/gvfs/third-party2/glibc/f20197cf3d4bd50339c9777aaa0b2ccadad9e2cb/2.23/gcc-5-glibc-2.23/ca1d1c0
SNAPPY_BASE=/mnt/gvfs/third-party2/snappy/6427ce8c7496e4ab06c2da81543b94c0de8be3d0/1.1.3/gcc-5-glibc-2.23/9bc6787
ZLIB_BASE=/mnt/gvfs/third-party2/zlib/8f1e8b867d26efef93eac2fabbdb2e1d512665d7/1.2.8/gcc-5-glibc-2.23/9bc6787
BZIP2_BASE=/mnt/gvfs/third-party2/bzip2/70471c0571559fe0af7db6d7e8860b93a7eadfe1/1.0.6/gcc-5-glibc-2.23/9bc6787
LZ4_BASE=/mnt/gvfs/third-party2/lz4/453c89d6f0e68cdf1c151c769197fabedad9cac8/r131/gcc-5-glibc-2.23/9bc6787
ZSTD_BASE=/mnt/gvfs/third-party2/zstd/00a40fa5f8bd2cd0622f2e868552793aef37ccf4/1.3.0/gcc-5-glibc-2.23/03859b5
GFLAGS_BASE=/mnt/gvfs/third-party2/gflags/47eef08f9acb77de982fbda6047c26d330739538/2.2.0/gcc-5-glibc-2.23/9bc6787
JEMALLOC_BASE=/mnt/gvfs/third-party2/jemalloc/4414ddc78df8008b35cc4adac23590ad29148584/master/gcc-5-glibc-2.23/d506c82
NUMA_BASE=/mnt/gvfs/third-party2/numa/9d7ae2693d05d62f9a579cb21e6b717cf257a75d/2.0.11/gcc-5-glibc-2.23/9bc6787
LIBUNWIND_BASE=/mnt/gvfs/third-party2/libunwind/2b2dd58e3a52ccf2c1d827def59e5f740de0ad15/1.2/gcc-5-glibc-2.23/b443de1
TBB_BASE=/mnt/gvfs/third-party2/tbb/379addf7ab2468a2b4293b47456cfcd1c9cb318d/4.3/gcc-5-glibc-2.23/9bc6787
KERNEL_HEADERS_BASE=/mnt/gvfs/third-party2/kernel-headers/3f68f5fe65a85b7c2d3e66852268fbd1efdb3151/4.0.9-36_fbk5_2933_gd092e3f/gcc-5-glibc-2.23/da39a3e
BINUTILS_BASE=/mnt/gvfs/third-party2/binutils/b9fab0aec99d9c36408e810b2677e91c12807afd/2.28/centos6-native/da39a3e
VALGRIND_BASE=/mnt/gvfs/third-party2/valgrind/423431d61786b20bcc3bde8972901130cb29e6b3/3.11.0/gcc-5-glibc-2.23/9bc6787
LUA_BASE=/mnt/gvfs/third-party2/lua/3b0bb3bd9a0f690a069c479fcc0f7424fc7456d2/5.2.3/gcc-5-glibc-2.23/65372bd


# location of libgcc
LIBGCC_INCLUDE=" -I $LIBGCC_BASE/include"
LIBGCC_LIBS=" -L $LIBGCC_BASE/lib"

# location of glibc
GLIBC_INCLUDE=" -I $GLIBC_BASE/include"
GLIBC_LIBS=" -L $GLIBC_BASE/lib"

# location of snappy headers and libraries
SNAPPY_INCLUDE=" -I $SNAPPY_BASE/include/"
SNAPPY_LIBS=" $SNAPPY_BASE/lib/libsnappy.a"

# location of zlib headers and libraries
ZLIB_INCLUDE=" -I $ZLIB_BASE/include/"
ZLIB_LIBS=" $ZLIB_BASE/lib/libz.a"

LZ4_INCLUDE=" -I $LZ4_BASE/include/"
LZ4_LIBS=" $LZ4_BASE/lib/liblz4.a"

# location of gflags headers and libraries
GFLAGS_INCLUDE=" -I $GFLAGS_BASE/include/"
GFLAGS_LIBS=" $GFLAGS_BASE/lib/libgflags.a"

# location of jemalloc
JEMALLOC_INCLUDE=" -I $JEMALLOC_BASE/include/"
JEMALLOC_LIB=" -Wl,--whole-archive  $JEMALLOC_BASE/lib/libjemalloc.a"

# use Intel SSE support for checksum calculations
export USE_SSE=" -msse -msse4.2 "

CC="$GCC_BASE/bin/gcc"
CXX="$GCC_BASE/bin/g++ $JINCLUDE $SNAPPY_INCLUDE $ZLIB_INCLUDE $BZIP_INCLUDE $LZ4_INCLUDE $GFLAGS_INCLUDE"
AR="$BINUTILS_BASE/bin/ar"
RANLIB="$BINUTILS_BASE/bin/ranlib"

CFLAGS="-B$BINUTILS/bin/gold -m64 -mtune=generic"
CFLAGS+=" $LIBGCC_INCLUDE $GLIBC_INCLUDE"
CFLAGS+=" -DROCKSDB_PLATFORM_POSIX -DROCKSDB_ATOMIC_PRESENT -DROCKSDB_FALLOCATE_PRESENT"
CFLAGS+=" -DSNAPPY -DZLIB -DLZ4"

EXEC_LDFLAGS+=" -B$BINUTILS_BASE/bin/gold"
EXEC_LDFLAGS+=" -Wl,--dynamic-linker,/usr/local/fbcode/gcc-5-glibc-2.23/lib/ld.so"
EXEC_LDFLAGS+=" $LIBUNWIND"
EXEC_LDFLAGS+=" -Wl,-rpath=/usr/local/fbcode/gcc-5-glibc-2.23/lib"

PLATFORM_LDFLAGS="$LIBGCC_LIBS $GLIBC_LIBS "

EXEC_LDFLAGS_SHARED="$SNAPPY_LIBS $ZLIB_LIBS $LZ4_LIBS $GFLAGS_LIBS"

VALGRIND_VER="$VALGRIND_BASE/bin/"

export CC CXX AR RANLIB CFLAGS EXEC_LDFLAGS EXEC_LDFLAGS_SHARED VALGRIND_VER JEMALLOC_LIB JEMALLOC_INCLUDE
