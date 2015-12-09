#!/bin/sh
#
# Set environment variables so that we can compile rocksdb using
# fbcode settings.  It uses the latest g++ compiler and also
# uses jemalloc

# location of libgcc
LIBGCC_BASE="/mnt/gvfs/third-party2/libgcc/d00277f4559e261ed0a81f30f23c0ce5564e359e/4.8.1/gcc-4.8.1-glibc-2.17/8aac7fc"
LIBGCC_INCLUDE="$LIBGCC_BASE/include"
LIBGCC_LIBS=" -L $LIBGCC_BASE/libs"

# location of glibc
GLIBC_REV=0600c95b31226b5e535614c590677d87c62d8016
GLIBC_INCLUDE="/mnt/gvfs/third-party2/glibc/$GLIBC_REV/2.17/gcc-4.8.1-glibc-2.17/99df8fc/include"
GLIBC_LIBS=" -L /mnt/gvfs/third-party2/glibc/$GLIBC_REV/2.17/gcc-4.8.1-glibc-2.17/99df8fc/lib"

# location of snappy headers and libraries
SNAPPY_REV=cbf6f1f209e5bd160bdc5d971744e039f36b1566
SNAPPY_INCLUDE=" -I /mnt/gvfs/third-party2/snappy/$SNAPPY_REV/1.1.3/gcc-4.8.1-glibc-2.17/c3f970a/include"
SNAPPY_LIBS=" /mnt/gvfs/third-party2/snappy/$SNAPPY_REV/1.1.3/gcc-4.8.1-glibc-2.17/c3f970a/lib/libsnappy.a"

# location of zlib headers and libraries
ZLIB_REV=6d39cb54708049f527e713ad19f2aadb9d3667e8
ZLIB_INCLUDE=" -I /mnt/gvfs/third-party2/zlib/$ZLIB_REV/1.2.8/gcc-4.8.1-glibc-2.17/c3f970a/include"
ZLIB_LIBS=" /mnt/gvfs/third-party2/zlib/$ZLIB_REV/1.2.8/gcc-4.8.1-glibc-2.17/c3f970a/lib/libz.a"

# location of bzip headers and libraries
BZIP_REV=d6c789bfc2ec4c51a63d66df2878926b8158cde8
BZIP_INCLUDE=" -I /mnt/gvfs/third-party2/bzip2/$BZIP_REV/1.0.6/gcc-4.8.1-glibc-2.17/c3f970a/include/"
BZIP_LIBS=" /mnt/gvfs/third-party2/bzip2/$BZIP_REV/1.0.6/gcc-4.8.1-glibc-2.17/c3f970a/lib/libbz2.a"

LZ4_REV=6858fac689e0f92e584224d91bdb0e39f6c8320d
LZ4_INCLUDE=" -I /mnt/gvfs/third-party2/lz4/$LZ4_REV/r131/gcc-4.8.1-glibc-2.17/c3f970a/include"
LZ4_LIBS=" /mnt/gvfs/third-party2/lz4/$LZ4_REV/r131/gcc-4.8.1-glibc-2.17/c3f970a/lib/liblz4.a"

ZSTD_REV=810b81b4705def5243e998b54701f3c504e4009e
ZSTD_INCLUDE=" -I /mnt/gvfs/third-party2/zstd/$ZSTD_REV/0.4.2/gcc-4.8.1-glibc-2.17/c3f970a/include"
ZSTD_LIBS=" /mnt/gvfs/third-party2/zstd/$ZSTD_REV/0.4.2/gcc-4.8.1-glibc-2.17/c3f970a/lib/libzstd.a"

# location of gflags headers and libraries
GFLAGS_REV=c7275a4ceae0aca0929e56964a31dafc53c1ee96
GFLAGS_INCLUDE=" -I /mnt/gvfs/third-party2/gflags/$GFLAGS_REV/2.1.1/gcc-4.8.1-glibc-2.17/c3f970a/include/"
GFLAGS_LIBS=" /mnt/gvfs/third-party2/gflags/$GFLAGS_REV/2.1.1/gcc-4.8.1-glibc-2.17/c3f970a/lib/libgflags.a"

# location of jemalloc
JEMALLOC_REV=c370265e58c4b6602e798df23335a1e9913dae52
JEMALLOC_INCLUDE=" -I /mnt/gvfs/third-party2/jemalloc/$JEMALLOC_REV/4.0.3/gcc-4.8.1-glibc-2.17/8d31e51/include"
JEMALLOC_LIB="/mnt/gvfs/third-party2/jemalloc/$JEMALLOC_REV/4.0.3/gcc-4.8.1-glibc-2.17/8d31e51/lib/libjemalloc.a"

# location of numa
NUMA_REV=ae54a5ed22cdabb1c6446dce4e8ffae5b4446d73
NUMA_INCLUDE=" -I /mnt/gvfs/third-party2/numa/$NUMA_REV/2.0.8/gcc-4.8.1-glibc-2.17/c3f970a/include/"
NUMA_LIB=" /mnt/gvfs/third-party2/numa/$NUMA_REV/2.0.8/gcc-4.8.1-glibc-2.17/c3f970a/lib/libnuma.a"

# location of libunwind
LIBUNWIND_REV=121f1a75c4414683aea8c70b761bfaf187f7c1a3
LIBUNWIND="/mnt/gvfs/third-party2/libunwind/$LIBUNWIND_REV/trunk/gcc-4.8.1-glibc-2.17/675d945/lib/libunwind.a"

# use Intel SSE support for checksum calculations
export USE_SSE=1

BINUTILS="/mnt/gvfs/third-party2/binutils/75670d0d8ef4891fd1ec2a7513ef01cd002c823b/2.25/centos6-native/da39a3e/bin"
AR="$BINUTILS/ar"

DEPS_INCLUDE="$SNAPPY_INCLUDE $ZLIB_INCLUDE $BZIP_INCLUDE $LZ4_INCLUDE $ZSTD_INCLUDE $GFLAGS_INCLUDE $NUMA_INCLUDE"

GCC_BASE="/mnt/gvfs/third-party2/gcc/c0064002d2609ab649603f769f0bd110bbe48029/4.8.1/centos6-native/cc6c9dc"
STDLIBS="-L $GCC_BASE/lib64"

if [ -z "$USE_CLANG" ]; then
  # gcc
  CC="$GCC_BASE/bin/gcc"
  CXX="$GCC_BASE/bin/g++"
  
  CFLAGS="-B$BINUTILS/gold -m64 -mtune=generic"
  CFLAGS+=" -isystem $GLIBC_INCLUDE"
  CFLAGS+=" -isystem $LIBGCC_INCLUDE"
else
  # clang 
  CLANG_BASE="/mnt/gvfs/third-party2/clang/ab054e9a490a8fd4537c0b6ec56e5c91c0f81c91/3.4"
  CLANG_INCLUDE="$CLANG_BASE/gcc-4.8.1-glibc-2.17/fb0f730/lib/clang/3.4/include"
  CC="$CLANG_BASE/centos6-native/9cefd8a/bin/clang"
  CXX="$CLANG_BASE/centos6-native/9cefd8a/bin/clang++"

  KERNEL_HEADERS_INCLUDE="/mnt/gvfs/third-party2/kernel-headers/1a48835975c66d30e47770ec419758ed3b9ba010/3.10.62-62_fbk17_03959_ge29cc63/gcc-4.8.1-glibc-2.17/da39a3e/include/"

  CFLAGS="-B$BINUTILS/gold -nostdinc -nostdlib"
  CFLAGS+=" -isystem $LIBGCC_BASE/include/c++/4.8.1 "
  CFLAGS+=" -isystem $LIBGCC_BASE/include/c++/4.8.1/x86_64-facebook-linux "
  CFLAGS+=" -isystem $GLIBC_INCLUDE"
  CFLAGS+=" -isystem $LIBGCC_INCLUDE"
  CFLAGS+=" -isystem $CLANG_INCLUDE"
  CFLAGS+=" -isystem $KERNEL_HEADERS_INCLUDE/linux "
  CFLAGS+=" -isystem $KERNEL_HEADERS_INCLUDE "
  CXXFLAGS="-nostdinc++"
fi

CFLAGS+=" $DEPS_INCLUDE"
CFLAGS+=" -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DROCKSDB_FALLOCATE_PRESENT -DROCKSDB_MALLOC_USABLE_SIZE"
CFLAGS+=" -DSNAPPY -DGFLAGS=google -DZLIB -DBZIP2 -DLZ4 -DZSTD -DNUMA"
CXXFLAGS+=" $CFLAGS"

EXEC_LDFLAGS=" $SNAPPY_LIBS $ZLIB_LIBS $BZIP_LIBS $LZ4_LIBS $ZSTD_LIBS $GFLAGS_LIBS $NUMA_LIB"
EXEC_LDFLAGS+=" -Wl,--dynamic-linker,/usr/local/fbcode/gcc-4.8.1-glibc-2.17/lib/ld.so"
EXEC_LDFLAGS+=" $LIBUNWIND"
EXEC_LDFLAGS+=" -Wl,-rpath=/usr/local/fbcode/gcc-4.8.1-glibc-2.17/lib"

PLATFORM_LDFLAGS="$LIBGCC_LIBS $GLIBC_LIBS $STDLIBS -lgcc -lstdc++"

EXEC_LDFLAGS_SHARED="$SNAPPY_LIBS $ZLIB_LIBS $BZIP_LIBS $LZ4_LIBS $ZSTD_LIBS $GFLAGS_LIBS"

VALGRIND_REV=af85c56f424cd5edfc2c97588299b44ecdec96bb
VALGRIND_VER="/mnt/gvfs/third-party2/valgrind/$VALGRIND_REV/3.8.1/gcc-4.8.1-glibc-2.17/c3f970a/bin/"

export CC CXX AR CFLAGS CXXFLAGS EXEC_LDFLAGS EXEC_LDFLAGS_SHARED VALGRIND_VER JEMALLOC_LIB JEMALLOC_INCLUDE
