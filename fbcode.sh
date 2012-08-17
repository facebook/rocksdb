#!/bin/sh
#
# Set environment variables so that we can compile leveldb using
# fbcode settings.  It uses the latest g++ compiler and also
# uses jemalloc

TOOLCHAIN_REV=d28c90311ca14f9f0b2bb720f4e34b285513d4f4
TOOLCHAIN_EXECUTABLES="/mnt/gvfs/third-party/$TOOLCHAIN_REV/centos5.2-native"
TOOLCHAIN_LIB_BASE="/mnt/gvfs/third-party/$TOOLCHAIN_REV/gcc-4.6.2-glibc-2.13"

# always build thrift server
export USE_THRIFT=1

# location of libhdfs libraries
if test "$USE_HDFS"; then
  JAVA_HOME="/usr/local/jdk-6u22-64"
  JINCLUDE="-I$JAVA_HOME/include -I$JAVA_HOME/include/linux"
  GLIBC_RUNTIME_PATH="/usr/local/fbcode/gcc-4.6.2-glibc-2.13"
  HDFSLIB=" -Wl,--no-whole-archive hdfs/libhdfs.a -L$JAVA_HOME/jre/lib/amd64 "
  HDFSLIB+=" -L$JAVA_HOME/jre/lib/amd64/server -L$GLIBC_RUNTIME_PATH/lib " 
  HDFSLIB+=" -ldl -lverify -ljava -ljvm "
fi

# location of snappy headers and libraries
SNAPPY_INCLUDE=" -I ./snappy"
SNAPPY_LIBS=" -L./snappy/libs"

# location of boost headers and libraries
THRIFT_INCLUDE=" -I $TOOLCHAIN_LIB_BASE/boost/default/eed002c/include -std=gnu++0x"
THRIFT_INCLUDE+=" -I./thrift -I./thrift/gen-cpp -I./thrift/lib/cpp"
THRIFT_LIBS=" -L $TOOLCHAIN_LIB_BASE/boost/default/eed002c/lib"

CC="$TOOLCHAIN_EXECUTABLES/gcc/gcc-4.6.2-glibc-2.13/bin/gcc" 
CXX="$TOOLCHAIN_EXECUTABLES/gcc/gcc-4.6.2-glibc-2.13/bin/g++ $JINCLUDE $SNAPPY_INCLUDE $THRIFT_INCLUDE"
AR=$TOOLCHAIN_EXECUTABLES/binutils/binutils-2.21.1/da39a3e/bin/ar
RANLIB=$TOOLCHAIN_EXECUTABLES/binutils/binutils-2.21.1/da39a3e/bin/ranlib

CFLAGS="-B$TOOLCHAIN_EXECUTABLES/binutils/binutils-2.21.1/bin/gold -m64 -mtune=generic -fPIC"
CFLAGS+=" -I $TOOLCHAIN_LIB_BASE/jemalloc/jemalloc-2.2.5/96de4f9/include -DHAVE_JEMALLOC"

EXEC_LDFLAGS=" -Wl,--whole-archive $TOOLCHAIN_LIB_BASE/jemalloc/jemalloc-2.2.4/96de4f9/lib/libjemalloc.a "
EXEC_LDFLAGS+="-Wl,--no-whole-archive $TOOLCHAIN_LIB_BASE/libunwind/libunwind-20100810/4bc2c16/lib/libunwind.a"
EXEC_LDFLAGS+="$HDFSLIB $SNAPPY_LIBS $THRIFT_LIBS "
EXEC_LDFLAGS_SHARED="$SNAPPY_LIBS $TOOLCHAIN_LIB_BASE/jemalloc/jemalloc-2.2.4/96de4f9/lib/libjemalloc.so"

export CC CXX AR RANLIB CFLAGS EXEC_LDFLAGS EXEC_LDFLAGS_SHARED
