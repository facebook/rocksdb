#!/usr/bin/env bash


set -e

J=-j8

## Install static dependencies for compiling static rocksdbjava with cmake.
## Note that .a are made with -fPIC, as they end up in a .so
## Also note that this should be enhanced to arc specific defines.
## generate cmake flags

CMAKEFLAGS="-DWITH_JNI=OFF -DWITH_LIBURING=OFF -DWITH_LZ4=OFF -DWITH_SNAPPY=OFF -DWITH_ZLIB=OFF -DWITH_ZSTD=OFF -DWITH_BZ2=OFF -DWITH_GFLAGS=OFF -DWITH_JEMALLOC=OFF"

## For mac
case "$ARCH" in
    x86_64 ) ARCH_CFLAGS="-arch x86_64";;
    arm64 ) ARCH_CFLAGS="-arch arm64";;
esac


usage ()
{
    echo ' installs dependencies, and generate cmake flags for rocksdb compilation.
    ./build_tools/install_deps.sh <dirname> packages ...
    cmake $(cat <dirname>/cmake-flags) ...
    packages:'
    sed -ne 's/^with_\(.*\)().*#\(.*\)/\t\1:    \t\2/p' < $0
}

cmakeflags()
{
    NAME=WITH_$1
    shift
    CMAKEFLAGS=${CMAKEFLAGS//-D$NAME=OFF/}
    CMAKEFLAGS="$CMAKEFLAGS -D$NAME=ON $@"
}

download ()
{
    file=$1
    url=$2
    if [ -f "$file" ]
    then
        echo "Reusing downloaded file $file, delete if it's corrupt." >&2
    else
        curl --fail --silent --show-error --location --output $file $url
    fi
}

with_iouring()  # makes liburing.a with -fPIC
{
    echo Compiling iouring to $DIR/iouring/ >&2
    cd $DIR/
    download liburing-0.7.tar.gz https://github.com/axboe/liburing/archive/refs/tags/liburing-0.7.tar.gz
    rm -fr liburing-liburing-0.7 $DIR/iouring/
    tar xf liburing-0.7.tar.gz
    cd liburing-liburing-0.7/
    ./configure --prefix=$DIR/iouring/
    make $J V=1 -C src ENABLE_SHARED=0 CFLAGS=" -g -fomit-frame-pointer -O2 -fPIC" install
    cmakeflags LIBURING -During_INCLUDE_DIR=$DIR/iouring/include -During_LIBRARIES=$DIR/iouring/lib/liburing.a
}

with_lz4() # makes liblz4.a with -fPIC
{
    echo Compiling lz4 to $DIR/lz4/ >&2
    cd $DIR/
    download lz4-v1.9.4.tar.gz https://github.com/lz4/lz4/archive/refs/tags/v1.9.4.tar.gz
    rm -fr lz4-1.9.4 $DIR/lz4/
    tar xf lz4-v1.9.4.tar.gz
    cd lz4-1.9.4
    case $OS in
        *Win* )
            cd build/cmake
            cmake -B build -DBUILD_SHARED_LIBS=OFF -DBUILD_STATIC_LIBS=1  -DCMAKE_INSTALL_PREFIX=$DIR/lz4/ -DCMAKE_BUILD_TYPE=Release
            cmake --build build/ -t install -v --config Release
            cmakeflags LZ4 -DLZ4_INCLUDE=$DIR/lz4/include -DLZ4_LIB_RELEASE=$DIR/lz4/lib/lz4.lib
            ;;
        * )
            make $J V=1 PREFIX=$DIR/lz4/ CFLAGS="-O3 -fPIC $ARCH_CFLAGS"  BUILD_SHARED=no BUILD_STATIC=yes install
            cmakeflags LZ4 -Dlz4_INCLUDE_DIRS=$DIR/lz4/include -Dlz4_LIBRARIES=$DIR/lz4/lib/liblz4.a
            ;;
    esac
}

with_jemalloc()  # makes libjemalloc_pic.a
{
    echo Compiling jemalloc to $DIR/jemalloc >&2
    cd $DIR
    download jemalloc-5.3.0.tar.gz https://github.com/jemalloc/jemalloc/archive/refs/tags/5.3.0.tar.gz
    rm -fr jemalloc-5.3.0 $DIR/jemalloc
    tar xf jemalloc-5.3.0.tar.gz
    cd jemalloc-5.3.0
    ## Sadly ... it needs autoconf.
    autoconf
    ./autogen.sh   --prefix=$DIR/jemalloc --disable-initial-exec-tls
    make $J install_lib_static install_include
    cmakeflags JEMALLOC -DJeMalloc_INCLUDE_DIRS=$DIR/jemalloc/include -DJeMalloc_LIBRARIES=$DIR/jemalloc/lib/libjemalloc_pic.a
}

with_jni() # ensure JAVA_HOME is set.
{
    if [ -z "$JAVA_HOME" ]
    then
        echo JAVA_HOME not set for JNI >&2
        exit 1
    fi
    cmakeflags JNI
}

write_cmakeflags()
{
    echo $CMAKEFLAGS | tr ' ' '\n' > $DIR/cmake-flags
}

if [ -z "$1" ]
then
    usage
    exit 1
fi
DIR=$( mkdir -p "$1" ; cd "$1" ; pwd)
shift
for a in "$@"
do
    case $a in
        jni|java )      with_jni       > $DIR/jni.log;;
        uring|iouring)  with_iouring   > $DIR/iouring.log;;
        lz4)            with_lz4       > $DIR/lz4.log;;
        jemalloc)       with_jemalloc  > $DIR/jemalloc.log;;
        *) usage;;
    esac
done

write_cmakeflags
echo
cat $DIR/cmake-flags
echo
echo "Installed third party dependencies to $DIR"
echo "CMake flags saved to $DIR/cmake-flags {e.g. cmake \$(cat $DIR/cmake-flags)}"
