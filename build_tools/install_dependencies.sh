#!/usr/bin/env bash


set -e


SCRIPT_DIR=$( cd $(dirname $0) && pwd)

J=-j8

## Install static dependencies for compiling static rocksdbjava with cmake.
## Note that .a are made with -fPIC, as they end up in a .so
## Also note that this should be enhanced to arch specific defines.


CMAKEFLAGS="-DWITH_JNI=OFF -DWITH_LIBURING=OFF -DWITH_LZ4=OFF -DWITH_SNAPPY=OFF -DWITH_ZLIB=OFF -DWITH_ZSTD=OFF -DWITH_BZ2=OFF -DWITH_GFLAGS=OFF -DWITH_JEMALLOC=OFF"

ARCH_CFLAGS=""
## For mac
case "$ARCH" in
    x86_64 ) ARCH_CFLAGS="-arch x86_64";;
    arm64 ) ARCH_CFLAGS="-arch arm64";;
esac


usage ()
{
    echo ' installs dependencies, and generates cmake flags for rocksdb compilation.
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
    ORG=axboe
    REPO=liburing
    TAG=liburing-2.3
    ROOT=$REPO-$TAG
    DEST=iouring

    echo Compiling $ORG/$REPO[$TAG] to $DIR/$DEST/ >&2
    cd $DIR/
    download $REPO-$TAG.tar.gz https://github.com/$ORG/$REPO/archive/refs/tags/$TAG.tar.gz
    rm -fr $REPO-$TAG $DIR/$DEST/
    tar xf $REPO-$TAG.tar.gz
    cd $ROOT/

    ./configure --prefix=$DIR/$DEST/
    make $J V=1 -C src ENABLE_SHARED=0 CFLAGS=" -g -fomit-frame-pointer -O2 -fPIC" install
    cmakeflags LIBURING -During_INCLUDE_DIR=$DIR/$DEST/include -During_LIBRARIES=$DIR/$DEST/lib/liburing.a
}

with_lz4() # makes liblz4.a with -fPIC
{
    ORG=lz4
    REPO=lz4
    TAG=v1.9.4
    DEST=lz4
    ROOT=$REPO-${TAG:1}

    echo Compiling $ORG/$REPO[$TAG] to $DIR/$DEST/ >&2
    cd $DIR/
    download $REPO-$TAG.tar.gz https://github.com/$ORG/$REPO/archive/refs/tags/$TAG.tar.gz
    rm -fr $REPO-$TAG $DIR/$DEST/
    tar xf $REPO-$TAG.tar.gz
    cd $ROOT/

    case $OS in
        *Win* )
            cd build/cmake
            cmake -B build -DBUILD_SHARED_LIBS=OFF -DBUILD_STATIC_LIBS=1  -DCMAKE_INSTALL_PREFIX=$DIR/$DEST/ -DCMAKE_BUILD_TYPE=Release
            cmake --build build/ -t install -v --config Release
            cmakeflags LZ4 -DLZ4_INCLUDE=$DIR/$DEST/include -DLZ4_LIB_RELEASE=$DIR/$DEST/lib/lz4.lib
            ;;
        * )
            make $J V=1 PREFIX=$DIR/$DEST/ CFLAGS="-O3 -fPIC $ARCH_CFLAGS"  BUILD_SHARED=no BUILD_STATIC=yes install
            cmakeflags LZ4 -Dlz4_INCLUDE_DIRS=$DIR/$DEST/include -Dlz4_LIBRARIES=$DIR/$DEST/lib/liblz4.a
            ;;
    esac
}

with_jemalloc()  # makes libjemalloc_pic.a
{
    ORG=jemalloc
    REPO=jemalloc
    TAG=5.3.0
    DEST=jemalloc
    ROOT=$REPO-$TAG

    echo Compiling $ORG/$REPO[$TAG] to $DIR/$DEST/ >&2
    cd $DIR/
    download $REPO-$TAG.tar.gz https://github.com/$ORG/$REPO/archive/refs/tags/$TAG.tar.gz
    rm -fr $REPO-$TAG $DIR/$DEST/
    tar xf $REPO-$TAG.tar.gz
    cd $ROOT

    ## Sadly ... it needs autoconf.
    autoconf
    ./autogen.sh   --prefix=$DIR/$DEST --disable-initial-exec-tls
    make $J install_lib_static install_include
    cmakeflags JEMALLOC -DJeMalloc_INCLUDE_DIRS=$DIR/$DEST/include -DJeMalloc_LIBRARIES=$DIR/$DEST/lib/libjemalloc_pic.a
}

with_snappy()
{
    ORG=google
    REPO=snappy
    TAG=1.1.9
    DEST=snappy
    ROOT=$REPO-$TAG

    echo Compiling $ORG/$REPO[$TAG] to $DIR/$DEST/ >&2
    cd $DIR/
    download $REPO-$TAG.tar.gz https://github.com/$ORG/$REPO/archive/refs/tags/$TAG.tar.gz
    rm -fr $REPO-$TAG $DIR/$DEST/
    tar xf $REPO-$TAG.tar.gz
    cd $ROOT

    patch -p0 < $SCRIPT_DIR/snappy.patch

    case $OS in
        *Win* )
            cmake -B build -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_PREFIX=$DIR/$DEST/  -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON
            cmake --build build/ -t install -v --config Release

            cmakeflags SNAPPY -DSNAPPY_INCLUDE=$DIR/$DEST/include -DSNAPPY_LIB_RELEASE=$DIR/$DEST/lib/snappy.lib ;;

        * )
            cmake -B build -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_PREFIX=$DIR/$DEST/ -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_CXX_COMPILER_ID=Clang -DARCH_CFLAGS="$ARCH_CFLAGS"
            cmake --build build/ -t install -v --config Release

            cmakeflags SNAPPY -DSnappy_INCLUDE_DIRS=$DIR/$DEST/include -DSnappy_LIBRARIES=$DIR/$DEST/lib/libsnappy.a ;;
    esac
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
        snappy)         with_snappy    > $DIR/snappy.log;;
        *) usage;;
    esac
done

write_cmakeflags
echo
cat $DIR/cmake-flags
echo
echo "Installed third party dependencies to $DIR"
echo "CMake flags saved to $DIR/cmake-flags {e.g. cmake \$(cat $DIR/cmake-flags)}"
