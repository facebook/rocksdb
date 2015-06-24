#!/bin/sh

if test -z $ROCKSDB_PATH; then
  ROCKSDB_PATH=~/rocksdb
fi
source $ROCKSDB_PATH/build_tools/fbcode_config.sh

EXTRA_LDFLAGS=""

if test -z $ALLOC; then
  # default
  ALLOC=tcmalloc
elif [[ $ALLOC == "jemalloc" ]]; then
  ALLOC=system
  EXTRA_LDFLAGS+=" -Wl,--whole-archive $JEMALLOC_LIB -Wl,--no-whole-archive"
fi

# we need to force mongo to use static library, not shared
TEMP_COMPILE_DIR=`mktemp -d /tmp/tmp.mongo_compile.XXX`
ln -s $SNAPPY_LIBS $TEMP_COMPILE_DIR
ln -s $LZ4_LIBS $TEMP_COMPILE_DIR

EXTRA_LDFLAGS+=" -L $TEMP_COMPILE_DIR"

set -x

EXTRA_CMD=""
if ! test -e version.json; then
  # this is Mongo 3.0
  EXTRA_CMD="--rocksdb \
    --variant-dir=linux2/norm
    --cxx=${CXX} \
    --cc=${CC} \
    --use-system-zlib"  # add this line back to normal code path
                        # when https://jira.mongodb.org/browse/SERVER-19123 is resolved
fi

scons \
  LINKFLAGS="$EXTRA_LDFLAGS $EXEC_LDFLAGS $PLATFORM_LDFLAGS" \
  CCFLAGS="$CXXFLAGS -L $TEMP_COMPILE_DIR" \
  LIBS="lz4 gcc stdc++" \
  LIBPATH="$ROCKSDB_PATH" \
  CPPPATH="$ROCKSDB_PATH/include" \
  -j32 \
  --allocator=$ALLOC \
  --nostrip \
  --opt=on \
  --use-system-snappy \
  --disable-warnings-as-errors \
  $EXTRA_CMD $*

set +x
rm -rf $TEMP_COMPILE_DIR
