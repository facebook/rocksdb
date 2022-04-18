#!/bin/sh
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
# Update dependencies.sh file with the latest avaliable versions

BASEDIR=$(dirname $0)
OUTPUT=""

function log_header()
{
  echo "# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved." >> "$OUTPUT"
  echo "# The file is generated using update_dependencies.sh." >> "$OUTPUT"
}


function log_variable()
{
  echo "$1=${!1}" >> "$OUTPUT"
}


TP2_LATEST="/data/users/$USER/fbsource/fbcode/third-party2/"
## $1 => lib name
## $2 => lib version (if not provided, will try to pick latest)
## $3 => platform (if not provided, will try to pick latest gcc)
##
## get_lib_base will set a variable named ${LIB_NAME}_BASE to the lib location
function get_lib_base()
{
  local lib_name=$1
  local lib_version=$2
  local lib_platform=$3

  local result="$TP2_LATEST/$lib_name/"
  
  # Lib Version
  if [ -z "$lib_version" ] || [ "$lib_version" = "LATEST" ]; then
    # version is not provided, use latest
    result=`ls -dr1v $result/*/ | head -n1`
  else
    result="$result/$lib_version/"
  fi
  
  # Lib Platform
  if [ -z "$lib_platform" ]; then
    # platform is not provided, use latest gcc
    result=`ls -dr1v $result/gcc-*[^fb]/ | head -n1`
  else
    echo $lib_platform
    result="$result/$lib_platform/"
  fi
  
  result=`ls -1d $result/*/ | head -n1`

  echo Finding link $result
  
  # lib_name => LIB_NAME_BASE
  local __res_var=${lib_name^^}"_BASE"
  __res_var=`echo $__res_var | tr - _`
  # LIB_NAME_BASE=$result
  eval $__res_var=`readlink -f $result`
  
  log_variable $__res_var
}

###########################################################
#                platform010 dependencies                 #
###########################################################

OUTPUT="$BASEDIR/dependencies_platform010.sh"

rm -f "$OUTPUT"
touch "$OUTPUT"

echo "Writing dependencies to $OUTPUT"

# Compilers locations
GCC_BASE=`readlink -f $TP2_LATEST/gcc/11.x/centos7-native/*/`
CLANG_BASE=`readlink -f $TP2_LATEST/llvm-fb/12/platform010/*/`

log_header
log_variable GCC_BASE
log_variable CLANG_BASE

# Libraries locations
get_lib_base libgcc     11.x    platform010
get_lib_base glibc      2.34    platform010
get_lib_base snappy     LATEST  platform010
get_lib_base zlib       LATEST  platform010
get_lib_base bzip2      LATEST  platform010
get_lib_base lz4        LATEST  platform010
get_lib_base zstd       LATEST  platform010
get_lib_base gflags     LATEST  platform010
get_lib_base jemalloc   LATEST  platform010
get_lib_base numa       LATEST  platform010
get_lib_base libunwind  LATEST  platform010
get_lib_base tbb        2018_U5 platform010
get_lib_base liburing   LATEST  platform010
get_lib_base benchmark  LATEST  platform010

get_lib_base kernel-headers fb platform010
get_lib_base binutils   LATEST centos7-native
get_lib_base valgrind   LATEST platform010
get_lib_base lua        5.3.4  platform010

git diff $OUTPUT


###########################################################
#                platform009 dependencies                 #
###########################################################

OUTPUT="$BASEDIR/dependencies_platform009.sh"

rm -f "$OUTPUT"
touch "$OUTPUT"

echo "Writing dependencies to $OUTPUT"

# Compilers locations
GCC_BASE=`readlink -f $TP2_LATEST/gcc/9.x/centos7-native/*/`
CLANG_BASE=`readlink -f $TP2_LATEST/llvm-fb/9.0.0/platform009/*/`

log_header
log_variable GCC_BASE
log_variable CLANG_BASE

# Libraries locations
get_lib_base libgcc     9.x     platform009
get_lib_base glibc      2.30    platform009
get_lib_base snappy     LATEST  platform009
get_lib_base zlib       LATEST  platform009
get_lib_base bzip2      LATEST  platform009
get_lib_base lz4        LATEST  platform009
get_lib_base zstd       LATEST  platform009
get_lib_base gflags     LATEST  platform009
get_lib_base jemalloc   LATEST  platform009
get_lib_base numa       LATEST  platform009
get_lib_base libunwind  LATEST  platform009
get_lib_base tbb        2018_U5 platform009
get_lib_base liburing   LATEST  platform009
get_lib_base benchmark  LATEST  platform009

get_lib_base kernel-headers fb platform009
get_lib_base binutils   LATEST centos7-native
get_lib_base valgrind   LATEST platform009
get_lib_base lua        5.3.4  platform009

git diff $OUTPUT
