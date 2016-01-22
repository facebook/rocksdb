#!/bin/sh
#
# Update dependencies.sh file with the latest avaliable versions

BASEDIR=$(dirname $0)
OUTPUT="$BASEDIR/dependencies.sh"

rm -f "$OUTPUT"
touch "$OUTPUT"

function log_variable()
{
  echo "$1=${!1}" >> "$OUTPUT"
}


TP2_LATEST="/mnt/vol/engshare/fbcode/third-party2"
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
  if [ -z "$lib_version" ]; then
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
    result="$result/$lib_platform/"
  fi
  
  result="$result/*/"
  
  # lib_name => LIB_NAME_BASE
  local __res_var=${lib_name^^}"_BASE"
  # LIB_NAME_BASE=$result
  eval $__res_var=`readlink -f $result`
  
  log_variable $__res_var
}

echo "Writing dependencies to $OUTPUT"

# Compilers locations
GCC_BASE="$TP2_LATEST/gcc/4.9.x/centos6-native/*"
CLANG_BASE="$TP2_LATEST/clang/3.7.1"

log_variable GCC_BASE
log_variable CLANG_BASE

# Libraries locations
get_lib_base libgcc
get_lib_base glibc 2.20 gcc-4.9-glibc-2.20
get_lib_base snappy
get_lib_base zlib
get_lib_base bzip2
get_lib_base lz4
get_lib_base zstd
get_lib_base gflags
get_lib_base jemalloc
get_lib_base numa
get_lib_base libunwind

git diff $OUTPUT
