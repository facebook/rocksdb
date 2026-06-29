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
# GCC is pinned to 11.x because the only newer GCC in third-party2 (13.x) is
# built for centos9/glibc>=2.35 -- its libgcc_s.so.1 has a hard reference
# to _dl_find_object@GLIBC_2.35, but platform010 ships glibc 2.34. Bumping
# GCC requires a platform with glibc >= 2.35.
GCC_BASE=`readlink -f $TP2_LATEST/gcc/11.x/centos9-native/*/`
# Clang is pinned to the latest tested major (21). Bump deliberately.
CLANG_BASE=`readlink -f $TP2_LATEST/llvm-fb/21/platform010/*/`

log_header
log_variable GCC_BASE
log_variable CLANG_BASE

# Libraries locations
# libgcc is pinned to 11.x to match the GCC 11 compiler above (libstdc++/
# libgcc runtime, ABI, and C++ headers). Bump in lockstep with GCC_BASE.
get_lib_base libgcc     11.x    platform010
# glibc 2.34 is the platform010 ABI baseline (ld.so + libc); it is also the
# only version available in third-party2, and defines the platform -- do
# not bump independently.
get_lib_base glibc      2.34    platform010
get_lib_base snappy     LATEST  platform010
# zlib is pinned to 1.2.8: it is the latest version with an x86_64
# platform010 build (1.2.13 / 1.3.1 are centos9 / aarch64 only). At time of
# writing, LATEST here doesn't work: get_lib_base picks the newest version
# dir first and only then appends the platform, with no fallback -- so
# LATEST would resolve to 1.3.1, find no platform010 build, and emit an
# empty ZLIB_BASE.
get_lib_base zlib       1.2.8   platform010
get_lib_base bzip2      LATEST  platform010
get_lib_base lz4        LATEST  platform010
get_lib_base zstd       LATEST  platform010
get_lib_base gflags     LATEST  platform010
get_lib_base jemalloc   LATEST  platform010
get_lib_base numa       LATEST  platform010
get_lib_base libunwind  LATEST  platform010
get_lib_base liburing   LATEST  platform010
get_lib_base benchmark  LATEST  platform010

get_lib_base kernel-headers fb platform010
get_lib_base binutils   LATEST centos9-native
get_lib_base valgrind   LATEST platform010

git diff $OUTPUT
