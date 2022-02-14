#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
# A shell script to build and run different versions of ldb to check for
# expected forward and backward compatibility with "current" version. The
# working copy must have no uncommitted changes.
#
# Usage: <SCRIPT> [ref_for_current]
# `ref_for_current` can be a revision, tag, commit or branch name. Default is HEAD.
#
# Return value 0 means all regression tests pass. 1 if not pass.
#
# Environment options:
#  SHORT_TEST=1 - Test only the oldest branch for each kind of test. This is
#    a good choice for PR validation as it is relatively fast and will find
#    most issues.
#  USE_SSH=1 - Connect to GitHub with ssh instead of https

if ! git diff-index --quiet HEAD; then
  echo "You have uncommitted changes. Aborting."
  exit 1
fi

current_checkout_name=${1:-HEAD}
# This allows the script to work even if with transient refs like "HEAD"
current_checkout_hash="$(git rev-parse --quiet --verify $current_checkout_name)"

if [ "$current_checkout_hash" == "" ]; then
  echo "Not a recognized ref: $current_checkout_name"
  exit 1
fi

# To restore to prior branch at the end
orig_branch="$(git rev-parse --abbrev-ref HEAD)"
tmp_branch=_tmp_format_compatible
tmp_origin=_tmp_origin

# Don't depend on what current "origin" might be
set -e
git remote remove $tmp_origin 2>/dev/null || true
if [ "$USE_SSH" ]; then
  git remote add $tmp_origin "git@github.com:facebook/rocksdb.git"
else
  git remote add $tmp_origin "https://github.com/facebook/rocksdb.git"
fi
git fetch $tmp_origin

# Used in building some ancient RocksDB versions where by default it tries to
# use a precompiled libsnappy.a checked in to the repo.
export SNAPPY_LDFLAGS=-lsnappy

cleanup() {
  echo "== Cleaning up"
  git reset --hard || true
  git checkout "$orig_branch" || true
  git branch -D $tmp_branch || true
  git remote remove $tmp_origin || true
}
trap cleanup EXIT # Always clean up, even on failure or Ctrl+C

scriptpath=`dirname ${BASH_SOURCE[0]}`

test_dir=${TEST_TMPDIR:-"/tmp"}"/rocksdb_format_compatible_$USER"
rm -rf ${test_dir:?}

# For saving current version of scripts as we checkout different versions to test
script_copy_dir=$test_dir"/script_copy"
mkdir -p $script_copy_dir
cp -f $scriptpath/*.sh $script_copy_dir

# For shared raw input data
input_data_path=$test_dir"/test_data_input"
mkdir -p $input_data_path
# For external sst ingestion test
ext_test_dir=$test_dir"/ext"
mkdir -p $ext_test_dir
# For DB dump test
db_test_dir=$test_dir"/db"
mkdir -p $db_test_dir
# For backup/restore test (uses DB test)
bak_test_dir=$test_dir"/bak"
mkdir -p $bak_test_dir

python_bin=$(which python3 || which python || echo python3)

# Generate random files.
for i in {1..6}
do
  input_data[$i]=$input_data_path/data$i
  echo == Generating random input file ${input_data[$i]}
  $python_bin - <<EOF
import random
random.seed($i)
symbols=['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
with open('${input_data[$i]}', 'w') as f:
  for i in range(1,1024):
    k = ""
    for j in range(1, random.randint(1,32)):
      k=k + symbols[random.randint(0, len(symbols) - 1)]
    vb = ""
    for j in range(1, random.randint(0,128)):
      vb = vb + symbols[random.randint(0, len(symbols) - 1)]
    v = ""
    for j in range(1, random.randint(1, 5)):
      v = v + vb
    print(k + " ==> " + v, file=f)
EOF
done

# Generate file(s) with sorted keys.
sorted_input_data=$input_data_path/sorted_data
echo == Generating file with sorted keys ${sorted_input_data}
$python_bin - <<EOF
with open('${sorted_input_data}', 'w') as f:
  for i in range(0,10):
    k = str(i)
    v = "value" + k
    print(k + " ==> " + v, file=f)
EOF

# db_backward_only_refs defined below the rest

# To check for DB forward compatibility with loading options (old version
# reading data from new), as well as backward compatibility
declare -a db_forward_with_options_refs=("6.6.fb" "6.7.fb" "6.8.fb" "6.9.fb" "6.10.fb" "6.11.fb" "6.12.fb" "6.13.fb" "6.14.fb" "6.15.fb" "6.16.fb" "6.17.fb" "6.18.fb" "6.19.fb" "6.20.fb" "6.21.fb" "6.22.fb" "6.23.fb" "6.24.fb" "6.25.fb" "6.26.fb" "6.27.fb" "6.28.fb" "6.29.fb")
# To check for DB forward compatibility without loading options (in addition
# to the "with loading options" set), as well as backward compatibility
declare -a db_forward_no_options_refs=() # N/A at the moment

# To check for SST ingestion backward compatibility (new version reading
# data from old) (ldb ingest_extern_sst added in 5.16.x, back-ported to
# 5.14.x, 5.15.x)
declare -a ext_backward_only_refs=("5.14.fb" "5.15.fb" "5.16.fb" "5.17.fb" "5.18.fb" "6.0.fb" "6.1.fb" "6.2.fb" "6.3.fb" "6.4.fb" "6.5.fb")
# To check for SST ingestion forward compatibility (old version reading
# data from new) as well as backward compatibility
declare -a ext_forward_refs=("${db_forward_no_options_refs[@]}" "${db_forward_with_options_refs[@]}")

# To check for backup backward compatibility (new version reading data
# from old) (ldb backup/restore added in 4.11.x)
declare -a bak_backward_only_refs=("4.11.fb" "4.12.fb" "4.13.fb" "5.0.fb" "5.1.fb" "5.2.fb" "5.3.fb" "5.4.fb" "5.5.fb" "5.6.fb" "5.7.fb" "5.8.fb" "5.9.fb" "5.10.fb" "5.11.fb" "5.12.fb" "5.13.fb" "${ext_backward_only_refs[@]}")
# To check for backup forward compatibility (old version reading data
# from new) as well as backward compatibility
declare -a bak_forward_refs=("${db_forward_no_options_refs[@]}" "${db_forward_with_options_refs[@]}")

# Branches (git refs) to check for DB backward compatibility (new version
# reading data from old) (in addition to the "forward compatible" list)
# NOTE: 2.7.fb.branch shows assertion violation in some configurations
declare -a db_backward_only_refs=("2.2.fb.branch" "2.3.fb.branch" "2.4.fb.branch" "2.5.fb.branch" "2.6.fb.branch" "2.7.fb.branch" "2.8.1.fb" "3.0.fb.branch" "3.1.fb" "3.2.fb" "3.3.fb" "3.4.fb" "3.5.fb" "3.6.fb" "3.7.fb" "3.8.fb" "3.9.fb" "4.2.fb" "4.3.fb" "4.4.fb" "4.5.fb" "4.6.fb" "4.7.fb" "4.8.fb" "4.9.fb" "4.10.fb" "${bak_backward_only_refs[@]}")

if [ "$SHORT_TEST" ]; then
  # Use only the first (if exists) of each list
  db_backward_only_refs=(${db_backward_only_refs[0]})
  db_forward_no_options_refs=(${db_forward_no_options_refs[0]})
  db_forward_with_options_refs=(${db_forward_with_options_refs[0]})
  ext_backward_only_refs=(${ext_backward_only_refs[0]})
  ext_forward_refs=(${ext_forward_refs[0]})
  bak_backward_only_refs=(${bak_backward_only_refs[0]})
  bak_forward_refs=(${bak_forward_refs[0]})
fi

# De-duplicate & accumulate
declare -a checkout_refs=()
for checkout_ref in "${db_backward_only_refs[@]}" "${db_forward_no_options_refs[@]}" "${db_forward_with_options_refs[@]}" "${ext_backward_only_refs[@]}" "${ext_forward_refs[@]}" "${bak_backward_only_refs[@]}" "${bak_forward_refs[@]}"
do
  if [ ! -e $db_test_dir/$checkout_ref ]; then
    mkdir -p $db_test_dir/$checkout_ref
    checkout_refs+=($checkout_ref)
  fi
done

generate_db()
{
    set +e
    $script_copy_dir/generate_random_db.sh $1 $2
    if [ $? -ne 0 ]; then
        echo ==== Error loading data from $2 to $1 ====
        exit 1
    fi
    set -e
}

compare_db()
{
    set +e
    $script_copy_dir/verify_random_db.sh $1 $2 $3 $4 $5
    if [ $? -ne 0 ]; then
        echo ==== Read different content from $1 and $2 or error happened. ====
        exit 1
    fi
    set -e
}

write_external_sst()
{
    set +e
    $script_copy_dir/write_external_sst.sh $1 $2 $3
    if [ $? -ne 0 ]; then
        echo ==== Error writing external SST file using data from $1 to $3 ====
        exit 1
    fi
    set -e
}

ingest_external_sst()
{
    set +e
    $script_copy_dir/ingest_external_sst.sh $1 $2
    if [ $? -ne 0 ]; then
        echo ==== Error ingesting external SST in $2 to DB at $1 ====
        exit 1
    fi
    set -e
}

backup_db()
{
    set +e
    $script_copy_dir/backup_db.sh $1 $2
    if [ $? -ne 0 ]; then
        echo ==== Error backing up DB $1 to $2 ====
        exit 1
    fi
    set -e
}

restore_db()
{
    set +e
    $script_copy_dir/restore_db.sh $1 $2
    if [ $? -ne 0 ]; then
        echo ==== Error restoring from $1 to $2 ====
        exit 1
    fi
    set -e
}

member_of_array()
{
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}

force_no_fbcode()
{
  # Not all branches recognize ROCKSDB_NO_FBCODE and we should not need
  # to patch old branches for changes to available FB compilers.
  sed -i -e 's|-d /mnt/gvfs/third-party|"$ROCKSDB_FORCE_FBCODE"|' build_tools/build_detect_platform
}

# General structure from here:
# * Check out, build, and do stuff with the "current" branch.
# * For each older branch under consideration,
#   * Check out, build, and do stuff with it, potentially using data
#     generated from "current" branch.
# * (Again) check out, build, and do (other) stuff with the "current"
#    branch, potentially using data from older branches.
#
# This way, we only do at most n+1 checkout+build steps, without the
# need to stash away executables.

# Decorate name
current_checkout_name="$current_checkout_name ($current_checkout_hash)"

echo "== Building $current_checkout_name debug"
git checkout -B $tmp_branch $current_checkout_hash
force_no_fbcode
make clean
DISABLE_WARNING_AS_ERROR=1 make ldb -j32

echo "== Using $current_checkout_name, generate DB with extern SST and ingest"
current_ext_test_dir=$ext_test_dir"/current"
write_external_sst $input_data_path ${current_ext_test_dir}_pointless $current_ext_test_dir
ingest_external_sst ${current_ext_test_dir}_ingest $current_ext_test_dir

echo "== Generating DB from $current_checkout_name ..."
current_db_test_dir=$db_test_dir"/current"
generate_db $input_data_path $current_db_test_dir

echo "== Creating backup of DB from $current_checkout_name ..."
current_bak_test_dir=$bak_test_dir"/current"
backup_db $current_db_test_dir $current_bak_test_dir

for checkout_ref in "${checkout_refs[@]}"
do
  echo "== Building $checkout_ref debug"
  git reset --hard $tmp_origin/$checkout_ref
  force_no_fbcode
  make clean
  DISABLE_WARNING_AS_ERROR=1 make ldb -j32

  # We currently assume DB backward compatibility for every branch listed
  echo "== Use $checkout_ref to generate a DB ..."
  generate_db $input_data_path $db_test_dir/$checkout_ref

  if member_of_array "$checkout_ref" "${ext_backward_only_refs[@]}" ||
    member_of_array "$checkout_ref" "${ext_forward_refs[@]}"
  then
    echo "== Use $checkout_ref to generate DB with extern SST file"
    write_external_sst $input_data_path $ext_test_dir/${checkout_ref}_pointless $ext_test_dir/$checkout_ref
  fi

  if member_of_array "$checkout_ref" "${ext_forward_refs[@]}"
  then
    echo "== Use $checkout_ref to ingest extern SST file and compare vs. $current_checkout_name"
    ingest_external_sst $ext_test_dir/${checkout_ref}_ingest $ext_test_dir/$checkout_ref
    compare_db $ext_test_dir/${checkout_ref}_ingest ${current_ext_test_dir}_ingest db_dump.txt 1 1

    rm -rf ${ext_test_dir:?}/${checkout_ref}_ingest
    echo "== Use $checkout_ref to ingest extern SST file from $current_checkout_name"
    ingest_external_sst $ext_test_dir/${checkout_ref}_ingest $current_ext_test_dir
    compare_db $ext_test_dir/${checkout_ref}_ingest ${current_ext_test_dir}_ingest db_dump.txt 1 1
  fi

  if member_of_array "$checkout_ref" "${db_forward_no_options_refs[@]}" ||
    member_of_array "$checkout_ref" "${db_forward_with_options_refs[@]}"
  then
    echo "== Use $checkout_ref to open DB generated using $current_checkout_name..."
    compare_db $db_test_dir/$checkout_ref $current_db_test_dir forward_${checkout_ref}_dump.txt 0
  fi

  if member_of_array "$checkout_ref" "${db_forward_with_options_refs[@]}"
  then
    echo "== Use $checkout_ref to open DB generated using $current_checkout_name with its options..."
    compare_db $db_test_dir/$checkout_ref $current_db_test_dir forward_${checkout_ref}_dump.txt 1 1
  fi

  if member_of_array "$checkout_ref" "${bak_backward_only_refs[@]}" ||
    member_of_array "$checkout_ref" "${bak_forward_refs[@]}"
  then
    echo "== Use $checkout_ref to backup DB"
    backup_db $db_test_dir/$checkout_ref $bak_test_dir/$checkout_ref
  fi

  if member_of_array "$checkout_ref" "${bak_forward_refs[@]}"
  then
    echo "== Use $checkout_ref to restore DB from $current_checkout_name"
    rm -rf ${db_test_dir:?}/$checkout_ref
    restore_db $current_bak_test_dir $db_test_dir/$checkout_ref
    compare_db $db_test_dir/$checkout_ref $current_db_test_dir forward_${checkout_ref}_dump.txt 0
  fi
done

echo "== Building $current_checkout_name debug (again, final)"
git reset --hard $current_checkout_hash
force_no_fbcode
make clean
DISABLE_WARNING_AS_ERROR=1 make ldb -j32

for checkout_ref in "${checkout_refs[@]}"
do
  # We currently assume DB backward compatibility for every branch listed
  echo "== Use $current_checkout_name to open DB generated using $checkout_ref..."
  compare_db $db_test_dir/$checkout_ref $current_db_test_dir db_dump.txt 1 0

  if member_of_array "$checkout_ref" "${ext_backward_only_refs[@]}" ||
    member_of_array "$checkout_ref" "${ext_forward_refs[@]}"
  then
    rm -rf ${ext_test_dir:?}/${checkout_ref}_ingest
    echo "== Use $current_checkout_name to ingest extern SST file from $checkout_ref"
    ingest_external_sst $ext_test_dir/${checkout_ref}_ingest $current_ext_test_dir
    compare_db $ext_test_dir/${checkout_ref}_ingest ${current_ext_test_dir}_ingest db_dump.txt 1 1
  fi

  if member_of_array "$checkout_ref" "${bak_backward_only_refs[@]}" ||
    member_of_array "$checkout_ref" "${bak_forward_refs[@]}"
  then
    echo "== Use $current_checkout_name to restore DB from $checkout_ref"
    rm -rf ${db_test_dir:?}/$checkout_ref
    restore_db $bak_test_dir/$checkout_ref $db_test_dir/$checkout_ref
    compare_db $db_test_dir/$checkout_ref $current_db_test_dir db_dump.txt 1 0
  fi
done

echo ==== Compatibility Test PASSED ====
