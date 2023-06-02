#! /usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.

set -e

if [ ! -d unreleased_history ]; then
  echo "Can't find unreleased_history/ directory"
  exit 1
fi

GIT_PATHS="unreleased_history/ HISTORY.md"
if [ ! "$DRY_RUN" ]; then
  # Check for uncommitted changes
  UNCOMMITTED="$(git diff -- $GIT_PATHS)"
  if [ "$UNCOMMITTED" ]; then
    echo 'Uncommitted changes to files to be modified. Please commit first to'
    echo 'ensure a clean revert path. You can always `git commit -a --amend`'
    echo 'to add more changes to your commit.'
    exit 2
  fi
fi

# Add first part of existing HISTORY file to new version
awk '{ print } /NOTE/ { exit(0) }' < HISTORY.md > HISTORY.new

# And a blank line separator
echo >> HISTORY.new

# Add new version header
awk '/#define ROCKSDB_MAJOR/ { major = $3 }
     /#define ROCKSDB_MINOR/ { minor = $3 }
     /#define ROCKSDB_PATCH/ { patch = $3 }
     END { printf "## " major "." minor "." patch }' < include/rocksdb/version.h >> HISTORY.new
echo " (`date +%x`)" >> HISTORY.new

function process_file () {
  # use awk to correct extra or missing newlines, missing '* ' on first line
  awk '/./ { if (notfirstline || $1 == "*") print;
             else print "* " $0;
             notfirstline=1; }' < $1 >> HISTORY.new
  echo git rm $1
  if [ ! "$DRY_RUN" ]; then
    git rm $1
  fi
}

PROCESSED_DIRECTORIES=""

function process_dir () {
  PROCESSED_DIRECTORIES="$PROCESSED_DIRECTORIES $1"
  # ls will sort the files, including the permanent header file
  FILES="$(ls unreleased_history/$1/)"
  if [ "$FILES" ]; then
    echo "### $2" >> HISTORY.new
    for FILE in $FILES; do
      process_file "unreleased_history/$1/$FILE"
    done
    echo >> HISTORY.new
    echo "Saved entries from $1"
  else
    echo "Nothing new in $1"
  fi
}

# Process dirs and files
process_dir new_features "New Features"
process_dir public_api_changes "Public API Changes"
process_dir behavior_changes "Behavior Changes"
process_dir bug_fixes "Bug Fixes"
process_dir performance_improvements "Performance Improvements"

# Check for unexpected files or dirs at top level. process_dir/process_file
# will deal with contents of these directories
EXPECTED_REGEX="[^/]*[.]sh|README[.]txt|$(echo $PROCESSED_DIRECTORIES | tr ' ' '|')"
platform=`uname`
if [ $platform = 'Darwin' ]; then
  UNEXPECTED="$(find -E unreleased_history -mindepth 1 -maxdepth 1 -not -regex "[^/]*/($EXPECTED_REGEX)")"
else
  UNEXPECTED="$(find unreleased_history/ -mindepth 1 -maxdepth 1 -regextype egrep -not -regex "[^/]*/($EXPECTED_REGEX)")"
fi
if [ "$UNEXPECTED" ]; then
  echo "Unexpected files I don't know how to process:"
  echo "$UNEXPECTED"
  rm HISTORY.new
  exit 3
fi

# Add rest of existing HISTORY file to new version (collapsing newlines)
awk '/./ { if (note) pr=1 }
     /NOTE/ { note=1 }
     { if (pr) print }' < HISTORY.md >> HISTORY.new

if [ "$DRY_RUN" ]; then
  echo '==========================================='
  diff -U3 HISTORY.md HISTORY.new || true
  rm HISTORY.new
else
  mv HISTORY.new HISTORY.md
  echo "Done. Revert command: git checkout HEAD -- $GIT_PATHS"
fi
