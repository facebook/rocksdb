#! /usr/bin/env bash

set -e

if [ ! -d unreleased_history ]; then
  echo "Can't find unreleased_history/ directory"
  exit 1
fi

if [ ! "$DRY_RUN" ]; then
  # Check for uncommitted changes
  UNCOMMITTED="$(git diff -- unreleased_history/ HISTORY.md)"
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
     END { print "## " major "." minor "." patch " (" strftime("%F") ")" }' < include/rocksdb/version.h >> HISTORY.new

function process_file () {
  # use awk to correct extra or missing newlines, missing '* ' on first line
  awk '/./ { if (notfirstline || $1 == "*") print;
             else print "* " $0;
             notfirstline=1; }' < $1 >> HISTORY.new
  # Part of the reason for putting the header in a file in the dir is because
  # git won't track empty directories, but we want the directories to stick
  # around even when there are no unreleased entries for the group.
  if echo "$1" | grep -q 000_HEADER.md; then
    cat $1
  else
    echo git rm $1
    if [ ! "$DRY_RUN" ]; then
      git rm $1
    fi
  fi
}

function process_dir () {
  # ls will sort the files, including the permanent header file
  FILES="$(ls $1/*)"
  COUNT="$(echo "$FILES" | wc -l)"
  if [ "$COUNT" -gt 1 ]; then
    for FILE in $FILES; do
      process_file $FILE
    done
    echo >> HISTORY.new
    echo "Saved $COUNT from $1"
  else
    echo "Nothing new in $1"
  fi
}

ORDERED_DIRECTORIES="new_features public_api_changes behavior_changes bug_fixes"

# Check for unexpected files or dirs at top level. process_dir/process_file
# will deal with contents of these directories
EXPECTED_REGEX="README.txt|$(echo $ORDERED_DIRECTORIES | tr ' ' '|')"
UNEXPECTED="$(find unreleased_history/ -mindepth 1 -maxdepth 1 -regextype egrep -not -regex "[^/]*/($EXPECTED_REGEX)")"
if [ "$UNEXPECTED" ]; then
  echo "Unexpected files I don't know how to process:"
  echo "$UNEXPECTED"
  rm HISTORY.new
  exit 3
fi

# Process dirs and files
for DIR in $ORDERED_DIRECTORIES; do
  process_dir unreleased_history/$DIR
done

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
  echo 'Done. Revert command: git checkout HEAD -- unreleased_history/ HISTORY.md'
fi
