#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
# If clang_format_diff.py command is not specfied, we assume we are able to
# access directly without any path.
if [ -z $CLANG_FORMAT_DIFF ]
then
CLANG_FORMAT_DIFF="clang-format-diff.py"
fi

# Check clang-format-diff.py
if ! which $CLANG_FORMAT_DIFF &> /dev/null
then
  echo "You didn't have clang-format-diff.py and/or clang-format available in your computer!"
  echo "You can download clang-format-diff.py by running: "
  echo "    curl --location http://goo.gl/iUW1u2 -o ${CLANG_FORMAT_DIFF}"
  echo "You can download clang-format by running:"
  echo "    brew install clang-format"
  echo "  Or"
  echo "    apt install clang-format"
  echo "  This might work too:"
  echo "    yum install git-clang-format"
  echo "Then, move both files (i.e. ${CLANG_FORMAT_DIFF} and clang-format) to some directory within PATH=${PATH}"
  echo "and make sure ${CLANG_FORMAT_DIFF} is executable."
  exit 128
fi

# Check argparse, a library that clang-format-diff.py requires.
python 2>/dev/null << EOF
import argparse
EOF

if [ "$?" != 0 ]
then
  echo "To run clang-format-diff.py, we'll need the library "argparse" to be"
  echo "installed. You can try either of the follow ways to install it:"
  echo "  1. Manually download argparse: https://pypi.python.org/pypi/argparse"
  echo "  2. easy_install argparse (if you have easy_install)"
  echo "  3. pip install argparse (if you have pip)"
  exit 129
fi

# TODO(kailiu) following work is not complete since we still need to figure
# out how to add the modified files done pre-commit hook to git's commit index.
#
# Check if this script has already been added to pre-commit hook.
# Will suggest user to add this script to pre-commit hook if their pre-commit
# is empty.
# PRE_COMMIT_SCRIPT_PATH="`git rev-parse --show-toplevel`/.git/hooks/pre-commit"
# if ! ls $PRE_COMMIT_SCRIPT_PATH &> /dev/null
# then
#   echo "Would you like to add this script to pre-commit hook, which will do "
#   echo -n "the format check for all the affected lines before you check in (y/n):"
#   read add_to_hook
#   if [ "$add_to_hook" == "y" ]
#   then
#     ln -s `git rev-parse --show-toplevel`/build_tools/format-diff.sh $PRE_COMMIT_SCRIPT_PATH
#   fi
# fi
set -e

uncommitted_code=`git diff HEAD`

# If there's no uncommitted changes, we assume user are doing post-commit
# format check, in which case we'll try to check the modified lines vs. the
# facebook/rocksdb.git master branch. Otherwise, we'll check format of the
# uncommitted code only.
if [ -z "$uncommitted_code" ]
then
  # Attempt to get name of facebook/rocksdb.git remote.
  [ "$FORMAT_REMOTE" ] || FORMAT_REMOTE="$(git remote -v | grep 'facebook/rocksdb.git' | head -n 1 | cut -f 1)"
  # Fall back on 'origin' if that fails
  [ "$FORMAT_REMOTE" ] || FORMAT_REMOTE=origin
  # Use master branch from that remote
  [ "$FORMAT_UPSTREAM" ] || FORMAT_UPSTREAM="$FORMAT_REMOTE/master"
  # Get the common ancestor with that remote branch. Everything after that
  # common ancestor would be considered the contents of a pull request, so
  # should be relevant for formatting fixes.
  FORMAT_UPSTREAM_MERGE_BASE="$(git merge-base "$FORMAT_UPSTREAM" HEAD)"
  # Get the differences
  diffs=$(git diff -U0 "$FORMAT_UPSTREAM_MERGE_BASE" | $CLANG_FORMAT_DIFF -p 1)
else
  # Check the format of uncommitted lines,
  diffs=$(git diff -U0 HEAD | $CLANG_FORMAT_DIFF -p 1)
fi

if [ -z "$diffs" ]
then
  echo "Nothing needs to be reformatted!"
  exit 0
fi

# Highlight the insertion/deletion from the clang-format-diff.py's output
COLOR_END="\033[0m"
COLOR_RED="\033[0;31m"
COLOR_GREEN="\033[0;32m"

echo -e "Detect lines that doesn't follow the format rules:\r"
# Add the color to the diff. lines added will be green; lines removed will be red.
echo "$diffs" |
  sed -e "s/\(^-.*$\)/`echo -e \"$COLOR_RED\1$COLOR_END\"`/" |
  sed -e "s/\(^+.*$\)/`echo -e \"$COLOR_GREEN\1$COLOR_END\"`/"

if [[ "$OPT" == *"-DTRAVIS"* ]]
then
  exit 1
fi

echo -e "Would you like to fix the format automatically (y/n): \c"

# Make sure under any mode, we can read user input.
exec < /dev/tty
read to_fix

if [ "$to_fix" != "y" ]
then
  exit 1
fi

# Do in-place format adjustment.
if [ -z "$uncommitted_code" ]
then
  git diff -U0 "$FORMAT_UPSTREAM_MERGE_BASE" | $CLANG_FORMAT_DIFF -i -p 1
else
  git diff -U0 HEAD^ | $CLANG_FORMAT_DIFF -i -p 1
fi
echo "Files reformatted!"

# Amend to last commit if user do the post-commit format check
if [ -z "$uncommitted_code" ]; then
  echo -e "Would you like to amend the changes to last commit (`git log HEAD --oneline | head -1`)? (y/n): \c"
  read to_amend

  if [ "$to_amend" == "y" ]
  then
    git commit -a --amend --reuse-message HEAD
    echo "Amended to last commit"
  fi
fi
