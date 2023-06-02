#! /usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.

set -e
set -o pipefail

if [ "$1" ]; then
  # Target file specified on command line
  TARGET="$1"
else
  # Interactively choose a group and file name
  DIRS="`find unreleased_history/ -mindepth 1 -maxdepth 1 -type d`"
  echo "Choose a group for new release note:"
  echo "$DIRS" | grep -nEo '[^/]+$'
  echo -n "Enter a number: "
  while [ ! "$DIRNUM" ]; do read -r DIRNUM; done
  DIR="$(echo "$DIRS" | head -n "$DIRNUM" | tail -1)"
  echo "Choose a file name for new release note (e.g. improved_whatever.md):"
  while [ ! "$FNAME" ]; do read -re FNAME; done
  # Replace spaces with underscores
  TARGET="$(echo "$DIR/$FNAME" | tr ' ' '_')"
fi

# Edit/create the file
${EDITOR:-nano} "$TARGET"
# Add to version control (easy to forget!)
git add "$TARGET"
