#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

set -euo pipefail

timeout_seconds="${CI_COMMAND_TIMEOUT_SECONDS:-6900}"
if [[ ! "$timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "::error::CI_COMMAND_TIMEOUT_SECONDS must be a positive integer."
  exit 1
fi

if [[ "$#" -eq 0 ]]; then
  echo "::error::No command was provided."
  exit 1
fi

if command -v timeout >/dev/null 2>&1; then
  timeout_binary=timeout
elif command -v gtimeout >/dev/null 2>&1; then
  timeout_binary=gtimeout
else
  echo "::error::GNU timeout is required."
  exit 1
fi

set +e
"$timeout_binary" -s TERM -k 30s "${timeout_seconds}s" "$@"
exit_code=$?
set -e

if [[ "$exit_code" -eq 124 || "$exit_code" -eq 137 ||
      "$exit_code" -eq 143 ]]; then
  echo "::error::Command timed out after ${timeout_seconds} seconds."
  exit 1
fi

exit "$exit_code"
