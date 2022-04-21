#!/bin/sh

# While wrapped command runs, a background subshell prints its stack traces
# after two minutes of waiting.

# Enable job control so background subshell gets its own process group ID.
set -m
# For cleaning up background jobs on exit (adapted from
# https://stackoverflow.com/a/2173421).
# Negating the PIDs passed to kill causes the whole process group to be killed.
trap 'trap - TERM && jobs -p | xargs printf -- "-%s " | xargs kill --' INT TERM EXIT

"$@" &
test_pid=$!
(sleep 120; pstack $test_pid) &
wait $test_pid
