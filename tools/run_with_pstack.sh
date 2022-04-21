#!/bin/sh

# While wrapped command runs, a background subshell prints its stack traces
# after every minute of waiting.

# Enable job control so background subshell gets its own process group ID.
set -m
# For cleaning up background loop on exit (adapted from
# https://stackoverflow.com/a/2173421).
trap 'trap - SIGTERM && jobs -p | xargs printf -- "-%s " | xargs kill --' SIGINT SIGTERM EXIT

"$@" &
test_pid=$!
while true; do sleep 60; pstack $test_pid; done &
wait $test_pid
