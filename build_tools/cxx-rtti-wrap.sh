#!/usr/bin/env bash
set -euox pipefail

args=()
for a in "$@"; do
  case "$a" in
    -fno-rtti|-fno-exceptions) continue ;;
  esac
  args+=("$a")
done

# Force policy at the end (last flag wins)
exec "${REAL_CXX:-g++}" "${args[@]}" -frtti -fexceptions
