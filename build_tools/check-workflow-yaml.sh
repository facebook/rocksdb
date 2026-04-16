#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
# Validate GitHub Actions workflow YAML before it reaches CI runtime.

set -euo pipefail

if ! command -v ruby >/dev/null 2>&1; then
  echo "ruby is required to validate GitHub Actions workflow YAML"
  exit 1
fi

ruby <<'RUBY'
require "psych"

bad = false
workflow_files = Dir[".github/workflows/*.{yml,yaml}"].sort

if workflow_files.empty?
  warn "No workflow YAML files found under .github/workflows"
  exit 1
end

workflow_files.each do |path|
  begin
    Psych.parse_file(path)
    puts "OK #{path}"
  rescue Psych::Exception => e
    warn "Invalid YAML in #{path}: #{e.message}"
    bad = true
  end
end

exit(bad ? 1 : 0)
RUBY
