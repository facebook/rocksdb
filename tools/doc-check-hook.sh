#!/bin/bash
# Pre-commit hook: warn if staged source files are referenced in docs/components/
#
# Install: ln -sf ../../tools/doc-check-hook.sh .git/hooks/pre-commit
#
# This hook prints warnings but does NOT block commits.

DOC_DIR="docs/components"

if [ ! -d "$DOC_DIR" ]; then
  exit 0
fi

# Get staged .cc and .h files
STAGED=$(git diff --cached --name-only --diff-filter=ACMR | grep -E '\.(cc|h)$' || true)

if [ -z "$STAGED" ]; then
  exit 0
fi

WARNINGS=()

while IFS= read -r src_file; do
  src_basename=$(basename "$src_file")
  matching_docs=$(grep -rl "$src_basename" "$DOC_DIR" 2>/dev/null || true)

  if [ -z "$matching_docs" ]; then
    continue
  fi

  while IFS= read -r doc; do
    WARNINGS+=("  $doc  (references $src_file)")
  done <<< "$matching_docs"
done <<< "$STAGED"

if [ ${#WARNINGS[@]} -gt 0 ]; then
  echo ""
  echo "WARNING: The following docs may need updating:"
  echo ""
  for w in "${WARNINGS[@]}"; do
    echo "$w"
  done
  echo ""
  echo "Run: grep -rl \"<file>\" docs/components/ to check references."
  echo ""
fi

exit 0
