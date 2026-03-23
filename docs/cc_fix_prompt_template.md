# Address Reviews: {{COMPONENT}} Documentation

You are addressing review feedback for the **{{COMPONENT}}** component documentation.

## Review Files

Read both review files:
1. `docs/reviews/{{COMPONENT}}_review_cc.md` (Claude Code review)
2. `docs/reviews/{{COMPONENT}}_review_codex.md` (Codex review)

## Procedure

### Step 1: Categorize Feedback
For each issue raised by either reviewer:
- **Agreed by both** → Fix immediately
- **Raised by one, not contradicted by other** → Verify against code, fix if correct
- **Disagreement between reviewers** → Investigate and resolve (see Step 2)

### Step 2: Handle Disagreements
When CC and Codex disagree on a factual claim:
1. Read the relevant source code yourself
2. Determine who is correct
3. Document the disagreement in `docs/reviews/{{COMPONENT}}_debates.md`:
   ```
   ## Debate: [Topic]
   - **CC position**: [what CC said]
   - **Codex position**: [what Codex said]
   - **Code evidence**: [what the code actually does, with file/function references]
   - **Resolution**: [who was right and why]
   - **Risk level**: [low/medium/high — how subtle is this?]
   ```
4. Apply the correct fix

### Step 3: Apply Fixes
- Fix all valid issues in the chapter files and index.md
- Re-verify the self-review checklist:
  - [ ] No line number references
  - [ ] No inline code quotes
  - [ ] No box-drawing characters
  - [ ] INVARIANT used correctly
  - [ ] Every claim validated against current code
  - [ ] index.md is SHORT (40-80 lines)
  - [ ] Each chapter has **Files:** line

### Step 4: Summary
Write a fix summary to `docs/reviews/{{COMPONENT}}_fix_summary.md`:
```
# Fix Summary: {{COMPONENT}}

## Issues Fixed
(Count by category: correctness, completeness, structure, style)

## Disagreements Found
(Count and brief description — details in debates.md)

## Changes Made
(List of files modified and what changed)
```

## Important
- When in doubt about a claim, check the code. The code is always right.
- If you create the debates.md file, it means there ARE disagreements for the human to review.
- Don't skip disagreements — document them ALL.
