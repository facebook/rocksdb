# RocksDB Documentation: Stitch Index for {{COMPONENT}}

## Your Task

Multiple CC sessions have written chapters for the **{{COMPONENT}}** component. Your job is to:
1. Read all the chapter files they produced
2. Write `docs/components/{{COMPONENT}}/index.md` following the compression pattern
3. Verify chapter quality and consistency
4. Remove the old single-file `docs/components/{{COMPONENT}}.md`

## Chapters Written
Read all files in `docs/components/{{COMPONENT}}/` (all `NN_*.md` files).

## Index Pattern (follow exactly)

Study `docs/components/compression/index.md` as your exemplar. Your index.md must have:

1. **H1 title**
2. **Overview paragraph** (2-4 sentences describing the component)
3. **Key source files** line listing main headers and .cc files
4. **Chapters table** (numbered, with file link + one-sentence summary for each chapter)
5. **Key Characteristics** (bulleted, 5-10 items summarizing the most important behaviors)
6. **Key Invariants** (bulleted, 3-5 items — only true correctness invariants)

The index must be SHORT: 40-80 lines max. It's a navigation hub, not a deep dive.

## Quality Checks

While reading the chapters, check for:
- Consistent terminology across chapters (same names for same concepts)
- No overlap/duplication between chapters
- No gaps (important topics that no chapter covers)
- Style compliance: no line numbers, no code quotes, no box-drawing characters
- Each chapter has a **Files:** line
- INVARIANT used only for true correctness invariants

If you find issues, fix them in the chapter files.

## Final Steps
1. Write `docs/components/{{COMPONENT}}/index.md`
2. Delete `docs/components/{{COMPONENT}}.md` (the old single-file doc)
3. Verify the directory structure is clean
