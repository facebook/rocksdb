# RocksDB Documentation Enrichment: {{COMPONENT}}

## Your Task

You are enriching the RocksDB AI documentation for the **{{COMPONENT}}** component. You have three types of external source material (wiki pages, blog posts, workplace posts) that may contain valuable insights about this component. However, these sources are frequently outdated — RocksDB evolves fast. Your job is to:

1. Read the external sources to discover WHAT topics and complexities exist
2. Validate EVERY claim against the CURRENT codebase before including it
3. Add or update chapters in the documentation following the established pattern

## Documentation Pattern (MANDATORY)

Study the compression component as your exemplar:
- Read `docs/components/compression/index.md` for the index structure
- Read at least 2-3 chapter files (e.g., `01_types_and_selection.md`, `03_block_compression.md`, `11_parallel_compression.md`) for chapter style

### Index File Rules (`docs/components/{{COMPONENT}}/index.md`)
- SHORT (40-80 lines max)
- Overview paragraph (2-4 sentences)
- **Key source files** line with the main headers/cc files
- Chapter table (numbered, with file link + one-sentence summary)
- Key Characteristics (bulleted, 5-10 items)
- Key Invariants (bulleted, 3-5 items)

### Chapter File Rules (`docs/components/{{COMPONENT}}/NN_name.md`)
- Title as H1
- **Files:** line listing relevant source files (headers and .cc files, use paths relative to repo root)
- Organized with H2 sections
- Workflow descriptions ("Step 1 → Step 2 → Step 3") are highly valued — document the flow, not just the data structures
- Tables for comparing options, types, or configurations

### Style Rules (from reviewer feedback — STRICT)
- **NO line number references** — never say "line 42" or "L42". Refer to function/class/struct names instead.
- **NO inline code quoting** — never paste code snippets. Instead, refer to the header file + struct/function name. Example: "See `CompressionOptions` in `include/rocksdb/advanced_options.h`" instead of pasting the struct.
- **NO box-drawing characters** (─, │, ┌, ┐, └, ┘, ├, ┤, ┬, ┴, ┼) — use markdown tables or plain text instead.
- **INVARIANT sparingly** — only use "Key Invariant" for true correctness invariants that would cause data corruption or crashes if violated. For behavioral observations, use "Note:" or "Important:" instead.
- Refer to options by their struct field name AND the header where they're defined. Example: "`max_bytes_for_level_base` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`)"

## External Sources for {{COMPONENT}}

### Wiki Pages (in `docs/wiki_source/`)
{{WIKI_LIST}}

### Blog Posts (in `docs/_posts/`)
{{BLOG_LIST}}

### Workplace Posts (in `docs/workplace_posts_curated/`)
Scan `docs/workplace_posts_curated/INDEX.md` for posts relevant to {{COMPONENT}}. Read any that look relevant based on title. These are internal Meta engineering posts — often high-quality deep dives but potentially outdated.

## Procedure

### Phase A: Deep Dive into Current Codebase (MOST CRITICAL — DO NOT SKIP)
This is the foundation. You MUST develop a deep understanding of the current implementation before reading any external material. External sources are often outdated — you need a solid baseline to judge them against.

1. Identify the key source files for this component (headers, .cc files, test files)
2. Read the main implementation files thoroughly — understand the data structures, algorithms, and control flow
3. Trace the key code paths end-to-end (e.g., how a write flows through, how a read is served, how background operations are triggered)
4. Understand how this component interacts with other components (e.g., how compaction interacts with flush, how cache interacts with reads)
5. Note the complex parts — anything with subtle invariants, tricky concurrency, non-obvious behavior, or multiple code paths
6. Build a mental model of the component's architecture and key design decisions

### Phase B: Read Current Documentation
1. Read the current `docs/components/{{COMPONENT}}.md` (the existing single-file doc)
2. Compare against your codebase understanding — note what's already well-covered, what's missing, and what might be wrong

### Phase C: Read External Sources (Discovery)
Now that you have a deep codebase understanding, read external sources to discover additional topics and complexity worth documenting.

1. Read each wiki page and blog post listed above, IN FULL
2. Scan workplace posts INDEX.md and read any relevant posts
3. For each source, note:
   - What complex topic does it cover?
   - What specific claims does it make about behavior?
   - What date was it written? (older = more likely outdated)
4. Compile a list of topics/claims that are candidates for documentation

### Phase D: Validate External Claims Against Codebase
For EACH candidate topic/claim from external sources:
1. Find the relevant source files in the current codebase
2. Read the actual implementation
3. Determine if the claim is:
   - **Still accurate** → include in docs
   - **Partially accurate** → update with current behavior, note what changed
   - **Completely outdated** → do NOT include, but note the topic if the current behavior is worth documenting
   - **Cannot verify** → do NOT include

BE THOROUGH. Read the actual code deeply. Don't assume external docs are correct. You already have your Phase A baseline — use it to catch outdated claims quickly.

### Phase E: Restructure into Index + Chapters
1. Create `docs/components/{{COMPONENT}}/` directory
2. Design the chapter structure based on:
   - Current documentation content
   - Validated external source material
   - Code complexity that deserves documentation
3. Write `index.md` following the compression pattern exactly
4. Write each chapter file, including:
   - Content migrated from the existing single-file doc
   - New content from validated external sources
   - Workflow descriptions for complex operations
5. Remove the old single-file `docs/components/{{COMPONENT}}.md`

### Phase F: Self-Review Checklist
Before finishing, verify:
- [ ] No line number references anywhere
- [ ] No inline code quotes — only header/struct references
- [ ] No box-drawing characters
- [ ] INVARIANT used only for true invariants
- [ ] Every claim from external sources validated against current code
- [ ] index.md is SHORT (40-80 lines)
- [ ] Each chapter has a **Files:** line
- [ ] Workflow descriptions for complex operations
- [ ] Key source files listed in index.md overview

## Important Warning

External documentation (especially wiki pages from 2014-2018 and older workplace posts) is FREQUENTLY WRONG about current RocksDB behavior. Common issues:
- Options renamed or removed
- Algorithms completely rewritten
- Features deprecated or replaced
- Default values changed
- New features added that change behavior

Treat external docs as **topic discovery** — they tell you WHAT to investigate, not WHAT to write. The codebase is the only source of truth.
