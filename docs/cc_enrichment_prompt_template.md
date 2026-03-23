# RocksDB Documentation Enrichment: {{COMPONENT}}

## Your Task

You are enriching the RocksDB AI documentation for the **{{COMPONENT}}** component. You have three types of external source material (wiki pages, blog posts, workplace posts) that may contain valuable insights. However, these sources are frequently outdated — RocksDB evolves fast. Your job is to:

1. Develop deep understanding of the current codebase (this is the foundation)
2. Use sub-agents to process external sources and extract key topics/claims
3. Validate external claims against the codebase before including them
4. Restructure the documentation into the established index + chapters pattern

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
The file `docs/workplace_posts_curated/INDEX.md` lists 407 curated posts from the RocksDB Dev Workplace group. Sub-agents should scan this index for posts relevant to {{COMPONENT}}.

## Procedure

### Phase A: Deep Dive into Current Codebase (MOST CRITICAL — DO NOT SKIP)
This is the foundation. You MUST develop a deep understanding of the current implementation before anything else. External sources are often outdated — you need a solid baseline to judge them against.

1. Identify the key source files for this component (headers, .cc files)
2. Read the main implementation files — understand the data structures, algorithms, and control flow
3. Trace the key code paths end-to-end (e.g., how a write flows through, how a read is served, how background operations are triggered)
4. Understand how this component interacts with other components (e.g., how compaction interacts with flush, how cache interacts with reads)
5. Note the complex parts — anything with subtle invariants, tricky concurrency, non-obvious behavior, or multiple code paths
6. Build a mental model of the component's architecture and key design decisions

**Context management tip:** Read headers first to understand the API surface, then selectively read .cc files for the most complex/important code paths. You do NOT need to read every line of every file — focus on understanding the architecture, key algorithms, and non-obvious behavior. Skip test files in this phase.

### Phase B: Read Current Documentation
1. Read the current `docs/components/{{COMPONENT}}.md` (the existing single-file doc)
2. Compare against your codebase understanding — note what's already well-covered, what's missing, and what might be wrong

### Phase C: Process External Sources via Sub-Agents
**IMPORTANT: Do NOT read external source files yourself.** Spawn sub-agents to read them and return structured summaries. This keeps your context focused on codebase knowledge.

Spawn up to 3 sub-agents in parallel:

**Sub-agent 1 — Wiki Pages:**
```
Read the following wiki pages IN FULL:
{{WIKI_LIST}}

For each page, extract:
1. TITLE and approximate date (check git blame or content references)
2. KEY TOPICS: What complex aspects of {{COMPONENT}} does it cover?
3. SPECIFIC CLAIMS: List every concrete technical claim about behavior, defaults, algorithms, or invariants. Quote the exact claim.
4. COMPLEXITY FLAGS: What does this doc suggest is complex, subtle, or commonly misunderstood?
5. CONFIGURATION: Any options, tuning advice, or recommended settings mentioned.

Return a structured summary organized by topic. Be thorough — capture every technical claim, even small ones. The main agent needs these to validate against current code.
```

**Sub-agent 2 — Blog Posts:**
```
Read the following blog posts IN FULL:
{{BLOG_LIST}}

For each post, extract:
1. TITLE, DATE, and AUTHOR
2. KEY TOPICS: What aspect of {{COMPONENT}} does it cover?
3. SPECIFIC CLAIMS: List every concrete technical claim. Quote the exact claim.
4. DESIGN RATIONALE: Any explanation of WHY things work the way they do.
5. PERFORMANCE DATA: Any benchmarks, measurements, or performance characteristics mentioned.

Return a structured summary organized by topic.
```

**Sub-agent 3 — Workplace Posts:**
```
First read docs/workplace_posts_curated/INDEX.md to find posts relevant to {{COMPONENT}}.
Then read each relevant post IN FULL.

For each post, extract:
1. TITLE, DATE, and AUTHOR
2. KEY TOPICS: What aspect of {{COMPONENT}} does it cover?
3. SPECIFIC CLAIMS: List every concrete technical claim.
4. INTERNAL INSIGHTS: Any production experience, debugging stories, or operational wisdom.
5. PERFORMANCE DATA: Any measurements or benchmarks.

Return a structured summary organized by topic.
```

Wait for all sub-agents to complete before proceeding.

### Phase D: Validate External Claims Against Codebase
Now you have: (1) deep codebase knowledge from Phase A, and (2) structured claim summaries from sub-agents.

For EACH claim from the sub-agent summaries:
1. Check against your codebase understanding from Phase A
2. For claims you're uncertain about, go back and read the relevant source code
3. Categorize each claim:
   - **Still accurate** → include in docs
   - **Partially accurate** → update with current behavior, note what changed
   - **Completely outdated** → do NOT include, but if the TOPIC is still relevant, document the current behavior
   - **Cannot verify** → do NOT include

BE THOROUGH. The codebase is the only source of truth.

### Phase E: Restructure into Index + Chapters
1. Create `docs/components/{{COMPONENT}}/` directory
2. Design the chapter structure based on:
   - Current documentation content (Phase B)
   - Validated external source material (Phase D)
   - Code complexity discovered in Phase A that deserves documentation
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
- [ ] INVARIANT used only for true correctness invariants
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
