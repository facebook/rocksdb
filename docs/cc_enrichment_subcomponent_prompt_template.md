# RocksDB Documentation Enrichment: {{COMPONENT}} — {{SUBCOMPONENT_NAME}}

## Your Task

You are writing chapters for the **{{COMPONENT}}** component documentation, specifically covering: **{{SUBCOMPONENT_DESCRIPTION}}**

This is part of a larger effort — other CC sessions are writing chapters for other aspects of {{COMPONENT}}. You are responsible for these chapters:
{{CHAPTER_LIST}}

## Key Source Files to Read
{{KEY_FILES}}

## Documentation Pattern (MANDATORY)

Study the compression component as your exemplar:
- Read `docs/components/compression/index.md` for the index structure
- Read at least 2-3 chapter files (e.g., `01_types_and_selection.md`, `03_block_compression.md`, `11_parallel_compression.md`) for chapter style

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
The file `docs/workplace_posts_curated/INDEX.md` lists 407 curated posts from the RocksDB Dev Workplace group. Sub-agents should scan this index for posts relevant to your sub-component scope.

## Procedure

### Phase A: Deep Dive into Current Codebase (MOST CRITICAL — DO NOT SKIP)
This is the foundation. You MUST develop a deep understanding of the implementation for your assigned sub-component.

1. Read ALL the key source files listed above — headers AND implementation files
2. Trace the key code paths end-to-end
3. Understand how this sub-component interacts with other parts of {{COMPONENT}} and with other RocksDB components
4. Note the complex parts — subtle invariants, tricky concurrency, non-obvious behavior, multiple code paths
5. Build a mental model of the architecture and key design decisions

### Phase B: Read Current Documentation
1. Read the current `docs/components/{{COMPONENT}}.md` (the existing single-file doc)
2. Focus on the sections relevant to your assigned chapters
3. Note what's already well-covered, what's missing, and what might be wrong

### Phase C: Process External Sources via Sub-Agents
**IMPORTANT: Do NOT read external source files yourself.** Spawn sub-agents to read them and return structured summaries. This keeps your context focused on codebase knowledge.

Spawn up to 3 sub-agents in parallel:

**Sub-agent 1 — Wiki Pages:**
```
Read the following wiki pages IN FULL:
{{WIKI_LIST}}

Focus on content relevant to: {{SUBCOMPONENT_DESCRIPTION}}

For each page, extract:
1. TITLE and approximate date
2. KEY TOPICS relevant to the sub-component scope
3. SPECIFIC CLAIMS: List every concrete technical claim about behavior, defaults, algorithms, or invariants. Quote the exact claim.
4. COMPLEXITY FLAGS: What does this doc suggest is complex, subtle, or commonly misunderstood?
5. CONFIGURATION: Any options, tuning advice, or recommended settings mentioned.

Return a structured summary organized by topic. Be thorough — capture every technical claim, even small ones.
```

**Sub-agent 2 — Blog Posts:**
```
Read the following blog posts IN FULL:
{{BLOG_LIST}}

Focus on content relevant to: {{SUBCOMPONENT_DESCRIPTION}}

For each post, extract:
1. TITLE, DATE, and AUTHOR
2. KEY TOPICS relevant to the sub-component scope
3. SPECIFIC CLAIMS: List every concrete technical claim. Quote the exact claim.
4. DESIGN RATIONALE: Any explanation of WHY things work the way they do.
5. PERFORMANCE DATA: Any benchmarks, measurements, or performance characteristics.

Return a structured summary organized by topic.
```

**Sub-agent 3 — Workplace Posts:**
```
First read docs/workplace_posts_curated/INDEX.md to find posts relevant to: {{SUBCOMPONENT_DESCRIPTION}}
Then read each relevant post IN FULL.

For each post, extract:
1. TITLE, DATE, and AUTHOR
2. KEY TOPICS relevant to the sub-component scope
3. SPECIFIC CLAIMS: List every concrete technical claim.
4. INTERNAL INSIGHTS: Any production experience, debugging stories, or operational wisdom.
5. PERFORMANCE DATA: Any measurements or benchmarks.

Return a structured summary organized by topic.
```

Wait for all sub-agents to complete before proceeding.

### Phase D: Validate External Claims Against Codebase
For EACH claim from the sub-agent summaries:
1. Check against your codebase understanding from Phase A
2. For claims you're uncertain about, go back and read the relevant source code
3. Categorize each claim:
   - **Still accurate** → include in docs
   - **Partially accurate** → update with current behavior
   - **Completely outdated** → do NOT include, but document current behavior if the TOPIC is still relevant
   - **Cannot verify** → do NOT include

### Phase E: Write Chapters
1. Write your assigned chapter files to `docs/components/{{COMPONENT}}/`
2. Use the chapter numbering provided in the chapter list above
3. Include:
   - Content migrated from relevant sections of the existing single-file doc
   - New content from validated external sources
   - Workflow descriptions for complex operations
   - Content from your codebase deep dive that isn't in any external source

**Do NOT write index.md** — a separate session will stitch all chapters into the index.
**Do NOT delete the old single-file doc** — the stitcher session will handle that.

### Phase F: Self-Review Checklist
Before finishing, verify:
- [ ] No line number references anywhere
- [ ] No inline code quotes — only header/struct references
- [ ] No box-drawing characters
- [ ] INVARIANT used only for true correctness invariants
- [ ] Every claim from external sources validated against current code
- [ ] Each chapter has a **Files:** line
- [ ] Workflow descriptions for complex operations

## Important Warning

External documentation (especially wiki pages from 2014-2018 and older workplace posts) is FREQUENTLY WRONG about current RocksDB behavior. Treat external docs as **topic discovery** — they tell you WHAT to investigate, not WHAT to write. The codebase is the only source of truth.
