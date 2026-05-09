# Code Review Skill

This document defines the methodology for performing thorough, multi-perspective
code reviews on RocksDB changes. It is used both by CI (GitHub Actions) and
local review workflows.

## Prerequisites

Before starting a review:
- Read `CLAUDE.md` in the repository root for project-specific guidelines
- Read any files in `claude_md/` for architecture context
- Identify the commit/diff/PR to review and parse the changed files

## Review Methodology

Conduct the review from multiple perspectives, then synthesize findings into a
single report. Each perspective catches different classes of bugs.

### Perspective 1: Codebase Context & Call-Chain Analysis

Before reviewing the diff itself, build deep context:
- For each changed function/method, trace the call chain UPWARD 3-5 levels.
  Who consumes the changed behavior? What invariants do callers rely on?
- Trace DOWNWARD into callees. What side effects do they have? What shared
  state do they mutate (counters, sequence numbers, flags, metadata)?
- Identify the "sibling" or "reference" implementation that handles the same
  scenario the standard way. Compare behaviors.
- For data written to shared structures (memtable, SST block, cache entry),
  find ALL readers and verify their visibility rules match the writer.

### Perspective 2: Correctness & Edge Cases

- Thread safety and concurrency (lock ordering, data races)
- Error handling and Status propagation
- Edge cases: empty inputs, overflow, boundary conditions
- Data corruption scenarios
- Behavioral contract changes: do return value semantics match callers?

### Perspective 3: Cross-Component & Adversarial Analysis

Check the change in ALL execution contexts:

| Context | Key difference |
|---------|---------------|
| WritePreparedTxnDB | read_callback_ controls visibility |
| ReadOnly DB / SecondaryInstance | No mutable memtable |
| CompactionService / Remote compaction | Different process |
| User-defined timestamps | Extra key comparison dimension |
| MemPurge | Memtable-to-memtable path |
| BlobDB | Values in blob files, not inline |
| Old snapshots | Seqno far behind current |
| Concurrent writers | Lock-free vs locked paths |
| FIFO / Universal compaction | Different invariants |
| Prefix seek / total order seek | Different iterator behavior |

When the change claims a property ("logically redundant", "safe because X"),
systematically try to break it: list preconditions, construct counterexamples.

### Perspective 4: Performance

RocksDB is a high-performance storage engine.
- Memory allocation on hot paths
- Unnecessary copies (prefer move semantics, Slice, string_view)
- Cache efficiency and data locality
- Loop optimization, branch prediction (LIKELY/UNLIKELY)

### Perspective 5: API, Compatibility & Testing

- Public API backwards compatibility
- Serialization format correctness and versioning
- Test coverage for edge cases, failure modes, and system-level interactions

## How to Explore the Codebase

Use available tools to explore deeply — do NOT just review the diff in isolation.
The most critical bugs hide at component boundaries.

- Read the header files for changed implementations
- Search for callers of changed functions (3-5 levels up)
- Read existing tests to understand guaranteed behaviors
- Check related implementations for conventions and patterns

## Output Format

### Summary
Brief overall assessment (1-2 sentences).

### Issues Found
Categorize by severity:
- :red_circle: **Critical**: Must fix (correctness, data corruption, security)
- :yellow_circle: **Suggestion**: Should consider (performance, edge cases)
- :green_circle: **Nitpick**: Minor style issues

For each issue include:
- File and line reference
- Which review perspective found it
- Description with root cause analysis
- Suggested fix

### Cross-Component Analysis
Execution context checks and assumption stress-test results.

### Positive Observations
Good patterns, clever optimizations, or improvements.
