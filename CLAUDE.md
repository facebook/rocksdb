# RocksDB Code Generation and Review Guidance

This document provides guidance for generating and reviewing code in the RocksDB project, derived from analysis of code review feedback across hundreds of complex merged Pull Requests. Use this as a reference when writing code with AI assistants or conducting code reviews.

---

## General Best Practices

### Code Quality and Maintainability

**Clarity and Readability:** Write clear, self-documenting code. Use meaningful variable names, add comments for complex logic, and structure code to minimize cognitive load. Avoid clever tricks that sacrifice readability for marginal performance gains unless absolutely necessary.

**Consistent Style:** Follow existing code style conventions. RocksDB uses `.clang-format` for formatting, specific naming conventions, and structural patterns. Deviations from these patterns are frequently flagged in reviews.

**Error Handling:** Ensure robust error handling throughout the codebase. Use RocksDB's `Status` type consistently, propagate errors appropriately, and avoid silently ignoring failures. Reviewers pay close attention to edge cases and failure modes.

### Testing Philosophy

**Comprehensive Coverage:** Every change should include appropriate test coverage. This includes unit tests for isolated functionality, integration tests for component interactions, and stress tests for concurrency and performance validation. Reviewers will ask for additional tests if coverage is insufficient.

**Edge Cases and Failure Modes:** Tests should explicitly cover edge cases, boundary conditions, and potential failure scenarios. This is especially important for changes affecting core database operations, compaction, or recovery logic.

**Platform-Specific Testing:** RocksDB supports multiple platforms (Linux, Windows, macOS) and compilers (GCC, Clang, MSVC). Changes should be tested across relevant platforms, particularly when touching platform-specific code or using compiler-specific features.

### Performance Considerations

**⚠️ PERFORMANCE IS CRITICAL:** RocksDB is a high-performance storage engine where every CPU cycle and memory access matters. When writing code, always evaluate from a performance perspective. This is not optional—performance-aware coding is a fundamental requirement for all contributions.

**Benchmarking and Profiling:** Performance claims should be backed by empirical evidence. Use RocksDB's benchmarking tools (e.g., `db_bench`) to validate improvements. Reviewers will request benchmark results for changes that could impact performance.

**Memory Allocation:** Minimize dynamic memory allocations, especially in hot paths. Prefer stack allocation over heap allocation. Reuse buffers when possible. Consider using arena allocators or memory pools for frequent small allocations. Every `new`, `malloc`, or container resize has a cost.

**Memory Copy:** Avoid unnecessary memory copies. Use move semantics, `std::string_view`, `Slice`, and pass-by-reference where appropriate. Be aware of implicit copies in STL containers and function returns. Prefer in-place operations over copy-and-modify patterns.

**CPU Cache Efficiency:** Design data structures and access patterns to be cache-friendly. Keep frequently accessed data together (data locality). Prefer sequential memory access over random access. Be mindful of cache line sizes (typically 64 bytes) and avoid false sharing in concurrent code. Consider struct packing and field ordering to improve cache utilization.

**Loop Optimization:** Look for opportunities to collapse nested loops, reduce loop overhead, and minimize branch mispredictions. Hoist invariant computations out of loops. Consider loop unrolling for tight inner loops. Batch operations when possible to amortize per-operation overhead.

**SIMD and Vectorization:** Leverage SIMD instructions (SSE, AVX) for data-parallel operations when appropriate. Structure data to enable auto-vectorization by the compiler. Consider explicit SIMD intrinsics for critical hot paths like checksum computation, encoding/decoding, and bulk data processing.

**Branch Prediction:** Minimize unpredictable branches in hot paths. Use `LIKELY`/`UNLIKELY` macros to hint branch prediction. Consider branchless alternatives for simple conditionals. Order switch cases and if-else chains by frequency.

**Memory and Resource Management:** Be mindful of memory allocations, especially in hot paths. Use RAII patterns, smart pointers, and RocksDB's memory management utilities appropriately.

**Hot Path Analysis:** When deciding how aggressively to optimize code, consider whether it's on a hot path:
- **Hot path** (executed thousands+ times, e.g., data access, iteration, compaction loops): Performance is paramount. Apply all optimization techniques—loop collapsing, SIMD, cache optimization, pre-allocation, etc. The cost of each operation is multiplied by execution frequency.
- **Cold path** (executed rarely, e.g., DB open, configuration parsing, error handling): Maintainability and clarity are more important. Prefer readable code over micro-optimizations. Complex optimizations here add maintenance burden with negligible performance benefit.
- **Warm path** (moderate frequency): Balance both concerns. Use profiling data to guide optimization decisions.

**Avoid Premature Optimization:** While performance is critical, focus on correctness first, then optimize based on profiling data. However, be performance-aware from the start—choosing the right algorithm and data structure upfront is not premature optimization. Use the hot path analysis above to decide how much optimization effort is warranted.

### API Design and Compatibility

**Backwards Compatibility:** RocksDB maintains strong backwards compatibility guarantees. Breaking changes are rare and require extensive justification. When deprecating features, follow the project's deprecation policy (typically spanning multiple releases).

**API Consistency:** New APIs should be consistent with existing patterns. Use similar naming conventions, parameter ordering, and return types. Reviewers will suggest changes to improve consistency with the broader codebase.

**Documentation:** Public APIs must be thoroughly documented. Include usage examples, parameter descriptions, and notes on thread safety, performance characteristics, and compatibility considerations.

---

## Component-Specific Guidance

### Database Core (`db`)

The database core handles write-ahead logging (WAL), memtables, compaction, and recovery. This component receives the most scrutiny in code reviews.

**Concurrency and Thread Safety:** Database operations are highly concurrent. Reviewers carefully examine locking strategies, atomic operations, and memory ordering. Document synchronization assumptions clearly. Use appropriate memory ordering semantics (`acquire`/`release` vs. `seq_cst`).

**Compaction Logic:** Changes to compaction are complex and high-risk. Ensure that compaction logic respects configured parameters, handles edge cases (empty databases, single-file compactions), and maintains correctness under concurrent operations.

**Error Propagation:** Database operations can fail in many ways (I/O errors, corruption, resource exhaustion). Ensure that errors are properly propagated, logged, and handled. Avoid assertions in production code paths.

**Testing:** Database core changes require extensive testing, including unit tests, integration tests, and stress tests. Test with various configurations, compaction styles, and concurrent workloads.

### Public Headers (`include`)

Public headers define RocksDB's API surface. Changes here have the highest compatibility impact.

**API Design:** New APIs should be intuitive, consistent with existing patterns, and well-documented. Consider how the API will be used in practice and avoid adding unnecessary complexity.

**Backwards Compatibility:** Breaking changes to public APIs require extensive justification and a deprecation plan. Maintain ABI compatibility for bug fixes and patch releases.

**Documentation:** Every public API must be thoroughly documented with usage examples, parameter descriptions, and notes on thread safety and performance characteristics.

**Deprecation:** When deprecating APIs, follow the project's policy. Mark deprecated APIs clearly, provide migration guidance, and maintain support for at least one major release.

### Internal Utilities (`util`)

Internal utilities provide common functionality used throughout the codebase.

**Code Reuse:** Utilities should be general-purpose and reusable. Avoid duplicating functionality that already exists elsewhere in the codebase.

**Error Handling:** Utility functions should handle errors robustly and propagate them appropriately. Consider edge cases like overflow, underflow, and invalid inputs.

**Testing:** Utility functions should have comprehensive test coverage, including edge cases and failure modes. Consider adding death tests for assertions.

**Performance:** Utilities are often used in hot paths. Ensure that implementations are efficient and avoid unnecessary allocations or copies.

### Table Management (`table`)

Table management handles SST file format, block-based tables, and table readers/writers.

**Block Format and Checksums:** Changes to block format require extreme care. Ensure that checksums are computed and verified correctly. Test with various compression algorithms and block sizes.

**Iterator Correctness:** Table iterators are used throughout the codebase. Ensure that iterator semantics (Seek, Next, Prev) are correct, especially at boundaries and with deletions.

**Caching and Prefetching:** Table readers interact with the block cache and prefetching logic. Ensure that cache keys are unique and that prefetching respects configured limits.

**Performance:** Table operations are performance-critical. Benchmark changes that could impact read or write performance.

### Utilities (`utilities`)

Utilities include optional features like transactions, backup engine, and checkpoint.

**Feature Isolation:** Utilities should be self-contained and not introduce unnecessary dependencies on core database internals.

**Deprecation and Cleanup:** Legacy features are being phased out. When removing deprecated code, ensure that migration paths are documented and that users have sufficient warning.

**Cross-Platform Compatibility:** Utilities often interact with OS-specific APIs. Ensure that code works on all supported platforms.

### Options and Configuration (`options`)

Options define RocksDB's configuration system.

**Type Safety:** Use appropriate types for options (e.g., `uint32_t` for flags, scoped enums for enumerated values).

**Deprecation Policy:** When deprecating options, follow the project's policy. Document the deprecation, provide migration guidance, and maintain support for at least one major release.

**Dynamic Configuration:** Some options can be changed dynamically. Ensure that dynamic changes are thread-safe and take effect correctly.

**Validation:** Validate option values and provide clear error messages for invalid configurations.

### Cache (`cache`)

Cache management is critical for RocksDB's performance.

**Concurrency:** Cache operations are highly concurrent. Ensure that implementations are thread-safe and use appropriate synchronization primitives.

**Performance:** Cache operations are in the hot path. Optimize for low latency and high throughput. Benchmark changes carefully.

**Memory Management:** Cache implementations must manage memory carefully to avoid leaks and excessive allocations.

**Eviction Policies:** Changes to eviction policies should be well-tested and benchmarked to ensure they improve overall performance.

---

## Code Review Checklist

When reviewing RocksDB code (or preparing code for review), use this checklist:

### Correctness
- [ ] Does the change preserve database semantics (e.g., snapshot isolation, key ordering)?
- [ ] Are all error cases handled appropriately?
- [ ] Is the change thread-safe? Are synchronization primitives used correctly?
- [ ] Are there any potential data races or deadlocks?

### Testing
- [ ] Does the change include appropriate test coverage?
- [ ] Are edge cases and failure modes tested?
- [ ] Have the tests been run on all supported platforms?
- [ ] Are stress tests passing?

### Performance
- [ ] Are there benchmark results for performance-sensitive changes?
- [ ] Does the change avoid unnecessary allocations or copies?
- [ ] Are hot paths optimized appropriately?

### API and Compatibility
- [ ] Is the change backwards compatible?
- [ ] Are new APIs consistent with existing patterns?
- [ ] Is the public API documented?
- [ ] Are deprecated features handled according to policy?

### Code Quality
- [ ] Does the code follow RocksDB's style conventions?
- [ ] Is the code clear and maintainable?
- [ ] Are comments and documentation sufficient?
- [ ] Are there any code smells or anti-patterns?

---

## Common Review Feedback Patterns

The following patterns emerged as frequent sources of review feedback:

1. **Test Coverage:** Reviewers frequently request additional tests for edge cases, platform-specific behavior, and failure modes. Complex changes require comprehensive test coverage including unit tests, integration tests, and stress tests.

2. **Error Handling:** Ensure proper error propagation using RocksDB's `Status` type. Avoid silent failures and provide clear error messages that include context about what failed and why.

3. **API Design:** New APIs should be consistent with existing patterns. Use descriptive names that follow established conventions. Avoid breaking changes without strong justification and a clear deprecation plan.

4. **Documentation:** Public APIs must be documented with usage examples and notes on thread safety, performance characteristics, and compatibility considerations. Complex internal logic should also be well-commented.

5. **Performance:** Performance-sensitive changes require benchmark results to validate improvements. Use `db_bench` and other profiling tools to measure impact. Avoid premature optimization that adds complexity without measurable benefit.

6. **Concurrency:** Thread safety is critical in RocksDB. Document synchronization assumptions clearly. Use appropriate memory ordering semantics. Consider potential race conditions and deadlocks.

7. **Code Style:** Follow existing conventions for naming, formatting, and structure. Use `.clang-format` for consistent formatting. Prefer scoped enums (`enum class`) over unscoped enums.

8. **Backwards Compatibility:** RocksDB maintains strong compatibility guarantees. Breaking changes require extensive justification. When deprecating features, provide migration guidance and maintain support across multiple releases.

9. **Refactoring:** Reviewers appreciate refactoring that improves code readability and maintainability. Look for opportunities to deduplicate code and simplify complex logic.

10. **Platform Compatibility:** Ensure changes work correctly on all supported platforms (Linux, Windows, macOS) and with all supported compilers (GCC, Clang, MSVC).

---

## Important tips

### Build system
* There are 3 build system. Make, CMake, BUCK(meta internal).
* When a new .cc file is added, update Makefile, CMakeLists.txt, src.mk, BUCK.
* Don't manually edit BUCK file, after updating src.mk, run
    /usr/local/bin/python3 buckifier/buckify_rocksdb.py to update it
* Use make to build and run the test. CMake and BUCK are not used locally.
* Use `make dbg` command to build all of the unit test in debug mode.
* For -j in make command, use the number of CPU cores to decide it.

### Unit Test
* After all of the unit tests are added, review them and try to extract common
    reusable utility functions to reduce code duplication due to copy past between
    unit tests. This should be done every time unit test is updated.
* Don't use sleep to wait for certain events to happen. This will cause test to
    be flaky. Instead, use sync point to synchronize thread progress.
* Cap unit test execution with 60 seconds timeout.
* When there are multiple unit tests need to be executed, try to use
    gtest_parallel.py if available. E.g.
    python3 ${GTEST_PARALLEL}/gtest_parallel.py ./table_test

### Unit test dedup guidelines
* Extract helper functions for repeated patterns such as object
    construction, round-trip (encode → decode → verify), and common
    assertion sequences.
* Use table-driven tests (struct array + loop) when multiple test cases
    share the same logic but differ only in input/expected data.
* Prefer randomized tests over exhaustive parameter permutations. Use
    `Random` from `util/random.h` (not `std::mt19937`). Use a time-based
    seed with `SCOPED_TRACE("seed=" + std::to_string(seed))` so failures
    are reproducible.
* Keep deterministic edge-case tests separate from randomized tests
    (error paths, boundary conditions, format verification).
* Methods only used in tests should be private with `friend class` +
    `TEST_F` fixture wrappers. In wrappers, always fully qualify the
    target method to avoid infinite recursion.

### Adding new public API
    Refer to claude_md/add_public_api.md

### Adding new option
    Refer to claude_md/add_option.md

### Removing deprecated option
    Refer to claude_md/remove_option.md

### Metrics
* When adding a new feature, evaluate whether there is opportunity to add
    metrics. Try to avoid causing performance regression on hot path when adding
    metrics.

### Stress test
* When adding a new feature, make sure stress test covers the new option.

### DB bench update
* When adding a performance related feature, support it in db_bench

### Adding release note
* Release note should be kept short at high level for external user consumption.

### Blog posts (docs/_posts)
* Blog post authors must be defined in `docs/_data/authors.yml` to be displayed

### Final verification of the change
* Execute make clean to clean all of the changes.
* Execute make check to build all of the changes and execute all of the tests.
    Note that executing all of the tests could take multiple minutes.

### Monitoring make check progress
* Use `make check-progress` to get machine-parseable JSON progress while
    `make check` is running. This is useful for Claude Code to monitor long
    builds without timeout issues.
* Run `make check` in background, then poll progress:
    ```bash
    make check &
    # Poll periodically:
    make check-progress
    ```
* The output shows current phase and progress:
    ```json
    {"status":"running","phase":"compiling","completed":300,"total":919,...}
    {"status":"running","phase":"testing","completed":1500,"total":29962,"failed":0,"percent":5,...}
    {"status":"completed","phase":"testing","completed":29962,"total":29962,"failed":0,"percent":100,...}
    ```
* Phases: `compiling` -> `linking` -> `generating` -> `testing` -> `completed`
* Key fields: `status`, `phase`, `completed`, `total`, `failed`, `percent`
* When tests fail, `failed_tests` array shows details (up to 10 failures):
    ```json
    {"status":"running",...,"failed":3,"failed_tests":[
      {"test":"cache_test-CacheTest.Usage","exit_code":1,"signal":0,"output":"...test log..."},
      {"test":"env_test-EnvTest.Open","exit_code":0,"signal":11,"output":"...Segmentation fault..."}
    ]}
    ```
* `exit_code`: non-zero means test assertion failed
* `signal`: non-zero means test was killed (e.g., 9=SIGKILL, 6=SIGABRT, 11=SIGSEGV)
* `output`: last 50 lines of test log including error messages and stack traces

### Executing benchmark using db_bench
* Since the goal is to measure performance, we need to build a release binary
    using `make clean && DEBUG_LEVEL=0 make db_bench`. If there is an engine
    crash due to bug, we need to switch back to debug build. Make sure to run
    `make clean` before running `make dbg`.

### Formatting code
* After making change, use `make format-auto` to auto-apply formatting without
    interactive prompts (Claude Code friendly).
