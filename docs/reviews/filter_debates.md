# Debates: filter

No direct disagreements between CC and Codex reviewers were found. Both reviewers agreed on all factual issues they raised. Codex found significantly more correctness issues (10 vs 4 from CC), but none of their claims contradicted each other.

## Near-Debate: LegacyBloom FP Rate Threshold

- **CC position**: "32-bit hash only -- significant FP rate degradation above ~3 million keys" is misleading because the ~40 million inflection point is the actual degradation threshold, while 3 million is the warning threshold.
- **Codex position**: Did not raise this specific threshold issue, but raised the related `delta == 0` explanation issue.
- **Code evidence**: `util/bloom_impl.h:113-115` says inflection point is ~40 million keys at 10 bits/key. `filter_policy.cc` fires a warning at >= 3 million keys when estimated FP exceeds 1.5x expected. These are two different thresholds.
- **Resolution**: Both are correct in their respective sub-issues. CC correctly identified the 40M vs 3M distinction. Codex correctly identified the `delta == 0` imprecision (it's about zero increment within cache-line address space, not literal zero).
- **Risk level**: medium -- conflating these thresholds could lead to unnecessary alarm at 3M keys or false confidence below 40M keys.

## Near-Debate: Corruption Handling

- **CC position**: Did not specifically address the corruption-to-always-true issue.
- **Codex position**: The doc claims corruption degrades to always-true, but actually the build fails.
- **Code evidence**: `block_based_table_builder.cc:WriteFilterBlock()` breaks on `Status::Corruption` and sets the builder status to failed. `WriteMaybeCompressedBlock()` also propagates post-verification failures. The builder does not silently install always-true filters.
- **Resolution**: Codex is correct. The doc's claim about graceful degradation to always-true was wrong -- corruption stops the build.
- **Risk level**: high -- this could mislead maintainers into thinking corruption is silently tolerated.
