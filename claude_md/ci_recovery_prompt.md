Read review-findings.md. This file contains partial code review findings
from a session that ran out of turns before finishing. Reformat them into
a clean final review comment.

## Required Output Structure

Use this exact structure so the PR page stays scrollable:

```markdown
## Summary

> **Partial review** — the analysis was interrupted before all perspectives
> were completed. Findings below cover only the perspectives that finished.
> You can request a fresh full review with `/claude-review`.

<!-- One or two sentences of overall assessment. -->

**High-severity findings (N):**
- **[file.cc:123]** One-line description. <!-- repeat per HIGH finding -->

<!-- If no HIGH findings were salvaged, write: -->
<!-- _No high-severity findings recovered._ -->

<details>
<summary>Recovered findings (click to expand)</summary>

### Findings

#### :red_circle: HIGH
... (H1, H2, ...)

#### :yellow_circle: MEDIUM
... (M1, M2, ...)

#### :green_circle: LOW / NIT
... (L1, L2, ...)

### Cross-Component Analysis
<!-- Whatever cross-component results were captured. -->

### Positive Observations
<!-- Optional. -->

</details>
```

## Rules
- The `## Summary`, the partial-review notice, and the HIGH bullet list MUST
  stay outside the `<details>` block.
- All per-finding detail and analysis MUST live inside the `<details>` block.
- Do NOT nest `<details>` blocks.
- Do NOT add any text after the closing `</details>` — the comment-builder
  appends its own footer.
- Output ONLY the formatted review. No commentary, no explanation.
