# Add Feature: Research, Plan, Implement Workflow

You are following the structured add-feature workflow. The core principle:
**never write code until a written plan has been reviewed and approved.**

The user wants to add the following feature: $ARGUMENTS

---

## Instructions

Follow these phases **in strict order**. Do NOT skip ahead. Do NOT write
implementation code until explicitly told to.

---

### PHASE 1: Research

1. Identify all relevant folders, files, and modules related to the feature.
2. Read them **in depth** — not just signatures, but actual logic, edge cases,
   data flows, locking protocols, concurrency patterns, and interactions between
   components.
3. Study the existing conventions and abstractions (e.g., `Status` return type,
   option registration patterns, build system conventions).
4. Identify related functionality that already exists (to avoid duplication).
5. Write all findings into a file called `research.md` in the project root.
   The research document must cover:
   - How the relevant subsystem works today, in great detail
   - Key data flows, locking protocols, and concurrency patterns
   - Existing conventions and abstractions
   - Potential pitfalls, invariants, or constraints
   - Related functionality that already exists

**Go deep.** Surface-level reading leads to plans that miss caching layers,
existing helper functions, or concurrency requirements. The most expensive
failure mode is implementations that work in isolation but break the surrounding
system.

When finished, tell the user:
> "Research is complete and written to `research.md`. Please review it to verify
> my understanding before we move to planning. Let me know if anything is wrong
> or missing."

**STOP here and wait for the user to review.**

---

### PHASE 2: Planning

After the user confirms the research is acceptable:

1. Write a detailed implementation plan in a file called `plan.md` in the
   project root. The plan must include:
   - Detailed explanation of the approach
   - Code snippets showing the actual proposed changes
   - File paths that will be modified or created
   - Build system updates required (Makefile, CMakeLists.txt, src.mk, BUCK)
   - Test plan: unit tests, integration tests, stress test coverage
   - Metrics and observability considerations
   - Considerations and trade-offs
   - Alternatives considered and why they were rejected
2. If the user provided reference implementations (from other files in the
   codebase, open source repos, etc.), use them heavily. Concrete references
   produce dramatically better plans than designing from scratch.
3. Read actual source files before suggesting changes — base the plan on the
   real codebase, not assumptions.

When finished, tell the user:
> "The implementation plan is written to `plan.md`. Please review it and add
> any inline annotations — corrections, rejections, constraints, or domain
> knowledge. When you're done annotating, let me know and I will update the
> plan. **I will not implement anything until you say so.**"

**STOP here and wait for the user to review and annotate.**

---

### PHASE 3: Annotation Cycle

When the user says they have added notes/annotations to `plan.md`:

1. Re-read `plan.md` carefully, paying attention to every inline note.
2. Address every single annotation — corrections, rejections, constraints,
   domain knowledge injections.
3. Update the plan document accordingly.
4. **Do NOT implement yet.**

Tell the user:
> "I've updated `plan.md` to address all your annotations. Please review again.
> We can repeat this cycle as many times as needed. **I will not implement until
> you explicitly say to proceed.**"

**STOP and wait.** This cycle typically repeats 1-6 times. Each round refines
the plan with the user's judgement on:
- Product priorities and scope decisions
- Engineering trade-offs they are willing to make
- Domain knowledge the agent does not have
- Hard constraints (e.g., "these function signatures must not change")

---

### PHASE 4: Todo List

When the user is satisfied with the plan (or requests a todo list):

1. Add a granular task breakdown to `plan.md`:
   - Break the plan into numbered phases
   - Break each phase into individual checkable tasks: `- [ ] task description`
   - Include build, test, and formatting tasks
2. **Still do NOT implement** — wait for the user's explicit go-ahead.

Tell the user:
> "I've added a detailed task checklist to `plan.md`. Review the breakdown and
> let me know when you're ready for me to implement. Say **'implement'** to
> begin."

**STOP and wait for explicit implementation approval.**

---

### PHASE 5: Implementation

When the user explicitly says to implement:

1. **Implement everything in the plan** — do not cherry-pick or skip tasks.
2. **Mark tasks as completed** in `plan.md` (change `- [ ]` to `- [x]`) as
   you finish each one.
3. **Do not stop** until all tasks and phases are completed.
4. **Do not add unnecessary comments or documentation** — keep code clean and
   match existing style.
5. **Continuously verify** — run `make` or relevant build commands to catch
   problems early, not at the end.
6. **Follow project conventions** — match existing code style, patterns, and
   abstractions discovered during research.
7. **Run `make format-auto`** after making changes to apply formatting.
8. **Update build files** when adding new .cc files (Makefile, CMakeLists.txt,
   src.mk, then run `python3 buckifier/buckify_rocksdb.py`).

Implementation should be mechanical. All creative decisions were made during
the annotation cycles.

---

### PHASE 6: Feedback & Iterate

During implementation, expect terse corrections from the user. Handle them
immediately:
- If the user points out a missing piece, add it.
- If the user reverts a change, narrow scope and re-implement cleanly rather
  than patching a bad approach.
- If something is going wrong, stop, discuss, and re-plan rather than
  continuing down a bad path.

---

## Key Principles

- **The plan document is shared mutable state.** It persists, is editable, and
  survives context window limits. Always point back to it.
- **Never give total autonomy over what gets built.** The user makes judgement
  calls; you handle mechanical execution.
- **Reference existing code** when building similar features.
- **Run everything in a single long session** — research, planning, and
  implementation build on accumulated context.
- **When in doubt, ask** rather than guess. Wrong assumptions early compound
  into large rework.
