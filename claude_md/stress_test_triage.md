---
description: 'Use this skill to automate end-to-end analysis and fixing of RocksDB crash/stress test failures in the Meta internal monorepo (fbsource). Given a task ID containing a stress test failure report from Sandcastle, this skill guides through downloading artifacts, analyzing the failure, reproducing it, implementing a fix, and sending a Phabricator diff.'
apply_to_user_prompt: 'stress test|crash test|db_stress|crash_test|stress failure|assertion failed|rocksdb crash|sandcastle failure|T\d+.*stress|T\d+.*crash|analyze.*stress|fix.*crash test|reproduce.*stress'
oncalls: [rocksdb_point_of_contact]
---

# RocksDB Stress Test Failure Analysis & Fix (Internal Monorepo)

> **This is the Meta internal monorepo (fbsource) version of the stress test failure analysis
> skill. It uses Sapling (`sl`) for source control, Buck2 for building, and Phabricator/Jellyfish
> for code review. All work happens within `fbcode/internal_repo_rocksdb/repo/` in fbsource.**

## ⚠️ WORKFLOW ENFORCEMENT — READ THIS FIRST

> **🚨 MANDATORY: Before doing ANY work, you MUST create a todo list using the `todo_write` tool
> with ALL of the steps below. Update the todo list as you complete each step.
> Do NOT skip any step. Do NOT stop until all steps are completed or you are explicitly
> blocked. If you finish a step, IMMEDIATELY mark it completed and start the next one.**

### Step Checklist — Create This Todo List NOW

Use `todo_write` to create the following todo items at the very start:

```
1. "Step 1: Load failure task and extract error info, Sandcastle URL, source location"
2. "Step 2: Download crash DB artifact (find action ID, arc skycastle download, extract)"
3. "Steps 3+4: Spin up analysis agent team (6 analysts independently analyze & root-cause, then debate)"
4. "Steps 3+4 convergence: Collect team's consensus root cause and evidence summary"
5. "Step 5: Prepare working directory (create Sapling bookmark for the fix)"
6. "Step 6: Reproduce failure (write unit test FIRST, fall back to stress test if needed)"
7. "Step 7: Implement the fix"
8. "Step 8: Verify fix (re-run reproduction, REVERT fix and confirm test fails, re-apply fix)"
9. "Step 9: Present analysis summary (all 7 sections including 'why did it start failing recently')"
10. "Step 10: Create and submit Phabricator diff (sl commit, arc lint, jf submit --draft)"
11. "Step 11: Walk the user through the root cause and fix"
```

**Rules:**
- Mark each step `in_progress` when you start it
- Mark each step `completed` only when fully done
- After completing a step, CHECK THE TODO LIST and start the next pending step
- If you realize you need to go back to a prior step, mark the current step back to `pending`
- **NEVER stop working while there are still pending steps** — continue to the next step automatically

---

## Overview

This skill automates the full lifecycle of diagnosing and fixing RocksDB stress test (db_stress / crash test) failures reported via Sandcastle CI tasks, working entirely within the Meta internal monorepo (fbsource).

> **🚨 CRITICAL: RocksDB is an extremely complex codebase.** Stress test failures often involve
> subtle interactions between concurrent operations, fault injection, compaction, iterators,
> caching, and recovery logic. **Do NOT jump to conclusions or rush to implement a fix.**
> You MUST execute ALL analysis steps thoroughly — dive deep into the source code, carefully
> analyze every error message and stack trace, download and examine the crash DB, study the
> stress test parameters, and perform temporal analysis of recent changes — before even
> thinking about reproduction or implementing a fix. Superficial analysis will lead to
> incorrect root causes and wrong fixes.

> **🚨 CRITICAL RULES:**
> 1. You MUST use `todo_write` to track all 11 steps throughout the workflow
> 2. You MUST complete ALL analysis steps (Steps 1-4) thoroughly before moving to reproduction (Step 5) — do NOT skip any analysis sub-step
> 3. You MUST reproduce the failure locally BEFORE implementing any fix
> 4. You MUST present the analysis summary BEFORE sending a diff
> 5. You MUST NOT stop halfway — if a step is done, immediately start the next one

### Key Differences from Open Source Workflow

| Aspect | Open Source | Internal Monorepo (this skill) |
|--------|-------------|-------------------------------|
| Repository | `~/workspace/ws1/rocksdb` (git) | `fbsource/fbcode/internal_repo_rocksdb/repo/` |
| Source control | `git` | `sl` (Sapling) |
| Build system | `make` | `buck2` with `@//mode/dbg` |
| Branching | git worktree | Sapling bookmark |
| Code review | GitHub PR | Phabricator diff via `jf submit` |
| Formatting | `make format-auto` | `arc lint -a` |
| File paths | `db_stress_tool/file.cc` | `fbcode/internal_repo_rocksdb/repo/db_stress_tool/file.cc` |
| Commit message | No internal references | Task IDs and Sandcastle URLs are OK |

---

## Step 1: Load the Failure Task

**Input:** A task ID (e.g., `T257516637`)

Use `knowledge_load` to fetch the task details:

```
knowledge_load URL: https://www.internalfb.com/T{task_id}
```

From the task, extract:
- **Error log**: The assertion or crash message (e.g., `Assertion 'iters[i]->status().ok()' failed`)
- **Source location**: File and line number of the crash (e.g., `batched_ops_stress.cc:679`)
- **Sandcastle URL**: The job URL (e.g., `https://www.internalfb.com/intern/sandcastle/job/{job_id}/`)
- **Test name**: Which crash test variant failed (e.g., `fbcode_blackbox_crash_test`, `clang_ubsan_coerce_context_switch_crash_test`)

> **⚠️ IMPORTANT: A task may contain MULTIPLE stress test failure reports (in the description
> and in comments). Collect ALL Sandcastle job URLs — not just the latest one. Comparing
> db_stress parameters, LOG files, and DB states across multiple runs is critical for
> identifying which parameters or conditions consistently trigger the failure vs. which
> are incidental. Older runs may have expired artifacts (Sandcastle has a 30-day retention
> limit), but collect what is available.**
>
> To collect all runs:
> 1. Read through all task comments AND the description
> 2. Extract EVERY Sandcastle job URL posted (note the date of each)
> 3. Record them in a list for use in Steps 2 and 3 (e.g., `run_1: {url, date}`, `run_2: {url, date}`, ...)
> 4. Note which run is the most recent (its artifacts are most likely to still be available)

> **✅ CHECKPOINT: Mark Step 1 as `completed` in your todo list. Start Step 2 NOW.**

---

## Step 2: Download Crash DB Artifacts from All Runs

> **🚨 MULTI-RUN COLLECTION: If Step 1 identified multiple Sandcastle job URLs, spawn
> parallel agents to download and analyze artifacts from ALL available runs simultaneously.
> This dramatically speeds up data collection and enables cross-run comparison in Step 3.**

### 2a: Parallel Multi-Agent Data Collection (When Multiple Runs Exist)

If you collected N Sandcastle job URLs in Step 1, spawn N parallel agents (one per run) using the Agent tool. Each agent handles the full download-and-analyze pipeline for its assigned run.

**Agent task template** (customize `{run_index}`, `{job_id}`, and `{job_url}` for each):

```
You are collecting crash data from Sandcastle run #{run_index}.

1. Load the Sandcastle workflow to find the artifact action ID:
   knowledge_load URL: {job_url}
   Parse the `steps` JSON array, find the step with "description": "Artifact rocksdb_db.tar.gz",
   and extract its `step_id` (the action ID).

2. Download and extract the artifact to a run-specific directory:
   bash fbcode/internal_repo_rocksdb/.llms/scripts/download_crash_artifact.sh {action_id} /tmp/rocksdb_crash_db_run{run_index}
   If the script does not support a custom output path, download to a unique location manually:
     mkdir -p /tmp/rocksdb_crash_db_run{run_index}
     arc skycastle workflow-run get-action-artifact --action-id={action_id} --output-path=/tmp/rocksdb_crash_db_run{run_index}
     tar -xzf /tmp/rocksdb_crash_db_run{run_index}/*.tar.gz -C /tmp/rocksdb_crash_db_run{run_index}

3. Run the analysis script on this run's DB:
   bash fbcode/internal_repo_rocksdb/.llms/scripts/analyze_crash_db.sh /tmp/rocksdb_crash_db_run{run_index}
   If the script does not support a custom path, run the individual analysis commands manually
   against the extracted DB at /tmp/rocksdb_crash_db_run{run_index}/dev/shm/rocksdb_test/rocksdb_crashtest_blackbox/

4. From the Sandcastle workflow data, find the "Test" step and load its stdout artifact to extract
   the full db_stress command line and error output.

5. Report back with a structured summary:
   - Run index: {run_index}
   - Sandcastle job URL: {job_url}
   - Artifact status: (downloaded / expired / failed)
   - Code revision (git sha)
   - Full db_stress command line (all --flag=value parameters)
   - Error/assertion message and stack trace
   - LOG file highlights (errors, warnings, last activity before crash)
   - OPTIONS file highlights (key configuration values)
   - DB file summary (SST count, sizes, MANIFEST info)
```

Launch all agents in a single message to maximize parallelism:

```python
# Example: 3 runs collected in Step 1
Agent(prompt="Collect crash data from run #1: {job_url_1}", subagent_type="general-purpose", run_in_background=True)
Agent(prompt="Collect crash data from run #2: {job_url_2}", subagent_type="general-purpose", run_in_background=True)
Agent(prompt="Collect crash data from run #3: {job_url_3}", subagent_type="general-purpose", run_in_background=True)
```

Wait for all agents to complete, then proceed to Step 3 with the combined data.

> **NOTE:** If only ONE Sandcastle job URL was found, skip the multi-agent approach and
> use the single-run workflow below (2b-2d).

### 2b: Find the Artifact Action ID (Single Run)

Load the Sandcastle workflow to find the "Artifact rocksdb_db.tar.gz" action:

```
knowledge_load URL: https://www.internalfb.com/intern/sandcastle/job/{job_id}/
```

Parse the `steps` JSON array and find the step with `"description": "Artifact rocksdb_db.tar.gz"`. Extract its `step_id` — this is the **action ID**.

### 2c: Download and Extract the Artifact

```bash
bash fbcode/internal_repo_rocksdb/.llms/scripts/download_crash_artifact.sh {action_id}
```

This downloads the artifact via `arc skycastle` and extracts it to `/tmp/rocksdb_crash_db`.
The extracted DB is typically at:
```
/tmp/rocksdb_crash_db/dev/shm/rocksdb_test/rocksdb_crashtest_blackbox/
```

### Alternative: Download via Manifold Digest

If you have the digest from the Outputs tab of the Sandcastle UI (format: `{hash}_{size}_murmur3_128`):

```bash
manifold get skycastle_cas/flat/{digest} /tmp/rocksdb_db.tar.gz
```

Or using `arc skycastle cas get`:

```bash
arc skycastle cas get {digest} --output-path=/tmp/rocksdb_db_download
# Rename the downloaded artifact
mv /tmp/rocksdb_db_download/{digest_filename} /tmp/rocksdb_db.tar.gz
tar -xzf /tmp/rocksdb_db.tar.gz -C /tmp/rocksdb_crash_db
```

### 2d: Identify the Exact Code Revision

To find which commit the test ran against, run the analysis script (also covers Steps 3b and 3c):

```bash
bash fbcode/internal_repo_rocksdb/.llms/scripts/analyze_crash_db.sh
```

This prints: git sha, LOG errors/warnings, startup config, pre-crash activity, OPTIONS, and DB file summary.

Alternatively, to find the revision manually:
- Click stdout log of the first step ("Checkout revision" or "Clean") — search for the hg revision
- To see the last relevant RocksDB commit at that revision:

```bash
sl log -l 1 fbcode/internal_repo_rocksdb/repo
```

### 2e: Download Core Dump (If Available)

Core dumps provide stack traces and variable state at the time of crash, which is invaluable for debugging engine-level bugs.

#### Step 1: Find the Tupperware Task

From the Sandcastle job page, locate the **Tupperware access command** in the task description. It looks like:
```
tw ssh tsp_global/sandcastle/worker.fbcode.i7.xlarge.ecp24.prod/4304
```

The Tupperware task path is `tsp_global/sandcastle/worker.fbcode.i7.xlarge.ecp24.prod/4304`.

#### Step 2: Search for the Core Dump RUN_UUID

```bash
coredumps search {tupperware_task_path}
```

Example output:
```
ujuz6n4kf71d1y8n : db_stress crashed at 2022-07-24 13:11:04 PDT (1658693464) on [Tupperware] tsp_nao/sandcastle/worker.fbcode.i1.large.prod/262 (twshared44185.07.nao1)
```

The `RUN_UUID` is the string before the colon (e.g., `ujuz6n4kf71d1y8n`).

#### Step 3: Download the Core Dump

```bash
coredumps get {RUN_UUID}
```

#### Step 4: Build the Matching Binary

Find the changeset from the Sandcastle Build step's stdout log, then:

```bash
sl checkout {changeset}
buck2 build --verbose 2 @//mode/dbgo fbcode//internal_repo_rocksdb/repo:db_stress
```

#### Step 5: Debug with LLDB

```bash
lldb ~/fbsource/fbcode/buck-out/gen/internal_repo_rocksdb/repo/db_stress -c {RUN_UUID}
```

GDB also works if preferred — just override the binary location.

**Core dump reference:** https://www.internalfb.com/intern/wiki/Coredumper/User_Guide/

> **✅ CHECKPOINT: Mark Step 2 as `completed` in your todo list. Start Step 3 NOW.**

---

## Steps 3 + 4: Analysis & Root Cause via Agent Team

> **🚨 TEAM-BASED ANALYSIS: Instead of performing analysis sequentially as a single agent,
> spin up an agent team of 6 independent analysts. Each analyst independently executes the
> full analysis (Step 3) and forms their own root cause hypothesis (Step 4). Then they share
> findings and debate with each other to converge on the most likely root cause.**
>
> This ensemble approach reduces cognitive bias, surfaces alternative hypotheses that a single
> analyst might miss, and produces higher-confidence root cause conclusions. RocksDB bugs are
> often subtle — multiple independent perspectives dramatically improve accuracy.

### Why a Team?

- **Independent analysis prevents anchoring bias**: Each analyst forms their own hypothesis
  without being influenced by others' early conclusions
- **Diverse investigation paths**: Different analysts may explore different code paths,
  read different files, or prioritize different evidence
- **Debate surfaces weak hypotheses**: When analysts challenge each other's reasoning,
  incorrect hypotheses get eliminated early
- **Higher confidence**: When multiple independent analysts converge on the same root cause,
  confidence is much higher than a single analyst's conclusion

### 3+4a: Create the Analysis Team

Create a team named `stress_analysis_{task_id}` with 6 analyst agents:

```python
TeamCreate(team_name="stress_analysis_{task_id}", description="RocksDB stress test failure analysis for T{task_id}")
```

### 3+4b: Prepare the Shared Context Briefing

Before spawning the analysts, the lead agent must prepare a **context briefing** — a concise
summary of everything collected in Steps 1 and 2 that each analyst needs to begin their work.

The briefing must include:
- **Task ID and error info** from Step 1 (assertion message, source location, test name)
- **All Sandcastle job URLs** collected in Step 1
- **Crash DB location(s)** from Step 2 (e.g., `/tmp/rocksdb_crash_db/` or per-run paths)
- **Analysis script output** from Step 2d (git sha, LOG highlights, OPTIONS, DB summary)
- **Cross-run data** from Step 2a if multiple runs were collected (each run's parameters and error output)

### 3+4c: Spawn 6 Analyst Agents

Spawn all 6 analysts in a single message for maximum parallelism. Each analyst gets the same
context briefing but works independently. Use the Agent tool with `team_name` set to the team.

**Agent prompt template for each analyst:**

```
You are Analyst #{N} on a team of 6 independent analysts investigating a RocksDB stress
test failure. Your job is to:

1. INDEPENDENTLY perform the FULL analysis (all sub-steps below)
2. Form your OWN root cause hypothesis
3. When done, share your findings with all other analysts via broadcast
4. Read and critique other analysts' hypotheses
5. Debate to converge on the most likely root cause

## Context Briefing
{paste the context briefing from 3+4b here}

## Your Analysis Tasks

Perform ALL of the following sub-steps. You have access to the crash DB files,
source code, knowledge_search, and get_diff_details.

### A. Examine the Test Stdout Log (3a)
From the Sandcastle workflow data, load the "Test" step stdout artifact:
  knowledge_load URL: https://www.internalfb.com/intern/sandcastle/artifact/?manifold_handle=flat%2F{digest}&manifold_bucket=skycastle_cas

Extract the full db_stress command line and all --flag=value parameters.
Pay special attention to:
- Fault injection flags (--read_fault_one_in, --write_fault_one_in, --metadata_read_fault_one_in)
- Test mode flags (--test_batches_snapshots, --prefix_size, --use_txn)
- Configuration flags (--column_families, --compaction_style, --atomic_flush, --disable_wal)

### B. Cross-Run Comparison (3a-cross) — if multiple runs exist
Compare db_stress parameters across all available runs. Identify:
- Parameters ALWAYS present in failing runs (necessary conditions)
- Parameters that VARY (not the trigger)
- Code revision differences across runs

### C. Examine the RocksDB LOG File (3b)
Analyze the LOG file at the crash DB path:
- Look for errors, corruption, warnings
- Check startup configuration (first 100 lines)
- Check last activity before crash (last 100 lines)

### D. Examine the OPTIONS File (3c)
Review the OPTIONS file for relevant configuration (prefix_extractor, compaction style, etc.)

### E. Examine the Source Code (3d)
Read the source file at the crash location (±50 lines of context).
Also read related source files to understand the code path:
- The stress test file where the crash occurred
- The equivalent code in other stress test variants (e.g., how no_batched_ops_stress.cc
  handles the same operation — look for error handling patterns)
- The base class (db_stress_test_base.h) for utility functions like IsErrorInjectedAndRetryable()
- The underlying RocksDB engine code if the crash is in engine internals

Key source files (all under fbcode/internal_repo_rocksdb/repo/):
- db_stress_tool/batched_ops_stress.cc — BatchedOpsStressTest
- db_stress_tool/no_batched_ops_stress.cc — NonBatchedOpsStressTest (reference for patterns)
- db_stress_tool/cf_consistency_stress.cc — CfConsistencyStressTest
- db_stress_tool/db_stress_test_base.cc — Base class with OperateDb loop
- db_stress_tool/db_stress_test_base.h — IsErrorInjectedAndRetryable() definition
- db_stress_tool/db_stress_common.h — Common flags and utilities

### F. Check Common Root Cause Patterns (3e)
Compare the failure against these known patterns:
| Pattern | Typical Cause | Fix Approach |
|---------|--------------|--------------|
| assert(iter->status().ok()) with read_fault_one_in > 0 | Injected I/O error not handled | Use IsErrorInjectedAndRetryable() |
| assert(iter->Valid()) with fault injection | Iterator invalidated by injected fault | Check status().ok() before Valid() |
| Data mismatch between CFs | Race condition in multi-CF operations | Check locking, snapshot usage |
| Corruption after reopen | Recovery logic bug | Check WAL replay, MANIFEST handling |
| Assertion in compaction | Compaction logic error | Check key ordering, boundary handling |
| PrepareValue failure | Lazy value loading error | Check blob file handling |

### G. Temporal Analysis (3f)
Determine WHEN the failure first started appearing:
1. Check task creation time and comments for earlier occurrences
2. Search for related tasks with the same failure signature
3. Search for recent diffs that modified relevant source files:
   - Use knowledge_search with diff_modified_file_directories: ["fbsource/fbcode/internal_repo_rocksdb/repo"]
   - Use knowledge_search with diff_modified_file_paths for the specific failing file
4. For each suspect diff, use get_diff_details to read the change
5. Correlate: new failure → recent change; intermittent → pre-existing bug; after specific diff → prime suspect

### H. Form Your Root Cause Hypothesis (Step 4)
Based on ALL evidence above, determine:
1. Is this a test harness bug or a RocksDB engine bug?
2. Was it introduced by a recent change? Which diff?
3. What is the triggering condition? (specific flag combinations, timing, state)
4. What is the minimal set of parameters to reproduce?
5. What fix approach do you recommend?

## Sharing and Debate Phase

After completing your analysis:

1. **BROADCAST your findings** to all teammates using SendMessage with type "broadcast".
   Structure your message as:

   ANALYST #{N} FINDINGS:
   === Evidence Summary ===
   (key evidence from each sub-step, with specific file/line references)

   === Root Cause Hypothesis ===
   Category: (test harness bug / engine bug / race condition / config issue)
   Root cause: (clear explanation)
   Triggering conditions: (specific flags/state/timing)
   Suspect diff: (if any, with D-number)
   Why it started failing recently: (explanation)
   Confidence: (high/medium/low)
   Recommended fix: (brief description)

   === Key Evidence Supporting This Hypothesis ===
   (numbered list of the strongest evidence points)

   === Alternative Hypotheses Considered and Rejected ===
   (what else you considered and why you ruled it out)

2. **READ other analysts' findings** as they come in.

3. **CRITIQUE and DEBATE**: For each other analyst's hypothesis, send a direct message:
   - Do you agree or disagree? Why?
   - What evidence supports or contradicts their hypothesis?
   - Are there gaps in their analysis?
   - Does their hypothesis explain ALL the evidence, or only part of it?

4. **CONVERGE**: After reviewing all hypotheses and debate:
   - If you change your mind, broadcast your updated hypothesis with reasoning
   - If you still disagree with the majority, explain what evidence would change your mind
   - Vote on the final root cause by broadcasting: "VOTE: I support Analyst #X's hypothesis because..."

5. **FINAL SUMMARY**: Once debate settles, one analyst (the one whose hypothesis won)
   should broadcast a FINAL CONSENSUS message:

   CONSENSUS ROOT CAUSE:
   Category: ...
   Root cause: ...
   Triggering conditions: ...
   Suspect diff: ...
   Why it started failing recently: ...
   Recommended fix approach: ...
   Supporting analysts: #X, #Y, #Z
   Dissenting analysts (if any): #W (reason: ...)
```

Spawn all 6 analysts in parallel:

```python
# All 6 launched in a single message for maximum parallelism
for i in range(1, 7):
    Agent(
        prompt=f"Analyst #{i} prompt (with context briefing filled in)",
        subagent_type="general-purpose",
        name=f"analyst-{i}",
        team_name="stress_analysis_{task_id}",
        run_in_background=True
    )
```

### 3+4d: Lead Agent Monitors and Collects Consensus

The lead agent waits for all 6 analysts to complete their analysis and debate phases.
Messages from teammates are delivered automatically.

**The lead agent should:**

1. **Monitor the debate** as analyst messages come in — do NOT intervene unless the debate
   is going off-track or analysts are stuck
2. **Watch for convergence** — when most analysts agree on a root cause, note the consensus
3. **Break ties** if needed — if analysts are split 3-3, the lead agent can ask targeted
   questions to the dissenting group to resolve the disagreement
4. **Collect the final consensus** into a structured summary for use in later steps

### 3+4e: Extract the Consensus Root Cause

After the team converges, the lead agent extracts and records:

- **Consensus root cause**: The agreed-upon explanation
- **Confidence level**: Based on how many analysts agreed and how strong the evidence is
- **Key evidence**: The top evidence points that support the conclusion
- **Triggering conditions**: Specific flags/state/timing that cause the failure
- **Suspect diff**: If identified (with D-number)
- **Recommended fix approach**: The team's recommended fix strategy
- **Dissenting views**: Any minority opinions and their reasoning (these may be worth
  revisiting if the primary hypothesis doesn't pan out during reproduction)

### 3+4f: Shut Down the Analysis Team

After collecting the consensus, shut down the analysis team:

```python
# Send shutdown request to each analyst
for i in range(1, 7):
    SendMessage(type="shutdown_request", recipient=f"analyst-{i}", content="Analysis complete, shutting down")

# After all analysts confirm shutdown
TeamDelete()
```

> **✅ CHECKPOINT: Mark Steps 3+4 as `completed` in your todo list. Start Step 5 NOW.**
>
> **⚠️ BEFORE PROCEEDING: Verify the team has covered ALL of the following:**
> - [ ] All 6 analysts independently analyzed the test stdout and extracted db_stress parameters
> - [ ] If multiple runs: cross-run comparative analysis was performed
> - [ ] All analysts examined the RocksDB LOG file for errors/warnings
> - [ ] All analysts reviewed the OPTIONS file for relevant configuration
> - [ ] All analysts read the source code at the crash location and surrounding context
> - [ ] All analysts checked common root cause patterns
> - [ ] All analysts performed temporal analysis
> - [ ] All analysts formed independent root cause hypotheses
> - [ ] Analysts shared findings and debated with each other
> - [ ] The team converged on a consensus root cause
> - [ ] The lead agent recorded the consensus with supporting evidence
>
> **If the team did NOT converge, consider:**
> - Asking the dissenting analysts targeted questions
> - Looking for additional evidence that would resolve the disagreement
> - Proceeding with the majority hypothesis but keeping the minority view as a backup

---

## Step 5: Prepare Working Directory

> **🚨 Create a Sapling bookmark for your fix before making any code changes.
> This isolates your work and makes it easy to create a Phabricator diff later.**

### 5a: Create a Sapling Bookmark

```bash
cd ~/fbsource

# Create a bookmark for the fix using the task ID
sl bookmark fix_stress_test_T{task_id}
```

Example (for task T257516637):
```bash
sl bookmark fix_stress_test_T257516637
```

### 5b: Verify Clean State

```bash
sl status
```

If there are uncommitted changes, either commit or shelve them first:
```bash
sl shelve
```

**From this point forward, ALL file edits happen in `fbcode/internal_repo_rocksdb/repo/` within fbsource.**

> **✅ CHECKPOINT: Mark Step 5 as `completed` in your todo list. Start Step 6 NOW.**

---

## Step 6: Reproduce the Failure Locally (⚠️ MANDATORY)

> **🚨 DO NOT SKIP THIS STEP. DO NOT PROCEED TO STEP 7 (IMPLEMENT FIX) WITHOUT COMPLETING REPRODUCTION.**
>
> A root cause hypothesis is just a guess until you prove it by reproducing the crash.
> You MUST reproduce the issue and observe the same assertion/crash before writing
> any fix code. If you cannot reproduce, revisit your root cause analysis — your
> hypothesis may be wrong.

### Reproduction Strategy: Unit Test First, Stress Test as Fallback

**Always try a targeted unit test first.** Unit tests are:
- **Much faster** — seconds vs. minutes/hours for stress tests
- **More deterministic** — no probabilistic timing dependencies
- **Easier to debug** — isolated, reproducible, can step through in a debugger
- **Reusable** — becomes a permanent regression test after fixing the bug
- **Can use the actual crash DB** — if the error state is persisted in the downloaded DB, a unit test can reopen it and replay the exact failing logic

**Only fall back to stress test reproduction if:**
- The bug requires specific concurrent timing that cannot be set up in a unit test
- The bug requires the full stress test harness infrastructure
- The unit test approach fails to reproduce after several attempts

### 6a: Write a Targeted Unit Test (TRY THIS FIRST)

Based on the root cause analysis from Step 4, write a focused unit test that:
1. Sets up the exact conditions that trigger the bug
2. Exercises the specific code path where the crash occurs
3. Asserts the **expected post-fix behavior**, not just the pre-fix symptom

> **⚠️ CRITICAL: Reproduction ≠ Validation.**
>
> A test that reproduces the symptom does NOT automatically validate the fix. These are the
> same test **only** when the symptom and the fix live on the same code path. When the fix
> introduces a new conditional or handles a different sub-case of the same symptom, the
> reproduction test's setup may never exercise the fix's code path — causing the test to
> pass identically with or without the fix.
>
> **The litmus test**: your unit test **MUST fail without the fix and pass with it**. If it
> produces the same result in both versions, it is not validating the fix — it is confirming
> the symptom through a code path the fix does not affect. You must revise the test setup to
> exercise the fix's specific preconditions.
>
> For example: if a bug produces a misleading `Corruption` error when the real cause is an
> `IOError`, and the fix adds a conditional that propagates the `IOError` when the builder
> has an internal error, then a test that triggers the `Corruption` through a path where the
> builder has no internal error will pass regardless of the fix. The test must set up the
> builder's error state — the fix's precondition — to actually validate the fix.

#### Approach A: Reopen the Downloaded Crash DB

If the error state is **persisted in the DB** (e.g., data corruption, incorrect SST content, bad MANIFEST), you can write a unit test that reopens the downloaded DB and replays the failing logic. This is the fastest path to reproduction.

```cpp
TEST_F(StressTestReproTest, ReproduceIssueFromCrashDB) {
  // Reopen the downloaded crash DB with the same options
  Options options;
  // Set the relevant options from the OPTIONS file or LOG header
  // (extracted in Step 3c)
  options.create_if_missing = false;
  options.prefix_extractor.reset(NewFixedPrefixTransform(8));
  // ... set other options matching the crash ...

  DB* db;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  // Add column family descriptors matching the crash DB
  cf_descs.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, options));

  std::vector<ColumnFamilyHandle*> handles;
  Status s = DB::Open(options, "/tmp/rocksdb_crash_db/dev/shm/rocksdb_test/rocksdb_crashtest_blackbox",
                      cf_descs, &handles, &db);
  ASSERT_OK(s);

  // Now replay the exact failing operation
  ReadOptions ro;
  ro.snapshot = db->GetSnapshot();
  auto iter = std::unique_ptr<Iterator>(db->NewIterator(ro, handles[0]));

  // Exercise the specific code path that crashed
  iter->Seek(prefix_slice);
  while (iter->Valid() && iter->key().starts_with(prefix_slice)) {
    // Verify behavior that was failing
    ASSERT_OK(iter->status());
    iter->Next();
  }
  ASSERT_OK(iter->status());

  db->ReleaseSnapshot(ro.snapshot);
  for (auto* h : handles) delete h;
  delete db;
}
```

#### Approach B: Construct the Triggering State Programmatically

If the bug doesn't depend on the specific DB state (e.g., it's a logic error in the stress test harness, or a bug triggered by specific option combinations), construct the state in the test:

```cpp
TEST_F(StressTestReproTest, ReproducePrefixScanAssertionFailure) {
  Options options;
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(8));
  // Set options that match the failing configuration

  DestroyDB(dbname_, options);
  DB* db;
  ASSERT_OK(DB::Open(options, dbname_, &db));

  // Write data that triggers the bug
  WriteOptions wo;
  for (int i = 0; i < 100; i++) {
    // Write keys matching the stress test's key generation pattern
    ASSERT_OK(db->Put(wo, key, value));
  }
  ASSERT_OK(db->Flush(FlushOptions()));

  // Inject the condition that triggers the bug
  // (e.g., specific read options, fault injection via SyncPoint, etc.)

  ReadOptions ro;
  auto iter = std::unique_ptr<Iterator>(db->NewIterator(ro));
  iter->Seek(prefix);
  // Verify the behavior
  // ...

  delete db;
}
```

#### Approach C: Use SyncPoint for Timing-Dependent Bugs

For bugs that require specific timing (e.g., concurrent compaction + read), use RocksDB's SyncPoint framework:

```cpp
TEST_F(StressTestReproTest, ReproduceRaceCondition) {
  // Set up sync points to force the exact timing
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:Start", [&](void*) {
        // Force specific timing
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // ... set up DB and trigger the race ...

  SyncPoint::GetInstance()->DisableProcessing();
}
```

#### Where to Place the Unit Test

- For stress test harness bugs: `fbcode/internal_repo_rocksdb/repo/db_stress_tool/test/` or alongside the stress test file
- For RocksDB engine bugs: find the existing test file for the component:
  - Iterator bugs → `fbcode/internal_repo_rocksdb/repo/db/db_iterator_test.cc`
  - Compaction bugs → `fbcode/internal_repo_rocksdb/repo/db/compaction/compaction_job_test.cc`
  - Prefix scan bugs → `fbcode/internal_repo_rocksdb/repo/db/prefix_test.cc`
  - Block cache bugs → `fbcode/internal_repo_rocksdb/repo/table/block_based/block_based_table_reader_test.cc`
- Search for existing test patterns: `search_files` for the failing class/function name in `*_test.cc` files

#### Build and Run the Unit Test

```bash
# Build the specific test target
buck2 build @//mode/dbg fbcode//internal_repo_rocksdb/repo:{test_target_name}

# Run the specific test
buck2 test @//mode/dbg fbcode//internal_repo_rocksdb/repo:{test_target_name} -- --gtest_filter="*ReproduceIssue*"
```

To find the correct Buck target for a test file:
```bash
buck2 uquery "owner('fbcode/internal_repo_rocksdb/repo/{test_file}')"
```

If the unit test reproduces the crash → **proceed to Step 7 (implement fix)**. After fixing, the same unit test serves as the regression test in your diff — **but only if the test fails without the fix and passes with it**. If you find in Step 8 that the reproduction test passes regardless of the fix, you MUST revise the test to exercise the fix's specific preconditions before proceeding.

If the unit test cannot reproduce the issue (e.g., requires full stress test infrastructure, concurrent threads, or randomized parameters) → **fall back to stress test reproduction below**.

---

### 6b: Fall Back to Stress Test Reproduction (If Unit Test Insufficient)

If the targeted unit test approach above doesn't reproduce the issue, fall back to running `db_stress` directly.

#### Build db_stress

Build db_stress in debug mode. This is required because fault injection relies on `SyncPoint` which is only available in debug builds.

```bash
buck2 build @//mode/dbg fbcode//internal_repo_rocksdb/repo:db_stress
```

#### Derive Minimal Reproduction Parameters

Extract the failing db_stress command line from Step 3a and create a minimal reproduction.

### Strategy for Minimal Reproduction

1. **Start with the exact failing parameters** from the Sandcastle log
2. **Reduce duration**: Use `--ops_per_thread=100000` (instead of 100000000) and `--duration=60`
3. **Increase fault injection rate**: If `--read_fault_one_in=1000`, try `--read_fault_one_in=10`
4. **Focus on the failing operation**: If prefix scan failed, increase `--prefixpercent=50` and decrease other percentages
5. **Reduce key space**: Use `--max_key=10000` for faster iteration
6. **Use a fixed seed**: Add `--seed=12345` for reproducibility

### Example Minimal Reproduction Command

```bash
buck2 run @//mode/dbg fbcode//internal_repo_rocksdb/repo:db_stress -- \
  --db=/dev/shm/rocksdb_repro \
  --expected_values_dir=/dev/shm/rocksdb_repro_expected \
  --destroy_db_initially=1 \
  --max_key=10000 \
  --ops_per_thread=100000 \
  --threads=8 \
  --prefix_size=8 \
  --prefixpercent=40 \
  --readpercent=25 \
  --writepercent=25 \
  --delpercent=5 \
  --delrangepercent=5 \
  --iterpercent=0 \
  --read_fault_one_in=10 \
  --column_families=1 \
  --test_batches_snapshots=1 \
  [... other relevant flags from the failing run ...]
```

**IMPORTANT**: The stress test must be built in **debug mode** (`@//mode/dbg`) because fault injection with `read_fault_one_in` relies on `IGNORE_STATUS_IF_ERROR` which uses `SyncPoint` — only available in debug builds (not NDEBUG).

### 6c: Run the Reproduction (MUST DO)

> **You MUST actually execute the reproduction command. Do not just derive parameters
> and skip to the fix. Run the command and observe the output.**

```bash
# Set up directories
mkdir -p /dev/shm/rocksdb_repro /dev/shm/rocksdb_repro_expected

# Run with the derived parameters
buck2 run @//mode/dbg fbcode//internal_repo_rocksdb/repo:db_stress -- \
  --db=/dev/shm/rocksdb_repro \
  --expected_values_dir=/dev/shm/rocksdb_repro_expected \
  --destroy_db_initially=1 \
  [... reproduction flags ...]
```

### 6d: Verify Reproduction (MUST DO)

**Check the output for the same assertion/crash.** You must confirm one of these outcomes before proceeding:

✅ **Reproduction confirmed** — The same assertion fires or the same crash occurs → proceed to Step 7 (implement fix)

❌ **Cannot reproduce** — Try these escalation steps in order:
1. Run 3-5 more times (stress tests are probabilistic)
2. Increase fault injection rate (e.g., `--read_fault_one_in=5` or `--read_fault_one_in=1`)
3. Increase operation count (`--ops_per_thread=1000000`)
4. Increase the percentage of the failing operation type (e.g., `--prefixpercent=80`)
5. Use the exact full parameter set from the failing Sandcastle run (do not minimize)
6. If still cannot reproduce, revisit root cause analysis — your hypothesis may be wrong
7. Try downloading and using the actual crash DB with `--destroy_db_initially=0` and `--db=/tmp/rocksdb_crash_db/dev/shm/rocksdb_test/rocksdb_crashtest_blackbox`

### Reproduction Checklist

Before moving to Step 7, verify ALL of the following:
- [ ] Built db_stress or unit test with `buck2` in debug mode (`@//mode/dbg`)
- [ ] Derived minimal reproduction parameters from the failing command line
- [ ] Actually executed the reproduction command (not just planned it)
- [ ] Observed the same assertion/crash in the output
- [ ] Noted the exact error message for comparison after fix

> **✅ CHECKPOINT: Mark Step 6 as `completed` in your todo list. Start Step 7 NOW.**

---

## Step 7: Implement the Fix

> **All fix changes happen in `fbcode/internal_repo_rocksdb/repo/` within fbsource.**

### Common Fix Patterns

#### Pattern A: Handle Injected Read Faults in Stress Test

When the bug is in the stress test harness (not handling injected I/O errors):

```cpp
// BEFORE (buggy):
assert(iter->status().ok());

// AFTER (fixed):
if (!iter->status().ok()) {
  if (IsErrorInjectedAndRetryable(iter->status())) {
    // Injected fault, not a real bug — bail out gracefully
    db_->ReleaseSnapshot(snapshot);
    return iter->status();
  } else {
    // Real error — this is a genuine failure
    fprintf(stderr, "Iterator error: %s\n", iter->status().ToString().c_str());
    thread->shared->SetVerificationFailure();
  }
}
```

#### Pattern B: Check Iterator Status Before Validity

```cpp
// BEFORE (buggy):
assert(iters[i]->Valid() && iters[i]->key().starts_with(prefix));

// AFTER (fixed):
if (!iters[i]->status().ok()) {
  if (IsErrorInjectedAndRetryable(iters[i]->status())) {
    break;  // Exit iteration loop on injected fault
  }
  // Handle real error...
}
assert(iters[i]->Valid() && iters[i]->key().starts_with(prefix));
```

#### Pattern C: Fix in RocksDB Engine Code

For actual engine bugs, the fix depends on the specific issue. Common areas:
- Iterator positioning logic (`fbcode/internal_repo_rocksdb/repo/db/db_iter.cc`)
- Compaction boundary handling (`fbcode/internal_repo_rocksdb/repo/db/compaction/`)
- Block cache interactions (`fbcode/internal_repo_rocksdb/repo/table/block_based/`)

#### Pattern D: Disable Stress Test Coverage for New/Developing Features

> **🚨 IMPORTANT: Before modifying core engine code, evaluate whether the feature
> involved in the failure is new or still under active development.** Fixing a bug
> by changing established engine code (e.g., `LevelIterator`, `DBIter`, `MergingIterator`)
> can have unintended performance or correctness impacts on all other features that
> rely on that code path.

**How to determine if a feature is new/under development:**
1. Check the feature's header file or main source file for its initial commit date
   (e.g., `sl log <file> -l 1 --follow-first` or `git log --follow --diff-filter=A <file>`)
2. Look at the feature's integration into `db_crashtest.py` — recent additions with
   many compatibility workarounds (disabled operations, special flags) suggest the
   feature is still maturing
3. Check if the feature has limited operation support (e.g., only supports `Seek`+`Next`
   but not `SeekToFirst`/`Prev`) — this indicates incomplete integration

**When the feature is new/developing, prefer disabling the failing test scenario**
in `db_crashtest.py` (or `tools/db_crashtest.py`) rather than modifying core engine
code. This approach:
- Avoids risking regressions in stable, performance-critical code paths
- Keeps the fix scope minimal and easy to review
- Allows the feature owner to properly fix the integration later

**Example: disable an operation percentage when a feature flag is active:**
```python
# In db_crashtest.py finalize_and_sanitize():
if dest_params.get("use_new_feature") == 1:
    # New feature doesn't support X operation yet.
    # Redistribute its percentage to reads.
    dest_params["readpercent"] += dest_params.get("Xpercent", 0)
    dest_params["Xpercent"] = 0
```

**When to fix the engine instead:** If the feature is mature and widely used, or if
the bug is clearly in the engine logic (not a missing feature integration), then an
engine-level fix is appropriate.

### After Making Changes

```bash
# Lint and auto-fix the changed files
arc lint -a
```

> **✅ CHECKPOINT: Mark Step 7 as `completed` in your todo list. Start Step 8 NOW.**

---

## Step 8: Verify the Fix (⚠️ MANDATORY)

> **🚨 You MUST re-run the exact same reproduction from Step 6 after implementing the fix.
> The fix is not complete until the reproduction test passes cleanly.**
>
> Step 8 mirrors Step 6's two paths. Use the same reproduction method you used in Step 6:
> if you wrote a unit test in Step 6a, verify with that unit test. If you used stress test
> reproduction in Step 6b, verify with that stress test. Do NOT switch methods.

### 8a: Re-run the Reproduction Test

**If reproduction was a unit test (Step 6a):**

```bash
# Rebuild and run the specific unit test
buck2 test @//mode/dbg fbcode//internal_repo_rocksdb/repo:{test_target_name} \
  -- --gtest_filter="*YourTestName*"
```

**If reproduction was a stress test (Step 6b):**

```bash
# Rebuild db_stress with the fix
buck2 build @//mode/dbg fbcode//internal_repo_rocksdb/repo:db_stress

# Re-run the exact same reproduction command from Step 6
buck2 run @//mode/dbg fbcode//internal_repo_rocksdb/repo:db_stress -- \
  [... same reproduction flags from Step 6 ...]
```

The test should now complete without the assertion failure.

> **⚠️ IMPORTANT: Assert specific correct behavior, not just the absence of a crash.**
>
> A fix that silently swallows errors or masks the failure will also make the test "pass."
> Your test must verify the **specific correct outcome** — the right error type, the right
> return value, the right state — not merely that it doesn't crash. For example, if the fix
> changes a misleading `Corruption` error to the correct `IOError`, the test should assert
> `s.IsIOError()`, not just `ASSERT_NOK(s)`. If the fix makes an operation succeed that was
> incorrectly failing, the test should assert the correct result, not just that no exception
> was thrown.

### 8b: Revert the Fix and Confirm the Test Fails (⚠️ MANDATORY)

> **🚨 THIS IS NON-NEGOTIABLE. A test that passes both with and without the fix proves nothing
> about the fix. You MUST perform this round-trip check.**

After confirming the test passes with the fix applied, **revert the fix and re-run the test**.
The test MUST fail without the fix. If it does not:

1. **Your test is not validating the fix.** It is reproducing the symptom through a code path
   that the fix does not affect. The test's setup does not satisfy the fix's preconditions.
2. **You MUST revise the test** to exercise the specific condition the fix checks. Go back to
   Step 6 and adjust the test setup so that it targets the fix's code path, not just the
   symptom's surface.
3. **Do NOT proceed** until you have a test that cleanly differentiates the fixed and unfixed
   code — failing without the fix, passing with it.

After confirming the round-trip (fail → apply fix → pass), re-apply the fix and continue.

### 8c: Run Multiple Times for Confidence

**If reproduction was a unit test:** A single passing run is sufficient — unit tests are
deterministic. Skip to 8d.

**If reproduction was a stress test:** Stress tests are probabilistic. A single pass does not
guarantee the fix is correct — the test might have passed by chance. You must build confidence
through repeated runs with meaningful workloads:

- Use the **same parameters** from your Step 6 reproduction (same `--ops_per_thread`, same
  fault injection rates, same operation percentages). Do NOT weaken the parameters.
- Run at least **3 times** with `--ops_per_thread` no less than what you used in Step 6.
- If your Step 6 reproduction used `--duration`, keep the same duration.
- If any run still hits the original failure, your fix is incomplete — go back to Step 7.

### 8d: Handle Verification Failure

If the reproduction test still fails after applying the fix:

1. **Re-examine the failure output.** Is it the same assertion/crash, or a different one?
   - **Same failure**: Your fix does not address the root cause. Revisit the root cause
     analysis from Step 4. Your hypothesis may be wrong or incomplete.
   - **Different failure**: Your fix may have introduced a new bug or exposed a latent one.
     Analyze the new failure separately.
2. **Revise the fix.** Based on the new evidence, update your fix in Step 7 and re-run
   verification from 8a.
3. **If stuck after 2-3 revision attempts**, reassess the root cause. Consider whether:
   - The root cause is more complex than initially analyzed (e.g., multiple interacting bugs)
   - A different fix strategy is needed (e.g., fixing at a different layer)
   - You need to gather more evidence (e.g., add logging, examine more crash artifacts)
4. **Escalate to the user** if you cannot resolve the verification failure after reasonable
   attempts. Present what you've tried, what you've observed, and ask for guidance.

### 8e: Run Related Tests for Regression Check

Run the unit tests directly related to the modified component to make sure the fix doesn't
break existing functionality:

```bash
# Run the specific test target for the modified component
buck2 test @//mode/dbg fbcode//internal_repo_rocksdb/repo:{relevant_test_target}

# Or run all tests in the db_stress_tool directory
buck2 test @//mode/dbg fbcode//internal_repo_rocksdb/repo/db_stress_tool/...
```

**For engine-level fixes** (changes to core components like compaction, flush, iterators,
block cache, table builder), also consider running a broader sanity check:

- Run the crash test variant that originally failed (e.g., `fbcode_blackbox_crash_test`)
  with a short duration (`--duration=120`) to verify the fix doesn't introduce regressions
  in the broader stress test coverage.
- If the fix touches code shared across multiple crash test variants, run at least one
  other variant (e.g., `fbcode_whitebox_crash_test`) as a smoke test.

This broader check is recommended but not mandatory — the full crash test suite runs in CI
after diff submission and will catch regressions there.

### Verification Checklist

Before moving to Step 9, verify ALL of the following:
- [ ] Reproduction test passes with the fix applied
- [ ] Reproduction test fails with the fix reverted (round-trip confirmed)
- [ ] Test asserts specific correct behavior, not just absence of crash
- [ ] Fix re-applied after round-trip check
- [ ] Multiple runs passed (if stress test reproduction)
- [ ] Related unit tests pass without regressions

> **✅ CHECKPOINT: Mark Step 8 as `completed` in your todo list. Start Step 9 NOW.**

---

## Step 9: Present Analysis Summary (⚠️ MANDATORY)

> **🚨 Before sending a diff, you MUST present a comprehensive summary to the user.
> This summary documents the full investigation and serves as the basis for the diff description.**

At the end of the analysis and fix workflow, present a structured summary covering all of the following sections. This helps the user understand the full investigation, verify the reasoning, and provides material for the diff summary and test plan.

### Summary Template

Present the following to the user:

---

#### 1. Failure Overview
- **Task**: T{task_id}
- **Test name**: (e.g., `fbcode_blackbox_crash_test`)
- **Assertion/Crash**: (e.g., `iters[i]->status().ok()` at `batched_ops_stress.cc:679`)
- **Sandcastle jobs**: (list ALL job URLs examined, with dates)
- **First occurrence**: (date/time, and whether it's new or recurring)

#### 2. Data Examined
List every piece of evidence you analyzed and what you learned from each:
- **Task description**: (what error info was extracted)
- **Sandcastle test stdout**: (what db_stress flags were used, what error output was observed)
- **Cross-run comparison** (if multiple runs): (which parameters were consistent across all failing runs vs. which varied — e.g., "All 3 failing runs had `--read_fault_one_in > 0`; compaction style varied, ruling it out as a trigger")
- **RocksDB LOG file**: (any errors/warnings found, or confirmation of no corruption)
- **OPTIONS file**: (key configuration that matters for the bug)
- **Downloaded crash DBs**: (what the DB states reveal — file counts, sizes, any anomalies; note differences across runs if multiple were examined)
- **Core dump** (if downloaded): (stack trace, variable state at crash)
- **Source code**: (which files/functions were read, what the code does at the crash point)
- **Temporal analysis**: (when the failure first appeared, which recent diffs were examined)
- **Related code patterns**: (how similar code in other stress tests handles the same situation)

#### 3. Root Cause Analysis
- **Category**: Test harness bug / RocksDB engine bug / Race condition / Configuration issue
- **Root cause**: (clear explanation of WHY the crash happens)
- **Triggering conditions**: (what specific flags/state/timing combination triggers it)
- **How it was identified**: (step-by-step reasoning chain — e.g., "The assertion at line 679 assumes iterator status is always OK after iteration, but with `--read_fault_one_in=1000`, FaultInjectionTestFS can inject I/O errors during reads, causing the iterator to have a non-OK status. The non-batched stress test handles this correctly using `IsErrorInjectedAndRetryable()` at line 2937 of `no_batched_ops_stress.cc`, confirming this is a missing error-handling pattern in the batched version.")
- **Suspect diff** (if identified via temporal analysis): D{diff_number} — what it changed and why it caused this failure

#### 4. Reproduction
- **Reproduced?**: ✅ Yes / ❌ No (and why not)
- **Method used**: Unit test / Stress test / Both / Not yet reproduced
- **If unit test**:
  - Test file and test name
  - How it reproduces the issue (reopens crash DB / constructs state / uses SyncPoint)
  - Build and run command
  - Output showing the reproduction (the assertion/crash message)
- **If stress test**:
  - Exact db_stress command used
  - Key parameters that trigger the failure
  - How many runs needed to reproduce
  - Output showing the reproduction
- **If both**: Explain why both were needed

#### 5. Fix Description
- **Files changed**: (list each file and what was changed)
- **Fix approach**: (explain the fix strategy — e.g., "Added `IsErrorInjectedAndRetryable()` checks before the assertions to gracefully handle injected faults, matching the pattern used in `no_batched_ops_stress.cc`")
- **Before/After code**: (show the key code change)
- **Why this fix is correct**: (explain why this doesn't mask real bugs)

#### 6. Verification
- **Reproduction test after fix**: (show that the same reproduction now passes)
- **Number of successful runs**: (e.g., "Ran 5 times without failure")
- **Unit tests**: (whether relevant unit tests pass)

---

### Example Summary

Here is a concrete example of what the summary should look like:

> #### 1. Failure Overview
> - **Task**: T257516637
> - **Test name**: `fbcode_blackbox_crash_test`
> - **Assertion**: `iters[i]->status().ok()` at `batched_ops_stress.cc:679`
> - **Sandcastle jobs**: https://www.internalfb.com/intern/sandcastle/job/67553994627249437/ (2026-02-27), https://www.internalfb.com/intern/sandcastle/job/67553994627249438/ (2026-02-28)
> - **First occurrence**: 2026-02-27, new failure
>
> #### 2. Data Examined
> - **Task description**: Extracted assertion message, source location, and Sandcastle URL
> - **Sandcastle test stdout**: Found `--read_fault_one_in=1000` and `--metadata_read_fault_one_in=32` in the db_stress command line — fault injection is enabled
> - **Cross-run comparison**: Both runs had `--read_fault_one_in > 0` and `--test_batches_snapshots=1`; compaction style and column family count varied
> - **RocksDB LOG file**: No corruption or errors — only normal compaction activity and one write stall warning
> - **OPTIONS file**: Confirmed `prefix_extractor=rocksdb.FixedPrefix.7` across all CFs
> - **Source code**: Read `batched_ops_stress.cc:550-700` — the `TestPrefixScan` method creates 10 iterators and asserts all have OK status after iteration. Read `no_batched_ops_stress.cc:2930-2940` — the non-batched version handles this correctly with `IsErrorInjectedAndRetryable()`
> - **Related patterns**: Found `IsErrorInjectedAndRetryable()` defined in `db_stress_test_base.h:30` — checks if error was injected by FaultInjectionTestFS
>
> #### 3. Root Cause Analysis
> - **Category**: Test harness bug
> - **Root cause**: `TestPrefixScan` in `batched_ops_stress.cc` uses hard `assert(iters[i]->status().ok())` without checking for injected faults. With `--read_fault_one_in=1000`, random I/O errors are injected during reads, causing iterator status to be non-OK — which is expected behavior under fault injection, not a real bug.
> - **How identified**: Cross-referenced the assertion with the db_stress flags; confirmed `read_fault_one_in > 0`; found the correct pattern in `no_batched_ops_stress.cc` that handles this case.
>
> #### 4. Reproduction
> - **Reproduced?**: ✅ Yes / ❌ No
> - **Method**: Unit test (reopened crash DB)
> - **Test**: `fbcode/internal_repo_rocksdb/repo/db/db_stress_repro_test.cc::ReproducePrefixScanFault`
> - **Command**: `buck2 test @//mode/dbg fbcode//internal_repo_rocksdb/repo:db_stress_repro_test -- --gtest_filter="*ReproducePrefixScanFault*"`
> - **Output**: `Assertion 'iters[i]->status().ok()' failed` ✅
>
> #### 5. Fix
> - **Files changed**: `fbcode/internal_repo_rocksdb/repo/db_stress_tool/batched_ops_stress.cc`
> - **Approach**: Added `IsErrorInjectedAndRetryable()` checks at lines 614 and 679
> - **Why correct**: Only skips injected faults; real errors still trigger `SetVerificationFailure()`
>
> #### 6. Verification
> - Reproduction test passes after fix (5 runs, 0 failures)
> - Related unit tests pass

#### 7. Why Did This Test Suddenly Start Failing Recently?

> **🚨 You MUST answer this question as a final sanity check on your analysis.
> If you cannot explain WHY the failure started appearing when it did, your
> root cause analysis may be incomplete or incorrect. Revisit your analysis
> before proceeding.**

This question forces a critical cross-check between the root cause and the timeline. You must provide one of the following explanations:

- **A recent code change introduced the bug**: Identify the specific diff/commit that caused the regression and explain how it broke the code path. Reference the temporal analysis from Step 3f.
  - Example: *"D94903299 added a new `use_trie_index` flag to the stress test but did not update `TestPrefixScan` to handle the new index type, causing assertion failures when `use_trie_index=1` is randomly selected."*

- **A recent code change exposed a pre-existing bug**: The underlying bug existed before, but a recent change made it more likely to trigger.
  - Example: *"The missing `IsErrorInjectedAndRetryable()` check has always been absent in `batched_ops_stress.cc`, but D94800000 increased the default `read_fault_one_in` from 0 to 1000 in `db_crashtest.py`, causing the fault injection to now trigger during `TestPrefixScan`."*

- **The failure is intermittent/probabilistic and not tied to a specific change**: The stress test uses randomized parameters, and this particular combination was hit by chance.
  - Example: *"The stress test randomly selects `read_fault_one_in` values. The bug has existed since `BatchedOpsStressTest` was written, but only triggers when `read_fault_one_in > 0` AND `prefixpercent > 0` are selected together, which happens ~5% of the time."*

- **An external/infrastructure change caused it**: Changes in the build environment, compiler version, OS, or hardware.
  - Example: *"The new compiler version enables stricter undefined behavior checks that catch a signed integer overflow in the key encoding logic."*

If none of these explanations fit, your analysis is likely incomplete — go back to Step 3f (temporal analysis) and Step 4 (root cause analysis) and dig deeper.

> **✅ CHECKPOINT: Mark Step 9 as `completed` in your todo list. Start Step 10 NOW.**

---

## Step 10: Create and Submit Phabricator Diff

> **This step uses Sapling (`sl`) and Jellyfish (`jf`) to create a Phabricator diff.**
> **Always create diffs in draft state.**

### 10a: Lint and Format Code

```bash
# Auto-fix lint issues in changed files
arc lint -a
```

### 10b: Review Changes

```bash
# Review all changes
sl diff
sl status
```

Present the diff output to the user for review.

### 10c: Create the Commit

Sapling has no staging area — all tracked file modifications are included automatically.
If you only want to commit specific files, pass them as arguments.

```bash
sl commit -m "$(cat <<'EOF'
[RocksDB] Fix {concise description of the fix}

Summary:
{Detailed explanation of the root cause and fix.}

Task: T{task_id}
Sandcastle job: {sandcastle_url}

Test Plan:
{How it was verified — reference unit test names, buck test commands, and
number of stress test runs.}
EOF
)"
```

**Commit message example:**
```
[RocksDB] Fix crash in BatchedOpsStressTest::TestPrefixScan with fault injection

Summary:
TestPrefixScan in batched_ops_stress.cc asserts that all iterators
have OK status after prefix scan iteration. However, when read fault
injection is enabled (--read_fault_one_in), FaultInjectionTestFS can
inject I/O errors during reads, causing iterator status to be non-OK.
This is expected behavior under fault injection, not a real bug.

Fixed by adding IsErrorInjectedAndRetryable() checks before the
assertions, matching the pattern already used in
no_batched_ops_stress.cc.

Task: T257516637
Sandcastle job: https://www.internalfb.com/intern/sandcastle/job/67553994627249437/

Test Plan:
- Added unit test ReproducePrefixScanFault:
  buck2 test @//mode/dbg fbcode//internal_repo_rocksdb/repo:db_stress_repro_test -- --gtest_filter="*ReproducePrefixScanFault*"
- Ran db_stress with --read_fault_one_in=10 for 5 runs without failure:
  buck2 run @//mode/dbg fbcode//internal_repo_rocksdb/repo:db_stress -- --read_fault_one_in=10 ...
```

### 10d: Submit to Phabricator as Draft

```bash
jf submit --draft
```

This creates a new Phabricator diff in draft state. Note the diff number (D{number}) from the output.

After submitting, provide the user with the diff URL:
```
https://www.internalfb.com/diff/D{diff_number}
```

> **✅ CHECKPOINT: Mark Step 10 as `completed` in your todo list. Start Step 11 NOW.**

---

## Inspecting the Downloaded DB with ldb

The `ldb` tool can be used to examine the contents of the downloaded crash DB. The stress test uses multiple column families, so always specify the correct one.

```bash
# Build ldb
buck2 build @//mode/dbg fbcode//internal_repo_rocksdb/repo:ldb

# Scan the DB (default column family)
buck2 run @//mode/dbg fbcode//internal_repo_rocksdb/repo:ldb -- scan --hex --db=/tmp/rocksdb_crash_db/dev/shm/rocksdb_test/rocksdb_crashtest_blackbox

# Scan a specific column family (stress test uses CFs named '0', '1', '2', etc.)
buck2 run @//mode/dbg fbcode//internal_repo_rocksdb/repo:ldb -- scan --hex --db=/tmp/rocksdb_crash_db/dev/shm/rocksdb_test/rocksdb_crashtest_blackbox --column_family='3'

# Dump MANIFEST
buck2 run @//mode/dbg fbcode//internal_repo_rocksdb/repo:ldb -- manifest_dump --path=/tmp/rocksdb_crash_db/dev/shm/rocksdb_test/rocksdb_crashtest_blackbox/MANIFEST-*

# Dump SST file contents
buck2 run @//mode/dbg fbcode//internal_repo_rocksdb/repo:ldb -- dump_sst --file=/tmp/rocksdb_crash_db/dev/shm/rocksdb_test/rocksdb_crashtest_blackbox/000226.sst
```

**IMPORTANT**: When using `ldb` to dump the DB with scan operations, by default it uses the default column family. Make sure you pick the right column family with the `--column_family` option, especially when debugging stress test failures that involve specific CFs.

---

## Reference Documentation

- **Stress test wiki**: https://www.internalfb.com/wiki/RocksDB/RocksDB_Testing/Stress_test/
- **Core dump tool**: https://www.internalfb.com/intern/wiki/Coredumper/User_Guide/
- **Debug examples (basic + crash-recovery)**: [Slides](https://docs.google.com/presentation/d/1BTU-DSIKZnmxmkrRDuzUtOCkCwMTR5mPfDpfhvUm7Tc/edit?usp=sharing) / [Recording](https://fb.workplace.com/groups/rocksdb.internal/permalink/26218607264427874/)
- **Debug examples (non-crash-recovery)**: [Doc](https://docs.google.com/document/d/1lFBETvA9nh7qIeg88yoF_tMOO7cTvDZ8CIkRBwwsUkU/edit?usp=sharing)
- **Stress test overview talks**: [External tech talk](https://youtu.be/2AzWpf-40ok?t=8064), [Internal 101](https://fb.workplace.com/groups/rocksdb.internal/permalink/710660030643815/), [Internal 201](https://fb.workplace.com/groups/rocksdb.internal/permalink/25967396256215644/)
- **RocksDB dashboard**: https://www.internalfb.com/intern/rocksdb-dashboard (tests containing "crash" in their names are stress tests)

---

## Key Reference Files

| File | Purpose |
|------|---------|
| `repo/db_stress_tool/batched_ops_stress.cc` | BatchedOps stress test |
| `repo/db_stress_tool/no_batched_ops_stress.cc` | Non-batched stress test (reference for patterns) |
| `repo/db_stress_tool/cf_consistency_stress.cc` | CF consistency stress test |
| `repo/db_stress_tool/db_stress_test_base.cc` | Base class with `OperateDb` loop |
| `repo/db_stress_tool/db_stress_test_base.h` | `IsErrorInjectedAndRetryable()` definition |
| `repo/db_stress_tool/db_stress_common.h` | Common flags and utilities |
| `repo/db_stress_tool/db_stress_gflags.cc` | All db_stress flag definitions |
| `repo/db_stress_tool/db_stress_shared_state.h` | Shared state and fault injection setup |
| `repo/db_stress_tool/db_stress_listener.h` | Listener with fault injection context |
| `repo/tools/db_crashtest.py` | Crash test orchestrator script |
| `repo/crash_test.mk` | Makefile targets for crash tests |
| `repo/BUCK` | Buck2 build targets (`:db_stress`) |

All paths above are relative to `fbcode/internal_repo_rocksdb/`.

## Key Build Targets

| Target | Description |
|--------|-------------|
| `fbcode//internal_repo_rocksdb/repo:db_stress` | db_stress binary |
| `fbcode//rocks/tools:rocks_db_stress` | Wrapped db_stress with Meta extensions |
| `fbcode//internal_repo_rocksdb/repo:ldb` | ldb database inspection tool |

## Error Handling Utilities

- **`IsErrorInjectedAndRetryable(status)`**: Returns `true` if the error was injected by `FaultInjectionTestFS` and is retryable (not data loss). Use this to distinguish injected faults from real bugs.
- **`FaultInjectionTestFS::IsInjectedError(status)`**: Lower-level check for injected errors.
- **`IGNORE_STATUS_IF_ERROR(status)`**: Macro used in debug builds to mark status checks that may encounter injected errors.

## Troubleshooting

### Build Failures
- Ensure you're using `@//mode/dbg` for debug builds (required for fault injection)
- If Buck build fails, try: `buck2 clean && buck2 build @//mode/dbg fbcode//internal_repo_rocksdb/repo:db_stress`
- **NEVER run `buck kill` or `buck killall`** — if Buck is in a bad state, tell the user to run `buck killall` in their own terminal

### Cannot Reproduce
- The stress test is probabilistic — run multiple times
- Try increasing fault injection rates (`--read_fault_one_in=5`)
- Use the exact parameters from the failing Sandcastle run
- Ensure you're building in debug mode (fault injection requires `SyncPoint`)

### Artifact Download Fails
- Sandcastle artifacts are retained for **30 days** — older artifacts may be unavailable
- Try `arc skycastle workflow-run get-action-artifact` first
- If that fails, get the digest from the Outputs tab and use `manifold get skycastle_cas/flat/{digest}`
- For authentication issues with manifold, ensure your credentials are fresh

### Multiple Assertion Failures in Same Task
- Tasks may accumulate multiple Sandcastle comments with different failures
- Focus on the original failure in the task description first
- Check if additional comment failures are the same root cause or different issues

### Sapling Issues
- If `sl status` shows unexpected changes, investigate before proceeding
- Use `sl shelve` to save unrelated work-in-progress
- If you need to update the diff after initial submission, use `sl amend` then `jf submit --draft`
- **NEVER use `sl commit` when updating an existing diff** — that creates a NEW commit and a new diff. Use `sl amend` instead.

---

## Step 11: Walk the User Through the Root Cause and Fix

> **🚨 MANDATORY: Do NOT end the conversation without completing this step.
> After all code work is done, you MUST walk the user through the entire investigation
> in a clear, conversational narrative.**

Present a walkthrough to the user that explains the full story of the bug, step by step.
This is NOT the same as the structured summary in Step 9 — this is a **narrative explanation**
designed to help the user deeply understand what happened and why.

### Walkthrough Structure

**1. "Here's what the test was doing when it crashed"**
- Explain the stress test operation that failed (e.g., prefix scan, iteration, compaction)
- Show the exact assertion or crash with the source code context
- Explain what the code was trying to verify

**2. "Here's what I found when I investigated"**
- Walk through the key evidence you examined and what each piece told you:
  - What the Sandcastle test stdout revealed (key parameters, error output)
  - What the cross-run comparison revealed (if multiple runs were examined)
  - What the RocksDB LOG file showed (or didn't show — e.g., "no corruption detected")
  - What the downloaded crash DB state revealed
  - What the source code analysis uncovered
  - What the temporal analysis revealed (when it started failing, what changed)
- Explain your reasoning chain — how each piece of evidence led to the next

**3. "Here's the root cause"**
- Explain the root cause in plain terms
- Show the specific code that was wrong and explain WHY it was wrong
- If applicable, show how similar code elsewhere handles the same case correctly
- Answer: "Why did this start failing recently?"

**4. "Here's how I reproduced it"**
- Explain whether you used a unit test or stress test (and why)
- Show the reproduction command and the output confirming the crash
- If using a unit test: explain how the test sets up the conditions that trigger the bug

**5. "Here's how I fixed it"**
- Show the before/after code change
- Explain why the fix is correct and doesn't mask real bugs
- Explain how you verified the fix (reproduction test passes, number of runs)

**6. "Here's what I've prepared for you to review"**
- Phabricator diff URL (from Step 10)
- Any remaining items the user should be aware of (e.g., related tests to watch, whether to also submit an open source PR)

> **✅ FINAL CHECKPOINT: Mark Step 11 as `completed` in your todo list. ALL STEPS ARE DONE.**
