#!/usr/bin/env python3
"""
Run clang-tidy on locally changed code and filter results to changed lines.

This script detects local changes by combining:
  1. Uncommitted changes (staged + unstaged + untracked files)
  2. Committed-but-not-pushed changes (local commits not in the remote)

It then runs clang-tidy only on the changed .cc/.cpp files (in parallel) and
filters the output to show only warnings on lines that were actually modified.

Usage:
  python3 tools/run_clang_tidy.py [options]

Examples:
  # Basic usage (auto-detects base from remote tracking branch):
  python3 tools/run_clang_tidy.py

  # Specify clang-tidy binary and parallelism:
  python3 tools/run_clang_tidy.py --clang-tidy-binary clang-tidy-18 -j 14

  # Explicit diff base (useful in CI where the checkout is a merge commit):
  python3 tools/run_clang_tidy.py --diff-base HEAD~1

  # Save full (unfiltered) output to a file:
  python3 tools/run_clang_tidy.py -o full_output.txt

  # Show all warnings, not just on changed lines:
  python3 tools/run_clang_tidy.py --verbose

  # CI mode with GitHub annotations and step summary:
  python3 tools/run_clang_tidy.py --diff-base HEAD~1 --github-annotations --github-step-summary
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def log(msg=""):
    """Print and flush immediately so output is visible in real time."""
    print(msg, flush=True)


def run_cmd(cmd, cwd=None):
    """Run a command and return (stdout, return_code)."""
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    return result.stdout.strip(), result.returncode


def get_repo_root():
    """Get the git repository root directory."""
    out, rc = run_cmd(["git", "rev-parse", "--show-toplevel"])
    if rc != 0:
        log("Error: not inside a git repository.")
        sys.exit(1)
    return out


def find_remote_base(repo_root):
    """
    Auto-detect the base commit to diff against.

    Strategy:
      1. Use the upstream tracking branch of the current branch if available.
      2. Fall back to origin/main, origin/master, upstream/main, upstream/master.
      3. Return the merge-base of HEAD and that ref.
    """
    out, rc = run_cmd(
        ["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"],
        cwd=repo_root,
    )
    if rc == 0 and out:
        base_ref = out
    else:
        base_ref = None
        for candidate in [
            "origin/main", "origin/master",
            "upstream/main", "upstream/master",
        ]:
            _, rc = run_cmd(["git", "rev-parse", "--verify", candidate], cwd=repo_root)
            if rc == 0:
                base_ref = candidate
                break
        if base_ref is None:
            log(
                "Error: cannot determine remote base branch.\n"
                "Set an upstream: git branch --set-upstream-to=<remote>/<branch>\n"
                "Or use --diff-base <ref> to specify the base explicitly."
            )
            sys.exit(1)

    merge_base, rc = run_cmd(["git", "merge-base", "HEAD", base_ref], cwd=repo_root)
    if rc != 0:
        log(f"Error: cannot compute merge-base with {base_ref}.")
        sys.exit(1)

    return merge_base, base_ref


def resolve_diff_base(diff_base_arg, repo_root):
    """
    Resolve --diff-base to a concrete commit SHA.

    When --diff-base is given, resolve the ref and return (sha, display_name).
    Otherwise, fall back to auto-detection via find_remote_base().
    """
    if diff_base_arg:
        sha, rc = run_cmd(
            ["git", "rev-parse", "--verify", diff_base_arg], cwd=repo_root
        )
        if rc != 0:
            log(f"Error: --diff-base '{diff_base_arg}' is not a valid git ref.")
            sys.exit(1)
        return sha, diff_base_arg

    return find_remote_base(repo_root)


def parse_diff_for_changed_lines(diff_text):
    """
    Parse a unified diff and return {relative_path: set_of_new_line_numbers}.

    Only tracks added/modified lines (the '+' side of the diff).
    """
    changed = {}
    current_file = None

    for line in diff_text.split("\n"):
        m = re.match(r"^\+\+\+ b/(.*)", line)
        if m:
            current_file = m.group(1)
            changed.setdefault(current_file, set())
            continue

        m = re.match(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@", line)
        if m and current_file is not None:
            start = int(m.group(1))
            count = int(m.group(2)) if m.group(2) else 1
            if count == 0:
                continue
            for i in range(start, start + count):
                changed[current_file].add(i)

    return changed


def collect_changed_lines(repo_root, diff_base_arg=None):
    """
    Collect every locally-changed file and its changed line numbers.

    When diff_base_arg is provided, diffs HEAD against that ref directly.
    Otherwise, auto-detects the remote base and also picks up uncommitted
    and untracked changes.
    """
    base_sha, base_label = resolve_diff_base(diff_base_arg, repo_root)
    head_short, _ = run_cmd(["git", "rev-parse", "--short", "HEAD"], cwd=repo_root)

    log_out, _ = run_cmd(
        ["git", "log", "--oneline", f"{base_sha}..HEAD"], cwd=repo_root
    )
    local_commits = [l for l in log_out.split("\n") if l.strip()] if log_out else []

    log(f"  Diff base   : {base_label}  ({base_sha[:10]})")
    log(f"  HEAD        : {head_short}")
    log(f"  Commits in range: {len(local_commits)}")
    for c in local_commits[:20]:
        log(f"    {c}")
    if len(local_commits) > 20:
        log(f"    ... and {len(local_commits) - 20} more")

    all_changed = {}
    src_pattern = r"\.(cc|cpp|h)$"

    def merge_into(target, source):
        for f, lines in source.items():
            target.setdefault(f, set()).update(lines)

    # Committed changes: base..HEAD
    diff_committed, _ = run_cmd(
        ["git", "diff", "--unified=0", f"{base_sha}..HEAD",
         "--", "*.cc", "*.cpp", "*.h"],
        cwd=repo_root,
    )
    merge_into(all_changed, parse_diff_for_changed_lines(diff_committed))

    # When using explicit --diff-base (e.g. CI), skip working-tree checks
    if diff_base_arg is None:
        # Unstaged changes
        diff_unstaged, _ = run_cmd(
            ["git", "diff", "--unified=0", "--", "*.cc", "*.cpp", "*.h"],
            cwd=repo_root,
        )
        merge_into(all_changed, parse_diff_for_changed_lines(diff_unstaged))

        # Staged changes
        diff_staged, _ = run_cmd(
            ["git", "diff", "--unified=0", "--cached", "--", "*.cc", "*.cpp", "*.h"],
            cwd=repo_root,
        )
        merge_into(all_changed, parse_diff_for_changed_lines(diff_staged))

        # Untracked files — treat every line as changed
        untracked_out, _ = run_cmd(
            ["git", "ls-files", "--others", "--exclude-standard"], cwd=repo_root
        )
        for f in untracked_out.split("\n"):
            f = f.strip()
            if not f or not re.search(src_pattern, f):
                continue
            filepath = os.path.join(repo_root, f)
            if os.path.isfile(filepath):
                with open(filepath) as fh:
                    line_count = sum(1 for _ in fh)
                all_changed.setdefault(f, set()).update(range(1, line_count + 1))

    return all_changed


def load_compile_db(compile_db_path, repo_root):
    """Load compile_commands.json and return a set of known file paths (both abs and rel)."""
    if not os.path.exists(compile_db_path):
        log(
            f"Error: {compile_db_path} not found.\n"
            "Generate it with:\n"
            "  mkdir build && cd build && cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..\n"
            "  ln -sf build/compile_commands.json compile_commands.json"
        )
        sys.exit(1)

    with open(compile_db_path) as f:
        db = json.load(f)

    files = set()
    prefix = repo_root.rstrip("/") + "/"
    for entry in db:
        abs_path = entry["file"]
        files.add(abs_path)
        if abs_path.startswith(prefix):
            files.add(abs_path[len(prefix):])
    return files


def invoke_clang_tidy(clang_tidy_bin, compile_db_dir, filepath, repo_root):
    """Run clang-tidy on a single file. Returns (filepath, combined_output, return_code)."""
    abs_path = os.path.join(repo_root, filepath)
    cmd = [clang_tidy_bin, "-p", compile_db_dir, abs_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        return filepath, result.stdout + result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return filepath, f"TIMEOUT after 600s: {abs_path}\n", -1


def emit_github_annotations(filtered_lines, repo_root):
    """
    Emit GitHub Actions workflow commands for each warning/error so they
    appear as inline annotations on the PR diff.

    Format: ::warning file={path},line={line}::{message}

    Errors are emitted first so they occupy annotation slots before warnings,
    since GitHub Actions caps display at 10 warnings and 10 errors per step.
    Use --github-step-summary for the complete report.
    """
    prefix = repo_root.rstrip("/") + "/"

    annotations = []
    for line in filtered_lines:
        m = re.match(r"^(.*?):(\d+):(\d+): (warning|error): (.+)", line)
        if not m:
            continue
        filepath = m.group(1)
        lineno = m.group(2)
        col = m.group(3)
        severity = m.group(4)
        message = m.group(5)

        rel_path = filepath
        if filepath.startswith(prefix):
            rel_path = filepath[len(prefix):]

        gh_level = "error" if severity == "error" else "warning"
        annotations.append((gh_level, rel_path, lineno, col, message))

    annotations.sort(key=lambda a: (0 if a[0] == "error" else 1, a[1], int(a[2])))

    for gh_level, rel_path, lineno, col, message in annotations:
        log(f"::{gh_level} file={rel_path},line={lineno},col={col}::{message}")


COMMENT_MARKER = "<!-- clang-tidy-bot -->"


def _format_diagnostic_counts(diagnostic_lines):
    """Return a human-readable string like '3 error(s) and 5 warning(s)'."""
    n_errors = sum(1 for l in diagnostic_lines if re.search(r": error:", l))
    n_warnings = sum(1 for l in diagnostic_lines if re.search(r": warning:", l))
    parts = []
    if n_errors:
        parts.append(f"{n_errors} error(s)")
    if n_warnings:
        parts.append(f"{n_warnings} warning(s)")
    return " and ".join(parts) if parts else "0 findings"


def build_markdown_summary(diagnostic_lines, by_check, wall_time, repo_root):
    """Build a Markdown summary string from clang-tidy results."""
    prefix = repo_root.rstrip("/") + "/"
    lines = []

    if not diagnostic_lines:
        lines.append("## :white_check_mark: clang-tidy: No findings on changed lines")
        lines.append(f"\nCompleted in {wall_time:.1f}s.")
    else:
        counts = _format_diagnostic_counts(diagnostic_lines)
        has_errors = any(re.search(r": error:", l) for l in diagnostic_lines)
        icon = ":x:" if has_errors else ":warning:"
        lines.append(f"## {icon} clang-tidy: {counts} on changed lines")
        lines.append(f"\nCompleted in {wall_time:.1f}s.\n")

        lines.append("### Summary by check\n")
        lines.append("| Check | Count |")
        lines.append("|-------|------:|")
        for check in sorted(by_check):
            lines.append(f"| `{check}` | {len(by_check[check])} |")
        lines.append(f"| **Total** | **{len(diagnostic_lines)}** |")

        lines.append("\n### Details\n")
        by_file = {}
        for line in diagnostic_lines:
            m = re.match(r"^(.*?):(\d+):(\d+): (warning|error): (.+)", line)
            if m:
                filepath = m.group(1)
                if filepath.startswith(prefix):
                    filepath = filepath[len(prefix):]
                by_file.setdefault(filepath, []).append(line)

        for filepath in sorted(by_file):
            n_e = sum(1 for l in by_file[filepath] if ": error:" in l)
            n_w = sum(1 for l in by_file[filepath] if ": warning:" in l)
            file_parts = []
            if n_e:
                file_parts.append(f"{n_e} error(s)")
            if n_w:
                file_parts.append(f"{n_w} warning(s)")
            file_summary = ", ".join(file_parts)
            lines.append(f"<details><summary><code>{filepath}</code> ({file_summary})</summary>\n")
            lines.append("```")
            for w in by_file[filepath]:
                clean = w
                if clean.startswith(prefix):
                    clean = clean[len(prefix):]
                lines.append(clean)
            lines.append("```\n")
            lines.append("</details>\n")

    return "\n".join(lines)


def write_github_step_summary(warning_lines, by_check, wall_time, repo_root):
    """
    Write a Markdown summary to $GITHUB_STEP_SUMMARY.

    This appears on the job's summary page in GitHub Actions and has no
    practical size limit, unlike annotations (capped at 10+10 per step).
    """
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        log("  $GITHUB_STEP_SUMMARY not set; skipping step summary.")
        return

    md = build_markdown_summary(warning_lines, by_check, wall_time, repo_root)
    with open(summary_path, "a") as f:
        f.write(md + "\n")
    log(f"  Step summary written to $GITHUB_STEP_SUMMARY")


def write_comment_file(path, warning_lines, by_check, wall_time, repo_root):
    """
    Write the Markdown summary to a file for posting as a PR comment.

    Includes a hidden HTML marker so the workflow can find and update an
    existing comment instead of creating duplicates on re-runs.
    """
    md = build_markdown_summary(warning_lines, by_check, wall_time, repo_root)
    with open(path, "w") as f:
        f.write(COMMENT_MARKER + "\n" + md + "\n")
    log(f"  Comment body written to {path}")


def filter_to_changed_lines(raw_output, changed_lines, repo_root):
    """
    Parse clang-tidy output and keep only diagnostics whose location falls on
    a changed line.  Also keeps note/context lines that follow a kept warning.
    """
    prefix = repo_root.rstrip("/") + "/"
    results = []
    keep_current = False

    for line in raw_output.split("\n"):
        m = re.match(r"^(.*?):(\d+):\d+: (warning|error): (.+)", line)
        if m:
            filepath_abs = m.group(1)
            lineno = int(m.group(2))

            rel_path = filepath_abs
            if filepath_abs.startswith(prefix):
                rel_path = filepath_abs[len(prefix):]

            if rel_path in changed_lines and lineno in changed_lines[rel_path]:
                keep_current = True
                results.append(line)
            else:
                keep_current = False
            continue

        if keep_current:
            if line.strip():
                results.append(line)
            else:
                keep_current = False

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Run clang-tidy on locally changed code, filtered to changed lines.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--clang-tidy-binary",
        default="clang-tidy",
        help="Path to clang-tidy binary (default: %(default)s)",
    )
    parser.add_argument(
        "-p", "--compile-commands-dir",
        default=None,
        help="Directory containing compile_commands.json (default: repo root)",
    )
    parser.add_argument(
        "-j", "--jobs",
        type=int, default=None,
        help="Number of parallel clang-tidy jobs (default: CPU count)",
    )
    parser.add_argument(
        "--diff-base",
        default=None,
        metavar="REF",
        help=(
            "Explicit git ref to diff against (e.g. HEAD~1, a commit SHA, or a "
            "branch name). When set, only the committed diff from REF to HEAD is "
            "analyzed (working-tree changes are ignored). This is useful in CI "
            "where the checkout is a merge commit: --diff-base HEAD~1 gives "
            "exactly the PR's changes. When omitted, the base is auto-detected "
            "from the remote tracking branch."
        ),
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Write full (unfiltered) clang-tidy output to this file",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Print all clang-tidy output, not just warnings on changed lines",
    )
    parser.add_argument(
        "--github-annotations",
        action="store_true",
        help=(
            "Emit GitHub Actions workflow commands (::warning) so that "
            "clang-tidy findings appear as inline annotations on the PR's "
            "\"Files changed\" tab.  Note: GitHub caps this at 10 warnings "
            "and 10 errors per step."
        ),
    )
    parser.add_argument(
        "--github-step-summary",
        action="store_true",
        help=(
            "Write a Markdown summary of all findings to $GITHUB_STEP_SUMMARY. "
            "This appears on the job's summary page with no size limit, "
            "complementing the capped inline annotations."
        ),
    )
    parser.add_argument(
        "--comment-output",
        default=None,
        metavar="FILE",
        help=(
            "Write a Markdown summary to FILE for posting as a PR comment. "
            "Includes a hidden marker so the CI workflow can find and update "
            "an existing comment instead of creating duplicates on re-runs."
        ),
    )
    args = parser.parse_args()

    repo_root = get_repo_root()
    compile_db_dir = args.compile_commands_dir or repo_root
    compile_db_path = os.path.join(compile_db_dir, "compile_commands.json")
    jobs = args.jobs or os.cpu_count() or 4

    # ------------------------------------------------------------------
    # Step 1 — detect changes
    # ------------------------------------------------------------------
    log("=" * 70)
    log("Step 1: Detecting changes")
    log("=" * 70)
    changed_lines = collect_changed_lines(repo_root, args.diff_base)

    if not changed_lines:
        log("\nNo changes detected. Nothing to check.")
        if args.comment_output:
            write_comment_file(args.comment_output, [], {}, 0, repo_root)
        return 0

    total_lines = sum(len(v) for v in changed_lines.values())
    log(f"\n  {len(changed_lines)} file(s) changed, {total_lines} line(s) total:")
    for f in sorted(changed_lines):
        log(f"    {f}  ({len(changed_lines[f])} lines)")

    # ------------------------------------------------------------------
    # Step 2 — select compilable files present in compile_commands.json
    # ------------------------------------------------------------------
    db_files = load_compile_db(compile_db_path, repo_root)
    cc_changed = sorted(
        f for f in changed_lines
        if re.search(r"\.(cc|cpp)$", f)
        and (f in db_files or os.path.join(repo_root, f) in db_files)
    )

    if not cc_changed:
        log("\nNo compilable changed files found in compile_commands.json.")
        if args.comment_output:
            write_comment_file(args.comment_output, [], {}, 0, repo_root)
        return 0

    log(f"\n{'=' * 70}")
    log(f"Step 2: Running clang-tidy on {len(cc_changed)} file(s)  [jobs={jobs}]")
    log("=" * 70)

    # ------------------------------------------------------------------
    # Step 3 — run clang-tidy in parallel via ThreadPoolExecutor
    # ------------------------------------------------------------------
    all_raw_output = []
    all_filtered = []
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=jobs) as pool:
        futures = {
            pool.submit(
                invoke_clang_tidy,
                args.clang_tidy_binary,
                compile_db_dir,
                f,
                repo_root,
            ): f
            for f in cc_changed
        }

        done = 0
        for future in as_completed(futures):
            done += 1
            fpath = futures[future]
            fpath, output, rc = future.result()
            all_raw_output.append(output)

            filtered = filter_to_changed_lines(output, changed_lines, repo_root)
            all_filtered.extend(filtered)

            n_diags = sum(
                1 for l in filtered if re.search(r": (warning|error):", l)
            )
            elapsed = time.time() - t0
            if rc == 0:
                status = "clean"
            elif rc == -1:
                status = "TIMEOUT"
            else:
                status = f"{n_diags} on changed lines"
            log(
                f"  [{done:>{len(str(len(cc_changed)))}}/{len(cc_changed)}]"
                f" {elapsed:6.1f}s  {fpath}  ({status})"
            )

    wall_time = time.time() - t0

    # ------------------------------------------------------------------
    # Optional: save full output
    # ------------------------------------------------------------------
    if args.output:
        with open(args.output, "w") as f:
            f.write("\n".join(all_raw_output))
        log(f"\nFull clang-tidy output saved to {args.output}")

    # ------------------------------------------------------------------
    # Step 4 — report filtered results
    # ------------------------------------------------------------------
    log(f"\n{'=' * 70}")
    log(f"Step 3: Results  (wall time {wall_time:.1f}s)")
    log("=" * 70)

    if args.verbose:
        log("\n--- Full output ---")
        for chunk in all_raw_output:
            log(chunk)
        log("--- End full output ---\n")

    diagnostic_lines = [
        l for l in all_filtered if re.search(r": (warning|error):", l)
    ]
    if not diagnostic_lines:
        log("\nNo findings on changed lines. Clean!")
        if args.github_step_summary:
            write_github_step_summary([], {}, wall_time, repo_root)
        if args.comment_output:
            write_comment_file(args.comment_output, [], {}, wall_time, repo_root)
        return 0

    error_lines = [l for l in diagnostic_lines if re.search(r": error:", l)]
    warning_lines = [l for l in diagnostic_lines if re.search(r": warning:", l)]

    by_check = {}
    for line in diagnostic_lines:
        m = re.search(r"\[([\w.-]+)\]\s*$", line)
        check = m.group(1) if m else "unknown"
        by_check.setdefault(check, []).append(line)

    parts = []
    if error_lines:
        parts.append(f"{len(error_lines)} error(s)")
    if warning_lines:
        parts.append(f"{len(warning_lines)} warning(s)")
    log(f"\n{' and '.join(parts)} on changed lines:\n")
    for line in all_filtered:
        log(line)

    if args.github_annotations:
        log(f"\n{'=' * 70}")
        log("Emitting GitHub Actions annotations")
        log("=" * 70)
        emit_github_annotations(all_filtered, repo_root)

    if args.github_step_summary:
        log(f"\n{'=' * 70}")
        log("Writing GitHub step summary")
        log("=" * 70)
        write_github_step_summary(diagnostic_lines, by_check, wall_time, repo_root)

    if args.comment_output:
        log(f"\n{'=' * 70}")
        log("Writing PR comment body")
        log("=" * 70)
        write_comment_file(
            args.comment_output, diagnostic_lines, by_check, wall_time, repo_root
        )

    log(f"\n{'=' * 70}")
    log("Summary by check:")
    log("=" * 70)
    for check in sorted(by_check):
        log(f"  [{check}]  x{len(by_check[check])}")
    summary_parts = []
    if error_lines:
        summary_parts.append(f"{len(error_lines)} error(s)")
    if warning_lines:
        summary_parts.append(f"{len(warning_lines)} warning(s)")
    log(f"\n  Total: {' and '.join(summary_parts)}")

    return 1 if error_lines else 0


if __name__ == "__main__":
    sys.exit(main())
