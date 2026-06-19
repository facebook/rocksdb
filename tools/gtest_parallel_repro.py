#!/usr/bin/env python3
"""Run a gtest binary repeatedly under process-level scheduler pressure.

Synopsis:
  python3 tools/gtest_parallel_repro.py --binary ./env_test \\
      --gtest_filter='*ReserveThreads*' --processes-per-iteration 100 \\
      --iteration-count 100

This tool is for flaky tests that do not reproduce with a single-process
`--gtest_repeat` loop. Some RocksDB tests only fail when their process competes
with many other CPU-heavy test processes, because OS scheduling can delay
arbitrary worker threads and expose timing assumptions.

`COERCE_CONTEXT_SWITCH=1` is still useful and this runner can build with it, but
it is not enough for this class of flaky test by itself.
COERCE_CONTEXT_SWITCH adds voluntary sleeps/yields at selected RocksDB hooks,
while busy CI shards cause involuntary preemption and CPU starvation across the
whole process. Running many fresh test processes in parallel, optionally pinned
to a small CPU set, more closely matches that failure mode.
"""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple


FAILURE_LINE_RE = re.compile(r"^(.+?:\d+): Failure$")
SANITIZER_RE = re.compile(r"^==\d+==ERROR: ([^:]+): (.+)$")
ASSERT_RE = re.compile(r"Assertion `([^`]+)' failed\.")
HELP_EPILOG = """\
Examples:
  python3 tools/gtest_parallel_repro.py --binary ./env_test \\
      --gtest_filter='*ReserveThreads*' --processes-per-iteration 100 \\
      --iteration-count 100 --cpu-count 8

  python3 tools/gtest_parallel_repro.py --binary ./env_test \\
      --gtest_filter='*ReserveThreads*' --processes-per-iteration 32 \\
      --iteration-count 50 --build --coerce-context-switch --make-arg=-j40
"""


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def parse_env(overrides: Iterable[str]) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for override in overrides:
        key, sep, value = override.partition("=")
        if not sep or not key:
            raise argparse.ArgumentTypeError(
                f"environment override must be KEY=VALUE: {override}"
            )
        env[key] = value
    return env


def resolve_cpu_list(cpu_count: Optional[int], cpus: Optional[str]) -> Optional[str]:
    if cpus:
        return cpus
    if cpu_count is None:
        return None
    if hasattr(os, "sched_getaffinity"):
        available = sorted(os.sched_getaffinity(0))
    else:
        available = list(range(os.cpu_count() or 1))
    if cpu_count > len(available):
        raise ValueError(
            f"--cpu-count={cpu_count} exceeds available CPU count {len(available)}"
        )
    return ",".join(str(cpu) for cpu in available[:cpu_count])


def build_if_requested(args: argparse.Namespace, env: Dict[str, str]) -> None:
    if not args.build:
        if args.coerce_context_switch:
            print(
                "NOTE: --coerce-context-switch only affects compilation. "
                "Rebuild with --build, or make sure the binary was already "
                "built with COERCE_CONTEXT_SWITCH=1.",
                file=sys.stderr,
            )
        return

    target = args.make_target or Path(args.binary).name
    build_env = os.environ.copy()
    build_env.update(env)
    if args.coerce_context_switch:
        build_env["COERCE_CONTEXT_SWITCH"] = "1"
    if args.clean:
        print("Cleaning: make clean", flush=True)
        subprocess.run(["make", "clean"], env=build_env, check=True)
    cmd = ["make"] + args.make_arg + [target]
    print("Building:", " ".join(cmd), flush=True)
    subprocess.run(cmd, env=build_env, check=True)


def make_run_command(args: argparse.Namespace, cpu_list: Optional[str]) -> List[str]:
    cmd = [str(Path(args.binary).resolve())]
    if args.gtest_filter:
        cmd.append(f"--gtest_filter={args.gtest_filter}")
    cmd.extend(args.gtest_arg)
    if cpu_list:
        taskset = shutil.which("taskset")
        if not taskset:
            raise RuntimeError("--cpus/--cpu-count requires taskset in PATH")
        cmd = [taskset, "-c", cpu_list] + cmd
    return cmd


def terminate_process(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except OSError:
        proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except OSError:
            proc.kill()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print(
                f"WARNING: process {proc.pid} did not exit after SIGKILL",
                file=sys.stderr,
            )


def close_log_handles(processes: List[Dict[str, Any]]) -> None:
    for info in processes:
        log_fh = info.get("log_fh")
        if hasattr(log_fh, "close") and not getattr(log_fh, "closed", False):
            log_fh.close()


def terminate_processes(processes: List[Dict[str, Any]]) -> None:
    for info in processes:
        proc = info.get("proc")
        if isinstance(proc, subprocess.Popen) and proc.poll() is None:
            terminate_process(proc)


def extract_failure_keys(log_path: Path) -> List[str]:
    keys: List[str] = []
    try:
        lines = log_path.read_text(errors="replace").splitlines()
    except OSError:
        return ["<missing log>"]
    for line in lines:
        match = FAILURE_LINE_RE.match(line)
        if match:
            keys.append(match.group(1))
            continue
        match = SANITIZER_RE.match(line)
        if match:
            keys.append(f"{match.group(1)}: {match.group(2)}")
            continue
        match = ASSERT_RE.search(line)
        if match:
            keys.append(f"assertion failed: {match.group(1)}")
    return keys or ["<non-gtest failure>"]


def write_jsonl(path: Path, records: Iterable[Dict[str, object]]) -> None:
    with path.open("w") as fh:
        for record in records:
            fh.write(json.dumps(record, sort_keys=True) + "\n")


def launch_process(
    cmd: List[str],
    base_env: Dict[str, str],
    run_dir: Path,
    proc_index: int,
) -> Dict[str, Any]:
    tmp_dir = run_dir / "tmp"
    run_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "log.txt"
    env = base_env.copy()
    env["TEST_TMPDIR"] = str(tmp_dir)
    log_fh = log_path.open("w")
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )
    except BaseException:
        log_fh.close()
        raise
    return {
        "proc": proc,
        "proc_index": proc_index,
        "run_dir": run_dir,
        "log_path": log_path,
        "log_fh": log_fh,
        "start": time.monotonic(),
        "timed_out": False,
    }


def launch_iteration_processes(
    args: argparse.Namespace,
    iteration_dir: Path,
    cmd: List[str],
    base_env: Dict[str, str],
) -> List[Dict[str, Any]]:
    processes: List[Dict[str, Any]] = []
    try:
        for proc_index in range(1, args.processes_per_iteration + 1):
            processes.append(
                launch_process(
                    cmd,
                    base_env,
                    iteration_dir / f"proc_{proc_index:04d}",
                    proc_index,
                )
            )
    except BaseException:
        terminate_processes(processes)
        close_log_handles(processes)
        raise
    return processes


def monitor_processes(processes: List[Dict[str, Any]], timeout: int) -> None:
    remaining = set(range(len(processes)))
    while remaining:
        now = time.monotonic()
        for index in list(remaining):
            proc = processes[index]["proc"]
            assert isinstance(proc, subprocess.Popen)
            if proc.poll() is not None:
                processes[index]["end"] = time.monotonic()
                remaining.remove(index)
                continue
            elapsed = now - float(processes[index]["start"])
            if elapsed > timeout:
                processes[index]["timed_out"] = True
                terminate_process(proc)
                processes[index]["end"] = time.monotonic()
                remaining.remove(index)
        if remaining:
            time.sleep(0.05)


def collect_process_result(
    info: Dict[str, Any],
    iteration: int,
    keep_success_artifacts: bool,
) -> Tuple[Optional[Dict[str, object]], Dict[str, int]]:
    log_fh = info["log_fh"]
    assert hasattr(log_fh, "close")
    log_fh.close()
    proc = info["proc"]
    assert isinstance(proc, subprocess.Popen)
    timed_out = bool(info["timed_out"])
    log_path = Path(info["log_path"])
    run_dir = Path(info["run_dir"])
    elapsed = float(info.get("end", time.monotonic())) - float(info["start"])
    if not timed_out and proc.returncode == 0:
        if not keep_success_artifacts:
            shutil.rmtree(run_dir)
        return None, {}

    keys = extract_failure_keys(log_path)
    histogram = {key: keys.count(key) for key in set(keys)}
    return (
        {
            "iteration": iteration,
            "proc": info["proc_index"],
            "returncode": "TIMEOUT" if timed_out else proc.returncode,
            "elapsed_sec": round(elapsed, 3),
            "log": str(log_path),
            "failure_keys": keys,
        },
        histogram,
    )


def collect_iteration_results(
    processes: List[Dict[str, Any]],
    iteration: int,
    keep_success_artifacts: bool,
) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    failures: List[Dict[str, object]] = []
    histogram: Dict[str, int] = {}
    for info in processes:
        failure, process_histogram = collect_process_result(
            info, iteration, keep_success_artifacts
        )
        if failure is not None:
            failures.append(failure)
        for key, count in process_histogram.items():
            histogram[key] = histogram.get(key, 0) + count
    return failures, histogram


def run_iteration(
    args: argparse.Namespace,
    iteration: int,
    out_dir: Path,
    cmd: List[str],
    base_env: Dict[str, str],
) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    processes: List[Dict[str, Any]] = []
    iteration_dir = out_dir / f"iteration_{iteration:04d}"
    iteration_dir.mkdir(parents=True, exist_ok=True)

    try:
        processes = launch_iteration_processes(args, iteration_dir, cmd, base_env)
        monitor_processes(processes, args.timeout)
    except BaseException:
        terminate_processes(processes)
        close_log_handles(processes)
        raise

    return collect_iteration_results(
        processes, iteration, args.keep_success_artifacts
    )


def add_gtest_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--binary", required=True, help="Path to gtest binary")
    parser.add_argument(
        "--gtest_filter",
        "--gtest-filter",
        dest="gtest_filter",
        help="gtest filter to pass to the binary",
    )
    parser.add_argument(
        "--gtest_arg",
        "--gtest-arg",
        dest="gtest_arg",
        action="append",
        default=[],
        help="Additional gtest argument; repeat for multiple arguments",
    )


def add_run_control_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--processes-per-iteration",
        type=positive_int,
        default=8,
        help="Number of gtest processes to run concurrently in each iteration",
    )
    parser.add_argument(
        "--iteration-count",
        type=positive_int,
        default=1,
        help=(
            "Number of iterations to run; total invocations are iterations "
            "times processes per iteration"
        ),
    )
    parser.add_argument("--timeout", type=positive_int, default=60)


def add_output_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--out",
        default=None,
        help="Output directory. Defaults to /tmp/gtest_parallel_repro_<time>",
    )
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        help="Environment override for build and test process, KEY=VALUE",
    )
    parser.add_argument(
        "--keep-success-artifacts",
        action="store_true",
        help=(
            "Keep each successful process run directory, including log.txt and "
            "TEST_TMPDIR. By default successful run directories are deleted."
        ),
    )
    parser.add_argument(
        "--stop-on-failure",
        action="store_true",
        help=(
            "Stop after the first iteration with any failed process. The "
            "current iteration always finishes and cleans up first."
        ),
    )


def add_cpu_args(parser: argparse.ArgumentParser) -> None:
    cpu_group = parser.add_mutually_exclusive_group()
    cpu_group.add_argument(
        "--cpus",
        help="CPU list for taskset, for example 0-7 or 0,1,2,3",
    )
    cpu_group.add_argument(
        "--cpu-count",
        type=positive_int,
        help="Pin all test processes to the first N CPUs available to this process",
    )


def add_build_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--build",
        action="store_true",
        help="Run make before the repro loop",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Run make clean before --build. Use when switching build flags.",
    )
    parser.add_argument(
        "--make-target",
        help="Target to build. Defaults to basename of --binary",
    )
    parser.add_argument(
        "--make-arg",
        action="append",
        default=[],
        help="Extra make argument; use --make-arg=-j40 for options",
    )
    parser.add_argument(
        "--coerce-context-switch",
        action="store_true",
        help=(
            "When --build is set, build with COERCE_CONTEXT_SWITCH=1. "
            "The repro loop still uses process-level parallelism because "
            "COERCE_CONTEXT_SWITCH alone does not simulate CPU starvation."
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run many fresh gtest processes concurrently to reproduce flaky "
            "tests that depend on CPU contention or process-level scheduling."
        ),
        epilog=HELP_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_gtest_args(parser)
    add_run_control_args(parser)
    add_output_args(parser)
    add_cpu_args(parser)
    add_build_args(parser)
    return parser


def parse_env_or_error(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> Dict[str, str]:
    try:
        return parse_env(args.env)
    except argparse.ArgumentTypeError as err:
        parser.error(str(err))
    raise AssertionError("unreachable")


def validate_build_args(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> None:
    if args.clean and not args.build:
        parser.error("--clean requires --build")
    binary = Path(args.binary)
    if not binary.exists() and not args.build:
        parser.error(f"--binary does not exist: {args.binary}")


def make_output_dir(parser: argparse.ArgumentParser, args: argparse.Namespace) -> Path:
    out_dir = Path(args.out or f"/tmp/gtest_parallel_repro_{int(time.time())}")
    if out_dir.exists() and any(out_dir.iterdir()):
        parser.error(f"--out exists and is not empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def write_metadata(
    out_dir: Path,
    args: argparse.Namespace,
    cmd: List[str],
    cpu_list: Optional[str],
    env_overrides: Dict[str, str],
) -> None:
    metadata = {
        "cmd": cmd,
        "processes_per_iteration": args.processes_per_iteration,
        "iteration_count": args.iteration_count,
        "timeout": args.timeout,
        "cpu_list": cpu_list,
        "coerce_context_switch": args.coerce_context_switch,
        "env_overrides": env_overrides,
        "out_dir": str(out_dir),
    }
    (out_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")


def prepare_run(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> Tuple[Path, List[str], Dict[str, str]]:
    validate_build_args(parser, args)
    env_overrides = parse_env_or_error(parser, args)
    build_if_requested(args, env_overrides)
    if not Path(args.binary).exists():
        parser.error(f"--binary does not exist after build: {args.binary}")

    try:
        cpu_list = resolve_cpu_list(args.cpu_count, args.cpus)
        cmd = make_run_command(args, cpu_list)
    except (RuntimeError, ValueError) as err:
        parser.error(str(err))

    out_dir = make_output_dir(parser, args)
    base_env = os.environ.copy()
    base_env.update(env_overrides)
    write_metadata(out_dir, args, cmd, cpu_list, env_overrides)
    return out_dir, cmd, base_env


def merge_histogram(total: Dict[str, int], update: Dict[str, int]) -> None:
    for key, count in update.items():
        total[key] = total.get(key, 0) + count


def print_failure_progress(
    iteration: int, failures: int, total_failures: int
) -> None:
    print(
        f"iteration {iteration}: failures={failures} "
        f"total_failures={total_failures}",
        flush=True,
    )


def run_iterations(
    args: argparse.Namespace,
    out_dir: Path,
    cmd: List[str],
    base_env: Dict[str, str],
) -> Tuple[List[Dict[str, object]], Dict[str, int], int, bool]:
    all_failures: List[Dict[str, object]] = []
    total_histogram: Dict[str, int] = {}
    total_runs = 0
    interrupted = False
    try:
        for iteration in range(1, args.iteration_count + 1):
            failures, histogram = run_iteration(
                args, iteration, out_dir, cmd, base_env
            )
            total_runs += args.processes_per_iteration
            all_failures.extend(failures)
            merge_histogram(total_histogram, histogram)
            if failures:
                print_failure_progress(iteration, len(failures), len(all_failures))
                if args.stop_on_failure:
                    break
    except KeyboardInterrupt:
        interrupted = True
        print(
            "\nInterrupted; active child process groups were terminated.",
            file=sys.stderr,
        )
    return all_failures, total_histogram, total_runs, interrupted


def print_summary(
    out_dir: Path,
    all_failures: List[Dict[str, object]],
    total_histogram: Dict[str, int],
    total_runs: int,
) -> None:
    print(f"TOTAL_RUNS={total_runs}")
    print(f"TOTAL_FAILURES={len(all_failures)}")
    print(f"FAILURES_FILE={out_dir / 'failures.jsonl'}")
    if total_histogram:
        print("FAILURE_HISTOGRAM:")
        for key, count in sorted(
            total_histogram.items(), key=lambda item: (-item[1], item[0])
        )[:20]:
            print(f"  {count} {key}")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    out_dir, cmd, base_env = prepare_run(parser, args)
    print("Command:", " ".join(cmd))
    print("Output:", out_dir)
    print(
        f"Running {args.iteration_count} iteration(s) x "
        f"{args.processes_per_iteration} process(es)"
    )
    all_failures, total_histogram, total_runs, interrupted = run_iterations(
        args, out_dir, cmd, base_env
    )
    write_jsonl(out_dir / "failures.jsonl", all_failures)
    print_summary(out_dir, all_failures, total_histogram, total_runs)
    if interrupted:
        return 130
    return 1 if all_failures else 0


if __name__ == "__main__":
    sys.exit(main())
