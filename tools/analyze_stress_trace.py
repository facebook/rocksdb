#!/usr/bin/env python3
"""Analyze stress trace dumps: merge threads, resolve symbols, find contention.

Usage:
  # Merge all thread traces into a unified timeline
  python3 tools/analyze_stress_trace.py /tmp/stress-trace-demo/stress-trace/

  # With symbol resolution (requires db_stress binary)
  python3 tools/analyze_stress_trace.py --binary ./db_stress /tmp/stress-trace-demo/stress-trace/

  # Show only sync events (mutex/CV/SyncPoint), skip function entries
  python3 tools/analyze_stress_trace.py --sync-only /tmp/stress-trace-demo/stress-trace/

  # Find contention: who held a mutex while others waited
  python3 tools/analyze_stress_trace.py --contention /tmp/stress-trace-demo/stress-trace/

  # Limit output to last N events per thread before crash
  python3 tools/analyze_stress_trace.py --last 50 /tmp/stress-trace-demo/stress-trace/
"""

import argparse
import os
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class TraceEvent:
    tsc: int
    thread_id: str  # short form, e.g. "T0", "T1"
    thread_hash: str  # original hash
    event_type: str  # ENTER, MUTEX_LOCK, MUTEX_UNLOCK, CV_WAIT_BEGIN, etc.
    detail: str  # symbol name or event payload
    obj: Optional[str] = None  # object pointer for sync events
    raw_line: str = ""


def parse_trace_file(filepath: str, thread_label: str) -> List[TraceEvent]:
    """Parse a single thread's trace file into TraceEvent objects."""
    events = []
    # Extract thread hash from filename
    m = re.search(r"thread-(\d+)\.txt$", filepath)
    thread_hash = m.group(1) if m else "unknown"

    with open(filepath, "r") as f:
        for line in f:
            line = line.rstrip()
            # Match: [<tsc>] ENTER <addr> (<addr>)
            m = re.match(r"\s+\[(\d+)\] ENTER (\S+)", line)
            if m:
                events.append(TraceEvent(
                    tsc=int(m.group(1)),
                    thread_id=thread_label,
                    thread_hash=thread_hash,
                    event_type="ENTER",
                    detail=m.group(2),
                    raw_line=line,
                ))
                continue
            # Match: [<tsc>] EVENT <type> <rest>
            m = re.match(r"\s+\[(\d+)\] EVENT (\S+)\s*(.*)", line)
            if m:
                event_type = m.group(2)
                rest = m.group(3)
                # Extract obj= pointer if present
                obj_match = re.search(r"obj=(\S+)", rest)
                obj = obj_match.group(1) if obj_match else None
                events.append(TraceEvent(
                    tsc=int(m.group(1)),
                    thread_id=thread_label,
                    thread_hash=thread_hash,
                    event_type=event_type,
                    detail=rest,
                    obj=obj,
                    raw_line=line,
                ))
                continue
    return events


def resolve_symbols(binary: str, addresses: Set[str]) -> Dict[str, str]:
    """Batch-resolve hex addresses to symbol names using addr2line."""
    if not addresses or not binary:
        return {}
    # Filter to valid hex addresses
    valid = sorted(a for a in addresses if re.match(r"0x[0-9a-fA-F]+", a))
    if not valid:
        return {}

    try:
        proc = subprocess.run(
            ["addr2line", "-f", "-C", "-e", binary] + valid,
            capture_output=True, text=True, timeout=30,
        )
        lines = proc.stdout.strip().split("\n")
        result = {}
        for i in range(0, len(lines) - 1, 2):
            func = lines[i].strip()
            loc = lines[i + 1].strip()
            addr = valid[i // 2]
            if func and func != "??":
                # Shorten common prefixes
                func = func.replace("rocksdb::", "r::")
                func = func.replace("std::", "s::")
                # Truncate long signatures
                if len(func) > 80:
                    paren = func.find("(")
                    if paren > 0:
                        func = func[:paren] + "(...)"
                result[addr] = func
            else:
                result[addr] = addr
        return result
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return {}


def assign_object_names(events: List[TraceEvent]) -> Dict[str, str]:
    """Assign human-readable names to mutex/CV objects based on usage patterns."""
    obj_threads: Dict[str, Set[str]] = defaultdict(set)
    obj_types: Dict[str, Set[str]] = defaultdict(set)
    obj_first_tsc: Dict[str, int] = {}

    for e in events:
        if e.obj:
            obj_threads[e.obj].add(e.thread_id)
            obj_types[e.obj].add(e.event_type)
            if e.obj not in obj_first_tsc:
                obj_first_tsc[e.obj] = e.tsc

    names = {}
    mutex_count = 0
    cv_count = 0
    for obj in sorted(obj_first_tsc, key=lambda o: obj_first_tsc[o]):
        types = obj_types[obj]
        n_threads = len(obj_threads[obj])
        if any("CV_" in t for t in types):
            names[obj] = f"cv{cv_count}({n_threads}t)"
            cv_count += 1
        elif any("MUTEX_" in t for t in types):
            names[obj] = f"mtx{mutex_count}({n_threads}t)"
            mutex_count += 1
        else:
            names[obj] = obj[-6:]  # last 6 hex chars
    return names


@dataclass
class ContentionEvent:
    """A period where one thread held a mutex while another waited."""
    mutex: str
    holder_thread: str
    holder_lock_tsc: int
    waiter_thread: str
    waiter_lock_tsc: int  # when waiter started trying to lock
    waiter_got_tsc: int   # when waiter actually got the lock
    wait_cycles: int      # waiter_got_tsc - waiter_lock_tsc


def find_contention(events: List[TraceEvent], obj_names: Dict[str, str]) -> List[ContentionEvent]:
    """Find mutex contention: cases where lock acquisition took a long time,
    indicating another thread was holding it."""
    # Track per-mutex state
    # For each mutex, track who holds it and when they got it
    mutex_holder: Dict[str, Tuple[str, int]] = {}  # obj -> (thread, lock_tsc)
    # Track pending lock attempts (thread waiting for mutex)
    # We infer contention from MUTEX_LOCK events with long gaps between
    # the surrounding function calls

    # Build per-thread event sequences
    thread_events: Dict[str, List[TraceEvent]] = defaultdict(list)
    for e in events:
        if e.obj and "MUTEX_" in e.event_type:
            thread_events[e.thread_id].append(e)

    # Simulate mutex state timeline
    # Sort ALL mutex events globally by tsc
    mutex_events = sorted(
        [e for e in events if e.obj and "MUTEX_" in e.event_type],
        key=lambda e: e.tsc,
    )

    contentions = []
    for e in mutex_events:
        if e.event_type == "MUTEX_LOCK":
            # Check if someone else held this mutex
            if e.obj in mutex_holder:
                holder_thread, holder_tsc = mutex_holder[e.obj]
                if holder_thread != e.thread_id:
                    # This is NOT actual contention detection (we'd need
                    # MUTEX_LOCK_BEGIN/END for that). Instead, we log
                    # ownership transfers.
                    pass
            mutex_holder[e.obj] = (e.thread_id, e.tsc)
        elif e.event_type == "MUTEX_UNLOCK":
            if e.obj in mutex_holder:
                del mutex_holder[e.obj]

    # Simpler approach: find MUTEX_LOCK events where the hold time
    # (lock -> unlock) overlaps with another thread's lock attempt.
    # Build lock intervals per mutex.
    lock_intervals: Dict[str, List[Tuple[str, int, int]]] = defaultdict(list)
    pending_locks: Dict[Tuple[str, str], int] = {}  # (obj, thread) -> lock_tsc

    for e in mutex_events:
        key = (e.obj, e.thread_id)
        if e.event_type == "MUTEX_LOCK":
            pending_locks[key] = e.tsc
        elif e.event_type == "MUTEX_UNLOCK":
            if key in pending_locks:
                lock_intervals[e.obj].append((e.thread_id, pending_locks[key], e.tsc))
                del pending_locks[key]

    # Find overlapping intervals (contention)
    for obj, intervals in lock_intervals.items():
        intervals.sort(key=lambda x: x[1])  # sort by lock time
        for i in range(len(intervals)):
            t1, start1, end1 = intervals[i]
            for j in range(i + 1, min(i + 5, len(intervals))):
                t2, start2, end2 = intervals[j]
                if t1 != t2 and start2 < end1:
                    # Thread t2 got lock while t1 still held it?
                    # Actually t2's lock_tsc is AFTER acquisition,
                    # so this means t2 acquired right after t1 released.
                    # Approximate contention: t2 tried to lock before t1 unlocked.
                    wait_cycles = start2 - end1 if start2 > end1 else end1 - start2
                    if start2 < end1:
                        contentions.append(ContentionEvent(
                            mutex=obj_names.get(obj, obj),
                            holder_thread=t1,
                            holder_lock_tsc=start1,
                            waiter_thread=t2,
                            waiter_lock_tsc=start2,
                            waiter_got_tsc=start2,
                            wait_cycles=end1 - start2,
                        ))
    return sorted(contentions, key=lambda c: c.wait_cycles, reverse=True)


def format_tsc(tsc: int, base_tsc: int) -> str:
    """Format TSC as relative microseconds (approximate, ~2GHz assumed)."""
    delta = tsc - base_tsc
    us = delta / 2000  # rough conversion: 2 GHz = 2000 cycles/us
    if us < 1000:
        return f"+{us:.0f}us"
    elif us < 1000000:
        return f"+{us / 1000:.1f}ms"
    else:
        return f"+{us / 1000000:.2f}s"


def main():
    parser = argparse.ArgumentParser(description="Analyze stress trace dumps")
    parser.add_argument("trace_dir", help="Directory containing trace-*.txt files")
    parser.add_argument("--binary", "-b", help="Path to db_stress binary for symbol resolution")
    parser.add_argument("--sync-only", "-s", action="store_true",
                        help="Show only sync events (mutex/CV/SyncPoint)")
    parser.add_argument("--contention", "-c", action="store_true",
                        help="Find and display mutex contention")
    parser.add_argument("--last", "-n", type=int, default=0,
                        help="Show only last N events per thread")
    parser.add_argument("--threads", "-t", type=int, default=0,
                        help="Show only top N threads by event count")
    parser.add_argument("--merge", "-m", action="store_true",
                        help="Merge all threads into unified timeline (default)")
    parser.add_argument("--no-funcs", action="store_true",
                        help="Exclude function ENTER events from output")
    args = parser.parse_args()

    trace_dir = args.trace_dir
    if not os.path.isdir(trace_dir):
        # Maybe it's a single file
        if os.path.isfile(trace_dir):
            trace_dir = os.path.dirname(trace_dir)
        else:
            print(f"Error: {trace_dir} is not a directory", file=sys.stderr)
            sys.exit(1)

    # Find all trace files
    trace_files = sorted([
        os.path.join(trace_dir, f) for f in os.listdir(trace_dir)
        if f.startswith("trace-") and f.endswith(".txt")
    ])
    if not trace_files:
        print(f"No trace-*.txt files found in {trace_dir}", file=sys.stderr)
        sys.exit(1)

    # Parse all threads
    all_events: List[TraceEvent] = []
    thread_labels = {}
    label_idx = 0
    nonempty = 0

    for filepath in trace_files:
        if os.path.getsize(filepath) == 0:
            continue
        m = re.search(r"thread-(\d+)\.txt$", filepath)
        thread_hash = m.group(1) if m else "unknown"
        label = f"T{label_idx}"
        thread_labels[thread_hash] = label
        label_idx += 1

        events = parse_trace_file(filepath, label)
        if events:
            nonempty += 1
            if args.last > 0:
                events = events[-args.last:]
            all_events.extend(events)

    if not all_events:
        print("No events found in trace files", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(all_events)} events from {nonempty} threads", file=sys.stderr)

    # Resolve symbols if binary provided
    sym_map = {}
    if args.binary:
        addrs = set(e.detail for e in all_events if e.event_type == "ENTER")
        # Also extract from (addr) form
        for e in all_events:
            if e.event_type == "ENTER":
                m = re.match(r"(0x[0-9a-fA-F]+)", e.detail)
                if m:
                    addrs.add(m.group(1))
        print(f"Resolving {len(addrs)} unique addresses...", file=sys.stderr)
        sym_map = resolve_symbols(args.binary, addrs)
        print(f"Resolved {len([v for v in sym_map.values() if not v.startswith('0x')])} symbols", file=sys.stderr)

    # Assign object names
    obj_names = assign_object_names(all_events)
    if obj_names:
        print(f"\nObject registry ({len(obj_names)} objects):", file=sys.stderr)
        for obj, name in sorted(obj_names.items(), key=lambda x: x[1]):
            print(f"  {name:20s} = {obj}", file=sys.stderr)
        print(file=sys.stderr)

    # Filter events
    if args.sync_only or args.no_funcs:
        all_events = [e for e in all_events if e.event_type != "ENTER"]

    # Select top threads
    if args.threads > 0:
        thread_counts = defaultdict(int)
        for e in all_events:
            thread_counts[e.thread_id] += 1
        top_threads = set(
            t for t, _ in sorted(thread_counts.items(), key=lambda x: -x[1])[:args.threads]
        )
        all_events = [e for e in all_events if e.thread_id in top_threads]

    # Contention analysis
    if args.contention:
        contentions = find_contention(all_events, obj_names)
        if contentions:
            base_tsc = min(e.tsc for e in all_events)
            print(f"\n{'='*70}")
            print(f"MUTEX CONTENTION ({len(contentions)} events)")
            print(f"{'='*70}")
            for c in contentions[:20]:
                print(f"  {c.mutex}: {c.holder_thread} held, {c.waiter_thread} waited "
                      f"~{c.wait_cycles/2000:.0f}us at {format_tsc(c.holder_lock_tsc, base_tsc)}")
        else:
            print("\nNo mutex contention detected", file=sys.stderr)
        if not args.merge and not args.sync_only:
            return

    # Sort by timestamp for unified timeline
    all_events.sort(key=lambda e: e.tsc)
    base_tsc = all_events[0].tsc if all_events else 0

    # Print unified timeline
    # Determine column widths
    max_thread_len = max(len(e.thread_id) for e in all_events)
    max_time_len = 10

    print(f"\n{'TIME':>{max_time_len}}  {'THREAD':<{max_thread_len}}  EVENT")
    print(f"{'-'*max_time_len}  {'-'*max_thread_len}  {'-'*50}")

    for e in all_events:
        time_str = format_tsc(e.tsc, base_tsc)
        if e.event_type == "ENTER":
            addr = re.match(r"(0x[0-9a-fA-F]+)", e.detail)
            if addr and addr.group(1) in sym_map:
                detail = f"→ {sym_map[addr.group(1)]}"
            else:
                detail = f"→ {e.detail}"
        elif e.obj and e.obj in obj_names:
            # Replace obj pointer with name in detail
            detail = f"{e.event_type} {obj_names[e.obj]}"
            # Add extra info for timed waits
            if "timeout=" in e.detail:
                m = re.search(r"timeout=(\d+)", e.detail)
                if m:
                    detail += f" timeout={m.group(1)}"
            if "timed_out=" in e.detail:
                m = re.search(r"timed_out=(\d+)", e.detail)
                if m:
                    detail += f" timed_out={'yes' if m.group(1)=='1' else 'no'}"
        elif e.event_type == "SYNCPOINT":
            detail = f"⚑ {e.detail}"
        else:
            detail = f"{e.event_type} {e.detail}"

        print(f"{time_str:>{max_time_len}}  {e.thread_id:<{max_thread_len}}  {detail}")


if __name__ == "__main__":
    main()
