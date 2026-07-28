#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

# pyre-strict

from __future__ import annotations

import json
import logging
import os
from collections import Counter
from dataclasses import asdict

from runner_build import Run
from runner_execute import OUTCOMES

logger: logging.Logger = logging.getLogger("runner")


def report(
    runs: list[Run], op: str, report_dir: str, base_seed: int
) -> dict[str, object]:
    counts = Counter(run.outcome for run in runs)
    summary: dict[str, object] = {
        "op": op,
        "base_seed": base_seed,  # re-run with --seed=<base_seed>; run i used base_seed+i
        "outcome_counts": {bucket: counts.get(bucket, 0) for bucket in OUTCOMES},
        "runs": [asdict(run) for run in runs],
    }
    with open(os.path.join(report_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    errors = counts.get("ERROR", 0)
    if errors:
        # ERROR is a per-run harness failure, not a db_stress data outcome, and separate
        # from the setup failures that fail fast in the preflight. Two causes, both rare
        # but expected: a timed-out run (a deadlock from the injected corruption, or just
        # slow under load), or gdb crashing mid-run / an injector bug (rarer, and worth
        # investigating -- it may point at something to fix). Surface the count, never fail
        # the sweep (no retry by design); dig in if it climbs.
        logger.warning(
            "%d/%d runs ended in ERROR; monitor that this stays low", errors, len(runs)
        )
    return summary
