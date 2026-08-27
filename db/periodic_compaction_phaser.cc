//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/periodic_compaction_phaser.h"

#include <algorithm>

#include "util/fastrange.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

bool PeriodicCompactionPhaser::SetConfig(int recovery_percent) {
  const bool changed = recovery_percent != recovery_percent_;
  recovery_percent_ = recovery_percent;
  return changed;
}

PeriodicCompactionPhaseParams PeriodicCompactionPhaser::ParamsForCf(
    uint32_t cf_id) const {
  PeriodicCompactionPhaseParams params;
  if (recovery_percent_ <= 0) {
    // Phasing disabled; return default (disabled) params.
    return params;
  }
  // DB-level base phase (a fixed-point fraction * 2^64), hashed from the DB ID.
  // The DB ID is stable across re-open and physical cloning/migration but
  // distinct across DBs, so DBs configured alike do not herd. db_id_ is read
  // live because it is populated after construction (during DB::Open): versions
  // built before it is known hash an empty id and self-heal on later versions.
  const uint64_t db_base_phase = Hash64(db_id_.data(), db_id_.size());
  // Spread this CF quasi-uniformly around the DB base phase (golden ratio) so
  // CFs of one DB do not share a phase.
  params.seed_hash = CfPhaseSeedHash(db_base_phase, cf_id);
  params.anchor_time = anchor_time_;
  params.recovery_percent = std::min(recovery_percent_, 100);
  return params;
}

uint64_t PeriodicCompactionPhaser::TriggerTime(
    uint64_t file_modification_time, uint64_t periodic_compaction_seconds,
    const PeriodicCompactionPhaseParams& params) {
  const uint64_t n = periodic_compaction_seconds;
  // Unphased deadline: file age reaches periodic_compaction_seconds. This is a
  // hard upper bound on age; phasing only ever triggers earlier.
  const uint64_t natural_trigger = file_modification_time + n;
  const int recovery_percent = params.recovery_percent;
  if (recovery_percent <= 0 || n == 0) {
    return natural_trigger;
  }
  // Preferred-phase time within each period, floor(p*n) with p == seed / 2^64.
  const uint64_t target = FastRange64(params.seed_hash, n);
  // Distance (in [0, n)) from the natural deadline back to the nearest
  // preferred-phase time at or before it.
  const uint64_t offset = (file_modification_time % n + n - target) % n;
  // Close recovery_percent% of that gap, triggering earlier. Over successive
  // re-stamped cycles the phase error decays geometrically toward the phase.
  // (A "snap to the exact phase" refinement was considered and rejected: under
  // unpredictable scheduling/queueing latency -- the delay between a file
  // becoming due and its periodic compaction actually running -- a snap can
  // split the fleet between the phase and a latency-induced fixed point
  // (re-herding, the opposite of the goal), whereas the plain geometric pull
  // degrades gracefully to a uniform, still well-spread shift.)
  const uint64_t early_pull =
      offset * static_cast<uint64_t>(std::min(recovery_percent, 100)) / 100;
  const uint64_t recovery_trigger = natural_trigger - early_pull;
  const uint64_t anchor = params.anchor_time;
  if (natural_trigger <= anchor) {
    // A whole cohort can be past its deadline when phasing took effect: after
    // the DB was down (e.g. DC power-outage recovery) or after
    // periodic_compaction_seconds was turned down. Instead of firing the cohort
    // immediately (a herd), spread it over the first quarter-period after the
    // anchor, on an absolute (epoch-aligned) grid of period n/4 with a stable
    // per-(DB, CF) phase: bounded lateness (<= ~n/4) in exchange for
    // anti-herding. The grid is absolute (not anchor + offset) so a rapidly
    // restarting DB is not perpetually re-anchored just short of its trigger
    // and thereby starved -- the grid point is a fixed wall-clock time that
    // arrives regardless of restarts.
    const uint64_t grid = n / 4;
    if (grid == 0) {
      return anchor;
    }
    const uint64_t phase = FastRange64(params.seed_hash, grid);
    // Smallest t >= anchor with t % grid == phase.
    return anchor + (phase + grid - anchor % grid) % grid;
  } else if (recovery_trigger >= anchor) {
    return recovery_trigger;
  }
  // Not yet past due, but the recovery target lands before the anchor: spread
  // the catch-up across [anchor, natural deadline] using the phase (no
  // recovery), so enabling phasing fleet-wide does not fire everything at once,
  // and never later than the natural deadline.
  return anchor + FastRange64(params.seed_hash, natural_trigger - anchor);
}

uint64_t PeriodicCompactionPhaser::CfPhaseSeedHash(uint64_t db_base_phase,
                                                   uint32_t cf_id) {
  // Golden-ratio conjugate ((sqrt(5)-1)/2) as a fixed-point fraction * 2^64.
  // The additive recurrence db_base + cf_id * kGolden (wrapping mod 2^64, i.e.
  // mod 1) is a low-discrepancy Weyl/Kronecker sequence, so a DB's CFs are
  // spread quasi-uniformly around the base phase for any number of CFs.
  constexpr uint64_t kGoldenRatioU64 = 0x9E3779B97F4A7C15ULL;
  return db_base_phase + static_cast<uint64_t>(cf_id) * kGoldenRatioU64;
}

}  // namespace ROCKSDB_NAMESPACE
