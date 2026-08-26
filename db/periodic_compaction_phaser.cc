//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/periodic_compaction_phaser.h"

#include <algorithm>

#include "rocksdb/options.h"  // kDbNameForScheduleSeed / kDbIdForScheduleSeed
#include "util/fastrange.h"
#include "util/hash.h"
#include "util/math128.h"

namespace ROCKSDB_NAMESPACE {

bool PeriodicCompactionPhaser::SetConfig(const std::string& seed,
                                         int recovery_percent) {
  const bool changed = seed != seed_ || recovery_percent != recovery_percent_;
  seed_ = seed;
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
  // Resolve the DB-level base phase (a fixed-point fraction * 2^64).
  uint64_t db_base_phase = 0;
  if (!TryParseExplicitPhaseSeed(seed_, &db_base_phase)) {
    // Not an explicit "0.<frac>" base: hash the seed, applying whole-value
    // token substitution (mirrors db_host_id / kHostnameForDbHostId). db_id_ is
    // read live so the default "__db_id__" resolves against the current
    // identity.
    const std::string* resolved = &seed_;
    if (seed_ == kDbNameForScheduleSeed) {
      resolved = &dbname_;
    } else if (seed_ == kDbIdForScheduleSeed) {
      resolved = &db_id_;
    }
    db_base_phase = Hash64(resolved->data(), resolved->size());
  }
  // Spread this CF quasi-uniformly around the DB base phase (golden ratio), so
  // the DB-level seed is meaningful however it is set (hashed or an explicit
  // base) and CFs of one DB do not share a phase.
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

bool PeriodicCompactionPhaser::TryParseExplicitPhaseSeed(
    const std::string& seed, uint64_t* seed_hash) {
  // Match "0.<digits>": leading "0.", then at least one character, all decimal
  // digits. This deliberately rejects a sign, an exponent ("0.5e3"), a bare
  // "0.", and any trailing characters, so ordinary token/hash seeds fall
  // through unchanged.
  if (seed.size() < 3 || seed[0] != '0' || seed[1] != '.') {
    return false;
  }
  // Accumulate the fraction as a 19-digit fixed-point integer frac_e19 ==
  // floor(fraction * 10^19), reading the first up-to-19 digits and padding
  // zeros on the right. 19 digits is the most that fits in uint64_t (10^19 <
  // 2^64). Excess digits are validated but drop below 2^64 precision, so
  // ignored.
  uint64_t frac_e19 = 0;
  int ndigits = 0;
  for (size_t i = 2; i < seed.size(); ++i) {
    if (seed[i] < '0' || seed[i] > '9') {
      return false;
    }
    if (ndigits < 19) {
      frac_e19 = frac_e19 * 10 + static_cast<uint64_t>(seed[i] - '0');
      ++ndigits;
    }
  }
  for (; ndigits < 19; ++ndigits) {
    frac_e19 *= 10;
  }
  // Map fraction p in [0, 1) to seed_hash == floor(p * 2^64) so that
  // FastRange64(seed_hash, n) == floor(p * n) for any period n. Computed
  // exactly in integer math via a 128-bit intermediate: p * 2^64 == frac_e19 *
  // 2^64 / 10^19 == (frac_e19 * floor(2^127 / 10^19)) >> 63. The magic constant
  // is floor(2^127 / 10^19); flooring it makes the shifted product undershoot
  // by ~1 ULP, so add 1 to round it back up -- then exact fractions land
  // cleanly (0.5 -> 2^63, not 2^63 - 1). The largest input (all-nines) shifts
  // to 2^64 - 3, so the +1 still cannot overflow uint64_t.
  constexpr uint64_t kTwoPow127DivTen19 = 17014118346046923173ULL;
  *seed_hash =
      Lower64of128(Multiply64to128(frac_e19, kTwoPow127DivTen19) >> 63) + 1;
  return true;
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
