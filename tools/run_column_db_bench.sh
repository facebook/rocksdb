#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "${DB_BENCH:-}" ]]; then
  if [[ -x "${SCRIPT_DIR}/db_bench" ]]; then
    DB_BENCH="${SCRIPT_DIR}/db_bench"
  else
    DB_BENCH="${SCRIPT_DIR}/../db_bench"
  fi
fi
WORKDIR="${1:-${WORKDIR:-/tmp/column_db_bench_${USER:-user}}}"
OUTDIR="${WORKDIR}/results"

NUM="${NUM:-1000}"
READS="${READS:-20000}"
THREADS="${THREADS:-1}"
KEY_SIZE="${KEY_SIZE:-16}"
FEATURES="${FEATURES:-1000}"
VALUE_SIZES="${VALUE_SIZES:-102400 1048576}"
PROJECTED_FEATURES="${PROJECTED_FEATURES:-5 10}"
PROJECTION_STRIDES="${PROJECTION_STRIDES:-1 97}"
BLOB_FILE_SIZE="${BLOB_FILE_SIZE:-1073741824}"
CACHE_SIZE="${CACHE_SIZE:-0}"
KEEP_DBS="${KEEP_DBS:-0}"
DIRECT_READS="${DIRECT_READS:-true}"
DIRECT_FLUSH="${DIRECT_FLUSH:-true}"
VERIFY_CHECKSUM="${VERIFY_CHECKSUM:-false}"
EXTRA_ARGS="${EXTRA_ARGS:-}"
TIME_BIN="${TIME_BIN:-/usr/bin/time}"

if [[ ! -x "${DB_BENCH}" ]]; then
  echo "db_bench binary is not executable: ${DB_BENCH}" >&2
  exit 1
fi

if [[ ! -x "${TIME_BIN}" ]]; then
  echo "time binary is not executable: ${TIME_BIN}" >&2
  exit 1
fi

mkdir -p "${OUTDIR}"

extra_args=()
if [[ -n "${EXTRA_ARGS}" ]]; then
  # shellcheck disable=SC2206
  extra_args=(${EXTRA_ARGS})
fi

common_args=(
  --num="${NUM}"
  --reads="${READS}"
  --threads="${THREADS}"
  --key_size="${KEY_SIZE}"
  --compression_type=none
  --compression_ratio=1.0
  --enable_blob_files=true
  --min_blob_size=0
  --blob_file_size="${BLOB_FILE_SIZE}"
  --blob_compression_type=none
  --disable_auto_compactions=true
  --cache_size="${CACHE_SIZE}"
  --use_direct_reads="${DIRECT_READS}"
  --use_direct_io_for_flush_and_compaction="${DIRECT_FLUSH}"
  --verify_checksum="${VERIFY_CHECKSUM}"
)

run_timed() {
  local time_log="$1"
  shift
  env LD_LIBRARY_PATH="${SCRIPT_DIR}:${LD_LIBRARY_PATH:-}" \
    "${TIME_BIN}" -v -o "${time_log}" "$@"
}

extract_metric() {
  local benchmark="$1"
  local log_file="$2"
  local metric="$3"
  awk -v benchmark="${benchmark}" -v metric="${metric}" '
    $1 == benchmark && $2 == ":" {
      for (i = 1; i <= NF; ++i) {
        if ($i == "micros/op") {
          micros = $(i - 1)
        } else if ($i == "ops/sec") {
          ops = $(i - 1)
        } else if ($i == "MB/s") {
          mbs = $(i - 1)
        }
      }
    }
    END {
      if (metric == "ops") {
        print ops
      } else if (metric == "micros") {
        print micros
      } else if (metric == "mbs") {
        print mbs
      }
    }
  ' "${log_file}"
}

ratio() {
  local numerator="$1"
  local denominator="$2"
  awk -v numerator="${numerator}" -v denominator="${denominator}" '
    BEGIN {
      if (denominator > 0) {
        printf "%.3f", numerator / denominator
      } else {
        printf "nan"
      }
    }
  '
}

sum_float() {
  local lhs="$1"
  local rhs="$2"
  awk -v lhs="${lhs:-0}" -v rhs="${rhs:-0}" '
    BEGIN {
      printf "%.3f", lhs + rhs
    }
  '
}

extract_time_metric() {
  local log_file="$1"
  local metric="$2"
  awk -v metric="${metric}" '
    function trim(value) {
      sub(/^[ \t]+/, "", value)
      sub(/[ \t]+$/, "", value)
      return value
    }
    function elapsed_to_seconds(value, parts, count) {
      count = split(value, parts, ":")
      if (count == 1) {
        return parts[1]
      }
      if (count == 2) {
        return parts[1] * 60 + parts[2]
      }
      return parts[1] * 3600 + parts[2] * 60 + parts[3]
    }
    {
      split($0, fields, ":")
      key = trim(fields[1])
      value = trim(substr($0, index($0, ":") + 1))
      if (metric == "cpu_pct" && key == "Percent of CPU this job got") {
        gsub(/%/, "", value)
        print value
      } else if (metric == "user_s" && key == "User time (seconds)") {
        print value
      } else if (metric == "sys_s" && key == "System time (seconds)") {
        print value
      } else if (metric == "elapsed_s" && $0 ~ /^Elapsed/) {
        printf "%.3f\n", elapsed_to_seconds($NF)
      } else if (metric == "max_rss_kb" && key == "Maximum resident set size (kbytes)") {
        print value
      } else if (metric == "fs_inputs" && key == "File system inputs") {
        print value
      } else if (metric == "fs_outputs" && key == "File system outputs") {
        print value
      }
    }
  ' "${log_file}"
}

summary="${OUTDIR}/summary.tsv"
printf "value_size\tprojected\tstride\tfull_read_ops\tcolumn_read_ops\tread_speedup\tfull_read_us\tcolumn_read_us\tfull_read_cpu_s\tcolumn_read_cpu_s\tread_cpu_s_ratio\tfull_read_cpu_pct\tcolumn_read_cpu_pct\tfull_read_fs_inputs\tcolumn_read_fs_inputs\tread_fs_input_ratio\tfull_read_fs_outputs\tcolumn_read_fs_outputs\tfull_read_max_rss_kb\tcolumn_read_max_rss_kb\tfull_fill_ops\tcolumn_fill_ops\n" > "${summary}"

for value_size in ${VALUE_SIZES}; do
  for projected in ${PROJECTED_FEATURES}; do
    for stride in ${PROJECTION_STRIDES}; do
      tag="value${value_size}_projected${projected}_stride${stride}"
      full_db="${WORKDIR}/db_full_${tag}"
      column_db="${WORKDIR}/db_column_${tag}"
      external_dir="${WORKDIR}/external_${tag}"
      full_fill_log="${OUTDIR}/full_fill_${tag}.log"
      full_read_log="${OUTDIR}/full_read_${tag}.log"
      column_fill_log="${OUTDIR}/column_fill_${tag}.log"
      column_read_log="${OUTDIR}/column_read_${tag}.log"
      full_fill_time="${OUTDIR}/full_fill_${tag}.time"
      full_read_time="${OUTDIR}/full_read_${tag}.time"
      column_fill_time="${OUTDIR}/column_fill_${tag}.time"
      column_read_time="${OUTDIR}/column_read_${tag}.time"

      rm -rf "${full_db}" "${column_db}" "${external_dir}"

      echo "Loading full blob baseline: ${tag}"
      run_timed "${full_fill_time}" \
        "${DB_BENCH}" \
        "${common_args[@]}" \
        "${extra_args[@]}" \
        --benchmarks=fillseq \
        --db="${full_db}" \
        --value_size="${value_size}" \
        > "${full_fill_log}" 2>&1

      echo "Reading full blob baseline: ${tag}"
      run_timed "${full_read_time}" \
        "${DB_BENCH}" \
        "${common_args[@]}" \
        "${extra_args[@]}" \
        --use_existing_db=true \
        --benchmarks=fullblobreadrandom \
        --db="${full_db}" \
        --value_size="${value_size}" \
        > "${full_read_log}" 2>&1

      echo "Loading ColumnDB projected database: ${tag}"
      run_timed "${column_fill_time}" \
        "${DB_BENCH}" \
        "${common_args[@]}" \
        "${extra_args[@]}" \
        --benchmarks=columnblobfillseq \
        --db="${column_db}" \
        --value_size="${value_size}" \
        --column_db_num_features="${FEATURES}" \
        --column_db_projected_features="${projected}" \
        --column_db_projection_stride="${stride}" \
        --column_db_external_dir="${external_dir}" \
        > "${column_fill_log}" 2>&1

      echo "Reading ColumnDB projected columns: ${tag}"
      run_timed "${column_read_time}" \
        "${DB_BENCH}" \
        "${common_args[@]}" \
        "${extra_args[@]}" \
        --use_existing_db=true \
        --benchmarks=columnblobreadrandom \
        --db="${column_db}" \
        --value_size="${value_size}" \
        --column_db_num_features="${FEATURES}" \
        --column_db_projected_features="${projected}" \
        --column_db_projection_stride="${stride}" \
        --column_db_external_dir="${external_dir}" \
        > "${column_read_log}" 2>&1

      full_read_ops="$(extract_metric fullblobreadrandom "${full_read_log}" ops)"
      column_read_ops="$(extract_metric columnblobreadrandom "${column_read_log}" ops)"
      full_read_us="$(extract_metric fullblobreadrandom "${full_read_log}" micros)"
      column_read_us="$(extract_metric columnblobreadrandom "${column_read_log}" micros)"
      full_fill_ops="$(extract_metric fillseq "${full_fill_log}" ops)"
      column_fill_ops="$(extract_metric columnblobfillseq "${column_fill_log}" ops)"
      speedup="$(ratio "${column_read_ops:-0}" "${full_read_ops:-0}")"

      full_read_user_s="$(extract_time_metric "${full_read_time}" user_s)"
      full_read_sys_s="$(extract_time_metric "${full_read_time}" sys_s)"
      column_read_user_s="$(extract_time_metric "${column_read_time}" user_s)"
      column_read_sys_s="$(extract_time_metric "${column_read_time}" sys_s)"
      full_read_cpu_s="$(sum_float "${full_read_user_s}" "${full_read_sys_s}")"
      column_read_cpu_s="$(sum_float "${column_read_user_s}" "${column_read_sys_s}")"
      cpu_s_ratio="$(ratio "${column_read_cpu_s}" "${full_read_cpu_s}")"
      full_read_cpu_pct="$(extract_time_metric "${full_read_time}" cpu_pct)"
      column_read_cpu_pct="$(extract_time_metric "${column_read_time}" cpu_pct)"
      full_read_fs_inputs="$(extract_time_metric "${full_read_time}" fs_inputs)"
      column_read_fs_inputs="$(extract_time_metric "${column_read_time}" fs_inputs)"
      fs_input_ratio="$(ratio "${column_read_fs_inputs:-0}" "${full_read_fs_inputs:-0}")"
      full_read_fs_outputs="$(extract_time_metric "${full_read_time}" fs_outputs)"
      column_read_fs_outputs="$(extract_time_metric "${column_read_time}" fs_outputs)"
      full_read_max_rss_kb="$(extract_time_metric "${full_read_time}" max_rss_kb)"
      column_read_max_rss_kb="$(extract_time_metric "${column_read_time}" max_rss_kb)"

      printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "${value_size}" "${projected}" "${stride}" \
        "${full_read_ops}" "${column_read_ops}" "${speedup}" \
        "${full_read_us}" "${column_read_us}" \
        "${full_read_cpu_s}" "${column_read_cpu_s}" "${cpu_s_ratio}" \
        "${full_read_cpu_pct}" "${column_read_cpu_pct}" \
        "${full_read_fs_inputs}" "${column_read_fs_inputs}" "${fs_input_ratio}" \
        "${full_read_fs_outputs}" "${column_read_fs_outputs}" \
        "${full_read_max_rss_kb}" "${column_read_max_rss_kb}" \
        "${full_fill_ops}" "${column_fill_ops}" \
        >> "${summary}"

      if [[ "${KEEP_DBS}" != "1" ]]; then
        rm -rf "${full_db}" "${column_db}" "${external_dir}"
      fi
    done
  done
done

echo "Summary: ${summary}"
cat "${summary}"
