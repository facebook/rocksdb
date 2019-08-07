#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
# A shell script to run a batch of pysims and combine individual pysim output files.
#
# Usage: bash block_cache_pysim.sh trace_file_path result_dir downsample_size warmup_seconds max_jobs
# trace_file_path: The file path that stores the traces.
# result_dir: The directory to store pysim results. The output files from a pysim is stores in result_dir/ml
# downsample_size: The downsample size used to collect the trace.
# warmup_seconds: The number of seconds used for warmup.
# max_jobs: The max number of concurrent pysims to run.

# Install required packages to run simulations.
# sudo dnf install -y numpy scipy python-matplotlib ipython python-pandas sympy python-nose atlas-devel
ulimit -c 0

if [ $# -ne 5 ]; then
  echo "Usage: ./block_cache_pysim.sh trace_file_path result_dir downsample_size warmup_seconds max_jobs"
  exit 0
fi

trace_file="$1"
result_dir="$2"
downsample_size="$3"
warmup_seconds="$4"
max_jobs="$5"
max_num_accesses=100000000
current_jobs=1

ml_tmp_result_dir="$result_dir/ml"
rm -rf "$ml_tmp_result_dir"
mkdir -p "$result_dir"
mkdir -p "$ml_tmp_result_dir"

# Report miss ratio in the trace.
current_jobs=$(ps aux | grep pysim | grep python | grep -cv grep)
for cf_name in "all"
do
for cache_size in "1G" "2G" "4G" "8G" "16G" #"12G" "16G" "1T"
do
for cache_type in "opt" "lru" "pylru" "pycctbbt" "pyhb" "ts" "trace" "lru_hybrid"  #"pycctblevelbt" #"lru_hybridn" "opt" #"pylru" "pylru_hybrid" "pycctbbt" "pycccfbt" "trace"
do
    if [[ $cache_type == "trace" && $cache_size != "16G" ]]; then
      # We only need to collect miss ratios observed in the trace once.
      continue
    fi
    while [ "$current_jobs" -ge "$max_jobs" ]
    do
      sleep 10
      echo "Waiting jobs to complete. Number of running jobs: $current_jobs"
      current_jobs=$(ps aux | grep pysim | grep python | grep -cv grep)
      echo "Waiting jobs to complete. Number of running jobs: $current_jobs"
    done
    output="log-ml-$cache_type-$cache_size-$cf_name"
    echo "Running simulation for $cache_type, cache size $cache_size, and cf_name $cf_name. Number of running jobs: $current_jobs. "
    nohup python block_cache_pysim.py "$cache_type" "$cache_size" "$downsample_size" "$warmup_seconds" "$trace_file" "$ml_tmp_result_dir" "$max_num_accesses" "$cf_name" >& "$ml_tmp_result_dir/$output" &
    current_jobs=$((current_jobs+1))
done
done
done

# Wait for all jobs to complete.
while [ $current_jobs -gt 0 ]
do
  sleep 10
  echo "Waiting jobs to complete. Number of running jobs: $current_jobs"
  current_jobs=$(ps aux | grep pysim | grep python | grep -cv grep)
  echo "Waiting jobs to complete. Number of running jobs: $current_jobs"
done

echo "Combine individual pysim output files"

rm -rf "$result_dir/ml_*"
for header in "header-" "data-"
do
for fn in "$ml_tmp_result_dir"/*
do
  sum_file=""
  time_unit=""
  capacity=""
  target_cf_name=""
  if [[ $fn == *"timeline"* ]]; then
    tmpfn="$fn"
    IFS='-' read -ra elements <<< "$tmpfn"
    time_unit_index=0
    capacity_index=0
    for i in "${elements[@]}"
    do
       if [[ $i == "timeline" ]]; then
         break
       fi
       time_unit_index=$((time_unit_index+1))
    done
    time_unit_index=$((time_unit_index+1))
    capacity_index=$((time_unit_index+2))
    target_cf_name_index=$((time_unit_index+3))
    time_unit="${elements[$time_unit_index]}_"
    capacity="${elements[$capacity_index]}_"
    target_cf_name="${elements[$target_cf_name_index]}_"
  fi

  if [[ $fn == *"${header}ml-policy-timeline"* ]]; then
    sum_file="$result_dir/ml_${target_cf_name}${capacity}${time_unit}policy_timeline"
  fi
  if [[ $fn == *"${header}ml-policy-ratio-timeline"* ]]; then
    sum_file="$result_dir/ml_${target_cf_name}${capacity}${time_unit}policy_ratio_timeline"
  fi
  if [[ $fn == *"${header}ml-miss-timeline"* ]]; then
    sum_file="$result_dir/ml_${target_cf_name}${capacity}${time_unit}miss_timeline"
  fi
  if [[ $fn == *"${header}ml-miss-ratio-timeline"* ]]; then
    sum_file="$result_dir/ml_${target_cf_name}${capacity}${time_unit}miss_ratio_timeline"
  fi
  if [[ $fn == *"${header}ml-mrc"* ]]; then
    tmpfn="$fn"
    IFS='-' read -ra elements <<< "$tmpfn"
    target_cf_name=${elements[-1]}
    sum_file="${result_dir}/ml_${target_cf_name}_mrc"
  fi
  if [[ $fn == *"${header}ml-avgmb"* ]]; then
    tmpfn="$fn"
    IFS='-' read -ra elements <<< "$tmpfn"
    time_unit=${elements[3]}
    target_cf_name=${elements[-1]}
    sum_file="${result_dir}/ml_${time_unit}_${target_cf_name}_avgmb"
  fi
  if [[ $fn == *"${header}ml-p95mb"* ]]; then
    tmpfn="$fn"
    IFS='-' read -ra elements <<< "$tmpfn"
    time_unit=${elements[3]}
    target_cf_name=${elements[-1]}
    sum_file="${result_dir}/ml_${time_unit}_${target_cf_name}_p95mb"
  fi
  if [[ $sum_file == "" ]]; then
    continue
  fi
  if [[ $header == "header-" ]]; then
    if [ -e "$sum_file" ]; then
      continue
    fi
  fi
  cat "$fn" >> "$sum_file"
done
done

echo "Done"
for fn in $result_dir/*
do
  if [[ $fn == *"_mrc" || $fn == *"_avgmb" || $fn == *"_p95mb" ]]; then
    # Sort MRC file by cache_type and cache_size.
    tmp_file="$result_dir/tmp_mrc"
    cat "$fn" | sort -t ',' -k1,1 -k4,4n > "$tmp_file"
    cat "$tmp_file" > "$fn"
    rm -rf "$tmp_file"
  fi
done
