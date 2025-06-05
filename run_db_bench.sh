#!/bin/bash

# Script to run db_bench with different parameter combinations
# Parameters varied:
# - compression_manager: mixed, none
# - same_value_percentage: 0, 50, 100
#
# To make this script executable, run: chmod +x run_db_bench.sh

# Exit on error
set -e

# Check if db_bench exists
if [ ! -f "./db_bench" ]; then
    echo "Error: db_bench executable not found in current directory"
    exit 1
fi

# Define parameter arrays
compression_managers=("mixed" "none")
same_value_percentages=(0 50 100)

# Create output directory if it doesn't exist
mkdir -p results

# Loop through all combinations
for cm in "${compression_managers[@]}"; do
    for svp in "${same_value_percentages[@]}"; do
        # Define output file
        output_file="results/bench_${cm}_${svp}.log"

        echo "Running with compression_manager=${cm}, same_value_percentage=${svp}"
        # Run db_bench with current parameters
        ./db_bench -db=/dev/shm/dbbench \
            --benchmarks=fillseq \
            -num=10000000 \
            -compaction_style=2 \
            -fifo_compaction_max_table_files_size_mb=1000 \
            -fifo_compaction_allow_compaction=0 \
            -disable_wal \
            -write_buffer_size=12000000 \
            -compression_type=zstd \
            -compression_parallel_threads=1 \
            -compression_manager="${cm}" \
            -same_value_percentage="${svp}" \
            --stats_level=5 \
            --statistics > "${output_file}" 2>&1

        echo "Completed. Results saved to ${output_file}"
    done
done

echo "All benchmarks completed successfully!"
