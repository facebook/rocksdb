#!/usr/bin/env python3
#  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

"""Run benchmark_compare.sh on the most recent build, for CI
"""

import argparse
import glob
import logging
import os
import re
import shutil
import subprocess
import sys

logging.basicConfig(level=logging.INFO)


class Config:
    def __init__(self, args):
        self.version_file = "./include/rocksdb/version.h"
        self.data_dir = os.path.expanduser(f"{args.db_dir}")
        self.results_dir = os.path.expanduser(f"{args.output_dir}")
        self.benchmark_script = f"{os.getcwd()}/tools/benchmark_compare.sh"
        self.benchmark_cwd = f"{os.getcwd()}/tools"

    benchmark_env_keys = [
        "LD_LIBRARY_PATH",
        "NUM_KEYS",
        "KEY_SIZE",
        "VALUE_SIZE",
        "CACHE_SIZE_MB",
        "DURATION_RW",
        "DURATION_RO",
        "MB_WRITE_PER_SEC",
        "NUM_THREADS",
        "COMPRESSION_TYPE",
        "MIN_LEVEL_TO_COMPRESS",
        "WRITE_BUFFER_SIZE_MB",
        "TARGET_FILE_SIZE_BASE_MB",
        "MAX_BYTES_FOR_LEVEL_BASE_MB",
        "MAX_BACKGROUND_JOBS",
        "CACHE_INDEX_AND_FILTER_BLOCKS",
        "USE_O_DIRECT",
        "STATS_INTERVAL_SECONDS",
        "SUBCOMPACTIONS",
        "COMPACTION_STYLE",
        "CI_TESTS_ONLY",
    ]


def read_version(config):
    majorRegex = re.compile(r"#define ROCKSDB_MAJOR\s([0-9]+)")
    minorRegex = re.compile(r"#define ROCKSDB_MINOR\s([0-9]+)")
    patchRegex = re.compile(r"#define ROCKSDB_PATCH\s([0-9]+)")
    with open(config.version_file, "r") as reader:
        major = None
        minor = None
        patch = None
        for line in reader:
            if major is None:
                major = majorRegex.match(line)
            elif minor is None:
                minor = minorRegex.match(line)
            elif patch is None:
                patch = patchRegex.match(line)

            if patch is not None:
                break

        if patch is not None:
            return (major.group(1), minor.group(1), patch.group(1))

    # Didn't complete a match
    return None


def prepare(version_str, config):
    old_files = glob.glob(f"{config.results_dir}/{version_str}/**", recursive=True)
    for f in old_files:
        if os.path.isfile(f):
            logging.debug(f"remove file {f}")
            os.remove(f)
    for f in old_files:
        if os.path.isdir(f):
            logging.debug(f"remove dir {f}")
            os.rmdir(f)

    db_bench_vers = f"{config.benchmark_cwd}/db_bench.{version_str}"

    # Create a symlink to the db_bench executable
    os.symlink(f"{os.getcwd()}/db_bench", db_bench_vers)


def results(version_str, config):
    # Copy the report TSV file back to the top level of results
    shutil.copyfile(
        f"{config.results_dir}/{version_str}/report.tsv",
        f"{config.results_dir}/report.tsv",
    )


def cleanup(version_str, config):
    # Remove the symlink to the db_bench executable
    db_bench_vers = f"{config.benchmark_cwd}/db_bench.{version_str}"
    os.remove(db_bench_vers)


def get_benchmark_env():
    env = []
    for key in Config.benchmark_env_keys:
        value = os.getenv(key)
        if value is not None:
            env.append((key, value))
    return env


def main():
    """Tool for running benchmark_compare.sh on the most recent build, for CI
    This tool will

    (1) Work out the current version of RocksDB
    (2) Run benchmark_compare with that version alone
    """

    parser = argparse.ArgumentParser(
        description="benchmark_compare.sh Python wrapper for CI."
    )

    # --tsvfile is the name of the file to read results from
    # --esdocument is the ElasticSearch document to push these results into
    #
    parser.add_argument(
        "--db_dir",
        default="~/tmp/rocksdb-benchmark-datadir",
        help="Database directory hierarchy to use",
    )
    parser.add_argument(
        "--output_dir",
        default="~/tmp/benchmark-results",
        help="Benchmark output goes here",
    )
    parser.add_argument(
        "--num_keys",
        default="10000",
        help="Number of database keys to use in benchmark test(s) (determines size of test job)",
    )
    args = parser.parse_args()
    config = Config(args)

    version = read_version(config)
    if version is None:
        raise Exception(f"Could not read RocksDB version from {config.version_file}")
    version_str = f"{version[0]}.{version[1]}.{version[2]}"
    logging.info(f"Run benchmark_ci with RocksDB version {version_str}")

    prepare(version_str, config)

    try:
        env = get_benchmark_env()
        env.append(("NUM_KEYS", args.num_keys))
        cmd = [
            config.benchmark_script,
            config.data_dir,
            config.results_dir,
            version_str,
        ]
        logging.info(f"Run {cmd} env={env} cwd={config.benchmark_cwd}")
        subprocess.run(cmd, env=dict(env), cwd=config.benchmark_cwd)

        results(version_str, config)
    finally:
        cleanup(version_str, config)

    return 0


if __name__ == "__main__":
    sys.exit(main())
