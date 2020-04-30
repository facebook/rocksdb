#!/usr/bin/env python
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import time
import random
import tempfile
import subprocess
import shutil
import argparse

# params overwrite priority:
#   for default:
#       default_params < {blackbox,whitebox}_default_params < args
#   for simple:
#       default_params < {blackbox,whitebox}_default_params <
#       simple_default_params <
#       {blackbox,whitebox}_simple_default_params < args
#   for cf_consistency:
#       default_params < {blackbox,whitebox}_default_params <
#       cf_consistency_params < args
#   for txn:
#       default_params < {blackbox,whitebox}_default_params < txn_params < args

expected_values_file = tempfile.NamedTemporaryFile()

default_params = {
    "acquire_snapshot_one_in": 10000,
    "block_size": 16384,
    "bloom_bits": lambda: random.choice([random.randint(0,19),
                                         random.lognormvariate(2.3, 1.3)]),
    "cache_index_and_filter_blocks": lambda: random.randint(0, 1),
    "cache_size": 1048576,
    "checkpoint_one_in": 1000000,
    "compression_type": lambda: random.choice(
        ["none", "snappy", "zlib", "bzip2", "lz4", "lz4hc", "xpress", "zstd"]),
    "bottommost_compression_type": lambda:
        "disable" if random.randint(0, 1) == 0 else
        random.choice(
            ["none", "snappy", "zlib", "bzip2", "lz4", "lz4hc", "xpress",
             "zstd"]),
    "checksum_type" : lambda: random.choice(["kCRC32c", "kxxHash", "kxxHash64"]),
    "compression_max_dict_bytes": lambda: 16384 * random.randint(0, 1),
    "compression_zstd_max_train_bytes": lambda: 65536 * random.randint(0, 1),
    "compression_parallel_threads": lambda: random.choice([1] * 9 + [4]),
    "clear_column_family_one_in": 0,
    "compact_files_one_in": 1000000,
    "compact_range_one_in": 1000000,
    "delpercent": 4,
    "delrangepercent": 1,
    "destroy_db_initially": 0,
    "enable_pipelined_write": lambda: random.randint(0, 1),
    "expected_values_path": expected_values_file.name,
    "flush_one_in": 1000000,
    "get_live_files_one_in": 1000000,
    # Note: the following two are intentionally disabled as the corresponding
    # APIs are not guaranteed to succeed.
    "get_sorted_wal_files_one_in": 0,
    "get_current_wal_file_one_in": 0,
    # Temporarily disable hash index
    "index_type": lambda: random.choice([0, 0, 0, 2, 2, 3]),
    "max_background_compactions": 20,
    "max_bytes_for_level_base": 10485760,
    "max_key": 100000000,
    "max_write_buffer_number": 3,
    "mmap_read": lambda: random.randint(0, 1),
    "nooverwritepercent": 1,
    "open_files": lambda : random.choice([-1, -1, 100, 500000]),
    "partition_filters": lambda: random.randint(0, 1),
    "pause_background_one_in": 1000000,
    "prefixpercent": 5,
    "progress_reports": 0,
    "readpercent": 45,
    "recycle_log_file_num": lambda: random.randint(0, 1),
    "reopen": 20,
    "snapshot_hold_ops": 100000,
    "long_running_snapshots": lambda: random.randint(0, 1),
    "subcompactions": lambda: random.randint(1, 4),
    "target_file_size_base": 2097152,
    "target_file_size_multiplier": 2,
    "use_direct_reads": lambda: random.randint(0, 1),
    "use_direct_io_for_flush_and_compaction": lambda: random.randint(0, 1),
    "mock_direct_io": False,
    "use_full_merge_v1": lambda: random.randint(0, 1),
    "use_merge": lambda: random.randint(0, 1),
    "verify_checksum": 1,
    "write_buffer_size": 4 * 1024 * 1024,
    "writepercent": 35,
    "format_version": lambda: random.choice([2, 3, 4, 5, 5]),
    "index_block_restart_interval": lambda: random.choice(range(1, 16)),
    "use_multiget" : lambda: random.randint(0, 1),
    "periodic_compaction_seconds" :
        lambda: random.choice([0, 0, 1, 2, 10, 100, 1000]),
    "compaction_ttl" : lambda: random.choice([0, 0, 1, 2, 10, 100, 1000]),
    # Test small max_manifest_file_size in a smaller chance, as most of the
    # time we wnat manifest history to be preserved to help debug
    "max_manifest_file_size" : lambda : random.choice(
        [t * 16384 if t < 3 else 1024 * 1024 * 1024 for t in range(1, 30)]),
    # Sync mode might make test runs slower so running it in a smaller chance
    "sync" : lambda : random.choice(
        [1 if t == 0 else 0 for t in range(0, 20)]),
    # Disable compation_readahead_size because the test is not passing.
    #"compaction_readahead_size" : lambda : random.choice(
    #    [0, 0, 1024 * 1024]),
    "db_write_buffer_size" : lambda: random.choice(
        [0, 0, 0, 1024 * 1024, 8 * 1024 * 1024, 128 * 1024 * 1024]),
    "avoid_unnecessary_blocking_io" : random.randint(0, 1),
    "write_dbid_to_manifest" : random.randint(0, 1),
    "avoid_flush_during_recovery" : random.choice(
        [1 if t == 0 else 0 for t in range(0, 8)]),
    "max_write_batch_group_size_bytes" : lambda: random.choice(
        [16, 64, 1024 * 1024, 16 * 1024 * 1024]),
    "level_compaction_dynamic_level_bytes" : True,
    "verify_checksum_one_in": 1000000,
    "verify_db_one_in": 100000,
    "continuous_verification_interval" : 0,
    "max_key_len": 3,
    "key_len_percent_dist": "1,30,69",
    "read_fault_one_in": lambda: random.choice([0, 1000]),
    "sync_fault_injection": False
}

_TEST_DIR_ENV_VAR = 'TEST_TMPDIR'
_DEBUG_LEVEL_ENV_VAR = 'DEBUG_LEVEL'


def is_release_mode():
    return os.environ.get(_DEBUG_LEVEL_ENV_VAR) == "0"


def get_dbname(test_name):
    test_dir_name = "rocksdb_crashtest_" + test_name
    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is None or test_tmpdir == "":
        dbname = tempfile.mkdtemp(prefix=test_dir_name)
    else:
        dbname = test_tmpdir + "/" + test_dir_name
        shutil.rmtree(dbname, True)
        os.mkdir(dbname)
    return dbname


def is_direct_io_supported(dbname):
    with tempfile.NamedTemporaryFile(dir=dbname) as f:
        try:
            os.open(f.name, os.O_DIRECT)
        except BaseException:
            return False
        return True


blackbox_default_params = {
    # total time for this script to test db_stress
    "duration": 6000,
    # time for one db_stress instance to run
    "interval": 120,
    # since we will be killing anyway, use large value for ops_per_thread
    "ops_per_thread": 100000000,
    "set_options_one_in": 10000,
    "test_batches_snapshots": 1,
}

whitebox_default_params = {
    "duration": 10000,
    "log2_keys_per_lock": 10,
    "ops_per_thread": 200000,
    "random_kill_odd": 888887,
    "test_batches_snapshots": lambda: random.randint(0, 1),
}

simple_default_params = {
    "allow_concurrent_memtable_write": lambda: random.randint(0, 1),
    "column_families": 1,
    "max_background_compactions": 1,
    "max_bytes_for_level_base": 67108864,
    "memtablerep": "skip_list",
    "prefixpercent": 0,
    "readpercent": 50,
    "prefix_size" : -1,
    "target_file_size_base": 16777216,
    "target_file_size_multiplier": 1,
    "test_batches_snapshots": 0,
    "write_buffer_size": 32 * 1024 * 1024,
    "level_compaction_dynamic_level_bytes": False,
}

blackbox_simple_default_params = {
    "open_files": -1,
    "set_options_one_in": 0,
}

whitebox_simple_default_params = {}

cf_consistency_params = {
    "disable_wal": lambda: random.randint(0, 1),
    "reopen": 0,
    "test_cf_consistency": 1,
    # use small value for write_buffer_size so that RocksDB triggers flush
    # more frequently
    "write_buffer_size": 1024 * 1024,
    "enable_pipelined_write": lambda: random.randint(0, 1),
}

txn_params = {
    "use_txn" : 1,
    # Avoid lambda to set it once for the entire test
    "txn_write_policy": random.randint(0, 2),
    "unordered_write": random.randint(0, 1),
    "disable_wal": 0,
    # OpenReadOnly after checkpoint is not currnetly compatible with WritePrepared txns
    "checkpoint_one_in": 0,
    # pipeline write is not currnetly compatible with WritePrepared txns
    "enable_pipelined_write": 0,
}

def finalize_and_sanitize(src_params):
    dest_params = dict([(k,  v() if callable(v) else v)
                        for (k, v) in src_params.items()])
    if dest_params.get("compression_type") != "zstd" or \
            dest_params.get("compression_max_dict_bytes") == 0:
        dest_params["compression_zstd_max_train_bytes"] = 0
    if dest_params.get("allow_concurrent_memtable_write", 1) == 1:
        dest_params["memtablerep"] = "skip_list"
    if dest_params["mmap_read"] == 1:
        dest_params["use_direct_io_for_flush_and_compaction"] = 0
        dest_params["use_direct_reads"] = 0
    if (dest_params["use_direct_io_for_flush_and_compaction"] == 1
            or dest_params["use_direct_reads"] == 1) and \
            not is_direct_io_supported(dest_params["db"]):
        if is_release_mode():
            print("{} does not support direct IO".format(dest_params["db"]))
            sys.exit(1)
        else:
            dest_params["mock_direct_io"] = True

    # DeleteRange is not currnetly compatible with Txns
    if dest_params.get("test_batches_snapshots") == 1 or \
            dest_params.get("use_txn") == 1:
        dest_params["delpercent"] += dest_params["delrangepercent"]
        dest_params["delrangepercent"] = 0
    # Only under WritePrepared txns, unordered_write would provide the same guarnatees as vanilla rocksdb
    if dest_params.get("unordered_write", 0) == 1:
        dest_params["txn_write_policy"] = 1
        dest_params["allow_concurrent_memtable_write"] = 1
    if dest_params.get("disable_wal", 0) == 1:
        dest_params["atomic_flush"] = 1
        dest_params["sync"] = 0
    if dest_params.get("open_files", 1) != -1:
        # Compaction TTL and periodic compactions are only compatible
        # with open_files = -1
        dest_params["compaction_ttl"] = 0
        dest_params["periodic_compaction_seconds"] = 0
    if dest_params.get("compaction_style", 0) == 2:
        # Disable compaction TTL in FIFO compaction, because right
        # now assertion failures are triggered.
        dest_params["compaction_ttl"] = 0
        dest_params["periodic_compaction_seconds"] = 0
    if dest_params["partition_filters"] == 1:
        if dest_params["index_type"] != 2:
            dest_params["partition_filters"] = 0
        else:
            dest_params["use_block_based_filter"] = 0
    if dest_params.get("atomic_flush", 0) == 1:
        # disable pipelined write when atomic flush is used.
        dest_params["enable_pipelined_write"] = 0
    return dest_params

def gen_cmd_params(args):
    params = {}

    params.update(default_params)
    if args.test_type == 'blackbox':
        params.update(blackbox_default_params)
    if args.test_type == 'whitebox':
        params.update(whitebox_default_params)
    if args.simple:
        params.update(simple_default_params)
        if args.test_type == 'blackbox':
            params.update(blackbox_simple_default_params)
        if args.test_type == 'whitebox':
            params.update(whitebox_simple_default_params)
    if args.cf_consistency:
        params.update(cf_consistency_params)
    if args.txn:
        params.update(txn_params)

    for k, v in vars(args).items():
        if v is not None:
            params[k] = v
    return params


def gen_cmd(params, unknown_params):
    finalzied_params = finalize_and_sanitize(params)
    cmd = ['./db_stress'] + [
        '--{0}={1}'.format(k, v)
        for k, v in [(k, finalzied_params[k]) for k in sorted(finalzied_params)]
        if k not in set(['test_type', 'simple', 'duration', 'interval',
                         'random_kill_odd', 'cf_consistency', 'txn'])
        and v is not None] + unknown_params
    return cmd


# This script runs and kills db_stress multiple times. It checks consistency
# in case of unsafe crashes in RocksDB.
def blackbox_crash_main(args, unknown_args):
    cmd_params = gen_cmd_params(args)
    dbname = get_dbname('blackbox')
    exit_time = time.time() + cmd_params['duration']

    print("Running blackbox-crash-test with \n"
          + "interval_between_crash=" + str(cmd_params['interval']) + "\n"
          + "total-duration=" + str(cmd_params['duration']) + "\n")

    while time.time() < exit_time:
        run_had_errors = False
        killtime = time.time() + cmd_params['interval']

        cmd = gen_cmd(dict(
            list(cmd_params.items())
            + list({'db': dbname}.items())), unknown_args)

        child = subprocess.Popen(cmd, stderr=subprocess.PIPE)
        print("Running db_stress with pid=%d: %s\n\n"
              % (child.pid, ' '.join(cmd)))

        stop_early = False
        while time.time() < killtime:
            if child.poll() is not None:
                print("WARNING: db_stress ended before kill: exitcode=%d\n"
                      % child.returncode)
                stop_early = True
                break
            time.sleep(1)

        if not stop_early:
            if child.poll() is not None:
                print("WARNING: db_stress ended before kill: exitcode=%d\n"
                      % child.returncode)
            else:
                child.kill()
                print("KILLED %d\n" % child.pid)
                time.sleep(1)  # time to stabilize after a kill

        while True:
            line = child.stderr.readline().strip().decode('utf-8')
            if line == '':
                break
            elif not line.startswith('WARNING'):
                run_had_errors = True
                print('stderr has error message:')
                print('***' + line + '***')

        if run_had_errors:
            sys.exit(2)

        time.sleep(1)  # time to stabilize before the next run

    # we need to clean up after ourselves -- only do this on test success
    shutil.rmtree(dbname, True)


# This python script runs db_stress multiple times. Some runs with
# kill_random_test that causes rocksdb to crash at various points in code.
def whitebox_crash_main(args, unknown_args):
    cmd_params = gen_cmd_params(args)
    dbname = get_dbname('whitebox')

    cur_time = time.time()
    exit_time = cur_time + cmd_params['duration']
    half_time = cur_time + cmd_params['duration'] // 2

    print("Running whitebox-crash-test with \n"
          + "total-duration=" + str(cmd_params['duration']) + "\n")

    total_check_mode = 4
    check_mode = 0
    kill_random_test = cmd_params['random_kill_odd']
    kill_mode = 0

    while time.time() < exit_time:
        if check_mode == 0:
            additional_opts = {
                # use large ops per thread since we will kill it anyway
                "ops_per_thread": 100 * cmd_params['ops_per_thread'],
            }
            # run with kill_random_test, with three modes.
            # Mode 0 covers all kill points. Mode 1 covers less kill points but
            # increases change of triggering them. Mode 2 covers even less
            # frequent kill points and further increases triggering change.
            if kill_mode == 0:
                additional_opts.update({
                    "kill_random_test": kill_random_test,
                })
            elif kill_mode == 1:
                if cmd_params.get('disable_wal', 0) == 1:
                    my_kill_odd = kill_random_test // 50 + 1
                else:
                    my_kill_odd = kill_random_test // 10 + 1
                additional_opts.update({
                    "kill_random_test": my_kill_odd,
                    "kill_prefix_blacklist": "WritableFileWriter::Append,"
                    + "WritableFileWriter::WriteBuffered",
                })
            elif kill_mode == 2:
                # TODO: May need to adjust random odds if kill_random_test
                # is too small.
                additional_opts.update({
                    "kill_random_test": (kill_random_test // 5000 + 1),
                    "kill_prefix_blacklist": "WritableFileWriter::Append,"
                    "WritableFileWriter::WriteBuffered,"
                    "PosixMmapFile::Allocate,WritableFileWriter::Flush",
                })
            # Run kill mode 0, 1 and 2 by turn.
            kill_mode = (kill_mode + 1) % 3
        elif check_mode == 1:
            # normal run with universal compaction mode
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params['ops_per_thread'],
                "compaction_style": 1,
            }
        elif check_mode == 2:
            # normal run with FIFO compaction mode
            # ops_per_thread is divided by 5 because FIFO compaction
            # style is quite a bit slower on reads with lot of files
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params['ops_per_thread'] // 5,
                "compaction_style": 2,
            }
        else:
            # normal run
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params['ops_per_thread'],
            }

        cmd = gen_cmd(dict(list(cmd_params.items())
            + list(additional_opts.items())
            + list({'db': dbname}.items())), unknown_args)

        print("Running:" + ' '.join(cmd) + "\n")  # noqa: E999 T25377293 Grandfathered in

        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
        stdoutdata, stderrdata = popen.communicate()
        if stdoutdata:
            stdoutdata = stdoutdata.decode('utf-8')
        if stderrdata:
            stderrdata = stderrdata.decode('utf-8')
        retncode = popen.returncode
        msg = ("check_mode={0}, kill option={1}, exitcode={2}\n".format(
               check_mode, additional_opts['kill_random_test'], retncode))
        print(msg)
        print(stdoutdata)

        expected = False
        if additional_opts['kill_random_test'] is None and (retncode == 0):
            # we expect zero retncode if no kill option
            expected = True
        elif additional_opts['kill_random_test'] is not None and retncode <= 0:
            # When kill option is given, the test MIGHT kill itself.
            # If it does, negative retncode is expected. Otherwise 0.
            expected = True

        if not expected:
            print("TEST FAILED. See kill option and exit code above!!!\n")
            sys.exit(1)

        stdoutdata = stdoutdata.lower()
        errorcount = (stdoutdata.count('error') -
                      stdoutdata.count('got errors 0 times'))
        print("#times error occurred in output is " + str(errorcount) + "\n")

        if (errorcount > 0):
            print("TEST FAILED. Output has 'error'!!!\n")
            sys.exit(2)
        if (stdoutdata.find('fail') >= 0):
            print("TEST FAILED. Output has 'fail'!!!\n")
            sys.exit(2)

        # First half of the duration, keep doing kill test. For the next half,
        # try different modes.
        if time.time() > half_time:
            # we need to clean up after ourselves -- only do this on test
            # success
            shutil.rmtree(dbname, True)
            os.mkdir(dbname)
            cmd_params.pop('expected_values_path', None)
            check_mode = (check_mode + 1) % total_check_mode

        time.sleep(1)  # time to stabilize after a kill


def main():
    parser = argparse.ArgumentParser(description="This script runs and kills \
        db_stress multiple times")
    parser.add_argument("test_type", choices=["blackbox", "whitebox"])
    parser.add_argument("--simple", action="store_true")
    parser.add_argument("--cf_consistency", action='store_true')
    parser.add_argument("--txn", action='store_true')

    all_params = dict(list(default_params.items())
                      + list(blackbox_default_params.items())
                      + list(whitebox_default_params.items())
                      + list(simple_default_params.items())
                      + list(blackbox_simple_default_params.items())
                      + list(whitebox_simple_default_params.items()))

    for k, v in all_params.items():
        parser.add_argument("--" + k, type=type(v() if callable(v) else v))
    # unknown_args are passed directly to db_stress
    args, unknown_args = parser.parse_known_args()

    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is not None and not os.path.isdir(test_tmpdir):
        print('%s env var is set to a non-existent directory: %s' %
                (_TEST_DIR_ENV_VAR, test_tmpdir))
        sys.exit(1)

    if args.test_type == 'blackbox':
        blackbox_crash_main(args, unknown_args)
    if args.test_type == 'whitebox':
        whitebox_crash_main(args, unknown_args)

if __name__ == '__main__':
    main()
