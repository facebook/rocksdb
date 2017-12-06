#! /usr/bin/env python
import os
import re
import sys
import time
import random
import logging
import tempfile
import subprocess
import shutil
import argparse

# params overwrite priority:
#   for default:
#       default_params < blackbox|whitebox_default_params < args
#   for simple:
#       simple_default_params < blackbox|whitebox_simple_default_params < args

default_params = {
    "acquire_snapshot_one_in": 10000,
    "block_size": 16384,
    "cache_size": 1048576,
    "use_clock_cache": "false",
    "delpercent": 5,
    "destroy_db_initially": 0,
    "disable_wal": 0,
    "allow_concurrent_memtable_write": 0,
    "iterpercent": 10,
    "max_background_compactions": 20,
    "max_bytes_for_level_base": 10485760,
    "max_key": 100000000,
    "max_write_buffer_number": 3,
    "memtablerep": "prefix_hash",
    "mmap_read": lambda: random.randint(0, 1),
    "open_files": 500000,
    "prefix_size": 7,
    "prefixpercent": 5,
    "progress_reports": 0,
    "readpercent": 45,
    "reopen": 20,
    "snapshot_hold_ops": 100000,
    "sync": 0,
    "target_file_size_base": 2097152,
    "target_file_size_multiplier": 2,
    "threads": 32,
    "verify_checksum": 1,
    "write_buffer_size": 4 * 1024 * 1024,
    "writepercent": 35,
    "log2_keys_per_lock": 2,
    "subcompactions": lambda: random.randint(1, 4),
    "use_merge": lambda: random.randint(0, 1),
    "use_full_merge_v1": lambda: random.randint(0, 1),
}


def get_dbname(test_name):
    test_tmpdir = os.environ.get("TEST_TMPDIR")
    if test_tmpdir is None or test_tmpdir == "":
        dbname = tempfile.mkdtemp(prefix='rocksdb_crashtest_' + test_name)
    else:
        dbname = test_tmpdir + "/rocksdb_crashtest_" + test_name
        shutil.rmtree(dbname, True)
    return dbname

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
    "nooverwritepercent": 1,
    "ops_per_thread": 200000,
    "test_batches_snapshots": lambda: random.randint(0, 1),
    "write_buffer_size": 4 * 1024 * 1024,
    "subcompactions": lambda: random.randint(1, 4),
    "random_kill_odd": 888887,
}

simple_default_params = {
    "block_size": 16384,
    "cache_size": 1048576,
    "use_clock_cache": "false",
    "column_families": 1,
    "delpercent": 5,
    "destroy_db_initially": 0,
    "disable_wal": 0,
    "allow_concurrent_memtable_write": lambda: random.randint(0, 1),
    "iterpercent": 10,
    "max_background_compactions": 1,
    "max_bytes_for_level_base": 67108864,
    "max_key": 100000000,
    "max_write_buffer_number": 3,
    "memtablerep": "skip_list",
    "mmap_read": lambda: random.randint(0, 1),
    "prefix_size": 0,
    "prefixpercent": 0,
    "progress_reports": 0,
    "readpercent": 50,
    "reopen": 20,
    "sync": 0,
    "target_file_size_base": 16777216,
    "target_file_size_multiplier": 1,
    "test_batches_snapshots": 0,
    "threads": 32,
    "verify_checksum": 1,
    "write_buffer_size": 32 * 1024 * 1024,
    "writepercent": 35,
    "subcompactions": lambda: random.randint(1, 4),
}

blackbox_simple_default_params = {
    "duration": 6000,
    "interval": 120,
    "open_files": -1,
    "ops_per_thread": 100000000,
    "set_options_one_in": 0,
    "test_batches_snapshots": 0,
}

whitebox_simple_default_params = {
    "duration": 10000,
    "log2_keys_per_lock": 10,
    "nooverwritepercent": 1,
    "open_files": 500000,
    "ops_per_thread": 200000,
    "write_buffer_size": 32 * 1024 * 1024,
    "subcompactions": lambda: random.randint(1, 4),
}


def finalize_and_sanitize(src_params):
    dest_params = dict([(k,  v() if callable(v) else v)
                        for (k, v) in src_params.items()])
    if dest_params.get("allow_concurrent_memtable_write", 1) == 1:
        dest_params["memtablerep"] = "skip_list"
    return dest_params


def gen_cmd_params(args):
    params = {}

    if args.simple:
        params.update(simple_default_params)
        if args.test_type == 'blackbox':
            params.update(blackbox_simple_default_params)
        if args.test_type == 'whitebox':
            params.update(whitebox_simple_default_params)

    if not args.simple:
        params.update(default_params)
        if args.test_type == 'blackbox':
            params.update(blackbox_default_params)
        if args.test_type == 'whitebox':
            params.update(whitebox_default_params)

    for k, v in vars(args).items():
        if v is not None:
            params[k] = v
    return params


def gen_cmd(params):
    cmd = ['./db_stress'] + [
        '--{0}={1}'.format(k, v)
        for k, v in finalize_and_sanitize(params).items()
        if k not in set(['test_type', 'simple', 'duration', 'interval',
                         'random_kill_odd'])
        and v is not None]
    return cmd


# This script runs and kills db_stress multiple times. It checks consistency
# in case of unsafe crashes in RocksDB.
def blackbox_crash_main(args):
    cmd_params = gen_cmd_params(args)
    dbname = get_dbname('blackbox')
    exit_time = time.time() + cmd_params['duration']

    print("Running blackbox-crash-test with \n"
          + "interval_between_crash=" + str(cmd_params['interval']) + "\n"
          + "total-duration=" + str(cmd_params['duration']) + "\n"
          + "threads=" + str(cmd_params['threads']) + "\n"
          + "ops_per_thread=" + str(cmd_params['ops_per_thread']) + "\n"
          + "write_buffer_size=" + str(cmd_params['write_buffer_size']) + "\n"
          + "subcompactions=" + str(cmd_params['subcompactions']) + "\n")

    while time.time() < exit_time:
        run_had_errors = False
        killtime = time.time() + cmd_params['interval']

        cmd = gen_cmd(dict(cmd_params.items() + {'db': dbname}.items()))

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
            line = child.stderr.readline().strip()
            if line != '' and not line.startswith('WARNING'):
                run_had_errors = True
                print('stderr has error message:')
                print('***' + line + '***')
            else:
                break

        if run_had_errors:
            sys.exit(2)

        time.sleep(1)  # time to stabilize before the next run

    # we need to clean up after ourselves -- only do this on test success
    shutil.rmtree(dbname, True)


# This python script runs db_stress multiple times. Some runs with
# kill_random_test that causes rocksdb to crash at various points in code.
def whitebox_crash_main(args):
    cmd_params = gen_cmd_params(args)
    dbname = get_dbname('whitebox')

    cur_time = time.time()
    exit_time = cur_time + cmd_params['duration']
    half_time = cur_time + cmd_params['duration'] / 2

    print("Running whitebox-crash-test with \n"
          + "total-duration=" + str(cmd_params['duration']) + "\n"
          + "threads=" + str(cmd_params['threads']) + "\n"
          + "ops_per_thread=" + str(cmd_params['ops_per_thread']) + "\n"
          + "write_buffer_size=" + str(cmd_params['write_buffer_size']) + "\n"
          + "subcompactions=" + str(cmd_params['subcompactions']) + "\n")

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
                additional_opts.update({
                    "kill_random_test": (kill_random_test / 10 + 1),
                    "kill_prefix_blacklist": "WritableFileWriter::Append,"
                    + "WritableFileWriter::WriteBuffered",
                })
            elif kill_mode == 2:
                # TODO: May need to adjust random odds if kill_random_test
                # is too small.
                additional_opts.update({
                    "kill_random_test": (kill_random_test / 5000 + 1),
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
                "ops_per_thread": cmd_params['ops_per_thread'] / 5,
                "compaction_style": 2,
            }
        else:
            # normal run
            additional_opts = additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params['ops_per_thread'],
            }

        cmd = gen_cmd(dict(cmd_params.items() + additional_opts.items()
                           + {'db': dbname}.items()))

        print "Running:" + ' '.join(cmd) + "\n"

        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
        stdoutdata, stderrdata = popen.communicate()
        retncode = popen.returncode
        msg = ("check_mode={0}, kill option={1}, exitcode={2}\n".format(
               check_mode, additional_opts['kill_random_test'], retncode))
        print msg
        print stdoutdata

        expected = False
        if additional_opts['kill_random_test'] is None and (retncode == 0):
            # we expect zero retncode if no kill option
            expected = True
        elif additional_opts['kill_random_test'] is not None and retncode < 0:
            # we expect negative retncode if kill option was given
            expected = True

        if not expected:
            print "TEST FAILED. See kill option and exit code above!!!\n"
            sys.exit(1)

        stdoutdata = stdoutdata.lower()
        errorcount = (stdoutdata.count('error') -
                      stdoutdata.count('got errors 0 times'))
        print "#times error occurred in output is " + str(errorcount) + "\n"

        if (errorcount > 0):
            print "TEST FAILED. Output has 'error'!!!\n"
            sys.exit(2)
        if (stdoutdata.find('fail') >= 0):
            print "TEST FAILED. Output has 'fail'!!!\n"
            sys.exit(2)

        # First half of the duration, keep doing kill test. For the next half,
        # try different modes.
        if time.time() > half_time:
            # we need to clean up after ourselves -- only do this on test
            # success
            shutil.rmtree(dbname, True)
            check_mode = (check_mode + 1) % total_check_mode

        time.sleep(1)  # time to stabilize after a kill


def main():
    parser = argparse.ArgumentParser(description="This script runs and kills \
        db_stress multiple times")
    parser.add_argument("test_type", choices=["blackbox", "whitebox"])
    parser.add_argument("--simple", action="store_true")

    all_params = dict(default_params.items()
                      + blackbox_default_params.items()
                      + whitebox_default_params.items()
                      + simple_default_params.items()
                      + blackbox_simple_default_params.items()
                      + whitebox_simple_default_params.items())

    for k, v in all_params.items():
        parser.add_argument("--" + k, type=type(v() if callable(v) else v))
    args = parser.parse_args()

    if args.test_type == 'blackbox':
        blackbox_crash_main(args)
    if args.test_type == 'whitebox':
        whitebox_crash_main(args)

if __name__ == '__main__':
    main()
