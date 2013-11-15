#! /usr/bin/env python
import os
import re
import sys
import time
import random
import getopt
import logging
import tempfile
import subprocess

# This script runs and kills db_stress multiple times. It checks consistency
# in case of unsafe crashes in Rocksdb.

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hd:t:i:o:b:")
    except getopt.GetoptError:
        print("db_crashtest.py -d <duration_test> -t <#threads> "
              "-i <interval for one run> -o <ops_per_thread> "
              "-b <write_buffer_size>\n")
        sys.exit(2)

    # default values, will be overridden by cmdline args
    interval = 120  # time for one db_stress instance to run
    duration = 6000  # total time for this script to test db_stress
    threads = 32
    # since we will be killing anyway, use large value for ops_per_thread
    ops_per_thread = 100000000
    write_buf_size = 4 * 1024 * 1024

    for opt, arg in opts:
        if opt == '-h':
            print("db_crashtest.py -d <duration_test>"
                  " -t <#threads> -i <interval for one run>"
                  " -o <ops_per_thread> -b <write_buffer_size>\n")
            sys.exit()
        elif opt == "-d":
            duration = int(arg)
        elif opt == "-t":
            threads = int(arg)
        elif opt == "-i":
            interval = int(arg)
        elif opt == "-o":
            ops_per_thread = int(arg)
        elif opt == "-b":
            write_buf_size = int(arg)
        else:
            print("db_crashtest.py -d <duration_test>"
                  " -t <#threads> -i <interval for one run>"
                  " -o <ops_per_thread> -b <write_buffer_size>\n")
            sys.exit(2)

    exit_time = time.time() + duration

    print("Running blackbox-crash-test with \ninterval_between_crash="
          + str(interval) + "\ntotal-duration=" + str(duration)
          + "\nthreads=" + str(threads) + "\nops_per_thread="
          + str(ops_per_thread) + "\nwrite_buffer_size="
          + str(write_buf_size) + "\n")

    while time.time() < exit_time:
        run_had_errors = False
        killtime = time.time() + interval

        cmd = re.sub('\s+', ' ', """
            ./db_stress
            --test_batches_snapshots=1
            --ops_per_thread=%s
            --threads=%s
            --write_buffer_size=%s
            --destroy_db_initially=0
            --reopen=0
            --readpercent=45
            --prefixpercent=5
            --writepercent=35
            --delpercent=5
            --iterpercent=10
            --db=%s
            --max_key=100000000
            --disable_seek_compaction=%s
            --mmap_read=%s
            --block_size=16384
            --cache_size=1048576
            --open_files=500000
            --verify_checksum=1
            --sync=%s
            --disable_wal=0
            --disable_data_sync=%s
            --target_file_size_base=2097152
            --target_file_size_multiplier=2
            --max_write_buffer_number=3
            --max_background_compactions=20
            --max_bytes_for_level_base=10485760
            --filter_deletes=%s
            """ % (ops_per_thread,
                   threads,
                   write_buf_size,
                   tempfile.mkdtemp(),
                   random.randint(0, 1),
                   random.randint(0, 1),
                   random.randint(0, 1),
                   random.randint(0, 1),
                   random.randint(0, 1)))

        child = subprocess.Popen([cmd],
                                 stderr=subprocess.PIPE, shell=True)
        print("Running db_stress with pid=%d: %s\n\n"
              % (child.pid, cmd))

        while time.time() < killtime:
            time.sleep(10)

        if child.poll() is not None:
            print("WARNING: db_stress ended before kill: exitcode=%d\n"
                  % child.returncode)
        else:
            child.kill()
            print("KILLED %d\n" % child.pid)
            time.sleep(1)  # time to stabilize after a kill

        while True:
            line = child.stderr.readline().strip()
            if line != '':
                run_had_errors = True
                print('***' + line + '^')
            else:
                break

        if run_had_errors:
            sys.exit(2)

        time.sleep(1)  # time to stabilize before the next run

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
