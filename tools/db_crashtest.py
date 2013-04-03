import os
import sys
import time
import shlex
import getopt
import logging
import subprocess

# This python script runs and kills db_stress multiple times with
# test-batches-snapshot ON,
# total operations much less than the total keys, and
# a high read percentage.
# This checks consistency in case of unsafe crashes in  Rocksdb

def main(argv):
    os.system("make -C ~/rocksdb db_stress")
    try:
        opts, args = getopt.getopt(argv, "hd:t:i:o:b:")
    except getopt.GetoptError:
        print "db_crashtest.py -d <duration_test> -t <#threads> " \
            "-i <interval for one run> -o <ops_per_thread>\n"
        sys.exit(2)

    # default values, will be overridden by cmdline args
    interval = 120  # time for one db_stress instance to run
    duration = 6000  # total time for this script to test db_stress
    threads = 32
    # since we will be killing anyway, use large value for ops_per_thread
    ops_per_thread = 10000000
    write_buf_size = 4 * 1024 * 1024

    for opt, arg in opts:
        if opt == '-h':
            print "db_crashtest.py -d <duration_test> -t <#threads> " \
                "-i <interval for one run> -o <ops_per_thread> "\
                "-b <write_buffer_size>\n"
            sys.exit()
        elif opt == ("-d"):
            duration = int(arg)
        elif opt == ("-t"):
            threads = int(arg)
        elif opt == ("-i"):
            interval = int(arg)
        elif opt == ("-o"):
            ops_per_thread = int(arg)
        elif opt == ("-b"):
            write_buf_size = int(arg)
        else:
            print "db_crashtest.py -d <duration_test> -t <#threads> " \
                "-i <interval for one run> -o <ops_per_thread> " \
                "-b <write_buffer_size>\n"
            sys.exit(2)

    exit_time = time.time() + duration

    while time.time() < exit_time:
        run_had_errors = False
        print "Running db_stress \n"
        os.system("mkdir -p /tmp/rocksdb/crashtest")
        killtime = time.time() + interval
        child = subprocess.Popen(['~/rocksdb/db_stress \
                        --test_batches_snapshots=1 \
                        --ops_per_thread=0' + str(ops_per_thread) + ' \
                        --threads=0' + str(threads) + ' \
                        --write_buffer_size=' + str(write_buf_size) + '\
                        --reopen=0 \
                        --readpercent=50 \
                        --db=/tmp/rocksdb/crashtest \
                        --max_key=1000'], stderr=subprocess.PIPE, shell=True)
        time.sleep(interval)
        while True:
            if time.time() > killtime:
                if child.poll() is not None:
                    logging.warn("WARNING: db_stress completed before kill\n")
                else:
                    child.kill()
                    print "KILLED \n"
                    time.sleep(1)  # time to stabilize after a kill

                while True:
                    line = child.stderr.readline().strip()
                    if line != '':
                        run_had_errors = True
                        print '***' + line + '^'
                    else:
                        break
                if run_had_errors:
                    sys.exit(2)
                break

            time.sleep(1)  # time to stabilize before the next run

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
