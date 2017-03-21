#!/bin/bash
set -e -x

ulimit -n 8192 || true
cd ..
if [[ "${JOB_NAME}" == 'unittests' ]]; then OPT=-DTRAVIS V=1 make -j4 check_some; fi
if [[ "${JOB_NAME}" == 'java_test' ]]; then OPT=-DTRAVIS V=1 make clean jclean rocksdbjava jtest; fi
if [[ "${JOB_NAME}" == 'lite_build' ]]; then OPT="-DTRAVIS -DROCKSDB_LITE" V=1 make -j4 static_lib; fi
