#!/bin/bash
# usage: 
# * trigger_jenkins_test.sh -- without parameters, submits the current patch to Jenkins for testing
# * trigger_jenkins_test.sh D12345 -- submits diff D12345
if [[ $# == 0 ]]; then
  diff=$(git log -1 --pretty=%b | perl -nle \
      'm!^Differential Revision: https://reviews\.facebook\.net/(D\d+)$! and print $1')
else
  diff=$1
fi

diff_len=`expr length "$diff"`
if [[ $diff_len < 6 ]] ; then
  echo "I don't think your diff ID ($diff) is correct"
  exit 1
fi

echo "Submitting build of diff $diff to Jenkins"
curl "https://ci-builds.fb.com/view/rocksdb/job/rocksdb_diff_check/buildWithParameters?token=AUTH&DIFF=$diff"
