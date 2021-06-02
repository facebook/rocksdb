#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
# Usage:
# 1. Enable ROCKS_LOG_DETAILS in util/logging.h
# 2. Run ./transaction_test --gtest_filter="MySQLStyleTransactionTest/MySQLStyleTransactionTest.TransactionStressTest/*" --gtest_break_on_failure
# 3. SET=1 # 2 or 3
# 4. LOG=/dev/shm/transaction_testdb_8600601584148590297/LOG
# 5. grep RandomTransactionVerify $LOG | cut -d' ' -f 12 | sort -n # to find verify snapshots
# 5. vn=1345
# 6. vn_1=1340
# 4. . tools/tools/analyze_txn_stress_test.sh
echo Input params:
# The rocksdb LOG path
echo $LOG
# Snapshot at which we got RandomTransactionVerify failure
echo $vn
# The snapshot before that where RandomTransactionVerify passed
echo $vn_1
# The stress tests use 3 sets, one or more might have shown inconsistent results.
SET=${SET-1} # 1 or 2 or 3
echo Checking set number $SET

# Find the txns that committed between the two snapshots, and gather their changes made by them in /tmp/changes.txt
# 2019/02/28-15:25:51.655477 7fffec9ff700 [DEBUG] [ilities/transactions/write_prepared_txn_db.cc:416] Txn 68497 Committing with 68498
grep Committing $LOG | awk '{if ($9 <= vn && $9 > vn_1) print $0}' vn=$vn vn_1=${vn_1} > /tmp/txn.txt
# 2019/02/28-15:25:49.046464 7fffe81f5700 [DEBUG] [il/transaction_test_util.cc:216] Commit of 65541 OK (txn12936193128775589751-9089)
for i in `cat /tmp/txn.txt | awk '{print $6}'`; do grep "Commit of $i " $LOG; done > /tmp/names.txt
for n in `cat /tmp/names.txt | awk '{print $9}'`; do grep $n $LOG; done > /tmp/changes.txt
echo "Sum of the changes:"
cat /tmp/changes.txt | grep Insert | awk '{print $12}' | cut -d= -f1 | cut -d+ -f2 | awk '{sum+=$1} END{print sum}'

# Gather read values at each snapshot
# 2019/02/28-15:25:51.655926 7fffebbff700 [DEBUG] [il/transaction_test_util.cc:347] VerifyRead at 67972 (67693): 000230 value: 15983
grep "VerifyRead at ${vn_1} (.*): 000${SET}" $LOG | cut -d' ' -f 9- > /tmp/va.txt
grep "VerifyRead at ${vn} (.*): 000${SET}" $LOG | cut -d' ' -f 9- > /tmp/vb.txt

# For each key in the 2nd snapshot, find the value read by 1st, do the adds, and see if the results match.
IFS=$'\n'
for l in `cat /tmp/vb.txt`; 
do
  grep $l /tmp/va.txt > /dev/null ; 
  if [[ $? -ne 0 ]]; then 
    #echo $l
    k=`echo $l | awk '{print $1}'`;
    v=`echo $l | awk '{print $3}'`;
    # 2019/02/28-15:25:19.350111 7fffe81f5700 [DEBUG] [il/transaction_test_util.cc:194] Insert (txn12936193128775589751-2298) OK snap: 16289 key:000219 value: 3772+95=3867
    exp=`grep "\<$k\>" /tmp/changes.txt | tail -1 | cut -d= -f2`;
    if [[ $v -ne $exp ]]; then echo $l; fi
  else
    k=`echo $l | awk '{print $1}'`;
    grep "\<$k\>" /tmp/changes.txt
  fi;
done

# Check that all the keys read in the 1st snapshot are still visible in the 2nd
for l in `cat /tmp/va.txt`; 
do
  k=`echo $l | awk '{print $1}'`;
  grep "\<$k\>" /tmp/vb.txt > /dev/null
  if [[ $? -ne 0 ]]; then
    echo missing key $k
  fi
done

# The following found a bug in ValidateSnapshot. It checks if the adds on each key match up.
grep Insert /tmp/changes.txt | cut -d' ' -f 10 | sort | uniq > /tmp/keys.txt
for k in `cat /tmp/keys.txt`;
do
  grep "\<$k\>" /tmp/changes.txt > /tmp/adds.txt;
  # 2019/02/28-15:25:19.350111 7fffe81f5700 [DEBUG] [il/transaction_test_util.cc:194] Insert (txn12936193128775589751-2298) OK snap: 16289 key:000219 value: 3772+95=3867
  START=`head -1 /tmp/adds.txt | cut -d' ' -f 12 | cut -d+ -f1`
  END=`tail -1 /tmp/adds.txt | cut -d' ' -f 12 | cut -d= -f2`
  ADDS=`cat /tmp/adds.txt | grep Insert | awk '{print $12}' | cut -d= -f1 | cut -d+ -f2 | awk '{sum+=$1} END{print sum}'`
  EXP=$((START+ADDS))
  # If first + all the adds != last then there was an issue with ValidateSnapshot.
  if [[ $END -ne $EXP ]]; then echo inconsistent txn: $k $START+$ADDS=$END; cat /tmp/adds.txt; return 1; fi
done
