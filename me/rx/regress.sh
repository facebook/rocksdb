myhw=$1
secs=$2
secs_ro=$3
nkeys=$4
nthreads=$5
odirect=$6
comp=$7
numa=$8

step=$9
start_diff=${10}
stop_diff=${11}
make=${12}

n=$step
while read v1 v2 ; do
  t=${v1}.${v2}

  if [ $v1 -lt $start_diff ]; then
    continue
  elif [ $v1 -gt $stop_diff ]; then
    echo Done
    exit 0
  fi

  if [ $n -eq $step ]; then
    n=1
    echo use $v1 :: $v2

    if [ $make -eq 1 ]; then
      git checkout main
      git checkout $v2
      make clean >& /dev/null

      t=${v1}.${v2}; DISABLE_WARNING_AS_ERROR=1 DEBUG_LEVEL=0 make V=1 VERBOSE=1 -j4 static_lib db_bench > reg.b.$t 2> reg.e.$t
      makeres=$?
      mv db_bench db_bench.$t

      git log | head -10 >> reg.b.$t
    else
      makeres=0
    fi

    if [ $makeres -ne 0 ]; then
      echo Make failed
    else
      bash x.sh $myhw $secs $secs_ro $nkeys $nthreads $odirect $comp $numa db_bench.$t
    fi

  else
    n=$(( $n + 1 ))
    echo skip $v1 :: $v2
  fi
done < hashes6.all

