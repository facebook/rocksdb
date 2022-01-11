
step=$1
startDiff=$2
stopDiff=$3

n=$step

while read v1 v2 ; do

  if [ $v1 -gt $stopDiff ]; then
    exit 0
  elif [ $v1 -lt $startDiff ]; then
    echo ignore $v1 :: $v2
    continue
  fi

  if [ $n -eq $step ]; then
    n=1
    git checkout main
    git checkout $v2

    mv build_tools build_tools.orig
    ln -s build_tools.6221 build_tools

    echo use $v1 :: $v2
    make clean >& /dev/null

    t=${v1}.${v2}; DISABLE_WARNING_AS_ERROR=1 DEBUG_LEVEL=0 make V=1 VERBOSE=1 -j16 static_lib db_bench > ../reg.b.${t} 2> ../reg.e.${t}
    makeres=$?

    git log | head -10 >> ../reg.b.${t}

    rm build_tools
    mv build_tools.orig build_tools

    if [ $makeres -ne 0 ]; then
      echo Make failed
    else
      mv db_bench db_bench.$t
      # run_perfcmp $t
    fi

  else
    n=$(( $n + 1 ))
    echo skip $v1 :: $v2
  fi
done < hashes6.all

