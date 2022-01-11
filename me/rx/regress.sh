step=$1
make=$2
nkeys=$3
nthreads=$4
numa=$5

function run_perfcmp {
  version=$1
  secs=600
  secs_ro=600
  odirect=0
  comp=none
  dflags=""
  # if [ $odirect -eq 1 ]; then dflags="DIRECT_IO=y" fi
  cm=1

  #args=( WRITE_BUF_MB=16 SST_MB=16 L1_MB=64 MAX_BG_JOBS=3 )
  #cache_mb=$(( 1024 * 12 ))
  #nsub=2

  args=( WRITE_BUF_MB=16 SST_MB=16 L1_MB=64 MAX_BG_JOBS=8 )
  cache_mb=$(( 1024 * 48 ))
  nsub=4

  args+=( NKEYS=$nkeys CACHE_MB=$cache_mb NSECS=$secs NSECS_RO=$secs_ro MB_WPS=2 NTHREADS=$nthreads COMP_TYPE=$comp CACHE_META=$cm $dflags )
  if [ $numa -eq 1 ]; then args+=( NUMA=1 ); fi

  # for leveled
  odir=bm.lc.nt${nthreads}.cm${cm}.d${odirect}
  echo leveled using $odir at $( date ) and $version
  myargs=( "${args[@]}" )
  myargs+=( ML2_COMP=3 )
  env "${myargs[@]}" bash perf_cmp2.sh /data/m/rx $odir $version
}

n=$step
while read v1 v2 ; do
  t=${v1}.${v2}

  if [ $v1 -gt 1378 ]; then
    continue
  elif [ $v1 -lt 1252 ]; then
    exit 0
  fi

  if [ $n -eq $step ]; then
    n=0
    echo use $v1 :: $v2

    if [ $make -eq 1 ]; then
      git checkout main
      git checkout $v2
      make clean >& /dev/null

      t=${v1}.${v2}; DISABLE_WARNING_AS_ERROR=1 DEBUG_LEVEL=0 make V=1 VERBOSE=1 -j4 static_lib db_bench > ../reg.b.${t} 2> ../reg.e.${t}
      makeres=$?
      mv db_bench db_bench.$t

      git log | head -10 >> ../reg.b.${t}
    else
      makeres=0
    fi

    if [ $makeres -ne 0 ]; then
      echo Make failed
    else
      run_perfcmp $t
    fi

  else
    n=$(( $n + 1 ))
    echo skip $v1 :: $v2
  fi
done < hashes.all

