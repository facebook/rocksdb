
for v in 6.0.2 6.7.3 6.14.6 6.20.3 6.26.1 ; do
  echo running for $v
  rm -f db_bench
  ln -s bin.ver/db_bench.v${v} db_bench
  bash pr1811.sh 1 180 100000000 /data/m/rx
  rm -rf pr.v${v}; mkdir pr.v${v}; mv b1811.* pr.v${v}
done
