setup=$1
secs=$2
num=$3
dbdir=$4

#secs=180
#num=100000000
#dbdir=/data/m/rx

dop=( 1 2 4 8 12 16 20 24 )

function do_report {
  tname=$1
  base=$2

  for t in "${dop[@]}" ; do
    echo -n -e "${t}\t"
  done >> ${base}.r
  echo "name" >> ${base}.r

  for t in "${dop[@]}" ; do
    sfx=nt${t}
    grep ^${tname} $base.$sfx | tail -1 | awk '{ printf "%s\t", $5 }'
  done >> ${base}.r
  echo "qps" >> ${base}.r

  for t in "${dop[@]}" ; do
    sfx=nt${t}
    cat $base.$sfx.vm | grep -v memory | grep -v swpd | awk '{ c += 1; s += $12 } END { printf "%.0f\t", s / c }'
  done >> ${base}.r
  echo "vm.cs" >> ${base}.r

  for t in "${dop[@]}" ; do
    sfx=nt${t}
    cat $base.$sfx.vm | grep -v memory | grep -v swpd | awk '{ c += 1; s += $13 } END { printf "%.0f\t", s / c }'
  done >> ${base}.r
  echo "vm.us" >> ${base}.r

  for t in "${dop[@]}" ; do
    sfx=nt${t}
    cat $base.$sfx.vm | grep -v memory | grep -v swpd | awk '{ c += 1; s += $14 } END { printf "%.0f\t", s / c }'
  done >> ${base}.r
  echo "vm.sy" >> ${base}.r

  echo "---" >> ${base}.r

  for t in "${dop[@]}" ; do
    sfx=nt${t}
    grep ^${tname} $base.$sfx | tail -1 | awk '{ printf "%.0f\t", $5 / nt }' nt=$t
  done >> ${base}.r
  echo "qps/t" >> ${base}.r

  for t in "${dop[@]}" ; do
    sfx=nt${t}
    q=$( grep ^${tname} $base.$sfx | tail -1 | awk '{ printf "%s", $5 }' )
    cat $base.$sfx.vm | grep -v memory | grep -v swpd | awk '{ c += 1; s += $12 } END { printf "%.0f\t", 1000000 * (s / c / q) }' q=$q
  done >> ${base}.r
  echo "vm.cs/qps" >> ${base}.r

  for t in "${dop[@]}" ; do
    sfx=nt${t}
    q=$( grep ^${tname} $base.$sfx | tail -1 | awk '{ printf "%s", $5 }' )
    cat $base.$sfx.vm | grep -v memory | grep -v swpd | awk '{ c += 1; s += $13 } END { printf "%.1f\t", 1000000 * (s / c / q) }' q=$q
  done >> ${base}.r
  echo "vm.us/qps" >> ${base}.r

  for t in "${dop[@]}" ; do
    sfx=nt${t}
    q=$( grep ^${tname} $base.$sfx | tail -1 | awk '{ printf "%s", $5 }' )
    cat $base.$sfx.vm | grep -v memory | grep -v swpd | awk '{ c += 1; s += $14 } END { printf "%.1f\t", 1000000 * (s / c / q) }' q=$q
  done >> ${base}.r
  echo "vm.sy/qps" >> ${base}.r
}

if [ $setup -eq 1 ]; then
  ./db_bench --db=$dbdir --use_existing_db=0 --benchmarks=overwrite,levelstats --num=$num >& b1811.load
  ./db_bench --db=$dbdir --use_existing_db=1 --num=$num --duration=$secs --benchmarks=readrandom --threads=1 >& b1811.warmup
fi

killall iostat; killall vmstat

for bs in 1 8 64 ; do
for t in "${dop[@]}" ; do
  echo Run multireadrandom for $bs batch size and $t threads
  sfx=bs${bs}.nt${t}
  iostat -y -mx 1  >& b1811.mrr.$sfx.io &
  ipid=$!
  vmstat 1         >& b1811.mrr.$sfx.vm &
  vpid=$!
  numactl --interleave=all \
    ./db_bench --db=$dbdir --use_existing_db=1 --num=$num --duration=$secs --benchmarks=multireadrandom --batch_size=$bs --threads=$t >& b1811.mrr.$sfx
  kill $ipid; kill $vpid
done

echo "multireadrandom with batch_size=${bs}" > b1811.mrr.bs${bs}.r
do_report multireadrandom b1811.mrr.bs${bs}
echo >> b1811.mrr.bs${bs}.r
done

for t in "${dop[@]}" ; do
  echo Run readrandom for $bs batch size and $t threads
  sfx=nt${t}
  iostat -y -mx 1  >& b1811.rr.$sfx.io &
  ipid=$!
  vmstat 1         >& b1811.rr.$sfx.vm &
  vpid=$!
  numactl --interleave=all \
    ./db_bench --db=$dbdir --use_existing_db=1 --num=$num --duration=$secs --benchmarks=readrandom --threads=$t >& b1811.rr.$sfx
  kill $ipid; kill $vpid
done
echo "readrandom with batch_size=${bs}" > b1811.rr.r
do_report readrandom b1811.rr ""
