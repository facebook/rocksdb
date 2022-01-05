rt=$1

gb=64
wl=overwrite
minmw=4

for sag in 10 15 20 30 40 50 60 70 80 90 100; do
echo; echo Test for sag $sag at $( date )
rm -f o.sag${sag}.s1
rm -f o.sag${sag}.s0
touch o.sag${sag}.s0

for ra in 4 ; do

  a=stcs
  pypy3 compsim.py --algorithm=$a --run_time=$rt --init_size_gb=$gb --space_amp_goal=$sag --stcs_min_threshold=$ra --workload=$wl > o.sag${sag}.r${ra}.$a 
  tail -1 o.sag${sag}.r${ra}.$a >> o.sag${sag}.s0
  gzip o.sag${sag}.r${ra}.$a

  a=tower
  pypy3 compsim.py --algorithm=$a --run_time=$rt --init_size_gb=$gb --space_amp_goal=$sag --tower_min_merge_width=$minmw --tower_l0_trigger=$ra --workload=$wl --tower_size_ratio=1.5 > o.sag${sag}.r${ra}.$a.sr15
  tail -1 o.sag${sag}.r${ra}.$a.sr15 >> o.sag${sag}.s0
  gzip o.sag${sag}.r${ra}.$a.sr15

  pypy3 compsim.py --algorithm=$a --run_time=$rt --init_size_gb=$gb --space_amp_goal=$sag --tower_min_merge_width=$minmw --tower_l0_trigger=$ra --workload=$wl --tower_size_ratio=1.1 > o.sag${sag}.r${ra}.$a.sr11
  tail -1 o.sag${sag}.r${ra}.$a.sr11 >> o.sag${sag}.s0
  gzip o.sag${sag}.r${ra}.$a.sr11

done

for ra in 8 12 16; do

  a=greedy
  pypy3 compsim.py --algorithm=$a --run_time=$rt --init_size_gb=$gb --space_amp_goal=$sag --greedy_read_amp_trigger=$ra --workload=$wl > o.sag${sag}.r${ra}.$a
  tail -1 o.sag${sag}.r${ra}.$a >> o.sag${sag}.s0
  gzip o.sag${sag}.r${ra}.$a

  a=prefix
  pypy3 compsim.py --algorithm=$a --run_time=$rt --init_size_gb=$gb --space_amp_goal=$sag --prefix_min_merge_width=$minmw --prefix_read_amp_trigger=$ra --workload=$wl --prefix_size_ratio=1.5 > o.sag${sag}.r${ra}.$a.sr15 
  tail -1 o.sag${sag}.r${ra}.$a.sr15 >> o.sag${sag}.s0
  gzip o.sag${sag}.r${ra}.$a.sr15

  a=prefix
  pypy3 compsim.py --algorithm=$a --run_time=$rt --init_size_gb=$gb --space_amp_goal=$sag --prefix_min_merge_width=$minmw --prefix_read_amp_trigger=$ra --workload=$wl --prefix_size_ratio=1.5 --prefix_v2 > o.sag${sag}.r${ra}.${a}_v2.sr15
  tail -1 o.sag${sag}.r${ra}.${a}_v2.sr15 >> o.sag${sag}.s0
  gzip o.sag${sag}.r${ra}.${a}_v2.sr15

  pypy3 compsim.py --algorithm=$a --run_time=$rt --init_size_gb=$gb --space_amp_goal=$sag --prefix_min_merge_width=$minmw --prefix_read_amp_trigger=$ra --workload=$wl --prefix_size_ratio=1.1 --prefix_v2 > o.sag${sag}.r${ra}.${a}_v2.sr11
  tail -1 o.sag${sag}.r${ra}.${a}_v2.sr11 >> o.sag${sag}.s0
  gzip o.sag${sag}.r${ra}.${a}_v2.sr11

done

sort -n -k 8 o.sag${sag}.s0 > o.sag${sag}.s1
# cat o.sag${sag}.s1

echo -e "wa\tra-max\tra-avg\tsa-max\tsa-avg\tname" > o.sag${sag}.s1.tsv
cat o.sag${sag}.s1 | awk '{ printf "%.1f\t%d\t%.1f\t%.1f\t%.1f\t%s.r%d\n", $8, $2, $3, $5, $6, $20, $16 }' >> o.sag${sag}.s1.tsv

cat o.sag${sag}.s1.tsv
echo
#cat o.sag${sag}.s1

# final: 35 18.6 runs(max,avg), 2.00 1.50 space-amp(max,avg), 4.0 write-amp, 195 sa_major, 0 ra_major, 13281 minor, 16 ra, 80 sag, stcs

done
