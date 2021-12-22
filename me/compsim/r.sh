rt=$1

gb=64
wl=overwrite
minmw=4

for ra in 4 8 16; do
for sag in 10 15 20 30 40 50 60 70 80 90 100; do
  echo; echo Test for ra $ra and sag $sag at $( date )

  a=stcs
  pypy3 compsim.py --algorithm=$a --run_time=$rt --init_size_gb=$gb --space_amp_goal=$sag --stcs_min_threshold=$ra --workload=$wl > o.sag${sag}.r${ra}.$a 
  tail -1 o.sag${sag}.r${ra}.$a
  gzip o.sag${sag}.r${ra}.$a

  if [ $ra -eq 4 ]; then
    continue
  fi

  a=tower
  pypy3 compsim.py --algorithm=$a --run_time=$rt --init_size_gb=$gb --space_amp_goal=$sag --tower_min_merge_width=$minmw --tower_l0_trigger=$ra --workload=$wl > o.sag${sag}.r${ra}.$a 
  tail -1 o.sag${sag}.r${ra}.$a
  gzip o.sag${sag}.r${ra}.$a

  a=prefix
  pypy3 compsim.py --algorithm=$a --run_time=$rt --init_size_gb=$gb --space_amp_goal=$sag --prefix_min_merge_width=$minmw --prefix_read_amp_trigger=$ra --workload=$wl > o.sag${sag}.r${ra}.$a 
  tail -1 o.sag${sag}.r${ra}.$a
  gzip o.sag${sag}.r${ra}.$a

  a=prefix
  pypy3 compsim.py --algorithm=$a --run_time=$rt --init_size_gb=$gb --space_amp_goal=$sag --prefix_min_merge_width=$minmw --prefix_read_amp_trigger=$ra --workload=$wl --prefix_v2 > o.sag${sag}.r${ra}.${a}_v2
  tail -1 o.sag${sag}.r${ra}.${a}_v2
  gzip o.sag${sag}.r${ra}.${a}_v2


done
done

