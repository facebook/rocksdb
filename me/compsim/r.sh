for ra in 4 8 16; do
# for sag in 20 30 40 50 60 70 80 90 100; do
for sag in 10 15 ; do
echo Test for ra $ra and sag $sag at $( date )
for a in stcs prefix tower; do
  pypy3 compsim.py --algorithm=$a --run_time=1000000 --init_size_gb=128 --space_amp_goal=$sag --min_merge_width=$ra --tower_l0_trigger=$ra --prefix_read_amp_trigger=$(( $ra + 1 )) --workload=overwrite  > o.sag${sag}.r${ra}.$a ;
done
done
done

