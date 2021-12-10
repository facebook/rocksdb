myhw=$1
secs=$2
secs_ro=$3
nkeys=$4
nthreads=$5
odirect=$6

dflags=""
if [ $odirect -eq 1 ]; then
  dflags="DIRECT_IO=y"
fi

cm=1

all_versions=( \
v6.0.2 \
v6.1.2 \
v6.2.4 \
v6.3.6 \
v6.4.6 \
v6.5.3 \
v6.6.4 \
v6.7.3 \
v6.8.1 \
v6.10.2 \
v6.11.6 \
v6.12.7 \
v6.13.3 \
v6.14.6 \
v6.15.5 \
v6.16.4 \
v6.17.3 \
v6.19.3 \
v6.20.3 \
v6.22.1 \
v6.23.3 \
v6.24.2 \
v6.25.3 \
v6.26.1 \
)

some_versions=( \
v6.0.2 \
v6.7.3 \
v6.14.6 \
v6.22.1 \
v6.23.3 \
v6.24.2 \
v6.25.3 \
v6.26.1 \
)

latest_versions=( v6.26.1 )

#versions="${some_versions[@]}"
#versions="${all_versions[@]}"
versions="${latest_versions[@]}"

case $myhw in
c4r16)
  # Options for 4-core, 16g RAM
  args=( WRITE_BUF_MB=16 SST_MB=16 L1_MB=64 MAX_BG_JOBS=2 )
  cache_mb=$(( 1024 * 12 ))
  nsub=2
  ;;
c16r16)
  # Options for 16-core, 16g RAM
  args=( WRITE_BUF_MB=16 SST_MB=16 L1_MB=64 MAX_BG_JOBS=8 )
  cache_mb=$(( 1024 * 12 ))
  nsub=4
  ;;
c16r64)
  # Options for 16-core, 64g RAM
  args=( WRITE_BUF_MB=16 SST_MB=16 L1_MB=64 MAX_BG_JOBS=8 )
  cache_mb=$(( 1024 * 48 ))
  nsub=4
  ;;
*)
  echo "HW config ( $myhw ) not supported"
  exit -1
esac

args+=( NKEYS=$nkeys CACHE_MB=$cache_mb NSECS=$secs NSECS_RO=$secs_ro MB_WPS=2 NTHREADS=$nthreads COMP_TYPE=lz4 CACHE_META=$cm $dflags )

# for universal
odir=bm.uc.nt${nthreads}.cm${cm}.d${odirect}.sc${nsub}
echo universal+subcomp using $odir at $( date ) 
myargs=( "${args[@]}" )
myargs+=( ML2_COMP=1 UNIV=1 SUBCOMP=$nsub )
echo env "${myargs[@]}" bash perf_cmp.sh /data/m/rx $odir ${versions[@]}
env "${myargs[@]}" bash perf_cmp.sh /data/m/rx $odir ${versions[@]}

odir=bm.uc.nt${nthreads}.cm${cm}.d${odirect}
echo universal using $odir at $( date )
myargs=( "${args[@]}" )
myargs+=( ML2_COMP=1 UNIV=1 )
env "${myargs[@]}" bash perf_cmp.sh /data/m/rx $odir ${versions[@]}

odir=bm.uc.nt${nthreads}.cm${cm}.d${odirect}.tm
echo universal+trivial_move using $odir at $( date )
myargs=( "${args[@]}" )
myargs+=( ML2_COMP=1 UNIV=1 UNIV_ALLOW_TRIVIAL_MOVE=1 )
env "${myargs[@]}" bash perf_cmp.sh /data/m/rx $odir ${versions[@]}

odir=bm.uc.nt${nthreads}.cm${cm}.d${odirect}.sc${nsub}.tm
echo universal+subcomp+trivial_move using $odir at $( date )
myargs=( "${args[@]}" )
myargs+=( ML2_COMP=1 UNIV=1 SUBCOMP=$nsub UNIV_ALLOW_TRIVIAL_MOVE=1 )
env "${myargs[@]}" bash perf_cmp.sh /data/m/rx $odir ${versions[@]}

# for leveled
odir=bm.lc.nt${nthreads}.cm${cm}.d${odirect}
echo leveled using $odir at $( date )
myargs=( "${args[@]}" )
myargs+=( ML2_COMP=3 )
env "${myargs[@]}" bash perf_cmp.sh /data/m/rx $odir ${versions[@]}

