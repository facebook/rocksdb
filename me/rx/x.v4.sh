myhw=$1
secs=$2
secs_ro=$3
nkeys=$4
nthreads=$5
odirect=$6
comp=$7
numa=$8
valbytes=$9
gcac=${10}
gcft=${11}
nbg=${12}
pendr=${13}

# Options: ${14}+ lists db_bench binaries to use, this is optional
 
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
v6.9.4 \
v6.10.2 \
v6.11.6 \
v6.12.7 \
v6.13.3 \
v6.14.6 \
v6.15.5 \
v6.16.4 \
v6.17.3 \
v6.18.1 \
v6.19.3 \
v6.20.3 \
v6.21.3 \
v6.22.1 \
v6.23.3 \
v6.24.2 \
v6.25.3 \
v6.26.1 \
v6.27.3 \
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
four_versions=( v4.1 )
five_versions=( v5.1.4 )
old_versions=( v4.1 v5.1.4 )

#use_versions="${some_versions[@]}"
#use_versions="${latest_versions[@]}"
#use_versions="${all_versions[@]}"
use_versions="${four_versions[@]}"

shift 8
if [ "$#" -eq 0 ] ; then
  versions="${use_versions[@]}"
  echo "No version args"
else
  versions="$@"
  echo Commmand line lists versions: "${versions[@]}"
fi

case $myhw in
c4r16)
  # Options for 4-core, 16g RAM
  args=( WRITE_BUF_MB=16 SST_MB=16 L1_MB=64 MAX_BG_JOBS=3 )
  cache_mb=$(( 1024 * 12 ))
  nsub=2
  ;;
c4bc1g)
  # Options for 4-core, 1g block cache
  args=( WRITE_BUF_MB=16 SST_MB=16 L1_MB=64 MAX_BG_JOBS=3 )
  cache_mb=$(( 1024 * 1 ))
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
c16bc1g)
  # Options for 16-core, 1g block cache
  args=( WRITE_BUF_MB=16 SST_MB=16 L1_MB=64 MAX_BG_JOBS=8 )
  cache_mb=$(( 1024 * 1 ))
  nsub=4
  ;;
c16r64b)
  # Options for 16-core, 64g RAM
  args=( WRITE_BUF_MB=128 SST_MB=8 L1_MB=64 MAX_BG_JOBS=8 )
  cache_mb=$(( 1024 * 48 ))
  nsub=4
  ;;
*)
  echo "HW config ( $myhw ) not supported"
  exit -1
esac

args+=( NKEYS=$nkeys CACHE_MB=$cache_mb NSECS=$secs NSECS_RO=$secs_ro MB_WPS=2 NTHREADS=$nthreads COMP_TYPE=$comp IBLOB_COMPRESSION_TYPE=$comp CACHE_META=$cm $dflags )
args+=( MAX_BG_JOBS=$nbg VALUE_BYTES=$valbytes IBLOB_GC_AGE_CUTOFF=$gcac IBLOB_GC_FORCE_THRESHOLD=$gcft PENDING_RATIO=$pendr )

if [ $numa -eq 1 ]; then
  args+=( NUMA=1 )
fi

# for leveled
odir=bm.lc.nt${nthreads}.cm${cm}.d${odirect}
echo leveled using $odir at $( date )
myargs=( "${args[@]}" )
myargs+=( ML2_COMP=3 COMP_STYLE=leveled )
env "${myargs[@]}" bash perf_cmp.v4.sh /data/m/rx $odir ${versions[@]}

exit

# for universal

odir=bm.uc.nt${nthreads}.cm${cm}.d${odirect}.sc${nsub}.tm
echo universal+subcomp+trivial_move using $odir at $( date )
myargs=( "${args[@]}" )
myargs+=( PCT_COMP=80 COMP_STYLE=universal SUBCOMP=$nsub UNIV_ALLOW_TRIVIAL_MOVE=1 )
env "${myargs[@]}" bash perf_cmp.v4.sh /data/m/rx $odir ${versions[@]}


