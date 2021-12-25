
# Generate a file that groups lines from the same test for all versions
basev=$1
nlines=$( awk '/^ops_sec/,/END/' ${basev}/report.tsv | grep -v ops_sec | wc -l )
hline=$( awk '/^ops_sec/ { print NR }' ${basev}/report.tsv )
sline=$(( $hline + 1 ))
eline=$(( $sline + $nlines - 1 ))

sum_file=summary2.tsv
rm -f $sum_file

for v in $*; do
  echo ${v}/report.tsv
done >> $sum_file
echo >> $sum_file

for x in $( seq $sline $eline ); do
  awk '{ if (NR == lno) { print $0 } }' lno=$hline ${basev}/report.tsv >> $sum_file
  for v in $*; do
    r=${v}/report.tsv
    awk '{ if (NR == lno) { print $0 } }' lno=$x $r >> $sum_file
  done
echo >> $sum_file
done
