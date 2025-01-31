# Exit if a command fails
# set -o errexit

# Throw error when accessing an unset variable
set -o nounset

method_arr=("default" "grd" "scan" "decom")
# method_arr=("default" "grd" "scan")
ksize=256
kvsize=1024
buffer_size=64
rep_buffer_size=4096
max_key=99999999

# prep_num_arr=(0 0 0 0 0 0 0 0 0 0);
# write_num_arr=(1800000 1400000 1000000 600000  200000   1000000 3000000 5000000 7000000 9000000)
# read_num_arr=(200000   600000  1000000 1400000 1800000  9000000 7000000 5000000 3000000 1000000)
# seek_num_arr=(0 0 0 0 0  0 0 0 0 0)
# rdelete_num_arr=(100000 100000 100000 100000 100000  500000 500000 500000 500000 500000)
prep_num_arr=(0);
write_num_arr=(7000000)
read_num_arr=(3000000)
seek_num_arr=(0)
rdelete_num_arr=(500000)

rdelete_len=100

level_comp=2
full_rtree=true
##############################################################################################

EXP_BIN="./tools/test_delete"
path="./testdb"
res_file="./res/testlog_3.txt"

experiment() {
    if [ $method = "default" ]; then
        # echo "default "
        ${EXP_BIN} --mode="default" --full_rtree=${full_rtree} --workload="test" --level_comp=${level_comp} --ksize=${ksize} --kvsize=${kvsize} \
            --buffer_size=${buffer_size} --rep_buffer_size=${rep_buffer_size} --max_key=${max_key} --prep_num=${prep_num} --write_num=${write_num} \
            --read_num=${read_num} --seek_num=${seek_num} --rdelete_num=${rdelete_num} --rdelete_len=${rdelete_len} >> $res_file

    elif [ $method = "scan" ]; then
        # echo "scan "
        ${EXP_BIN} --mode="scan" --full_rtree=${full_rtree} --workload="test" --level_comp=${level_comp} --ksize=${ksize} --kvsize=${kvsize} \
            --buffer_size=${buffer_size} --rep_buffer_size=${rep_buffer_size} --max_key=${max_key} --prep_num=${prep_num} --write_num=${write_num} \
            --read_num=${read_num} --seek_num=${seek_num} --rdelete_num=${rdelete_num} --rdelete_len=${rdelete_len} >> $res_file
    
    elif [ $method = "decom" ]; then
        # echo "decom "
        ${EXP_BIN} --mode="decom" --full_rtree=${full_rtree} --workload="test" --level_comp=${level_comp} --ksize=${ksize} --kvsize=${kvsize} \
            --buffer_size=${buffer_size} --rep_buffer_size=${rep_buffer_size} --max_key=${max_key} --prep_num=${prep_num} --write_num=${write_num} \
            --read_num=${read_num} --seek_num=${seek_num} --rdelete_num=${rdelete_num} --rdelete_len=${rdelete_len} >> $res_file
    
    elif [ $method = "grd" ]; then
        # echo "grd "
        ${EXP_BIN} --mode="grd" --full_rtree=${full_rtree} --workload="test" --level_comp=${level_comp} --ksize=${ksize} --kvsize=${kvsize} \
            --buffer_size=${buffer_size} --rep_buffer_size=${rep_buffer_size} --max_key=${max_key} --prep_num=${prep_num} --write_num=${write_num} \
            --read_num=${read_num} --seek_num=${seek_num} --rdelete_num=${rdelete_num} --rdelete_len=${rdelete_len} >> $res_file
    
    fi
}

touch $res_file
group_cnt=1

for idx in "${!write_num_arr[@]}"; do
    prep_num="${prep_num_arr[$idx]}"
    write_num="${write_num_arr[$idx]}"
    read_num="${read_num_arr[$idx]}"
    seek_num="${seek_num_arr[$idx]}"
    rdelete_num="${rdelete_num_arr[$idx]}"

    echo -e "\t Test Group: $group_cnt \t" >> $res_file
    ((group_cnt++))

    for method in "${method_arr[@]}"; do
        rm -rf $path
        experiment
    done
done