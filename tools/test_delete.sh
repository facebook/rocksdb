# sudo ./tools/test_delete --db_path=/home/wangfan/delete/global-range-delete/build/testdb --mode=mixseq --num=100000 --prep_num=100000 --write_num=100000 --read_num=100000 --zread_num=0 --seek_num=1000 --seek_keys=100 --range_del_num=1000
# sudo ./tools/test_delete --db_path=/home/wangfan/delete/global-range-delete/build/testdb --mode=default

# sudo ./tools/test_delete --mode=grd --workload=test --prep_num=10000 --write_num=10000 --read_num=10 --seek_num=10 --seek_len=5 --rdelete_num=10

# sudo ./tools/test_delete --mode=grd --workload=test --prep_num=10000 --write_num=10000 --read_num=0 --seek_num=10 --seek_len=5 --rdelete_num=10




# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --level_comp=1 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --level_comp=1 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --level_comp=0 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000 >> ./res/Testlog.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --level_comp=1 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --level_comp=1 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --level_comp=0 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --level_comp=1 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --level_comp=1 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --level_comp=0 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 >> ./res/Testlog.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --level_comp=1 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=2000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --level_comp=1 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=2000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --level_comp=0 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=2000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt



# rm -rf ./testdb
# sudo perf record -g -o ./res/perf_default.data --call-graph=dwarf ./tools/test_delete --mode=default --workload=test --level_comp=1 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000
# rm -rf ./testdb
# sudo perf record -g -o ./res/perf_grd.data --call-graph=dwarf ./tools/test_delete --mode=grd --workload=test --level_comp=10 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000



# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --ksize=32 --kvsize=512 --level_comp=1 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --ksize=32 --kvsize=512 --level_comp=1 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --ksize=32 --kvsize=512 --level_comp=10 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --ksize=128 --kvsize=512 --level_comp=1 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --ksize=128 --kvsize=512 --level_comp=1 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --ksize=256 --kvsize=512 --level_comp=1 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --ksize=256 --kvsize=512 --level_comp=1 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --ksize=256 --kvsize=512 --level_comp=10 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt



# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/log_1.txt
# # rm -rf ./testdb
# # ./tools/test_delete --mode=grd --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/log_1.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --enable_crosscheck=true --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/log_1.txt



# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=2500000 --write_num=500000 --read_num=500000 --seek_num=0 --rdelete_num=50000 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=2500000 --write_num=500000 --read_num=500000 --seek_num=0 --rdelete_num=50000 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=2500000 --write_num=500000 --read_num=500000 --seek_num=0 --rdelete_num=50000 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=2500000 --write_num=500000 --read_num=500000 --seek_num=0 --rdelete_num=50000 >> ./res/tmplog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=2500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/temlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=2500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/temlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=2500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/temlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=2500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/temlog.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/temlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --full_rtree=true --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/temlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/temlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/temlog.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/temlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=10000000 --write_num=2000000 --read_num=2000000 --seek_num=0 --rdelete_num=200000 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=16 --rep_buffer_size=164 --max_key=20000000 --prep_num=10000000 --write_num=2000000 --read_num=2000000 --seek_num=0 --rdelete_num=200000 >> ./res/tmplog.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=64 --rep_buffer_size=655 --max_key=20000000 --prep_num=10000000 --write_num=2000000 --read_num=2000000 --seek_num=0 --rdelete_num=200000 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=64 --rep_buffer_size=655 --max_key=20000000 --prep_num=10000000 --write_num=2000000 --read_num=2000000 --seek_num=0 --rdelete_num=200000 >> ./res/tmplog.txt



# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=32 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=32 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=32 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=32 --kvsize=96 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=32 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=32 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=32 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=32 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=20000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=20000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=20000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=20000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=10 >> ./res/log.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=200000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=200000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=200000 --rdelete_len=10 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=200000 --rdelete_len=10 >> ./res/log.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=200000 --rdelete_len=100 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=200000 --rdelete_len=100 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=200000 --rdelete_len=100 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=200000 --rdelete_len=100 >> ./res/log.txt



# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=200000 --rdelete_len=100 --rep_size_ratio=5 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=160 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=200000 --rdelete_len=100 --rep_size_ratio=100 >> ./res/log.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=512 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=200000 --rdelete_len=100 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=1024 --max_key=10000000 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=200000 --rdelete_len=100 >> ./res/log.txt



# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/log.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1500000 --write_num=5000000 --read_num=5000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1500000 --write_num=5000000 --read_num=5000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1500000 --write_num=5000000 --read_num=5000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/log.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1500000 --write_num=5000000 --read_num=5000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/log.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1500000 --write_num=5000000 --read_num=5000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1500000 --write_num=5000000 --read_num=5000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=2 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1500000 --write_num=5000000 --read_num=5000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --enable_rdfilter=false --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1500000 --write_num=5000000 --read_num=5000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/tmplog.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=1024 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=1024 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=1024 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=1024 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/tmplog.txt


# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/tmplog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=2 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/tmplog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --enable_rdfilter=false --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=16 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/tmplog.txt

####### Prepare
# ./tools/test_delete --mode=default --db_path=/home/wangfan/delete/global-range-delete/build/db/PrepareDB_K128_KV512_10M_default --workload=prepare --ksize=128 --kvsize=512 --buffer_size=64 --max_key=99999999 --prep_num=10000000 --level_comp=10 >> ./res/tmplog.txt
# ./tools/test_delete --mode=default --db_path=/home/wangfan/delete/global-range-delete/build/db/PrepareDB_K128_KV512_5M_default --workload=prepare --ksize=128 --kvsize=512 --buffer_size=64 --max_key=99999999 --prep_num=5000000 --level_comp=10 >> ./res/tmplog.txt
# ./tools/test_delete --mode=default --db_path=/home/wangfan/delete/global-range-delete/build/db/PrepareDB_K256_KV1024_10M_default --workload=prepare --ksize=256 --kvsize=1024 --buffer_size=64 --max_key=99999999 --prep_num=10000000 --level_comp=10 >> ./res/tmplog.txt
# ./tools/test_delete --mode=default --db_path=/home/wangfan/delete/global-range-delete/build/db/PrepareDB_K256_KV1024_5M_default --workload=prepare --ksize=256 --kvsize=1024 --buffer_size=64 --max_key=99999999 --prep_num=5000000 --level_comp=10 >> ./res/tmplog.txt

######## Test
# rm -rf ./testdb
# # cp -r ./db/PrepareDB_K128_KV512_10M_default ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# # cp -r ./db/PrepareDB_K128_KV512_10M_default ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K128_KV512_10M_default ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K128_KV512_10M_default ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt


# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_10M_default ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_10M_default ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K128_KV512_10M_default ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K128_KV512_10M_default ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=10000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt



# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --enable_rdfilter=false --level_comp=10 --ksize=128 --kvsize=512 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt


# echo "Test Group 1 ..." >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt



# echo "Test Group 2 ..." >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=5000000 --read_num=5000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=5000000 --read_num=5000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=5000000 --read_num=5000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=5000000 --read_num=5000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/testlog.txt


# echo "Test Group 3: Preload DB ..." >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_5M_default ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_5M_default ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_5M_default ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_5M_default ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt


# echo "Test Group 4: Preload DB ..." >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_5M_default ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=0 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_5M_default ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=0 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_5M_default ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=0 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_5M_default ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=0 --rdelete_len=100 >> ./res/testlog.txt


# echo "Test Group 4: Preload DB ..." >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_20M_default ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_20M_default ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_20M_default ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# cp -r ./db/PrepareDB_K256_KV1024_20M_default ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt


# echo "Test Group 1 ..." >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100 >> ./res/testlog.txt



echo "Test Group 10 ..." >> ./res/testlog_2.txt
rm -rf ./testdb
./tools/test_delete --mode=default --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=9000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/testlog_2.txt
rm -rf ./testdb
./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=9000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/testlog_2.txt
rm -rf ./testdb
./tools/test_delete --mode=scan --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=9000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/testlog_2.txt
rm -rf ./testdb
./tools/test_delete --mode=decom --full_rtree=true --workload=test --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=9000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 --rdelete_len=100 >> ./res/testlog_2.txt