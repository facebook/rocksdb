
# echo "Test Group 1 ..." >> ./res/pre_testlog_2.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test_1 --full_rtree=true --enable_rdfilter=false --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=0 --rdelete_len=100  >> ./res/pre_testlog_2.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test_1 --full_rtree=true --enable_rdfilter=false --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=500000 --rdelete_len=100  >> ./res/pre_testlog_2.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test_1 --full_rtree=true --enable_rdfilter=false --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=500000 --rdelete_len=100  >> ./res/pre_testlog_2.txt


# echo "Test Group 2 ..." >> ./res/pre_testlog_2.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test_2 --full_rtree=true --enable_rdfilter=false --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=0 --rdelete_len=100  >> ./res/pre_testlog_2.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test_2 --full_rtree=true --enable_rdfilter=false --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=500000 --rdelete_len=100  >> ./res/pre_testlog_2.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test_2 --full_rtree=true --enable_rdfilter=false --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=500000 --rdelete_len=100  >> ./res/pre_testlog_2.txt


echo "Test Group 3 ..." >> ./res/pre_testlog_3.txt
rm -rf ./testdb
./tools/test_delete --mode=default --workload=test_3 --full_rtree=true --enable_rdfilter=false --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=0 --rdelete_len=100  >> ./res/pre_testlog_3.txt
rm -rf ./testdb
./tools/test_delete --mode=default --workload=test_3 --full_rtree=true --enable_rdfilter=false --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=500000 --rdelete_len=100  >> ./res/pre_testlog_3.txt
rm -rf ./testdb
./tools/test_delete --mode=grd --workload=test_3 --full_rtree=true --enable_rdfilter=false --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=500000 --rdelete_len=100  >> ./res/pre_testlog_3.txt
rm -rf ./testdb
./tools/test_delete --mode=grd --workload=test_3 --full_rtree=true --enable_rdfilter=true --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=500000 --rdelete_len=100  >> ./res/pre_testlog_3.txt
rm -rf ./testdb
./tools/test_delete --mode=grd --workload=test_3 --full_rtree=true --enable_rdfilter=false --enable_crosscheck=true --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=500000 --rdelete_len=100  >> ./res/pre_testlog_3.txt

sudo perf record -g -o perf.data --call-graph=dwarf ./tools/test_delete --mode=grd --workload=test_3 --full_rtree=true --enable_rdfilter=false --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=500000 --rdelete_len=100
# echo "Test Group 4 ..." >> ./res/pre_testlog_2.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test_4 --full_rtree=true --enable_rdfilter=false --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=0 --rdelete_len=100  >> ./res/pre_testlog_2.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test_4 --full_rtree=true --enable_rdfilter=false --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=500000 --rdelete_len=100  >> ./res/pre_testlog_2.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test_4 --full_rtree=true --enable_rdfilter=false --level_comp=10 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=2048 --max_key=99999999 --rdelete_num=500000 --rdelete_len=100  >> ./res/pre_testlog_2.txt
