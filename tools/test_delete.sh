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



rm -rf ./testdb
./tools/test_delete --mode=default --workload=test --ksize=32 --kvsize=512 --level_comp=1 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
rm -rf ./testdb
./tools/test_delete --mode=grd --workload=test --ksize=32 --kvsize=512 --level_comp=1 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
rm -rf ./testdb
./tools/test_delete --mode=grd --workload=test --ksize=32 --kvsize=512 --level_comp=10 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt


rm -rf ./testdb
./tools/test_delete --mode=default --workload=test --ksize=128 --kvsize=512 --level_comp=1 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
rm -rf ./testdb
./tools/test_delete --mode=grd --workload=test --ksize=128 --kvsize=512 --level_comp=1 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
rm -rf ./testdb
./tools/test_delete --mode=grd --workload=test --ksize=128 --kvsize=512 --level_comp=10 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt


rm -rf ./testdb
./tools/test_delete --mode=default --workload=test --ksize=256 --kvsize=512 --level_comp=1 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
rm -rf ./testdb
./tools/test_delete --mode=grd --workload=test --ksize=256 --kvsize=512 --level_comp=1 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
rm -rf ./testdb
./tools/test_delete --mode=grd --workload=test --ksize=256 --kvsize=512 --level_comp=10 --buffer_size=64 --rep_buffer_size=655 --prep_num=1500000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
