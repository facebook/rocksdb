# sudo ./tools/test_delete --db_path=/home/wangfan/delete/global-range-delete/build/testdb --mode=mixseq --num=100000 --prep_num=100000 --write_num=100000 --read_num=100000 --zread_num=0 --seek_num=1000 --seek_keys=100 --range_del_num=1000
# sudo ./tools/test_delete --db_path=/home/wangfan/delete/global-range-delete/build/testdb --mode=default

# sudo ./tools/test_delete --mode=grd --workload=test --prep_num=10000 --write_num=10000 --read_num=10 --seek_num=10 --seek_len=5 --rdelete_num=10

# sudo ./tools/test_delete --mode=grd --workload=test --prep_num=10000 --write_num=10000 --read_num=0 --seek_num=10 --seek_len=5 --rdelete_num=10

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --prep_num=1000000 --write_num=100000 --read_num=50000 --seek_num=10 --rdelete_num=50000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --prep_num=1000000 --write_num=100000 --read_num=50000 --seek_num=10 --rdelete_num=50000 >> ./res/Testlog.txt
# ./tools/test_delete --mode=grd --workload=test --rep_buffer_size=1 --prep_num=1000000 --write_num=100000 --read_num=50000 --seek_num=10 --rdelete_num=50000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --prep_num=10000000 --write_num=1000000 --read_num=500000 --seek_num=10 --rdelete_num=500000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --prep_num=10000000 --write_num=1000000 --read_num=500000 --seek_num=10 --rdelete_num=500000 >> ./res/Testlog.txt




# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=5000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=5000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=10000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=10000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000 >> ./res/Testlog.txt

# # rm -rf ./testdb
# # ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt

# # rm -rf ./testdb
# # ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 >> ./res/Testlog.txt



# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=800 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=5000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=800 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=5000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=800 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=10000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=800 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=10000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=800 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=800 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=800 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=800 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=800 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=800 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 >> ./res/Testlog.txt



# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=64 --rep_buffer_size=655 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=5000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=64 --rep_buffer_size=655 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=5000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=64 --rep_buffer_size=655 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=10000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=64 --rep_buffer_size=655 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=10000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=64 --rep_buffer_size=655 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=64 --rep_buffer_size=655 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=64 --rep_buffer_size=655 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=64 --rep_buffer_size=655 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=64 --rep_buffer_size=655 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=64 --rep_buffer_size=655 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 >> ./res/Testlog.txt




# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=1024 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=5000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=1024 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=5000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=1024 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=10000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=1024 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=10000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=1024 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=1024 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=1024 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=1024 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt

# rm -rf ./testdb
# ./tools/test_delete --mode=default --workload=test --kvsize=1024 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 >> ./res/Testlog.txt
# rm -rf ./testdb
# ./tools/test_delete --mode=grd --workload=test --kvsize=1024 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 >> ./res/Testlog.txt


rm -rf ./testdb
./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000 >> ./res/Testlog.txt
rm -rf ./testdb
./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=50000 >> ./res/Testlog.txt

rm -rf ./testdb
./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt
rm -rf ./testdb
./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=100000 >> ./res/Testlog.txt

rm -rf ./testdb
./tools/test_delete --mode=default --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 >> ./res/Testlog.txt
rm -rf ./testdb
./tools/test_delete --mode=grd --workload=test --kvsize=128 --buffer_size=16 --rep_buffer_size=160 --prep_num=1000000 --write_num=1000000 --read_num=1000000 --seek_num=0 --rdelete_num=500000 >> ./res/Testlog.txt

