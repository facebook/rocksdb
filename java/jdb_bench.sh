java -server -d64 -XX:NewSize=4m -XX:+AggressiveOpts -Djava.library.path=.:../ -cp "rocksdbjni.jar:.:./*" org.rocksdb.benchmark.DbBenchmark $@
