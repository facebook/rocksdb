NATIVE_JAVA_CLASSES = org.rocksdb.RocksDB org.rocksdb.Options org.rocksdb.WriteBatch org.rocksdb.WriteBatchInternal org.rocksdb.WriteBatchTest org.rocksdb.WriteOptions org.rocksdb.Statistics
NATIVE_INCLUDE = ./include
ROCKSDB_JAR = rocksdbjni.jar

clean:
	-find . -name "*.class" -exec rm {} \;
	-find . -name "hs*.log" -exec rm {} \;
	rm -f $(ROCKSDB_JAR)

java:
	javac org/rocksdb/*.java org/rocksdb/util/*.java
	jar -cf $(ROCKSDB_JAR) org/rocksdb/*.class org/rocksdb/util/*.class
	javah -d $(NATIVE_INCLUDE) -jni $(NATIVE_JAVA_CLASSES)

sample: java
	javac -cp $(ROCKSDB_JAR) RocksDBSample.java
	@rm -rf /tmp/rocksdbjni
	@rm -rf /tmp/rocksdbjni_not_found
	java -ea -Djava.library.path=.:../ -cp ".:./*" RocksDBSample /tmp/rocksdbjni
	@rm -rf /tmp/rocksdbjni
	@rm -rf /tmp/rocksdbjni_not_found

test: java
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.WriteBatchTest

db_bench: java
	javac org/rocksdb/benchmark/*.java
	rm -rf /tmp/rocksdbjni-bench
	java -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.benchmark.DbBenchmark --threads=1 --benchmarks=fillseq,readrandom,readwhilewriting
	rm -rf /tmp/rocksdbjni-bench
	java -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.benchmark.DbBenchmark --threads=1 --benchmarks=fillseq,readrandom,readwhilewriting --cache_size=200000000
	rm -rf /tmp/rocksdbjni-bench
	java -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.benchmark.DbBenchmark --threads=2 --benchmarks=fillseq,readrandom,readwhilewriting
	rm -rf /tmp/rocksdbjni-bench
	java -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.benchmark.DbBenchmark --threads=2 --benchmarks=fillseq,readrandom,readwhilewriting --cache_size=200000000
	rm -rf /tmp/rocksdbjni-bench
	java -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.benchmark.DbBenchmark --threads=4 --benchmarks=fillseq,readrandom,readwhilewriting
	rm -rf /tmp/rocksdbjni-bench
	java -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.benchmark.DbBenchmark --threads=4 --benchmarks=fillseq,readrandom,readwhilewriting --cache_size=200000000
	rm -rf /tmp/rocksdbjni-bench
	java -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.benchmark.DbBenchmark --threads=8 --benchmarks=fillseq,readrandom,readwhilewriting
	rm -rf /tmp/rocksdbjni-bench
	java -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.benchmark.DbBenchmark --threads=8 --benchmarks=fillseq,readrandom,readwhilewriting --cache_size=200000000
	rm -rf /tmp/rocksdbjni-bench
	java -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.benchmark.DbBenchmark --threads=16 --benchmarks=fillseq,readrandom,readwhilewriting
	rm -rf /tmp/rocksdbjni-bench
	java -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.benchmark.DbBenchmark --threads=16 --benchmarks=fillseq,readrandom,readwhilewriting --cache_size=200000000
	rm -rf /tmp/rocksdbjni-bench
	java -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.benchmark.DbBenchmark --threads=32 --benchmarks=fillseq,readrandom,readwhilewriting
	rm -rf /tmp/rocksdbjni-bench
	java -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.benchmark.DbBenchmark --threads=32 --benchmarks=fillseq,readrandom,readwhilewriting --cache_size=200000000
