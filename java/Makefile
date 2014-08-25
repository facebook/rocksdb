NATIVE_JAVA_CLASSES = org.rocksdb.RocksDB org.rocksdb.Options org.rocksdb.WriteBatch org.rocksdb.WriteBatchInternal org.rocksdb.WriteBatchTest org.rocksdb.WriteOptions org.rocksdb.BackupableDB org.rocksdb.BackupableDBOptions org.rocksdb.Statistics org.rocksdb.RocksIterator org.rocksdb.VectorMemTableConfig org.rocksdb.SkipListMemTableConfig org.rocksdb.HashLinkedListMemTableConfig org.rocksdb.HashSkipListMemTableConfig org.rocksdb.PlainTableConfig org.rocksdb.BlockBasedTableConfig org.rocksdb.ReadOptions org.rocksdb.Filter org.rocksdb.BloomFilter org.rocksdb.RestoreOptions org.rocksdb.RestoreBackupableDB org.rocksdb.RocksEnv

NATIVE_INCLUDE = ./include
ROCKSDB_JAR = rocksdbjni.jar

clean:
	-find . -name "*.class" -exec rm {} \;
	-find . -name "hs*.log" -exec rm {} \;
	rm -f $(ROCKSDB_JAR)

java:
	javac org/rocksdb/util/*.java org/rocksdb/*.java
	@cp ../HISTORY.md ./HISTORY-CPP.md
	@rm -f ./HISTORY-CPP.md
	javah -d $(NATIVE_INCLUDE) -jni $(NATIVE_JAVA_CLASSES)

sample: java
	javac -cp $(ROCKSDB_JAR) RocksDBSample.java
	@rm -rf /tmp/rocksdbjni
	@rm -rf /tmp/rocksdbjni_not_found
	java -ea -Djava.library.path=.:../ -cp ".:./*" -Xcheck:jni RocksDBSample /tmp/rocksdbjni
	@rm -rf /tmp/rocksdbjni
	@rm -rf /tmp/rocksdbjni_not_found

test: java
	javac org/rocksdb/test/*.java
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.WriteBatchTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.BackupableDBTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.OptionsTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.ReadOptionsTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.StatisticsCollectorTest

db_bench: java
	javac org/rocksdb/benchmark/*.java
