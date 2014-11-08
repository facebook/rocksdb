NATIVE_JAVA_CLASSES = org.rocksdb.AbstractComparator\
	org.rocksdb.AbstractSlice\
	org.rocksdb.BackupableDB\
	org.rocksdb.BackupableDBOptions\
	org.rocksdb.BlockBasedTableConfig\
	org.rocksdb.BloomFilter\
	org.rocksdb.ColumnFamilyHandle\
	org.rocksdb.ColumnFamilyOptions\
	org.rocksdb.Comparator\
	org.rocksdb.ComparatorOptions\
	org.rocksdb.DBOptions\
	org.rocksdb.DirectComparator\
	org.rocksdb.DirectSlice\
	org.rocksdb.FlushOptions\
	org.rocksdb.Filter\
	org.rocksdb.GenericRateLimiterConfig\
	org.rocksdb.HashLinkedListMemTableConfig\
	org.rocksdb.HashSkipListMemTableConfig\
	org.rocksdb.MergeOperator\
	org.rocksdb.Options\
	org.rocksdb.PlainTableConfig\
	org.rocksdb.ReadOptions\
	org.rocksdb.RestoreBackupableDB\
	org.rocksdb.RestoreOptions\
	org.rocksdb.RocksDB\
	org.rocksdb.RocksEnv\
	org.rocksdb.RocksIterator\
	org.rocksdb.SkipListMemTableConfig\
	org.rocksdb.Slice\
	org.rocksdb.Statistics\
	org.rocksdb.VectorMemTableConfig\
	org.rocksdb.StringAppendOperator\
	org.rocksdb.WriteBatch\
	org.rocksdb.WriteBatch.Handler\
	org.rocksdb.WriteBatchInternal\
	org.rocksdb.WriteBatchTest\
	org.rocksdb.WriteOptions\

ROCKSDB_MAJOR = $(shell egrep "ROCKSDB_MAJOR.[0-9]" ../include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_MINOR = $(shell egrep "ROCKSDB_MINOR.[0-9]" ../include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_PATCH = $(shell egrep "ROCKSDB_PATCH.[0-9]" ../include/rocksdb/version.h | cut -d ' ' -f 3)

NATIVE_INCLUDE = ./include
ARCH := $(shell getconf LONG_BIT)
ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-linux$(ARCH).jar

ifeq ($(PLATFORM), OS_MACOSX)
ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-osx.jar
endif

clean:
	-find . -name "*.class" -exec rm {} \;
	-find . -name "hs*.log" -exec rm {} \;
	rm -rf javadoc/*

javadocs:
	mkdir -p javadoc; javadoc -d javadoc -sourcepath . -subpackages org

java: javadocs
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

column_family_sample: java
	javac -cp $(ROCKSDB_JAR) RocksDBColumnFamilySample.java
	@rm -rf /tmp/rocksdbjni
	java -ea -Djava.library.path=.:../ -cp ".:./*" -Xcheck:jni RocksDBColumnFamilySample /tmp/rocksdbjni
	@rm -rf /tmp/rocksdbjni

test: java
	@rm -rf /tmp/rocksdbjni_*
	javac org/rocksdb/test/*.java
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.WriteBatchTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.BackupableDBTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.BlockBasedTableConfigTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.DBOptionsTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.ColumnFamilyTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.ColumnFamilyOptionsTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.FilterTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.FlushTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.KeyMayExistTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.MemTableTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.MergeTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.MixedOptionsTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.OptionsTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.PlainTableConfigTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.ReadOnlyTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.ReadOptionsTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.RocksIteratorTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.SnapshotTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.StatisticsCollectorTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.ComparatorOptionsTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.ComparatorTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.DirectComparatorTest
	java -ea -Djava.library.path=.:../ -cp "$(ROCKSDB_JAR):.:./*" org.rocksdb.test.WriteBatchHandlerTest
	@rm -rf /tmp/rocksdbjni_*

db_bench: java
	javac org/rocksdb/benchmark/*.java
