NATIVE_JAVA_CLASSES = org.rocksdb.AbstractComparator\
	org.rocksdb.AbstractSlice\
	org.rocksdb.BackupableDB\
	org.rocksdb.BackupableDBOptions\
	org.rocksdb.BlockBasedTableConfig\
	org.rocksdb.BloomFilter\
	org.rocksdb.Checkpoint\
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
	org.rocksdb.test.WriteBatchInternal\
	org.rocksdb.test.WriteBatchTest\
        org.rocksdb.WriteOptions\
	org.rocksdb.WriteBatchWithIndex\
	org.rocksdb.WBWIRocksIterator

ROCKSDB_MAJOR = $(shell egrep "ROCKSDB_MAJOR.[0-9]" ../include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_MINOR = $(shell egrep "ROCKSDB_MINOR.[0-9]" ../include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_PATCH = $(shell egrep "ROCKSDB_PATCH.[0-9]" ../include/rocksdb/version.h | cut -d ' ' -f 3)

NATIVE_INCLUDE = ./include
ARCH := $(shell getconf LONG_BIT)
ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-linux$(ARCH).jar
ifeq ($(PLATFORM), OS_MACOSX)
ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-osx.jar
endif

JAVA_TESTS = org.rocksdb.test.BackupableDBOptionsTest\
        org.rocksdb.test.BackupableDBTest\
		org.rocksdb.test.BlockBasedTableConfigTest\
		org.rocksdb.test.CheckPointTest\
		org.rocksdb.test.ColumnFamilyOptionsTest\
		org.rocksdb.test.ColumnFamilyTest\
		org.rocksdb.test.ComparatorOptionsTest\
		org.rocksdb.test.ComparatorTest\
		org.rocksdb.test.DBOptionsTest\
		org.rocksdb.test.DirectComparatorTest\
		org.rocksdb.test.DirectSliceTest\
		org.rocksdb.test.EnvironmentTest\
		org.rocksdb.test.FilterTest\
		org.rocksdb.test.FlushTest\
		org.rocksdb.test.InfoLogLevelTest\
		org.rocksdb.test.KeyMayExistTest\
		org.rocksdb.test.MemTableTest\
		org.rocksdb.test.MergeTest\
		org.rocksdb.test.MixedOptionsTest\
		org.rocksdb.test.OptionsTest\
		org.rocksdb.test.PlainTableConfigTest\
		org.rocksdb.test.ReadOnlyTest\
		org.rocksdb.test.ReadOptionsTest\
		org.rocksdb.test.RocksDBTest\
		org.rocksdb.test.RocksEnvTest\
		org.rocksdb.test.RocksIteratorTest\
		org.rocksdb.test.SizeUnitTest\
		org.rocksdb.test.SliceTest\
		org.rocksdb.test.SnapshotTest\
		org.rocksdb.test.StatisticsCollectorTest\
		org.rocksdb.test.WriteBatchHandlerTest\
		org.rocksdb.test.WriteBatchTest\
		org.rocksdb.test.WriteOptionsTest\
		org.rocksdb.test.WriteBatchWithIndexTest

JAVA_TEST_LIBDIR = ./test-libs/
JAVA_JUNIT_JAR = $(JAVA_TEST_LIBDIR)junit-4.12-beta-2.jar
JAVA_HAMCR_JAR = $(JAVA_TEST_LIBDIR)hamcrest-core-1.3.jar
JAVA_MOCKITO_JAR = $(JAVA_TEST_LIBDIR)mockito-all-1.9.5.jar
JAVA_CGLIB_JAR = $(JAVA_TEST_LIBDIR)cglib-2.2.2.jar
JAVA_ASSERTJ_JAR = $(JAVA_TEST_LIBDIR)assertj-core-1.7.0.jar
JAVA_TESTCLASSPATH = $(ROCKSDB_JAR):$(JAVA_JUNIT_JAR):$(JAVA_HAMCR_JAR):$(JAVA_MOCKITO_JAR):$(JAVA_CGLIB_JAR):$(JAVA_ASSERTJ_JAR):.:./*

clean:
	-find . -name "*.class" -exec rm {} \;
	rm -rf include/*
	rm -rf javadoc/*
	rm -rf test-libs/
	rm -rf target
	rm -rf librocksdbjni*
	rm -f rocksdbjni*


javadocs:
	mkdir -p javadoc; javadoc -d javadoc -sourcepath . -subpackages org -exclude org.rocksdb.test

javalib: java javadocs

java: resolve_test_deps
	javac org/rocksdb/util/*.java org/rocksdb/*.java
	javac -cp $(JAVA_TESTCLASSPATH) org/rocksdb/test/*.java
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

resolve_test_deps:
	mkdir -p "$(JAVA_TEST_LIBDIR)"
	test -s "$(JAVA_JUNIT_JAR)" || curl -k -L -o $(JAVA_JUNIT_JAR) http://search.maven.org/remotecontent?filepath=junit/junit/4.12-beta-2/junit-4.12-beta-2.jar
	test -s "$(JAVA_HAMCR_JAR)" || curl -k -L -o $(JAVA_HAMCR_JAR) http://search.maven.org/remotecontent?filepath=org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar
	test -s "$(JAVA_MOCKITO_JAR)" || curl -k -L -o "$(JAVA_MOCKITO_JAR)" http://search.maven.org/remotecontent?filepath=org/mockito/mockito-all/1.9.5/mockito-all-1.9.5.jar
	test -s "$(JAVA_CGLIB_JAR)" || curl -k -L -o "$(JAVA_CGLIB_JAR)" http://search.maven.org/remotecontent?filepath=cglib/cglib/2.2.2/cglib-2.2.2.jar
	test -s "$(JAVA_ASSERTJ_JAR)" || curl -k -L -o "$(JAVA_ASSERTJ_JAR)" http://central.maven.org/maven2/org/assertj/assertj-core/1.7.0/assertj-core-1.7.0.jar

test: java resolve_test_deps
	java -ea -Xcheck:jni -Djava.library.path=.:../ -cp "$(JAVA_TESTCLASSPATH)" org.rocksdb.test.RocksJunitRunner $(JAVA_TESTS)

db_bench: java
	javac org/rocksdb/benchmark/*.java
