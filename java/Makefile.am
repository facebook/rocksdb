# $Id: Makefile.am,v 1.0.0-0 2016/05/07 08:41:30 Cnangel Exp $

AUTOMAKE_OPTIONS						= foreign

NATIVE_JAVA_CLASSES						= org.rocksdb.AbstractCompactionFilter\
                                          org.rocksdb.AbstractComparator\
                                          org.rocksdb.AbstractSlice\
                                          org.rocksdb.BackupEngine\
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
                                          org.rocksdb.Env\
                                          org.rocksdb.FlushOptions\
                                          org.rocksdb.Filter\
                                          org.rocksdb.GenericRateLimiterConfig\
                                          org.rocksdb.HashLinkedListMemTableConfig\
                                          org.rocksdb.HashSkipListMemTableConfig\
                                          org.rocksdb.Logger\
                                          org.rocksdb.MergeOperator\
                                          org.rocksdb.Options\
                                          org.rocksdb.PlainTableConfig\
                                          org.rocksdb.ReadOptions\
                                          org.rocksdb.RemoveEmptyValueCompactionFilter\
                                          org.rocksdb.RestoreOptions\
                                          org.rocksdb.RocksDB\
                                          org.rocksdb.RocksEnv\
                                          org.rocksdb.RocksIterator\
                                          org.rocksdb.RocksMemEnv\
                                          org.rocksdb.SkipListMemTableConfig\
                                          org.rocksdb.Slice\
                                          org.rocksdb.Statistics\
                                          org.rocksdb.TransactionLogIterator\
                                          org.rocksdb.TtlDB\
                                          org.rocksdb.VectorMemTableConfig\
                                          org.rocksdb.Snapshot\
                                          org.rocksdb.StringAppendOperator\
                                          org.rocksdb.WriteBatch\
                                          org.rocksdb.WriteBatch.Handler\
                                          org.rocksdb.WriteOptions\
                                          org.rocksdb.WriteBatchWithIndex\
                                          org.rocksdb.WBWIRocksIterator\
										  $(NULL)

NATIVE_JAVA_TEST_CLASSES 				= org.rocksdb.WriteBatchTest\
										  org.rocksdb.WriteBatchTestInternalHelper\
										  $(NULL)

JAVA_TESTS                              = org.rocksdb.BackupableDBOptionsTest\
                                          org.rocksdb.BackupEngineTest\
                                          org.rocksdb.BlockBasedTableConfigTest\
                                          org.rocksdb.util.BytewiseComparatorTest\
                                          org.rocksdb.CheckPointTest\
                                          org.rocksdb.ColumnFamilyOptionsTest\
                                          org.rocksdb.ColumnFamilyTest\
                                          org.rocksdb.ComparatorOptionsTest\
                                          org.rocksdb.ComparatorTest\
                                          org.rocksdb.CompressionOptionsTest\
                                          org.rocksdb.DBOptionsTest\
                                          org.rocksdb.DirectComparatorTest\
                                          org.rocksdb.DirectSliceTest\
                                          org.rocksdb.util.EnvironmentTest\
                                          org.rocksdb.FilterTest\
                                          org.rocksdb.FlushTest\
                                          org.rocksdb.InfoLogLevelTest\
                                          org.rocksdb.KeyMayExistTest\
                                          org.rocksdb.LoggerTest\
                                          org.rocksdb.MemTableTest\
                                          org.rocksdb.MergeTest\
                                          org.rocksdb.MixedOptionsTest\
                                          org.rocksdb.NativeLibraryLoaderTest\
                                          org.rocksdb.OptionsTest\
                                          org.rocksdb.PlainTableConfigTest\
                                          org.rocksdb.ReadOnlyTest\
                                          org.rocksdb.ReadOptionsTest\
                                          org.rocksdb.RocksDBTest\
                                          org.rocksdb.RocksEnvTest\
                                          org.rocksdb.RocksIteratorTest\
                                          org.rocksdb.RocksMemEnvTest\
                                          org.rocksdb.util.SizeUnitTest\
                                          org.rocksdb.SliceTest\
                                          org.rocksdb.SnapshotTest\
                                          org.rocksdb.TransactionLogIteratorTest\
                                          org.rocksdb.TtlDBTest\
                                          org.rocksdb.StatisticsCollectorTest\
                                          org.rocksdb.WriteBatchHandlerTest\
                                          org.rocksdb.WriteBatchTest\
                                          org.rocksdb.WriteBatchThreadedTest\
                                          org.rocksdb.WriteOptionsTest\
                                          org.rocksdb.WriteBatchWithIndexTest\
										  $(NULL)

JAVA_TEST_LIBDIR						= test-libs
JAVA_JUNIT_JAR							= $(JAVA_TEST_LIBDIR)/junit-4.12.jar
JAVA_HAMCR_JAR							= $(JAVA_TEST_LIBDIR)/hamcrest-core-1.3.jar
JAVA_MOCKITO_JAR						= $(JAVA_TEST_LIBDIR)/mockito-all-1.10.19.jar
JAVA_CGLIB_JAR							= $(JAVA_TEST_LIBDIR)/cglib-2.2.2.jar
JAVA_ASSERTJ_JAR						= $(JAVA_TEST_LIBDIR)/assertj-core-1.7.1.jar
JAVA_TESTCLASSPATH						= $(JAVA_JUNIT_JAR):$(JAVA_HAMCR_JAR):$(JAVA_MOCKITO_JAR):$(JAVA_CGLIB_JAR):$(JAVA_ASSERTJ_JAR)
MVN_LOCAL								= $(HOME)/.m2/repository

SUBDIRS									= . \
										  rocksjni \
										  $(NULL)

all: java sample column_family_sample 

java: 
	$(AM_V_GEN)$(mkdir_p) target/classes
	$(AM_V_at)javac -d target/classes \
		$(top_srcdir)/java/src/main/java/org/rocksdb/util/*.java \
		$(top_srcdir)/java/src/main/java/org/rocksdb/*.java 
	$(AM_V_at)javah -cp target/classes -d $(top_builddir)/java/include -jni $(NATIVE_JAVA_CLASSES)

sample : java 
	$(AM_V_GEN)$(mkdir_p) samples/target/classes
	$(AM_V_at)javac -cp target/classes -d samples/target/classes $(top_srcdir)/java/samples/src/main/java/RocksDBSample.java
	java -ea -Xcheck:jni -Djava.library.path=target -cp target/classes:samples/target/classes RocksDBSample /tmp/rocksdbjni

column_family_sample: java 
	$(AM_V_GEN)$(mkdir_p) samples/target/classes
	$(AM_V_at)javac -cp target/classes -d samples/target/classes $(top_srcdir)/java/samples/src/main/java/RocksDBColumnFamilySample.java
	java -ea -Xcheck:jni -Djava.library.path=target -cp target/classes:samples/target/classes RocksDBColumnFamilySample /tmp/rocksdbjni

resolve_test_deps:
	$(AM_V_GEN)$(mkdir_p) $(JAVA_TEST_LIBDIR)
	test -s "$(JAVA_JUNIT_JAR)" || cp $(MVN_LOCAL)/junit/junit/4.12/junit-4.12.jar $(JAVA_TEST_LIBDIR) || curl -k -L -o $(JAVA_JUNIT_JAR) http://search.maven.org/remotecontent?filepath=junit/junit/4.12/junit-4.12.jar
	test -s "$(JAVA_HAMCR_JAR)" || cp $(MVN_LOCAL)/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar $(JAVA_TEST_LIBDIR) || curl -k -L -o $(JAVA_HAMCR_JAR) http://search.maven.org/remotecontent?filepath=org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar
	test -s "$(JAVA_MOCKITO_JAR)" || cp $(MVN_LOCAL)/org/mockito/mockito-all/1.10.19/mockito-all-1.10.19.jar $(JAVA_TEST_LIBDIR) || curl -k -L -o "$(JAVA_MOCKITO_JAR)" http://search.maven.org/remotecontent?filepath=org/mockito/mockito-all/1.10.19/mockito-all-1.10.19.jar
	test -s "$(JAVA_CGLIB_JAR)" || cp $(MVN_LOCAL)/cglib/cglib/2.2.2/cglib-2.2.2.jar $(JAVA_TEST_LIBDIR) || curl -k -L -o "$(JAVA_CGLIB_JAR)" http://search.maven.org/remotecontent?filepath=cglib/cglib/2.2.2/cglib-2.2.2.jar
	test -s "$(JAVA_ASSERTJ_JAR)" || cp $(MVN_LOCAL)/org/assertj/assertj-core/1.7.1/assertj-core-1.7.1.jar $(JAVA_TEST_LIBDIR) || curl -k -L -o "$(JAVA_ASSERTJ_JAR)" http://central.maven.org/maven2/org/assertj/assertj-core/1.7.1/assertj-core-1.7.1.jar

java_test: resolve_test_deps
	$(AM_V_GEN)$(mkdir_p) target/test-classes
	$(AM_V_at)javac -cp target/classes:$(JAVA_TESTCLASSPATH) -d target/test-classes \
		$(top_srcdir)/java/src/test/java/org/rocksdb/test/*.java \
		$(top_srcdir)/java/src/test/java/org/rocksdb/util/*.java \
		$(top_srcdir)/java/src/test/java/org/rocksdb/*.java
	$(AM_V_at)javah -cp target/classes:target/test-classes -d $(top_builddir)/java/include -jni $(NATIVE_JAVA_TEST_CLASSES)

test: java resolve_test_deps java_test
	java -ea -Xcheck:jni -Djava.library.path=target -cp "target/classes:target/test-classes:$(JAVA_TESTCLASSPATH):target/*" org.rocksdb.test.RocksJunitRunner $(JAVA_TESTS)

bench: java 
	$(AM_V_GEN)$(mkdir_p) benchmark/target/classes
	$(AM_V_at)javac -cp target/classes -d benchmark/target/classes $(top_srcdir)/java/benchmark/src/main/java/org/rocksdb/benchmark/*.java

clean:
	$(AM_V_at)rm -rf include/*
	$(AM_V_at)rm -rf test-libs/
	$(AM_V_at)rm -rf target
	$(AM_V_at)rm -rf benchmark/target
	$(AM_V_at)rm -rf samples/target
