NATIVE_JAVA_CLASSES = org.rocksdb.RocksDB org.rocksdb.Options
NATIVE_INCLUDE = ./include
ROCKSDB_JAR = rocksdbjni.jar

clean:
	-find . -name "*.class" -exec rm {} \;
	-find . -name "hs*.log" -exec rm {} \;
	rm -f $(ROCKSDB_JAR)

java:
	javac org/rocksdb/*.java
	jar -cf $(ROCKSDB_JAR) org/rocksdb/*.class
	javah -d $(NATIVE_INCLUDE) -jni $(NATIVE_JAVA_CLASSES)

sample:
	javac -cp $(ROCKSDB_JAR) RocksDBSample.java
	@rm -rf /tmp/rocksdbjni
	@rm -rf /tmp/rocksdbjni_not_found
	java -ea -Djava.library.path=.:../ -cp ".:./*" RocksDBSample /tmp/rocksdbjni
	@rm -rf /tmp/rocksdbjni
	@rm -rf /tmp/rocksdbjni_not_found
