PLATFORM=64
if [ `getconf LONG_BIT` != "64" ]
then
  PLATFORM=32
fi
echo "Running benchmark in $PLATFORM-Bit mode."
java -server -d$PLATFORM -XX:NewSize=4m -XX:+AggressiveOpts -Djava.library.path=.:../ -cp "rocksdbjni.jar:.:./*" org.rocksdb.benchmark.DbBenchmark $@
