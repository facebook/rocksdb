# shellcheck disable=SC2148
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
export USE_HDFS=1
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$JAVA_HOME/jre/lib/amd64:$HADOOP_HOME/lib/native

export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
for f in `find /usr/lib/hadoop-hdfs | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
for f in `find /usr/lib/hadoop | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
for f in `find /usr/lib/hadoop/client | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
