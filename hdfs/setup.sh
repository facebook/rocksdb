# shellcheck disable=SC2148
export USE_HDFS=1
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$JAVA_HOME/jre/lib/amd64:$HADOOP_HOME/lib/native
export HADOOP_LOG_DIR=$HADOOP_HOME/logs

export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
#for f in `find $HADOOP_HOME/share/hadoop/hdfs | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
#for f in `find $HADOOP_HOME | grep '*.jar'`; do export CLASSPATH=$CLASSPATH:$f; done
#for f in `find $HADOOP_HOME/share/hadoop/client | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
