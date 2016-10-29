#!/bin/bash

VER=3.0.0-alpha1
TIMEOUT=5
THREADS=2

COMMON_VERSION=${COMMON_VERSION:-${VER}}
HDFS_VERSION=${HDFS_VERSION:-${VER}}
YARN_VERSION=${YARN_VERSION:-${VER}}
HIVE_VERSION=${HIVE_VERSION:-1.2.1}
TEZ_VERSION=${TEZ_VERSION:-0.7.1-SNAPSHOT-minimal}

ENV="JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64 \
  YARN_HOME=/users/kbavishi/software/hadoop-${YARN_VERSION} \
  HADOOP_LOG_DIR=/users/kbavishi/logs/hadoop \
  HADOOP_CONF_DIR=/users/kbavishi/conf \
  HADOOP_USER_CLASSPATH_FIRST=1 \
  HADOOP_COMMON_HOME=/users/kbavishi/software/hadoop-${COMMON_VERSION} \
  HADOOP_HDFS_HOME=/users/kbavishi/software/hadoop-${HDFS_VERSION} \
  HADOOP_YARN_HOME=/users/kbavishi/software/hadoop-${YARN_VERSION} \
  HADOOP_HOME=/users/kbavishi/software/hadoop-${COMMON_VERSION} \
  HADOOP_BIN_PATH=/users/kbavishi/software/hadoop-${COMMON_VERSION}/bin \
  HADOOP_SBIN=/users/kbavishi/software/hadoop-${COMMON_VERSION}/bin \
  HADOOP_MAPRED_HOME=/users/kbavishi/software/hadoop-${COMMON_VERSION} \
  HIVE_HOME=/users/kbavishi/software/hive-1.2.1 \
  TEZ_CONF_DIR=/users/kbavishi/software/conf \
  TEZ_JARS=/users/kbavishi/software/tez-${TEZ_VERSION}"

case "$1" in
  (-q|--quiet)
    for i in ${ENV}
    do
      export $i
    done
    ;;
  (*)
    echo "setting variables:"
    for i in $ENV
    do
      echo $i
      export $i
    done
    ;;
esac

export HADOOP_CLASSPATH=$HADOOP_HOME:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_CONF_DIR:$HIVE_HOME:$TEZ_JARS/*:$TEZ_JARS/lib/*:
export HADOOP_HEAPSIZE=10240

export PATH=/users/kbavishi/software/hadoop-${COMMON_VERSION}/bin:/users/kbavishi/software/hadoop-${COMMON_VERSION}/sbin:$HIVE_HOME/bin:$PATH
export LD_LIBRARY_PATH=${HADOOP_COMMON_HOME}/share/hadoop/common/lib/native/:${LD_LIBRARY_PATH}
export JAVA_LIBRARY_PATH=${LD_LIBRARY_PATH}


start_hdfs(){
	printf "\n==== START HDFS daemons ! ====\n"
	hdfs --daemon start namenode
	pdsh -R exec -f $THREADS -w ^instances ssh -o ConnectTimeout=$TIMEOUT %h '( . /users/kbavishi/run.sh -q ; hdfs --daemon start datanode;)'
	hdfs dfsadmin -safemode leave
}

stop_hdfs(){
	printf "\n==== STOP HDFS daemons ! ====\n"
	pdsh -R exec -f $THREADS -w ^instances ssh -o ConnectTimeout=$TIMEOUT %h '( . /users/kbavishi/run.sh -q ; hdfs --daemon stop datanode;)'
	hdfs --daemon stop namenode
}

start_yarn(){
	printf "\n===== START YARN daemons ! ====\n"
	yarn --daemon start resourcemanager
	pdsh -R exec -f $THREADS -w ^instances ssh -o ConnectTimeout=$TIMEOUT %h '( . /users/kbavishi/run.sh -q ; yarn --daemon start nodemanager;)'
}
 
stop_yarn(){
	printf "\n==== STOP YARN daemons ! ====\n"
	pdsh -R exec -f $THREADS -w ^instances ssh -o ConnectTimeout=$TIMEOUT %h '( . /users/kbavishi/run.sh -q ; yarn --daemon stop nodemanager;)'
	yarn --daemon stop resourcemanager
}

start_history_mr(){
	printf "\n==== START M/R history server ! ====\n"
	mapred --daemon start historyserver
}

stop_history_mr(){
	printf "\n==== STOP M/R history server ! ====\n"
	mapred --daemon	stop historyserver
}

start_timeline_server(){
	printf "\n==== START timelineserver ! ====\n"
	yarn --daemon start timelineserver
}

stop_timeline_server(){
	printf "\n==== STOP timelineserver ! ====\n"
	yarn --daemon stop timelineserver
}

start_all(){
	start_hdfs
	start_yarn
	start_timeline_server
	start_history_mr
}

stop_all(){
	stop_hdfs
	stop_yarn
	stop_timeline_server
	stop_history_mr
}

export -f start_hdfs
export -f start_yarn
export -f start_all
export -f stop_hdfs
export -f stop_yarn
export -f stop_all
export -f start_history_mr
export -f stop_history_mr
export -f start_timeline_server
export -f stop_timeline_server
