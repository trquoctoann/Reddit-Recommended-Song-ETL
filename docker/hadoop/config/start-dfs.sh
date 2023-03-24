echo "starting dfs daemons"

hadoop-daemon.sh --config $HADOOP_CONFIG_DIRECTORY start namenode
hadoop-daemon.sh --config $HADOOP_CONFIG_DIRECTORY start secondarynamenode
hadoop-daemon.sh --config $HADOOP_CONFIG_DIRECTORY start datanode