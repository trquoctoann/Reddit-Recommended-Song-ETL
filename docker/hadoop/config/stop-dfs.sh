echo "stopping dfs daemons"

hadoop-daemon.sh --config $HADOOP_CONFIG_DIRECTORY stop namenode
hadoop-daemon.sh --config $HADOOP_CONFIG_DIRECTORY stop secondarynamenode
hadoop-daemon.sh --config $HADOOP_CONFIG_DIRECTORY stop datanode
