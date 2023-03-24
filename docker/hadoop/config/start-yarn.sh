echo "starting yarn daemons"

yarn-daemon.sh --config $HADOOP_CONFIG_DIRECTORY start resourcemanager
yarn-daemon.sh --config $HADOOP_CONFIG_DIRECTORY start nodemanager