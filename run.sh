export HADOOP_CONF_DIR=${HOME}/sp19/Hadoop-Project/client-config
if (( $# >= 3 ));then 
	hadoop jar ${@:1}
fi
