activate_spark:
	${SPARK_HOME}/sbin/start-master.sh
	${SPARK_HOME}/sbin/start-worker.sh spark://ubuntu1:7077

deactivate_spark:
	${SPARK_HOME}/sbin/stop-master.sh
	${SPARK_HOME}/sbin/stop-worker.sh spark://ubuntu1:7077
