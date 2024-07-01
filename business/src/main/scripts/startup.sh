#!/bin/sh

APP_HOME=$(echo `pwd`)
APPIDFILE=$APP_HOME/lurker-biz-proxy.pid
HEAP_MEMORY=5120m
PERM_MEMORY=256m
YOUNG_MEMORY=256m
DIRECT_MEMORY=5120m
SERVER_NAME=lurker_biz_proxy
GC_LOG=${APP_HOME}/logs/gc.log

case $1 in
start)
    echo  "Starting Bigstore Service ... "

	ARGS=($*)
	for ((i=0; i<${#ARGS[@]}; i++)); do
	  case "${ARGS[$i]}" in
	  -D*) export JAVA_OPTS="${JAVA_OPTS} ${ARGS[$i]}" ;;
	  -Heap*) HEAP_MEMORY="${ARGS[$i+1]}" ;;
	  -Perm*) PERM_MEMORY="${ARGS[$i+1]}" ;;
	  -JmxPor:wqt*)  JMX_PORT="${ARGS[$i+1]}" ;;
	    *) parameters="${parameters} ${ARGS[$i]}" ;;
	  esac
	done

	JAVA_OPTS=" -server
              -XX:+UseG1GC
              -XX:G1HeapRegionSize=32M
	            -XX:+ScavengeBeforeFullGC
	            -Xms${HEAP_MEMORY}
	            -Xmx${HEAP_MEMORY}
	            -XX:MaxDirectMemorySize=${DIRECT_MEMORY}
	            -XX:PermSize=${PERM_MEMORY}
	            -XX:MaxPermSize=${PERM_MEMORY}
	            -Duser.dir=${APP_HOME}
	            -verbose:gc
	            -XX:+PrintGCDateStamps
	            -XX:+PrintGCDetails
	            -Xloggc:${GC_LOG}
	            -XX:+HeapDumpOnOutOfMemoryError
	            -XX:+UseCompressedOops "
	echo "start jvm args ${JAVA_OPTS}"
	nohup java $JAVA_OPTS -jar ${APP_HOME}/bigstore-business-2.0.0.jar > out.log &

    echo $! > $APPIDFILE
    echo STARTED
    ;;

stop)
    echo "Stopping Bigstore Service ... "
	  nohup java -cp ${APP_HOME}/bigstore-business-2.0.0.jar com.wuqing.client.bigstore.run.StopService > stop.log &
	  sleep 10
    if [ -f $APPIDFILE ]; then
      echo "kill bigstore...."
      PID=$(cat $APPIDFILE)
      kill  $PID
    fi
	;;
*)
    echo "Exec ... "
    ant $*
    ;;

esac

exit 0