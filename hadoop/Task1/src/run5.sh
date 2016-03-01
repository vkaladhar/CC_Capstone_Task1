#!/usr/bin/env bash

hadoop fs -rm -r -f /data/airline_ontime/Output

export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
hadoop com.sun.tools.javac.Main ./src/*.* -d build
jar -cvf Task1.jar -C build/ ./
hadoop jar Task1.jar Task1
