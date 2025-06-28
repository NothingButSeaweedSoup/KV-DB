#!/bin/bash

JAVA_CLASSPATH="target/classes;target/dependency/*"
JAVA_MAIN_CLASS="cli.CommandLineInterface"
DATA_PATH="data"

java -cp $JAVA_CLASSPATH $JAVA_MAIN_CLASS $DATA_PATH

exit_status=$?

if [ $exit_status -ne 0 ]; then
    echo "Java程序启动失败，退出状态码: $exit_status"
    exit $exit_status
fi

exit 0