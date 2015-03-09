#!/usr/bin/env bash

CP="`ls $(dirname $(dirname $(which voltdb)))/voltdb/voltdbclient-*.jar`"
SRC=`find src/benchmark -name "*.java"`

if [ ! -z "$SRC" ]; then
    mkdir -p obj
    javac -classpath $CP -d obj $SRC
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi

    jar cf client.jar -C obj .
    rm -rf obj

    java -classpath "client.jar:$CP" benchmark.Benchmark $*
         
fi

