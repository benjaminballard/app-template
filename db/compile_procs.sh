#!/usr/bin/env bash

CP="$(dirname $(dirname "$(which voltdb)"))/voltdb/voltdb-*.jar"
SRC=`find src -name "*.java"`

if [ ! -z "$SRC" ]; then
    mkdir -p obj
    javac -classpath $CP -d obj $SRC
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi

    jar cf procs.jar -C obj .
    rm -rf obj
fi



