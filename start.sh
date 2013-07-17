#!/bin/sh
javac -d bin -s src -cp lib/hadoop-core-1.1.2.jar:lib/commons-io-2.1.jar @sourceFiles
jar -cmf Manifest.mf HadoopMapReduce.jar -C bin . -C lib .
java -jar HadoopMapReduce.jar $1 $2 $3 $4
