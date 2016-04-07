#!/usr/bin/env bash

export LIB_DIR=/Users/mridul/.m2/repository

export CLASSPATH=${LIB_DIR}/com/google/guava/guava/14.0/guava-14.0.jar:\
${LIB_DIR}/commons-logging/commons-logging/1.2/commons-logging-1.2.jar:\
${LIB_DIR}/org/slf4j/slf4j-api/1.6.1/slf4j-api-1.6.1.jar:\
${LIB_DIR}/org/slf4j/slf4j-log4j12/1.6.1/slf4j-log4j12-1.6.1.jar:\
${LIB_DIR}/log4j/log4j/1.2.17/log4j-1.2.17.jar:\
${LIB_DIR}/org/apache/curator/curator-framework/2.9.1/curator-framework-2.9.1.jar\
:${LIB_DIR}/org/apache/curator/curator-client/2.9.1/curator-client-2.9.1.jar\
:${LIB_DIR}/org/apache/curator/curator-recipes/2.9.1/curator-recipes-2.9.1.jar\
:${LIB_DIR}/org/apache/zookeeper/zookeeper/3.4.8/zookeeper-3.4.8.jar\
:./target/hbase-mq-1.0-SNAPSHOT.jar

java -cp $CLASSPATH  au.gov.nsw.dec.icc.e3pi.sif.notification.TestApp