SET LIB_DIR=C:\Users\mdas2\.m2\repository

SET HOME_DIR=C:\Users\mdas2\code\hbase-mq\target

SET CLASSPATH=%LIB_DIR%\com\google\guava\guava\14.0\guava-14.0.jar;^
%LIB_DIR%\org\slf4j\slf4j-api\1.7.7\slf4j-api-1.7.7.jar;^
%LIB_DIR%\commons-logging\commons-logging\1.2\commons-logging-1.2.jar;^
%LIB_DIR%\org\slf4j\slf4j-log4j12\1.7.7\slf4j-log4j12-1.7.7.jar;^
%LIB_DIR%\log4j\log4j\1.2.17\log4j-1.2.17.jar;^
%LIB_DIR%\org\apache\curator\curator-client\2.10.0\curator-client-2.10.0.jar;^
%LIB_DIR%\org\apache\curator\curator-framework\2.10.0\curator-framework-2.10.0.jar;^
%LIB_DIR%\org\apache\zookeeper\zookeeper\3.4.5-cdh5.5.2\zookeeper-3.4.5-cdh5.5.2.jar;^
%LIB_DIR%\org\apache\curator\curator-recipes\2.7.1\curator-recipes-2.7.1.jar;^
%LIB_DIR%\org\apache\hbase\hbase-client\1.0.0-cdh5.5.2\hbase-client-1.0.0-cdh5.5.2.jar;^
%LIB_DIR%\org\apache\hbase\hbase-common\1.0.0-cdh5.5.2\hbase-common-1.0.0-cdh5.5.2.jar;^
%LIB_DIR%\org\apache\hadoop\hadoop-common\2.6.0-cdh5.5.2\hadoop-common-2.6.0-cdh5.5.2.jar;^
%LIB_DIR%\org\apache\hadoop\hadoop-auth\2.6.0-cdh5.5.2\hadoop-auth-2.6.0-cdh5.5.2.jar;^
%LIB_DIR%\commons-lang\commons-lang\2.6\commons-lang-2.6.jar;^
%LIB_DIR%\commons-collections\commons-collections\3.2.1\commons-collections-3.2.1.jar;^
%LIB_DIR%\commons-configuration\commons-configuration\1.10\commons-configuration-1.10.jar;^
%LIB_DIR%\commons-codec\commons-codec\1.10\commons-codec-1.10.jar;^
%LIB_DIR%\com\google\protobuf\protobuf-java\2.5.0\protobuf-java-2.5.0.jar;^
%LIB_DIR%\org\apache\hbase\hbase-protocol\1.0.0-cdh5.5.2\hbase-protocol-1.0.0-cdh5.5.2.jar;^
%LIB_DIR%\org\jboss\netty\netty\3.2.2.Final\netty-3.2.2.Final.jar;^
%LIB_DIR%\org\apache\htrace\htrace-core\3.2.0-incubating\htrace-core-3.2.0-incubating.jar;^
%LIB_DIR%\org\apache\htrace\htrace-core4\4.0.1-incubating\htrace-core4-4.0.1-incubating.jar;^
%HOME_DIR%\hbase-mq.jar

java -classpath %CLASSPATH% au.gov.nsw.dec.icc.e3pi.sif.notification.TestApp
rem java -classpath %CLASSPATH% au.gov.nsw.dec.icc.e3pi.sif.notification.test.Coordinator