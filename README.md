## Local Log processor 
A implementation of local log processing based on Apache Kafka and Apache Flink

## Start Kafka and Flink
start kafka
```
echo starting zookeeper
/usr/local/bin/zkServer start
echo starting kafka
kafka-server-start /usr/local/etc/kafka/server.properties &
echo Created topic "test".
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic demo
```

start flink
```
cd ./bin/start-local.sh
```

## Build and Run the Application
In the demo folder
```
$ mvn clean package
```
Run the demo
```
$ mvn exec:java -Dexec.mainClass=SampleRun
```

