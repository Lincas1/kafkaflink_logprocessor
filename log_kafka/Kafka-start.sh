echo starting zookeeper
/usr/local/bin/zkServer start

echo starting kafka
kafka-server-start /usr/local/etc/kafka/server.properties &

echo Created topic "test".
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
 
echo display current broker state
kafka-topics --zookeeper localhost:2181 --describe --topic test

echo push message to topic
kafka-console-producer --broker-list localhost:9092 --topic test
$ Message 1
$ Message 2
^D

echo pull topic from broker
kafka-console-consumer --zookeeper localhost:2181 --topic test --from-beginning
^C