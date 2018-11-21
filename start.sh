kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic Male
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic Female
kafka-topics --zookeeper localhost:2181 --list
confluent start