# Confluent Kafka with Avro Serializer

The program consists of 3 producers and 6 consumers. The producers read data from 3 different csv files and push the data into Kafka topics based on the `gender` field in the record. There are two topics : `male` and `female`. The consumers read the data from the topic and push them to PostgreSql DB. The consumers are divided into two groups with 3 members in a group.

Each topics have 10 partitions. You can edit the number of partitions by editing the start.sh file.

Installation and setup:
1. Install confluent
2. Clone the repo.
3. Install PostgreSql
4. Run `start.sh`
5. Start the consumers.
6. Start the producers.
==================================================
Now you can see the data being pushed into the DB.
==================================================