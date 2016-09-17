FROM spotify/kafka
WORKDIR /opt/kafka_2.11-0.8.2.1/bin/
CMD /opt/kafka_2.11-0.8.2.1/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic input_topic --partitions 1 --replication-factor 1
