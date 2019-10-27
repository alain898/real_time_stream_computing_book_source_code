## 启动Kafka

```
nohup bin/zookeeper-server-start.sh config/zookeeper.properties &
nohup bin/kafka-server-start.sh config/server.properties &
```

## 创建输入输出topic

```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic kafka-example --partition 1 --replication-factor 1
```
