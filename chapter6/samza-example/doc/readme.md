# Samza例子

## 参考资料
https://samza.apache.org/startup/quick-start/latest/samza.html


## 启动Kafka

```
cd kafka_2.11-0.10.1.1
nohup bin/zookeeper-server-start.sh config/zookeeper.properties &
nohup bin/kafka-server-start.sh config/server.properties &
```

## 创建输入数据流topic

```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic sample-text --partition 1 --replication-factor 1
```

## 启动程序

在Intellij中配置程序启动参数
```
--config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=src/main/resources/word-count-example.properties
```

## 灌入测试数据

```
bin/kafka-console-producer.sh --topic sample-text --broker localhost:9092 < ./sample-text.txt
```

## 查看程序结果

```
bin/kafka-console-consumer.sh --topic word-count-output --zookeeper localhost:2181 --from-beginning
```
