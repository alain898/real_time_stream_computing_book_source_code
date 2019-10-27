## 启动RabbitMQ

```
docker pull rabbitmq:management
docker run -d --name rabbit -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin -p 15672:15672 -p 5672:5672 -p 25672:25672 -p 61613:61613 -p 1883:1883 rabbitmq:management

访问：
http://127.0.0.1:15672/#/
http://127.0.0.1:15672/#/vhosts
```


## Kafka key 和 partition key

```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic input_events --partition 1 --replication-factor 1
bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic input_events2 --partition 1 --replication-factor 1
bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic click --partition 1 --replication-factor 1
bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic activate --partition 1 --replication-factor 1
bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic other --partition 1 --replication-factor 1
bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic subsystem1 --partition 1 --replication-factor 1
bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic subsystem2 --partition 1 --replication-factor 1
bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic subsystem3 --partition 1 --replication-factor 1
```

## Camel Kafka 2.18.2可配置项

```
{
	"autoCommitEnable": true,
	"autoCommitIntervalMs": 5000,
	"autoOffsetReset": "latest",
	"brokers": "localhost:9092",
	"bufferMemorySize": 33554432,
	"checkCrcs": true,
	"compressionCodec": "none",
	"connectionMaxIdleMs": 540000,
	"consumerRequestTimeoutMs": 40000,
	"consumerStreams": 10,
	"consumersCount": 1,
	"fetchMinBytes": 1024,
	"fetchWaitMaxMs": 500,
	"heartbeatIntervalMs": 3000,
	"kerberosBeforeReloginMinTime": 60000,
	"kerberosInitCmd": "/usr/bin/kinit",
	"kerberosRenewJitter": 0.05,
	"kerberosRenewWindowFactor": 0.8,
	"keyDeserializer": "org.apache.kafka.common.serialization.StringDeserializer",
	"keySerializerClass": "org.apache.kafka.common.serialization.StringSerializer",
	"lingerMs": 0,
	"maxBlockMs": 60000,
	"maxInFlightRequest": 5,
	"maxPartitionFetchBytes": 1048576,
	"maxRequestSize": 1048576,
	"metadataMaxAgeMs": 300000,
	"metricsSampleWindowMs": 30000,
	"noOfMetricsSample": 2,
	"partitionAssignor": "org.apache.kafka.clients.consumer.RangeAssignor",
	"partitioner": "org.apache.kafka.clients.producer.internals.DefaultPartitioner",
	"pollTimeoutMs": 5000,
	"producerBatchSize": 16384,
	"queueBufferingMaxMessages": 10000,
	"receiveBufferBytes": 32768,
	"reconnectBackoffMs": 50,
	"recordMetadata": true,
	"requestRequiredAcks": "1",
	"requestTimeoutMs": 30000,
	"retries": 0,
	"retryBackoffMs": 100,
	"saslMechanism": "GSSAPI",
	"securityProtocol": "PLAINTEXT",
	"seekToBeginning": false,
	"sendBufferBytes": 131072,
	"serializerClass": "org.apache.kafka.common.serialization.StringSerializer",
	"sessionTimeoutMs": 30000,
	"sslEnabledProtocols": "TLSv1.2,TLSv1.1,TLSv1",
	"sslKeymanagerAlgorithm": "SunX509",
	"sslKeystoreType": "JKS",
	"sslProtocol": "TLS",
	"sslTrustmanagerAlgorithm": "PKIX",
	"sslTruststoreType": "JKS",
	"valueDeserializer": "org.apache.kafka.common.serialization.StringDeserializer",
	"workerPoolCoreSize": 10,
	"workerPoolMaxSize": 20
}
```
