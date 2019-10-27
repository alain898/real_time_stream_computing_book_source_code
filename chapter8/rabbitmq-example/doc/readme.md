## 启动RabbitMQ

```
docker pull rabbitmq:management
docker run -d --name rabbit -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin -p 15672:15672 -p 5672:5672 -p 25672:25672 -p 61613:61613 -p 1883:1883 rabbitmq:management

访问：
http://127.0.0.1:15672/#/
http://127.0.0.1:15672/#/vhosts
```

## RabbitMQ的设计

```
参考资料：
https://stackoverflow.com/questions/32864644/rabbitmq-multiple-consumers-on-a-queue-only-one-get-the-message

RabbitMQ will round-robin the messages to the consumers, 
but only one consumer will receive the message from the queue. 
This is by design in RabbitMQ, when you have multiple consumers on a single queue.

If you need all consumers to receive all messages, 
then you need to change your configuration so that each consumer has it's own queue. 
Then you need to publish your message through an exchange that will deliver the message to all of the queues for all of the consumers.

The easiest way to do this is with a Fanout exchange type.
```
