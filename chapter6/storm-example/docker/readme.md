# storm例子

## 启动storm docker

```
docker-compose -f docker-storm.yml down
docker-compose -f docker-storm.yml up
```

## 提交storm topology到storm集群

```
docker run --link nimbus:job_nimbus --net book_images_default -it --rm -v ../target/storm-example-2.0.0.jar:/topology.jar storm storm jar /topology.jar com.alain898.book.realtimestreaming.chapter6.storm.streams.WordCountExample topology_world_count
```

## 查看示例运行结果

```
docker exec -it supervisor /bin/bash
root@8898fd7ac7d0:/apache-storm-2.0.0# cd /logs/
root@8898fd7ac7d0:/logs# ls -al
total 48
drwxr-xr-x 1 storm storm  4096 Aug 25 13:33 .
drwxr-xr-x 1 root  root   4096 Aug 25 13:32 ..
-rw-r--r-- 1 storm storm     0 Aug 25 13:32 access-supervisor.log
-rw-r--r-- 1 storm storm     0 Aug 25 13:32 access-web-supervisor.log
-rw-r--r-- 1 storm storm    24 Aug 25 13:33 console.log
-rw-r--r-- 1 storm storm 30219 Aug 25 13:33 supervisor.log
-rw-r--r-- 1 storm storm     0 Aug 25 13:32 supervisor.log.metrics
drwxr-xr-x 3 storm storm  4096 Aug 25 13:33 workers-artifacts
root@8898fd7ac7d0:/logs# tail -f console.log
(pear, 10)
(mango, 8)
(banana, 10)
(orange, 7)
(apple, 4)
(mango, 9)
(mango, 10)
(mango, 11)
(mango, 12)
(banana, 11)
```


## 我的本地docker进行（供比对版本用，包含了笔者用于其它样例程序使用的docker镜像）

```
$ docker images
REPOSITORY                       TAG                 IMAGE ID            CREATED             SIZE
zookeeper                        latest              3487af26dee9        10 days ago         225MB
storm                            latest              c99a826f7bde        10 days ago         571MB
zabbix/zabbix-server-mysql       latest              58c63c14e553        3 weeks ago         65.6MB
zabbix/zabbix-web-apache-mysql   latest              04cd1894f161        3 weeks ago         109MB
zabbix/zabbix-agent              latest              af33ad8702ce        3 weeks ago         16.7MB
mysql                            5.7                 f6509bac4980        4 weeks ago         373MB
daocloud.io/library/mysql        8                   c7109f74d339        2 months ago        443MB
ubuntu                           latest              7698f282e524        3 months ago        69.9MB
apache/nifi                      latest              c7e2bbb01fc2        4 months ago        1.91GB
ziyunhx/storm-ui                 latest              239d5bb15340        3 years ago         799MB
```


