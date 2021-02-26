## 用docker启动Flink相关命令

```

# 创建并启动 Flink
docker-compose -f docker-compose-flink.yml up -d

# 进入 flink-jobmanager 容器
docker exec -it flink-jobmanager /bin/sh

# 查看 $FLINK_HOME
echo $FLINK_HOME
/opt/flink

# 退出 flink-jobmanager 容器
exit


# 将 Flink CDC 相关jar包拷贝到每个flink容器的 $FLINK_HOME/lib 目录下
# course21-jars-1.0-SNAPSHOT-jar-with-dependencies.jar 由 course21jars 工程 package 构建而来
# flink-sql-connector-elasticsearch7_2.11-1.12.0.jar 从此处下载：https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.11/1.12.0/flink-sql-connector-elasticsearch7_2.11-1.12.0.jar
# flink-sql-connector-elasticsearch 必须参考 https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/elasticsearch.html
docker cp course21-jars-1.0-SNAPSHOT-jar-with-dependencies.jar flink-taskmanager:/opt/flink/lib
docker cp flink-sql-connector-elasticsearch7_2.11-1.12.0.jar flink-taskmanager:/opt/flink/lib

docker cp course21-jars-1.0-SNAPSHOT-jar-with-dependencies.jar flink-jobmanager:/opt/flink/lib
docker cp flink-sql-connector-elasticsearch7_2.11-1.12.0.jar flink-jobmanager:/opt/flink/lib


# 重启 Flink
docker-compose -f docker-compose-flink.yml restart


# 进入 flink-jobmanager 容器
docker exec -it flink-jobmanager /bin/sh

# 启动 Flink SQL 客户端
cd /opt/flink/bin
./sql-client.sh embedded

```

## docker-compose常用命令

```
# 创建并启动服务
docker-compose -f docker-compose-flink.yml up -d

# 停止运行中的容器，并且删除容器和网络
docker-compose -f docker-compose-flink.yml down

# 启动已有服务
docker-compose -f docker-compose-flink.yml start

# 停止运行中的容器，但不删除容器
docker-compose -f docker-compose-flink.yml stop

# 重启已有服务
docker-compose -f docker-compose-flink.yml restart
```
