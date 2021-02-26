## 使用 docker 安装 mysql5.7

```
# 拉取 mysql5.7 镜像
docker pull mysql:5.7
# 启动 mysql5.7 实例
docker run --name mysql5.7 -p 3306:3306 -e MYSQL_ROOT_PASSWORD=123456 -d mysql:5.7
# 查看 mysql5.7 是否启动完成，当看到"mysqld: ready for connections."时，表明已启动
docker logs mysql5.7
# 配置开启 binlog
docker exec mysql5.7 bash -c "echo 'log-bin=/var/lib/mysql/mysql-bin' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
# 配置 server-id
docker exec mysql5.7 bash -c "echo 'server-id=100001' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
# 重启 mysql5.7 实例
docker restart mysql5.7
# 再次查看 mysql5.7 是否启动完成，当看到"mysqld: ready for connections."时，表明已启动
docker logs mysql5.7
```

## 使用 docker 安装 elasticsearch7

```
docker pull docker.elastic.co/elasticsearch/elasticsearch:7.1.1

docker run -d --name es -p 9200:9200 -p 9300:9300 -e ES_JAVA_OPTS="-Xms512m -Xmx512m" -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.1.1

docker logs es
```