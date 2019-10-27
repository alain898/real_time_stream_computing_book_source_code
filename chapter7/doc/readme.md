## 启动Kafka

```
nohup bin/zookeeper-server-start.sh config/zookeeper.properties &
nohup bin/kafka-server-start.sh config/server.properties &
```
## 启动MySQL

```
docker run -d --name mysql -e MYSQL_ROOT_PASSWORD=111111 -p 3306:3306 mysql:5.7

docker exec -it mysql /bin/bash
mysql -u root -p 111111
CREATE DATABASE kappa;
USE kappa;
CREATE TABLE table_counts(id VARCHAR(64), start BIGINT, end BIGINT, product VARCHAR(32), v_count INT, layer VARCHAR(32), PRIMARY KEY(id));
CREATE INDEX index_start ON table_counts (start DESC);
CREATE INDEX index_end ON table_counts (end DESC);
INSERT INTO table_counts(id,start,end,product,v_count,layer) VALUES("c001",1566832263000,1566832263000,"prod_001", 10, "batch") ON DUPLICATE KEY UPDATE start="c001",start=1566832263000,end=1566832263000,product="prod_001",v_count=10,layer="batch";
```


## 创建输入输出topic

```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic event-input --partition 1 --replication-factor 1
bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic event-output --partition 1 --replication-factor 1
```

## 启动程序

BatchLayer

FastLayer


## 发送数据
KafkaSender

## 使用SQL查询

执行ServerLayer

```
ServerLayer输出：
timestamp: 1566870406268
SELECT product, sum(v_count) as s_count from
(
SELECT * FROM table_counts WHERE start=26114503 AND end=26114506 AND layer='batch'
UNION
SELECT * FROM table_counts WHERE start>=104458024 AND end<=104458027 AND layer='fast'
) as union_table GROUP BY product;
(product_0,38)
(product_1,46)
(product_2,39)
(product_3,40)
(product_4,61)


MySQL查询输出：
mysql> SELECT * FROM table_counts WHERE start=26114503 AND end=26114506 AND layer='batch';
+----------------------------------+----------+----------+-----------+---------+-------+
| id                               | start    | end      | product   | v_count | layer |
+----------------------------------+----------+----------+-----------+---------+-------+
| 12f2f0bc5415e92ec111e3d06555f8e6 | 26114503 | 26114506 | product_4 |      50 | batch |
| 6bfa5e6da314e48dbb567be3465e0124 | 26114503 | 26114506 | product_1 |      40 | batch |
| 89fefa99881b04955b56afec8935ae86 | 26114503 | 26114506 | product_0 |      30 | batch |
| a1127f57354d424fa963b472a008dd5f | 26114503 | 26114506 | product_3 |      30 | batch |
| c9c04ef12483b8103d104f2ca6755c62 | 26114503 | 26114506 | product_2 |      30 | batch |
+----------------------------------+----------+----------+-----------+---------+-------+
5 rows in set (0.00 sec)

mysql> SELECT * FROM table_counts WHERE start>=104458024 AND end<=104458027 AND layer='fast';
+----------------------------------+-----------+-----------+-----------+---------+-------+
| id                               | start     | end       | product   | v_count | layer |
+----------------------------------+-----------+-----------+-----------+---------+-------+
| 0ba2d7668a9c0e1d4f11adabad879e71 | 104458026 | 104458027 | product_3 |       4 | fast  |
| 17b96fe150a0baa524cc35a343f90224 | 104458025 | 104458026 | product_1 |       1 | fast  |
| 23e4b7e8c498da94b9bb6a2b9dcd05c5 | 104458026 | 104458027 | product_0 |       1 | fast  |
| 33a2f7e06719969c6baace88a33863a6 | 104458024 | 104458025 | product_0 |       2 | fast  |
| 49cf931de325ad9c2b15332bbd1f853e | 104458026 | 104458027 | product_1 |       3 | fast  |
| 4ba7686570e9b2de0249f52db54534a7 | 104458024 | 104458025 | product_4 |       4 | fast  |
| 85eb30de80dfe295bafcb2f12c68a8a1 | 104458025 | 104458026 | product_3 |       3 | fast  |
| 9740bdd53004cb9311b0b2a6ff2832b6 | 104458025 | 104458026 | product_4 |       2 | fast  |
| b3575702054461cd1d81112a655bf002 | 104458024 | 104458025 | product_3 |       3 | fast  |
| b8b3e05cdbd1301f266813f675948a3c | 104458025 | 104458026 | product_2 |       4 | fast  |
| c52bce945ce658b82040c918c1b21d25 | 104458024 | 104458025 | product_2 |       4 | fast  |
| d5380d88a4956632cbbc36986e89dd56 | 104458025 | 104458026 | product_0 |       5 | fast  |
| e348fcb745021d9c820e5872f5acb1b2 | 104458026 | 104458027 | product_2 |       1 | fast  |
| ebd4161922fe0de2a87d1574ce574642 | 104458026 | 104458027 | product_4 |       5 | fast  |
| f262cc1c2627ac532cc34f274a2e4995 | 104458024 | 104458025 | product_1 |       2 | fast  |
+----------------------------------+-----------+-----------+-----------+---------+-------+
15 rows in set (0.00 sec)

mysql> SELECT product, sum(v_count) as s_count from
    -> (
    -> SELECT * FROM table_counts WHERE start=26114503 AND end=26114506 AND layer='batch'
    -> UNION
    -> SELECT * FROM table_counts WHERE start>=104458024 AND end<=104458027 AND layer='fast'
    -> ) as union_table GROUP BY product;
+-----------+---------+
| product   | s_count |
+-----------+---------+
| product_0 |      38 |
| product_1 |      46 |
| product_2 |      39 |
| product_3 |      40 |
| product_4 |      61 |
+-----------+---------+
5 rows in set (0.00 sec)

```

