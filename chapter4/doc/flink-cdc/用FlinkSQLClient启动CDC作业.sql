
-- 在 MySQL 客户端创建实验用到的表。
CREATE DATABASE db001 DEFAULT CHARSET='utf8';

use db001;
CREATE TABLE table001(
  id INT PRIMARY KEY,
  name VARCHAR(255),
  counts INT,
  description VARCHAR(255)
);

TRUNCATE TABLE table001;

-- 使用 com.alain898.book.realtimestreaming.chapter4.cep.TestData 类，往 table001 里写入数据


-- 使用 Table/SQL API 方式实现 Flink CDC，需要用到 Flink SQL Client 客户端工具。
-- 在 Flink SQL Client 里执行以下 SQL。

-- 创建源数据库
CREATE TABLE sourceTable (
  id INT,
  name STRING,
  counts INT,
  description STRING
) WITH (
 'connector' = 'mysql-cdc',
 'hostname' = '127.0.0.1',
 'port' = '3306',
 'username' = 'root',
 'password' = '123456',
 'database-name' = 'db001',
 'table-name' = 'table001',
 'server-time-zone' = 'Asia/Shanghai'
);

-- 创建目标数据库
CREATE TABLE sinkTable (
  id INT,
  name STRING,
  counts INT,
  description STRING
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'table001'
);

-- 启动 Flink SQL CDC 作业
insert into sinkTable select id, name, counts from sourceTable;
