
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
